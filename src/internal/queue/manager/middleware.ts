import type { InvokeApi } from './invoke'
import type { Message, ProduceMessage } from './message'
import type { QueueDescriptor } from './registry'

/**
 * One produce call as its middleware sees it: the decoded batch (one call = one adapter
 * send), headers as they will go on the wire (the `produce` event hook already applied). A
 * middleware that passes a modified call to `next` transforms what gets encoded and sent.
 */
export interface ProduceCall {
  readonly queue: string
  readonly messages: ReadonlyArray<ProduceMessage<unknown>>
  /** The caller's per-call signal. The manager's `produceTimeout` is NOT composed in here: it
   * starts fresh inside the terminal stage, so a retry middleware gets a full bound per attempt. */
  readonly signal?: AbortSignal
  /** Adapter-specific produce context (e.g. a caller transaction), passed through verbatim. */
  readonly ctx?: unknown
  /**
   * Inline dispatch to this process's locally attached handler (see `Queue.invoke`): the
   * best-effort fallback seam for a middleware catching a send failure. Throws when the queue
   * has no local handler. A handler invoked on failure may ALSO run later from the queue (a
   * produce failure does not prove the send never committed) and must tolerate duplicates.
   */
  readonly invoke: InvokeApi['invoke']
  /** Look up a queue's declared descriptor by name; `undefined` for an undeclared name. */
  readonly getQueue: (name: string) => QueueDescriptor<unknown> | undefined
}

/**
 * The produce pipeline's continuation. The terminal stage encodes the messages, applies the
 * produce bound and sends, so a middleware catching from `next` sees the raw failure: the
 * adapter's error exactly as thrown, or the bound's abort reason.
 */
export type ProduceNext = (call: ProduceCall) => Promise<void>

/**
 * Middleware around the produce operation: spans, timing, retries, transformation, and
 * failure interception (resolve after catching and the produce call resolves; rethrow and it
 * rejects with what you threw).
 */
export type ProduceMiddleware = (next: ProduceNext) => ProduceNext

/**
 * One consume call: the messages of ONE invocation of the handler (a single message, or a
 * batch handler's slice), the same boundary `consumeTimeout` and the `handled` event use.
 */
export interface ConsumeCall {
  readonly queue: string
  /**
   * The batch, delivery-ordered. Replaceable BEFORE `next` for 1:1 transforms (decrypt,
   * annotate); an assignment that adds or drops messages throws. Dropping a message is only
   * ever expressed through {@link ConsumeCall.reject}.
   */
  messages: readonly Message<unknown>[]
  /**
   * Route one message to the failure path (nack under a `RetryPolicy`, else release) and drop
   * it from the batch. BEFORE `next` only; rejecting after the terminal started throws, as
   * does rejecting a message that is not in this batch.
   */
  reject(message: Message<unknown>, error?: unknown): void
  /** Aborts when the invocation is torn down (consumeTimeout, forced shutdown). */
  readonly signal: AbortSignal
}

/**
 * What `await next` returns: the invocation's result as DATA, never thrown exceptions.
 * Middleware observes handler failures but cannot swallow them, so a catching middleware can
 * never change settle semantics.
 */
export interface BatchResult {
  readonly outcome: 'ok' | 'error' | 'timeout' | 'shutdown'
  readonly size: number
  readonly ok: number
  /** Released without consuming retry budget: timeout / shutdown / abandoned. */
  readonly released: number
  /** Failed messages with their errors: handler throws, `BatchFailure` entries and every
   * middleware rejection. */
  readonly failures: readonly ConsumeFailure[]
}

/** One failed message and (when known) why. */
export interface ConsumeFailure {
  readonly message: Message<unknown>
  readonly error?: unknown
}

/** The consume pipeline's continuation: resolves when the invocation completes. */
export type ConsumeNext = (call: ConsumeCall) => Promise<BatchResult>

/**
 * Middleware around one consumption invocation: before `next` the messages arrived
 * (transform `messages`, `reject` individuals); after it, observe the {@link BatchResult}. A
 * throw FROM a middleware fails every message of the call.
 */
export type ConsumeMiddleware = (next: ConsumeNext) => ConsumeNext

/** A middleware entry: one stage per direction, each invoked once per produce call /
 * consumption invocation. */
export type QueueMiddleware = {
  readonly produce?: ProduceMiddleware
  readonly consume?: ConsumeMiddleware
}

/** The produce-stage view of a middleware list, in declaration order. */
export const produceStages = (list: readonly QueueMiddleware[] | undefined): ProduceMiddleware[] =>
  (list ?? []).flatMap((mw) => (mw.produce !== undefined ? [mw.produce] : []))

/**
 * Compose the consume chain over manager-level + local middleware: first declared outermost,
 * terminal innermost. Returns a wrap because the terminal closes over per-invocation state,
 * so a stage's `mw(next)` scope runs per invocation; longer-lived middleware state belongs in
 * the entry's factory closure.
 */
export const composeConsume = (
  shared: readonly QueueMiddleware[] | undefined,
  local: readonly QueueMiddleware[] | undefined
): ((terminal: ConsumeNext) => ConsumeNext) => {
  const stages = [...(shared ?? []), ...(local ?? [])].flatMap((mw) =>
    mw.consume !== undefined ? [mw.consume] : []
  )
  return (terminal) => stages.reduceRight((next, mw) => mw(next), terminal)
}

/** One message a middleware rejected, with its position in the original batch. */
export interface Rejection {
  readonly index: number
  readonly message: Message<unknown>
  readonly error?: unknown
}

/** Internal bookkeeping behind a {@link ConsumeCall}: what survived, what was rejected. */
export interface ConsumeCallState {
  /** The final (post-transform) batch the terminal processes. */
  current(): readonly Message<unknown>[]
  /** Original-batch index of each surviving message, aligned with `current()`. */
  originalIndexes(): readonly number[]
  rejected(): readonly Rejection[]
  /** Called when the terminal starts: rejects from then on throw. */
  seal(): void
}

/** Build a guarded {@link ConsumeCall} (1:1 transforms only, reject before `next` only). */
export function createConsumeCall(
  queue: string,
  messages: readonly Message<unknown>[],
  signal: AbortSignal
): { call: ConsumeCall; state: ConsumeCallState } {
  let current = [...messages]
  const originalIdx = messages.map((_, i) => i)
  const rejected: Rejection[] = []
  let sealed = false

  const call: ConsumeCall = {
    queue,
    signal,
    get messages(): readonly Message<unknown>[] {
      return current
    },
    set messages(next: readonly Message<unknown>[]) {
      if (next.length !== current.length) {
        throw new Error(
          'queue: consume middleware must not add or remove messages by assignment — ' +
            'transforms are 1:1; drop a message with reject(message, error)'
        )
      }
      current = [...next]
    },
    reject(message, error) {
      if (sealed) {
        throw new Error('queue: reject() after next() — rejection is a before-next operation')
      }
      const i = current.indexOf(message)
      if (i === -1) {
        throw new Error('queue: reject() called with a message that is not in this batch')
      }
      current.splice(i, 1)
      const [index] = originalIdx.splice(i, 1)
      rejected.push({ index, message, ...(error !== undefined ? { error } : {}) })
    },
  }

  return {
    call,
    state: {
      current: () => current,
      originalIndexes: () => originalIdx,
      rejected: () => rejected,
      seal: () => {
        sealed = true
      },
    },
  }
}
