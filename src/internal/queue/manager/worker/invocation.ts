import type { Message, PullMessage } from '../message'
import {
  type BatchResult,
  type ConsumeCall,
  type ConsumeFailure,
  type ConsumeNext,
  createConsumeCall,
  type Rejection,
} from '../middleware'
import { timeoutReason, whenAborted } from '../signals'
import {
  type BatchContext,
  BatchFailure,
  type BatchHandler,
  type Handler,
  type WorkerRuntime,
} from './types'

/** Why an invocation's signal aborted: its `consumeTimeout`, or shutdown. */
export type ReleaseReason = 'timeout' | 'shutdown'

const abortReason = (signal: AbortSignal): ReleaseReason =>
  signal.reason instanceof DOMException && signal.reason.name === 'TimeoutError'
    ? 'timeout'
    : 'shutdown'

/** What became of one message that reached the handler. */
export type MessageVerdict =
  | { readonly status: 'ok' }
  | { readonly status: 'failed'; readonly error?: unknown }
  | { readonly status: 'released'; readonly reason: ReleaseReason }

/** What one bounded consume-chain run concluded about its messages. */
export type ChainVerdict =
  /** The terminal ran and settled; its own verdict of each message stands. */
  | { readonly status: 'completed'; readonly result: BatchResult }
  /** A middleware threw, before or after `next`: every message of the call fails with it. */
  | { readonly status: 'threw'; readonly error: unknown }
  /** The bound fired before any handler started: every message settles for that reason (a
   * timeout as a failure, a shutdown as a release). */
  | { readonly status: 'abandoned'; readonly reason: ReleaseReason }
  /** The chain resolved without ever calling `next`: a middleware bug, failed loudly. */
  | { readonly status: 'no-next' }

/**
 * Run one consume-chain invocation under the attempt bound. `consumeTimeout` starts when the
 * chain is entered, so it covers the middleware stages as well as the handler; shutdown
 * aborts the same signal. Whichever fires first abandons the chain (it keeps running in the
 * background, signalled but never force-killed) unless the terminal had already started the
 * handler, whose prompt return is then awaited: a handler that settled is honoured. An
 * invocation abandoned before `next` never starts the handler, and a second `next` throws.
 * `body` runs the handler on the sealed call and returns the invocation's result.
 */
export const runInvocation = async (
  chain: (terminal: ConsumeNext) => ConsumeNext,
  call: ConsumeCall,
  body: (signal: AbortSignal) => Promise<BatchResult>,
  ac: AbortController,
  consumeTimeout: number | undefined
): Promise<ChainVerdict> => {
  const signal = ac.signal
  let nextCalls = 0
  let bodyDone: Promise<BatchResult> | undefined
  let result: BatchResult | undefined
  const terminal: ConsumeNext = () => {
    nextCalls++
    if (nextCalls > 1) {
      throw new Error(
        'queue: a consume middleware called next more than once — the invocation already ran'
      )
    }
    if (signal.aborted) {
      // Abandoned while a before-half ran: the messages are already released.
      const size = call.messages.length
      return Promise.resolve({
        outcome: abortReason(signal),
        size,
        ok: 0,
        released: size,
        failures: [],
      })
    }
    bodyDone = body(signal).then((r) => {
      result = r
      return r
    })
    return bodyDone
  }
  const timer =
    consumeTimeout === undefined
      ? undefined
      : setTimeout(() => ac.abort(timeoutReason()), consumeTimeout)
  let threw: { readonly error: unknown } | undefined
  // The async wrapper makes a synchronous throw from a stage the same failure as a rejection.
  const chainDone = (async () => chain(terminal)(call))().then(
    () => 'resolved' as const,
    (error: unknown) => {
      threw = { error }
      return 'threw' as const
    }
  )
  const release = whenAborted(signal, 'aborted' as const)
  try {
    let winner = await Promise.race([chainDone, release.aborted])
    if (winner === 'aborted' && bodyDone !== undefined) {
      // The handler phase was live when the bound fired: it returns promptly on abort (or
      // with its settle, which wins). A middleware throw racing it still fails the call.
      winner = await Promise.race([chainDone, bodyDone.then(() => 'resolved' as const)])
    }
    if (winner === 'threw') return { status: 'threw', error: threw?.error }
    if (winner === 'aborted') return { status: 'abandoned', reason: abortReason(signal) }
    if (nextCalls === 0) return { status: 'no-next' }
    if (bodyDone === undefined) return { status: 'abandoned', reason: abortReason(signal) }
    // A middleware that returned without awaiting `next` leaves the handler live: its own
    // return (bounded by the same abort) is what the invocation concludes on.
    return { status: 'completed', result: result ?? (await bodyDone) }
  } finally {
    if (timer !== undefined) clearTimeout(timer)
    release.dispose()
  }
}

/**
 * Run one handler call over `messages` and say what became of each, aligned with `messages`.
 * The call is all-or-nothing (ok, failed with the thrown error, or released for the abort's
 * reason) with one exception: a batch handler (`subset`) may throw {@link BatchFailure} to
 * fail only the messages it names. Whichever of the handler and the abort happens first wins,
 * but a handler that actually settled is always honoured. An empty `messages` never calls
 * the handler.
 */
export async function runMessages(
  messages: readonly Message<unknown>[],
  call: (signal: AbortSignal) => void | Promise<void>,
  subset: boolean,
  signal: AbortSignal
): Promise<MessageVerdict[]> {
  if (messages.length === 0) return []
  let settled: { readonly error?: unknown } | undefined
  const ran = (async (): Promise<void> => {
    try {
      await call(signal)
      settled = {}
    } catch (error) {
      settled = { error }
    }
  })()
  const release = whenAborted(signal)
  try {
    await Promise.race([ran, release.aborted])
  } finally {
    release.dispose()
  }
  const outcome = settled as { readonly error?: unknown } | undefined
  if (outcome === undefined) {
    const reason = abortReason(signal)
    return messages.map(() => ({ status: 'released', reason }))
  }
  if (!('error' in outcome)) return messages.map(() => ({ status: 'ok' }))
  const error = outcome.error
  if (subset && error instanceof BatchFailure) {
    const inCall = new Set(messages)
    const foreign = error.failed.some((m) => !inCall.has(m))
    const failed = new Set(error.failed)
    const perMessageError = error.cause ?? error
    return messages.map((m) =>
      foreign || failed.has(m) ? { status: 'failed', error: perMessageError } : { status: 'ok' }
    )
  }
  return messages.map(() => ({ status: 'failed', error }))
}

/** One message's fate inside an invocation; `undefined` never reached the handler. */
export type Slot = MessageVerdict | undefined

/**
 * The shared terminal of every invocation, worker deliveries and inline dispatch alike: seal
 * the call, fail what a middleware rejected, run the handler on what survives, and return
 * the result the chain observes. `recorded` receives each message's verdict at its ORIGINAL
 * position.
 */
export const runTerminal = async (
  state: ReturnType<typeof createConsumeCall>['state'],
  size: number,
  recorded: Slot[],
  run: (messages: readonly Message<unknown>[], signal: AbortSignal) => void | Promise<void>,
  subset: boolean,
  signal: AbortSignal
): Promise<BatchResult> => {
  state.seal()
  const failures: ConsumeFailure[] = state.rejected().map((r: Rejection) => {
    recorded[r.index] = { status: 'failed', ...('error' in r ? { error: r.error } : {}) }
    return { message: r.message, ...('error' in r ? { error: r.error } : {}) }
  })
  const messages = state.current()
  const positions = state.originalIndexes()
  const verdicts = await runMessages(messages, (s) => run(messages, s), subset, signal)
  let ok = 0
  let released = 0
  let releaseReason: ReleaseReason | undefined
  verdicts.forEach((verdict, k) => {
    recorded[positions[k]] = verdict
    if (verdict.status === 'ok') ok++
    else if (verdict.status === 'failed') {
      failures.push({ message: messages[k], error: verdict.error })
    } else {
      released++
      releaseReason = verdict.reason
    }
  })
  return {
    outcome: failures.length > 0 ? 'error' : (releaseReason ?? 'ok'),
    size,
    ok,
    released,
    failures,
  }
}

/** Call a single-message or batch handler with its context. */
export const callHandler = (
  handler: Handler<unknown> | BatchHandler<unknown>,
  batch: boolean,
  ctx: BatchContext<unknown>
): void | Promise<void> =>
  batch
    ? (handler as BatchHandler<unknown>)(ctx)
    : (handler as Handler<unknown>)({
        queue: ctx.queue,
        message: ctx.messages[0],
        signal: ctx.signal,
        heartbeat: ctx.heartbeat,
      })

/**
 * One worker invocation over `messages` (a single message, or a batch handler's slice)
 * through the consume chain. Returns one slot per message, aligned with `messages`, and
 * reports the invocation as ONE `handled` event, whatever the outcome.
 */
export async function deliver(
  rt: WorkerRuntime,
  pulled: readonly PullMessage[],
  messages: readonly Message<unknown>[],
  heartbeat: () => Promise<void>
): Promise<Slot[]> {
  const { queue } = rt
  const size = pulled.length
  const ac = new AbortController()
  const { call, state } = createConsumeCall(queue, messages, ac.signal)
  // The terminal's own record, which a handler finishing late may still write to.
  const recorded: Slot[] = new Array<Slot>(size)
  rt.inFlight.add(ac)
  const startedAt = Date.now()
  let verdict: ChainVerdict
  try {
    verdict = await runInvocation(
      rt.chain,
      call,
      (signal) =>
        runTerminal(
          state,
          size,
          recorded,
          (surviving, s) =>
            callHandler(rt.handler, rt.batch, { queue, messages: surviving, signal: s, heartbeat }),
          rt.batch,
          signal
        ),
      ac,
      rt.consumeTimeout
    )
  } finally {
    rt.inFlight.delete(ac)
  }

  let slots: Slot[]
  let outcome: BatchResult['outcome']
  let failed: number
  if (verdict.status === 'completed') {
    slots = recorded.slice()
    outcome = verdict.result.outcome
    failed = verdict.result.failures.length
  } else if (verdict.status === 'abandoned') {
    slots = new Array<Slot>(size).fill({ status: 'released', reason: verdict.reason })
    outcome = verdict.reason
    failed = 0
  } else {
    // A stage's throw (before or after next) or a chain that never called next: the whole
    // call fails, even messages the handler already completed.
    const error =
      verdict.status === 'threw'
        ? verdict.error
        : new Error(
            'queue: a consume middleware resolved without calling next — the invocation never ran'
          )
    slots = new Array<Slot>(size).fill({ status: 'failed', error })
    outcome = 'error'
    failed = size
  }
  rt.events?.handled?.({
    queue,
    outcome,
    size,
    failed,
    ...(size === 1 ? { attempt: pulled[0].attempt } : {}),
    durationMs: Date.now() - startedAt,
  })
  return slots
}
