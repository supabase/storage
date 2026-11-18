import { AsyncLocalStorage } from 'node:async_hooks'
import type { Codec } from './codec'
import {
  type AnyHandlerInstance,
  isHandlerInstance,
  isMessageInstance,
  type MessageInstance,
  toProduceMessage,
} from './declare'
import { MESSAGE_TYPE_HEADER, type Message } from './message'
import {
  type ConsumeNext,
  composeConsume,
  createConsumeCall,
  type QueueMiddleware,
} from './middleware'
import {
  type AnyMessageClass,
  type QueuesView,
  resolveClassQueue,
  resolveMessageQueue,
} from './registry'
import { shutdownReason } from './signals'
import { validateSubscribeOptions } from './validate'
import { callHandler, runInvocation, runTerminal } from './worker/invocation'
import {
  type BatchHandler,
  type Handler,
  parallelismOf,
  prefetchOf,
  type SubscribeOptions,
} from './worker/types'

/** Options for the `(queue, data)` form of `invoke`. */
export interface InvokeOptions {
  /** Grouping key stamped on the synthetic message. */
  readonly key?: string
  /** Headers stamped on the synthetic message. */
  readonly headers?: Readonly<Record<string, string>>
}

/** One message of an {@link InvokeApi.invokeMany} call. */
export interface InvokeMessage {
  readonly data: unknown
  /** Grouping key stamped on the synthetic message. */
  readonly key?: string
  /** Headers stamped on the synthetic message. */
  readonly headers?: Readonly<Record<string, string>>
}

/** Options for `invokeMany`. */
export interface InvokeManyOptions {
  /** Once aborted, no further invocation starts and the call rejects with `signal.reason`.
   * Invocations already running finish, each within the handler's `consumeTimeout`. */
  readonly signal?: AbortSignal
}

/** One locally attached handler as inline dispatch sees it. Registered when a handler
 * attaches and dropped when its worker stops. */
export interface LocalHandler {
  readonly queue: string
  readonly consumeTimeout: number | undefined
  /** The handler's `prefetch`: the most messages it is handed at once. */
  readonly prefetch: number
  /** The handler's `parallelism`: the most invocations it runs at once. */
  readonly parallelism: number
  /** True when batch-shaped: inline dispatch hands it every dispatched message in ONE call. */
  readonly batch: boolean
  /** The attachment's consume chain (manager-level outside attachment-level). */
  readonly chain: (terminal: ConsumeNext) => ConsumeNext
  readonly handler: Handler<unknown> | BatchHandler<unknown>
}

/** The positional forms every attachment entry point accepts (see `Queue.subscribe`). */
export type AttachArg =
  | string
  | ({ queue: string; batch?: boolean } & SubscribeOptions)
  | AnyHandlerInstance

/**
 * Normalize one attachment: THE shared reading of subscribe's positional forms, so workers,
 * the sync queue and a producer queue's dispatch-only registry resolve queue, batch
 * capability and the composed consume chain by exactly the same rules.
 */
export function localRegistration(
  arg1: AttachArg,
  arg2: Handler<unknown> | BatchHandler<unknown> | SubscribeOptions | undefined,
  deps: {
    /** The declared registry: class bindings, and each queue's liveness window. */
    readonly queues: QueuesView
    /** Manager-level middleware, composed OUTSIDE the attachment's own. */
    readonly middleware: readonly QueueMiddleware[] | undefined
  }
): { registration: LocalHandler; options: { queue: string } & SubscribeOptions } {
  let options: { queue: string } & SubscribeOptions
  let batch: boolean
  let handler: Handler<unknown> | BatchHandler<unknown>
  if (isHandlerInstance(arg1)) {
    // The method name is the batch opt-in on this form.
    batch = 'handleBatch' in arg1 && typeof arg1.handleBatch === 'function'
    if (batch && 'handle' in arg1 && typeof arg1.handle === 'function') {
      throw new Error(
        'queue: a handler object carries handle OR handleBatch, never both — the method name is its batch opt-in'
      )
    }
    const queue =
      'messages' in arg1 ? queueOfClasses(arg1.messages, deps.queues.classIndex) : arg1.queue
    // The instance's own `options` are the base; per-attachment overrides win.
    options = { ...arg1.options, ...(arg2 as SubscribeOptions | undefined), queue }
    if ((options as { batch?: unknown }).batch === true) {
      throw new Error(
        "queue: batch: true is the INLINE form's opt-in — a handler object opts in with its handleBatch method"
      )
    }
    // The arrows keep `this` bound to the instance across every delivery.
    const instance = arg1 as {
      handle?: Handler<unknown>
      handleBatch?: BatchHandler<unknown>
    }
    handler = batch
      ? (ctx: Parameters<BatchHandler<unknown>>[0]) => instance.handleBatch?.(ctx)
      : (ctx: Parameters<Handler<unknown>>[0]) => instance.handle?.(ctx)
  } else {
    options = typeof arg1 === 'string' ? { queue: arg1 } : arg1
    batch = typeof arg1 !== 'string' && arg1.batch === true
    handler = arg2 as Handler<unknown> | BatchHandler<unknown>
  }

  validateSubscribeOptions(
    { ...options, batch },
    options.queue,
    deps.queues.get(options.queue)?.options
  )
  return {
    registration: {
      queue: options.queue,
      consumeTimeout: options.consumeTimeout,
      prefetch: prefetchOf(options),
      parallelism: parallelismOf(options),
      batch,
      chain: composeConsume(deps.middleware, options.middleware),
      handler,
    },
    options,
  }
}

// A message-class handler names classes, not a queue: resolve each through the registry and
// require them to agree.
function queueOfClasses(
  classes: readonly AnyMessageClass[],
  classIndex: QueuesView['classIndex']
): string {
  let queue: string | undefined
  for (const cls of classes) {
    const resolved = resolveClassQueue(classIndex, cls)
    if (resolved === undefined) {
      throw new Error(
        `queue: message class "${cls.name}" is not bound to any queue — bind it in the registry with queue(${cls.name})`
      )
    }
    if (queue !== undefined && resolved !== queue) {
      throw new Error(
        `queue: handler spans queues "${queue}" and "${resolved}" — every listed message class must be bound to one queue`
      )
    }
    queue = resolved
  }
  if (queue === undefined) {
    throw new Error('queue: a message handler must list at least one message class')
  }
  return queue
}

/**
 * Register a config handler list as inline dispatch targets, all or nothing: every entry is
 * normalized (and validated) before any is added, so the `start()` that threw can be retried.
 */
export function registerConfigHandlers(
  handlers: readonly AttachArg[],
  deps: Parameters<typeof localRegistration>[2],
  into: Set<LocalHandler>
): void {
  const registrations = handlers.map((h) => localRegistration(h, undefined, deps).registration)
  for (const registration of registrations) into.add(registration)
}

/**
 * The local handler an inline dispatch on `queue` runs. Competing handlers on a queue would
 * receive a real delivery one between them, so the first attached stands in. No handler
 * throws: a silent no-op would read as delivered.
 */
export function selectHandler(
  handlers: ReadonlySet<LocalHandler>,
  queue: string,
  label: string
): LocalHandler {
  for (const reg of handlers) if (reg.queue === queue) return reg
  throw new Error(
    `queue: ${label} — no local handler on queue "${queue}" (config handlers attach at start())`
  )
}

/** The message an inline dispatch hands a handler in place of a real delivery. `data` is
 * expected in decoded wire shape already (round-tripped through the codec by the caller). */
export function syntheticMessage(
  data: unknown,
  key: string | undefined,
  headers: Readonly<Record<string, string>> | undefined
): Message<unknown> {
  const type = headers?.[MESSAGE_TYPE_HEADER]
  return {
    id: crypto.randomUUID(),
    data,
    headers: { ...headers },
    timestamp: Date.now(),
    attempt: 1,
    ...(key !== undefined ? { key } : {}),
    ...(type !== undefined ? { type } : {}),
  }
}

/**
 * The invocations a handler runs dispatched messages in, sized as its worker would size them.
 * A single-message handler takes one message per invocation. A batch handler takes slices: a
 * worker hands it at most `prefetch` messages per pull, split over `parallelism` calls, so no
 * slice here is larger than that and `parallelism` of them together stay within `prefetch`.
 */
const invocationsOf = (
  reg: LocalHandler,
  messages: readonly Message<unknown>[]
): Message<unknown>[][] => {
  const size = reg.batch ? Math.max(1, Math.ceil(reg.prefetch / reg.parallelism)) : 1
  const invocations: Message<unknown>[][] = []
  for (let start = 0; start < messages.length; start += size) {
    invocations.push(messages.slice(start, start + size))
  }
  return invocations
}

/**
 * Run one invocation of a local handler through its consume chain: the inline mirror of a
 * worker invocation, with a rejection in place of settles. A handler failure rejects with
 * THAT error; a middleware rejection, a timeout or a shutdown abort (with no redelivery
 * behind them, all failures here) reject too. `consumeTimeout` bounds it as on a worker.
 */
async function runLocalInvocation(
  reg: LocalHandler,
  invocation: readonly Message<unknown>[],
  inFlight: Set<AbortController>,
  label: string
): Promise<void> {
  const { queue } = reg
  const heartbeat = async (): Promise<void> => {} // no lease exists to renew
  const ac = new AbortController()
  const { call, state } = createConsumeCall(queue, invocation, ac.signal)
  inFlight.add(ac)
  // The verdict comes from what the terminal produced, never from what the chain returned,
  // so a middleware cannot hide a failure behind another result.
  const verdict = await runInvocation(
    reg.chain,
    call,
    (signal) =>
      runTerminal(
        state,
        invocation.length,
        [],
        (surviving, s) =>
          callHandler(reg.handler, reg.batch, {
            queue,
            messages: surviving,
            signal: s,
            heartbeat,
          }),
        reg.batch,
        signal
      ),
    ac,
    reg.consumeTimeout
  ).finally(() => inFlight.delete(ac))

  if (verdict.status === 'threw') throw verdict.error
  if (verdict.status === 'no-next') {
    throw new Error(
      `queue: ${label} — a consume middleware resolved without calling next on queue "${queue}"`
    )
  }
  const failure = verdict.status === 'completed' ? verdict.result.failures[0] : undefined
  if (failure !== undefined) {
    throw failure.error ?? new Error(`queue: ${label} — message rejected on queue "${queue}"`)
  }
  const released =
    verdict.status === 'abandoned'
      ? verdict.reason
      : verdict.result.released > 0
        ? verdict.result.outcome
        : undefined
  if (released !== undefined) {
    throw new Error(
      released === 'timeout'
        ? `queue: ${label} — invocation on queue "${queue}" timed out after ${reg.consumeTimeout}ms`
        : `queue: ${label} — invocation on queue "${queue}" aborted by shutdown`
    )
  }
}

/**
 * Run one local handler on dispatched messages, one invocation after another in dispatch
 * order, fail-fast: the first failing invocation rejects the call and the rest never start.
 * A batch-shaped handler receives the messages in slices no larger than its `prefetch`.
 */
export async function dispatchLocal(
  reg: LocalHandler,
  messages: readonly Message<unknown>[],
  inFlight: Set<AbortController>,
  label: string
): Promise<void> {
  for (const invocation of invocationsOf(reg, messages)) {
    await runLocalInvocation(reg, invocation, inFlight, label)
  }
}

/**
 * Run one local handler on many dispatched messages the way its worker would: the same
 * invocations as {@link dispatchLocal}, `parallelism` of them at a time. Every message gets
 * its turn: a failing invocation does not stop the others, and the call rejects with the
 * first failure once all have run.
 *
 * `signal` bounds the call: once it aborts no further invocation starts, the ones running
 * finish, and the call rejects with `signal.reason` if any message was left without a turn.
 */
export async function dispatchLocalMany(
  reg: LocalHandler,
  messages: readonly Message<unknown>[],
  inFlight: Set<AbortController>,
  label: string,
  signal?: AbortSignal
): Promise<void> {
  const invocations = invocationsOf(reg, messages)
  let next = 0
  let skipped = false
  let failure: { readonly error: unknown } | undefined
  const runUntilDrained = async (): Promise<void> => {
    while (next < invocations.length) {
      if (signal?.aborted === true) {
        skipped = true
        return
      }
      const invocation = invocations[next++] as Message<unknown>[]
      try {
        await runLocalInvocation(reg, invocation, inFlight, label)
      } catch (error) {
        failure ??= { error }
      }
    }
  }
  // A single-message handler never has more than `prefetch` messages in hand.
  const width = reg.batch ? reg.parallelism : Math.min(reg.parallelism, reg.prefetch)
  await Promise.all(Array.from({ length: Math.min(width, invocations.length) }, runUntilDrained))
  if (skipped) throw signal?.reason
  if (failure !== undefined) throw failure.error
}

/** One inline-dispatch call's arguments, read off either of invoke's two forms: a queue name
 * with data and options, or a message instance carrying its own key/headers. */
export function resolveInvokeTarget(
  classIndex: QueuesView['classIndex'],
  queueOrMessage: unknown,
  data: unknown,
  options: InvokeOptions | undefined
): { queue: string; data: unknown } & InvokeOptions {
  if (typeof queueOrMessage === 'string') {
    return { queue: queueOrMessage, data, key: options?.key, headers: options?.headers }
  }
  if (!isMessageInstance(queueOrMessage)) {
    throw new TypeError('queue: invoke expects a queue name or a message instance')
  }
  const message = toProduceMessage(queueOrMessage)
  return {
    queue: resolveMessageQueue(classIndex, queueOrMessage),
    data: message.data,
    key: message.key,
    headers: message.headers,
  }
}

/** The string-keyed invoke entry points; `Queue`'s registry-typed overloads are the public view. */
export interface InvokeApi {
  invoke(
    queueOrMessage: string | MessageInstance,
    data?: unknown,
    options?: InvokeOptions
  ): Promise<void>
  invokeMany(
    queue: string,
    messages: readonly InvokeMessage[],
    options?: InvokeManyOptions
  ): Promise<void>
}

/**
 * "Currently inside an invoke-dispatched handler". Ambient, so the guard survives any async
 * hop between the handler and a nested `invoke` (a produce-failure fallback that itself
 * produces is the canonical path), and module-level on purpose: the cascade it breaks is
 * just as unbounded when it crosses manager instances.
 */
const inInvoke = new AsyncLocalStorage<true>()

/**
 * Build the invoke entry point over the live local-handler registry. Inline dispatch runs
 * the handler through its consume chain but NOT the delivery machinery: nothing is sent, so
 * there is no retry, no dead-letter and no dedup. At-most-once by construction.
 */
export function createInvoker(deps: {
  /** Live, insertion-ordered registry of locally attached handlers. */
  readonly handlers: ReadonlySet<LocalHandler>
  readonly codec: Codec
  readonly queues: QueuesView
}): InvokeApi & { abortInFlight: () => void } {
  const { handlers, codec, queues } = deps
  const inFlight = new Set<AbortController>()

  const invoke: InvokeApi['invoke'] = async (queueOrMessage, dataArg, options) => {
    if (inInvoke.getStore() === true) {
      throw new Error(
        'queue: invoke called from inside an invoke-dispatched handler — refusing the inline cascade (depth cap 1)'
      )
    }
    const { queue, data, key, headers } = resolveInvokeTarget(
      queues.classIndex,
      queueOrMessage,
      dataArg,
      options
    )
    const reg = selectHandler(handlers, queue, 'invoke')
    // The payload round-trips through the codec so the handler sees exactly the wire shape
    // a real delivery would carry.
    const message = syntheticMessage(codec.decode(codec.encode(data)), key, headers)
    await inInvoke.run(true, () => dispatchLocal(reg, [message], inFlight, 'invoke'))
  }

  const invokeMany: InvokeApi['invokeMany'] = async (queue, items, options) => {
    if (inInvoke.getStore() === true) {
      throw new Error(
        'queue: invoke called from inside an invoke-dispatched handler — refusing the inline cascade (depth cap 1)'
      )
    }
    if (items.length === 0) return
    const reg = selectHandler(handlers, queue, 'invoke')
    const messages = items.map((item) =>
      syntheticMessage(codec.decode(codec.encode(item.data)), item.key, item.headers)
    )
    await inInvoke.run(true, () =>
      dispatchLocalMany(reg, messages, inFlight, 'invoke', options?.signal)
    )
  }

  // Shutdown: abort every in-flight invocation's signal. The invoke callers hold the
  // rejections; an aborted handler can only be signalled, never force-killed.
  const abortInFlight = (): void => {
    for (const ac of inFlight) ac.abort(shutdownReason())
  }

  return { invoke, invokeMany, abortInFlight }
}
