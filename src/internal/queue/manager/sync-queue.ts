import { NotSupportedError, type ProducerAdapter } from './adapter'
import { type Codec, json } from './codec'
import type { QueueEvents } from './events'
import {
  type AttachArg,
  dispatchLocal,
  type InvokeApi,
  type LocalHandler,
  localRegistration,
  registerConfigHandlers,
  resolveInvokeTarget,
  selectHandler,
  syntheticMessage,
} from './invoke'
import type { QueueMiddleware } from './middleware'
import { createProducer } from './producer'
import type { HandlerFor, Queue } from './queue'
import type { QueueMap, QueueRegistry } from './registry'
import { shutdownReason } from './signals'
import { validateRegistry } from './validate'
import type { BatchHandler, Handler, SubscribeOptions, Worker } from './worker'

export interface SyncQueueConfig<M extends QueueMap> {
  readonly queues: QueueRegistry<M>
  /** Handler instances attached at `start()`: the dispatch targets of every produce/invoke. */
  readonly handlers?: ReadonlyArray<HandlerFor<M>>
  /** Wire codec for the round trip every dispatched payload takes. Defaults to `json()`. */
  readonly codec?: Codec
  /** `produce` stages wrap every produce call (the terminal dispatches inline instead of
   * sending); `consume` stages wrap each inline invocation exactly as a worker's. */
  readonly middleware?: readonly QueueMiddleware[]
  /** Only the `produce` hook fires here: there are no deliveries. */
  readonly events?: QueueEvents
}

/**
 * A manager whose `produce` IS execution: every produced message runs the queue's attached
 * handler inline, in this process, and nothing is sent anywhere. The `Queue` interface
 * without a backend, for running an event-driven app with its queue turned off.
 *
 * - Dispatch mirrors `invoke`'s synthetic delivery (codec round-tripped payload, fresh
 *   message, `attempt: 1`, the full middleware chain, the first handler attached to the
 *   queue) but WITHOUT invoke's depth cap: a handler that produces recurses through this
 *   manager by design.
 * - `subscribe` ATTACHES without consuming; the returned worker's `stop()` detaches.
 * - At-most-once, no retry, no dead-letter, no dedup: `idempotencyKey` is accepted and
 *   ignored, `delayMs` is accepted and runs immediately.
 */
export function createSyncQueue<M extends QueueMap>(
  config: SyncQueueConfig<M>
): Queue<QueueRegistry<M>, never> {
  const codec = config.codec ?? json()
  const registry = config.queues
  const deps = { queues: registry, middleware: config.middleware }
  const registrations = new Set<LocalHandler>()
  const inFlight = new Set<AbortController>()
  let handlersAttached = false
  let closed = false

  const subscribe = (
    arg1: AttachArg,
    arg2?: Handler<unknown> | BatchHandler<unknown> | SubscribeOptions
  ): Worker => {
    const { registration } = localRegistration(arg1, arg2, deps)
    registrations.add(registration)
    // Worker-shaped detach handle: nothing polls, so there is nothing else to drain.
    let detached = false
    return {
      stop: async (): Promise<void> => {
        registrations.delete(registration)
        detached = true
      },
      get closed(): boolean {
        return detached
      },
    } as unknown as Worker
  }

  // `items` carry ALREADY codec-round-tripped data. A batch-shaped handler receives the
  // whole produced batch in one call; a single-message handler receives it in send order,
  // fail-fast.
  const dispatch = async (
    queue: string,
    items: readonly {
      readonly data: unknown
      readonly key?: string
      readonly headers?: Readonly<Record<string, string>>
    }[]
  ): Promise<void> => {
    if (closed) throw new Error('queue: sync queue is closed')
    const reg = selectHandler(registrations, queue, 'sync queue')
    const messages = items.map((item) => syntheticMessage(item.data, item.key, item.headers))
    await dispatchLocal(reg, messages, inFlight, 'sync queue')
  }

  // Produce = the real produce pipeline over an inline "adapter" whose send dispatches.
  const inlineAdapter: Pick<ProducerAdapter<unknown, never>, 'send' | 'delayedDelivery'> = {
    delayedDelivery: true, // accepted and executed immediately
    send: (queue, messages) =>
      dispatch(
        queue,
        messages.map((m) => ({ ...m, data: codec.decode(m.data) }))
      ),
  }
  const invoke: InvokeApi['invoke'] = async (queueOrMessage, data, options) => {
    const target = resolveInvokeTarget(registry.classIndex, queueOrMessage, data, options)
    await dispatch(target.queue, [{ ...target, data: codec.decode(codec.encode(target.data)) }])
  }
  const { produce } = createProducer<unknown, never>(inlineAdapter, codec, {
    ...config,
    queues: registry,
    invoke,
  })

  const start = async (): Promise<void> => {
    if (handlersAttached) return
    validateRegistry(registry)
    // All or nothing, flag after: a list with one bad entry attaches none of it.
    registerConfigHandlers((config.handlers ?? []) as readonly AttachArg[], deps, registrations)
    handlersAttached = true
  }

  const close = async (): Promise<void> => {
    closed = true
    for (const ac of inFlight) ac.abort(shutdownReason())
  }

  return {
    produce,
    subscribe,
    invoke,
    stats: () => Promise.reject(new NotSupportedError('stats')),
    start,
    maintenance: {
      start: (): void => {},
      runOnce: async (): Promise<void> => {},
      stop: async (): Promise<void> => {},
    },
    close,
    getQueue: (name: string) => registry.get(name),
    getQueues: () => registry,
  } as unknown as Queue<QueueRegistry<M>, never>
}
