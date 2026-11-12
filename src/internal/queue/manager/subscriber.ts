import type { QueueAdapter } from './adapter'
import type { Codec } from './codec'
import type { QueueEvents } from './events'
import {
  type AttachArg,
  createInvoker,
  type InvokeApi,
  type LocalHandler,
  localRegistration,
} from './invoke'
import type { QueueMiddleware } from './middleware'
import type { QueuesView } from './registry'
import { type BatchHandler, type Handler, type SubscribeOptions, Worker } from './worker'

/** Manager-level defaults applied to every worker unless overridden per worker. */
export interface ConsumeDefaults {
  readonly closeTimeout: number | undefined
  readonly pollIdleIntervalMs: number
  readonly pollBatchIntervalMs: number
  readonly middleware?: readonly QueueMiddleware[]
  readonly events?: QueueEvents
}

export interface SubscriberApi {
  /** Start a worker: `subscribe(queue, handler)`, `subscribe({ queue, ...options }, handler)`
   * or `subscribe(handlerInstance, overrides?)`. */
  subscribe(
    arg1: AttachArg,
    arg2?: Handler<unknown> | BatchHandler<unknown> | SubscribeOptions
  ): Worker
  /** Dispatch a message to this process's locally attached handler, inline (no send). */
  invoke: InvokeApi['invoke']
  /** Stop every worker this API spawned and abort in-flight invokes. */
  shutdown(): Promise<void>
}

/**
 * The consumption factory: mints `Worker`s over one adapter and tracks them, so `shutdown()`
 * can drain them all and `invoke` targets exactly the handlers live in this process.
 */
export function createSubscriber<W>(
  adapter: QueueAdapter<W, never>,
  codec: Codec<W>,
  defaults: ConsumeDefaults,
  queues: QueuesView
): SubscriberApi {
  const workers = new Set<Worker>()
  const localHandlers = new Set<LocalHandler>()
  // The consume path only ever decodes to `unknown`, so the wire type is irrelevant here.
  const wireAdapter = adapter as QueueAdapter<unknown, never>
  const wireCodec = codec as Codec

  const subscribe: SubscriberApi['subscribe'] = (arg1, arg2) => {
    const { registration, options } = localRegistration(arg1, arg2, {
      queues,
      middleware: defaults.middleware,
    })
    const worker = new Worker(
      {
        ...options,
        batch: registration.batch,
        adapter: wireAdapter,
        codec: wireCodec,
        defaultCloseTimeout: defaults.closeTimeout,
        defaultPollIdleIntervalMs: defaults.pollIdleIntervalMs,
        defaultPollBatchIntervalMs: defaults.pollBatchIntervalMs,
        chain: registration.chain,
        ...(defaults.events !== undefined ? { events: defaults.events } : {}),
        queues,
        onStop: () => {
          workers.delete(worker)
          localHandlers.delete(registration)
        },
      },
      registration.handler
    )
    workers.add(worker)
    localHandlers.add(registration)
    return worker
  }

  const { invoke, abortInFlight } = createInvoker({
    handlers: localHandlers,
    codec: wireCodec,
    queues,
  })

  const shutdown = async (): Promise<void> => {
    abortInFlight()
    await Promise.allSettled([...workers].map((w) => w.stop()))
  }

  return { subscribe, invoke, shutdown }
}
