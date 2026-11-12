import type { ProducerAdapter, QueueAdapter } from './adapter'
import type { Codec } from './codec'
import type { QueueBatchHandlerInstance, QueueHandlerInstance } from './declare'
import type { QueueEvents } from './events'
import {
  type AttachArg,
  createInvoker,
  type InvokeApi,
  type InvokeOptions,
  type LocalHandler,
  registerConfigHandlers,
} from './invoke'
import { createMaintenance, type Maintenance } from './maintenance'
import type { ProduceMessage } from './message'
import type { QueueMiddleware } from './middleware'
import { createProducer, type ProduceBatchOptions, type ProduceOptions } from './producer'
import { createProvisioning } from './provisioning'
import type {
  AnyMessageClass,
  MessagePayload,
  PayloadOf,
  QueueMap,
  QueueMapOf,
  QueueRegistry,
  QueuesSource,
  QueuesView,
} from './registry'
import { createStatsAccessor, type StatsQuery, type StatsReport } from './stats'
import { createSubscriber } from './subscriber'
import { requireFinite } from './validate'
import {
  type BatchContext,
  type BatchHandler,
  DEFAULT_POLL_BATCH_INTERVAL_MS,
  DEFAULT_POLL_IDLE_INTERVAL_MS,
  type Handler,
  type JobContext,
  type SubscribeOptions,
  type Worker,
} from './worker'

/** The union of message classes bound across registry `M`'s queues (via `queue(Class)`). */
export type QueueClasses<M extends QueueMap> = {
  [K in keyof M]: M[K] extends { readonly classes: infer Cs extends readonly AnyMessageClass[] }
    ? Cs[number]
    : never
}[keyof M]

/** A message instance producible on registry `M`: an instance of any bound class. */
export type MessageFor<M extends QueueMap> = InstanceType<QueueClasses<M>>

/**
 * A handler instance valid for registry `M`: a queue-name handler (its queue declared, its
 * payload matching) or a message-class handler (every listed class bound in the registry).
 */
export type HandlerFor<M extends QueueMap> =
  | { [K in keyof M & string]: QueueHandlerInstance<K, PayloadOf<M[K]>> }[keyof M & string]
  | { [K in keyof M & string]: QueueBatchHandlerInstance<K, PayloadOf<M[K]>> }[keyof M & string]
  | {
      readonly messages: ReadonlyArray<QueueClasses<M>>
      readonly options?: SubscribeOptions
      handle(ctx: JobContext<MessagePayload<QueueClasses<M>>>): void | Promise<void>
    }
  | {
      readonly messages: ReadonlyArray<QueueClasses<M>>
      readonly options?: SubscribeOptions
      handleBatch(ctx: BatchContext<MessagePayload<QueueClasses<M>>>): void | Promise<void>
    }

/**
 * The publishing role: `produce`, inline dispatch (`invoke`), lifecycle and stats. Consumption
 * is absent at the type level. The full {@link Queue} extends this interface, so publish-only
 * code written against `ProducerQueue` accepts either.
 *
 * The queues generic accepts the registry `defineQueues` returns (the usual annotation is
 * `ProducerQueue<typeof queues>`) or a raw declared map; `M` is derived, never passed. To
 * hold a manager without knowing its map, use {@link AnyProducerQueue}.
 */
export interface ProducerQueue<
  In extends QueuesSource,
  Ctx = never,
  M extends QueueMap = QueueMapOf<In>,
> {
  /** Send a single message. */
  produce<K extends keyof M & string>(
    queue: K,
    data: PayloadOf<M[K]>,
    options?: ProduceOptions<Ctx>
  ): Promise<void>
  /** Send many messages in one round trip (one `send`, atomic on transactional backends). */
  produce<K extends keyof M & string>(
    queue: K,
    messages: ReadonlyArray<ProduceMessage<PayloadOf<M[K]>>>,
    options?: ProduceBatchOptions<Ctx>
  ): Promise<void>
  /** Send a message instance: the queue resolves through the registry's class binding. The
   * instance's class must be bound in the registry or the call does not compile. */
  produce(message: MessageFor<M>, options?: ProduceBatchOptions<Ctx>): Promise<void>
  /** Send many message instances in one round trip. All must resolve to one queue. */
  produce(messages: ReadonlyArray<MessageFor<M>>, options?: ProduceBatchOptions<Ctx>): Promise<void>
  /**
   * Run the queue's locally attached handler on a message, inline in this process: nothing
   * is sent. Exactly one execution (the first handler attached to the queue), through the
   * full middleware chain, with a synthetic delivery: fresh id, `attempt: 1`, a codec
   * round-tripped payload, a signal honouring the handler's `consumeTimeout`, and a no-op
   * heartbeat. At-most-once: no retry, no dead-letter, no dedup. A handler failure rejects
   * with that error; no local handler throws; calling `invoke` from inside an
   * invoke-dispatched handler throws (the guard against a produce-fallback cascade).
   */
  invoke<K extends keyof M & string>(
    queue: K,
    data: PayloadOf<M[K]>,
    options?: InvokeOptions
  ): Promise<void>
  invoke(message: MessageFor<M>): Promise<void>
  /** Run the adapter's startup, provision every declared queue (unless `autoProvision:
   * false`) and attach the config `handlers`. Idempotent. Call once before producing. */
  start(): Promise<void>
  /** Drain what this manager runs, then close the adapter. Idempotent. */
  close(): Promise<void>
  /** One queue's descriptor as declared in the registry. */
  getQueue<K extends keyof M & string>(name: K): M[K]
  /** The declared queues, as the map-agnostic view (which is what lets a concrete manager be
   * held as an erased {@link AnyQueue} with no cast). */
  getQueues(): QueuesView
  /**
   * Sample the backend's queue gauges in one read-only round trip. Concurrent identical calls
   * coalesce and the settled report is reused for `statsTtlMs`. Rejects with
   * `NotSupportedError` when the adapter does not expose stats.
   */
  stats(query?: StatsQuery): Promise<StatsReport>
}

/** The full manager: the publishing role plus workers and maintenance. */
export interface Queue<In extends QueuesSource, Ctx = never, M extends QueueMap = QueueMapOf<In>>
  extends ProducerQueue<In, Ctx> {
  /** Attach a self-driving handler to a queue. Returns the running {@link Worker}. */
  subscribe<K extends keyof M & string>(queue: K, handler: Handler<PayloadOf<M[K]>>): Worker
  /** The inline BATCH form: `batch: true` retypes the callback to one pulled batch per call. */
  subscribe<K extends keyof M & string>(
    options: { queue: K; batch: true } & SubscribeOptions,
    handler: BatchHandler<PayloadOf<M[K]>>
  ): Worker
  subscribe<K extends keyof M & string>(
    options: { queue: K } & SubscribeOptions,
    handler: Handler<PayloadOf<M[K]>>
  ): Worker
  /** Attach a queue-name handler object; `overrides` adjust its `options` per attachment. */
  subscribe<K extends keyof M & string>(
    handler: QueueHandlerInstance<K, PayloadOf<M[K]>>,
    overrides?: SubscribeOptions
  ): Worker
  subscribe<K extends keyof M & string>(
    handler: QueueBatchHandlerInstance<K, PayloadOf<M[K]>>,
    overrides?: SubscribeOptions
  ): Worker
  /** Attach a message-class handler (a `QueueHandler(…)` subclass). Every listed class must
   * be bound in the registry or the call does not compile. */
  subscribe<const Cs extends readonly QueueClasses<M>[]>(
    handler: {
      readonly messages: Cs
      readonly options?: SubscribeOptions
      handle(ctx: JobContext<MessagePayload<Cs[number]>>): void | Promise<void>
    },
    overrides?: SubscribeOptions
  ): Worker
  subscribe<const Cs extends readonly QueueClasses<M>[]>(
    handler: {
      readonly messages: Cs
      readonly options?: SubscribeOptions
      handleBatch(ctx: BatchContext<MessagePayload<Cs[number]>>): void | Promise<void>
    },
    overrides?: SubscribeOptions
  ): Worker
  /** Opt-in background maintenance. Call `maintenance.start()` to enable. */
  readonly maintenance: Maintenance
}

/**
 * The erased manager: the type for holders that store a manager without knowing its queues
 * (a module singleton) and restore the concrete typing at their own accessor with a plain
 * downcast. Every concrete `Queue<…>` is assignable here with no cast.
 *
 * A distinct interface rather than a `Queue<QueueMap>` spelling on purpose: instantiations
 * of one generic compare by measured variance, under which a concrete manager never assigns
 * to a `QueueMap` instantiation of the SAME generic. A separate interface is compared
 * structurally, where the members do unify.
 */
export interface AnyQueue<Ctx = unknown> extends Queue<QueueMap, Ctx> {}

/** {@link AnyQueue}'s sibling for the publish surface. Every concrete producer queue and
 * every concrete full queue assigns here cast-free. */
export interface AnyProducerQueue<Ctx = unknown> extends ProducerQueue<QueueMap, Ctx> {}

/** What a producer-only manager is configured with. */
export interface ProducerQueueConfig<M extends QueueMap, W = unknown> {
  readonly queues: QueueRegistry<M>
  /**
   * Handler instances. On a full manager `start()` starts each one's worker after
   * provisioning; on a producer-only manager they are dispatch targets for `invoke` and the
   * produce-fallback seam (`ProduceCall.invoke`) ONLY, never polled.
   */
  readonly handlers?: ReadonlyArray<HandlerFor<M>>
  /** Override the adapter's codec. */
  readonly codec?: Codec<W>
  /** Provision declared queues during `start()`. Defaults to `true`. */
  readonly autoProvision?: boolean
  /** Observability sinks (a producer-only manager only ever emits `produce`). */
  readonly events?: Pick<QueueEvents, 'produce'>
  /** Manager-level middleware: `produce` stages wrap every produce call, `consume` stages
   * wrap every handler invocation (outside each worker's own middleware). */
  readonly middleware?: readonly QueueMiddleware[]
  /** Default bound (ms) on every produce call; past it the call rejects with a
   * `TimeoutError`. The bound is on the caller's WAIT: a timed-out send may still land. */
  readonly produceTimeout?: number
  /** TTL (ms) of `stats()`'s cached report. Defaults to 5000; `0` disables the cache. */
  readonly statsTtlMs?: number
}

export interface QueueConfig<M extends QueueMap, W = unknown> extends ProducerQueueConfig<M, W> {
  readonly events?: QueueEvents
  /** Default idle poll interval (ms) for every worker. Defaults to 1000. */
  readonly pollIdleIntervalMs?: number
  /** Default linger (ms) between non-empty pulls for every worker. Defaults to 100. */
  readonly pollBatchIntervalMs?: number
  /** Default grace period (ms) `stop()` / `close()` waits for in-flight handlers before
   * aborting their `signal`. Omitted: drain to completion. */
  readonly closeTimeout?: number
}

/** Assemble a producer-only manager over an adapter that only needs to send. */
export function createProducerQueue<M extends QueueMap, W = unknown, Ctx = never>(
  adapter: ProducerAdapter<W, Ctx>,
  config: ProducerQueueConfig<M, W>
): ProducerQueue<QueueRegistry<M>, Ctx> {
  const codec = config.codec ?? adapter.codec
  const registry = config.queues
  const provision = createProvisioning(adapter, registry, config.autoProvision ?? true)

  // Invoke's dispatch-only registry: the config handlers, normalized exactly as a worker
  // attachment would be, just never polled.
  const registrations = new Set<LocalHandler>()
  const invoker = createInvoker({
    handlers: registrations,
    codec: codec as Codec,
    queues: registry,
  })
  let closed = false
  const invoke: InvokeApi['invoke'] = (queueOrMessage, data, options) =>
    closed
      ? Promise.reject(new Error('queue: producer queue is closed'))
      : invoker.invoke(queueOrMessage, data, options)
  const { produce } = createProducer(adapter, codec, { ...config, queues: registry, invoke })

  // Handlers attach once, after provisioning succeeds. All or nothing, flag after: a list
  // with one bad entry attaches none of it, and the start() that threw can be retried.
  let handlersAttached = false
  const start = async (): Promise<void> => {
    await provision()
    if (handlersAttached) return
    registerConfigHandlers(
      (config.handlers ?? []) as readonly AttachArg[],
      { queues: registry, middleware: config.middleware },
      registrations
    )
    handlersAttached = true
  }

  const close = async (): Promise<void> => {
    if (closed) return
    closed = true
    invoker.abortInFlight()
    await adapter.close()
  }

  // The trusted seam: internals are string-keyed; the registry-typed overloads are the
  // public view.
  return {
    produce,
    invoke,
    start,
    close,
    getQueue: (name: string) => registry.get(name),
    getQueues: () => registry,
    stats: createStatsAccessor(adapter, config.statsTtlMs),
  } as unknown as ProducerQueue<QueueRegistry<M>, Ctx>
}

/** Assemble a typed queue manager over an adapter. */
export function createQueue<M extends QueueMap, W = unknown, Ctx = never>(
  adapter: QueueAdapter<W, Ctx>,
  config: QueueConfig<M, W>
): Queue<QueueRegistry<M>, Ctx> {
  const codec = config.codec ?? adapter.codec
  const registry = config.queues
  requireFinite('pollIdleIntervalMs', config.pollIdleIntervalMs, 0)
  requireFinite('pollBatchIntervalMs', config.pollBatchIntervalMs, 0)
  requireFinite('closeTimeout', config.closeTimeout, 0)

  const provision = createProvisioning(adapter, registry, config.autoProvision ?? true)
  // Consumption is assembled first: `ProduceCall.invoke` dispatches through the subscriber's
  // live handler registry.
  const consume = createSubscriber(
    adapter as QueueAdapter<W, never>,
    codec,
    {
      closeTimeout: config.closeTimeout,
      pollIdleIntervalMs: config.pollIdleIntervalMs ?? DEFAULT_POLL_IDLE_INTERVAL_MS,
      pollBatchIntervalMs: config.pollBatchIntervalMs ?? DEFAULT_POLL_BATCH_INTERVAL_MS,
      ...(config.middleware !== undefined ? { middleware: config.middleware } : {}),
      ...(config.events !== undefined ? { events: config.events } : {}),
    },
    registry
  )
  const { produce } = createProducer(adapter, codec, {
    ...config,
    queues: registry,
    invoke: consume.invoke,
  })
  const maintenance = createMaintenance(adapter)

  // Config handlers start exactly once, after provisioning succeeds, so no worker polls a
  // queue that does not exist yet. The flag flips before subscribing so a concurrent second
  // start() cannot double-attach.
  let handlersStarted = false
  const start = async (): Promise<void> => {
    await provision()
    if (handlersStarted) return
    handlersStarted = true
    for (const handler of config.handlers ?? []) consume.subscribe(handler as AttachArg)
  }

  let closed = false
  const close = async (): Promise<void> => {
    if (closed) return
    closed = true
    // Drain every worker (each honours its closeTimeout), then close the adapter last so no
    // in-flight work hits a closed connection. allSettled so one failure can't abandon the rest.
    await Promise.allSettled([maintenance.stop(), consume.shutdown()])
    await adapter.close()
  }

  return {
    produce,
    subscribe: consume.subscribe,
    invoke: consume.invoke,
    start,
    maintenance,
    stats: createStatsAccessor(adapter, config.statsTtlMs),
    close,
    getQueue: (name: string) => registry.get(name),
    getQueues: () => registry,
  } as unknown as Queue<QueueRegistry<M>, Ctx>
}
