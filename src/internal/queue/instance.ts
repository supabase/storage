import { abortDeadline } from '@internal/concurrency'
import { ERRORS } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import {
  type QueueDepthSample,
  queueJobBatchSize,
  queueJobError,
  registerQueueDepthObserver,
} from '@internal/monitoring/metrics'
import { NodeHttpHandler } from '@smithy/node-http-handler'
import type { PgBoss } from 'pg-boss'
import { getConfig } from '../../config'
import { type PgBossCtx, pgboss } from './adapters/pgboss'
import { sqs } from './adapters/sqs'
import { createQueueBoss, queueDefaults, runPgBossMigrations } from './boss'
import {
  type AnyProducerQueue,
  type AnyQueue,
  createProducerQueue,
  createQueue,
  createSyncQueue,
  type HandlerFor,
  type ProducerQueue,
  type QueueMap,
  type QueueRegistry,
} from './manager'
import {
  completionLog,
  schedulingMetrics,
  syncFallback,
  syncModeGuard,
  tenantDisableEvents,
} from './middleware'

const {
  queueAdapter,
  queueSqsEndpoint,
  queueSqsMaxSockets,
  queueSqsPrefix,
  queueSqsProvisioning,
  queueSqsRegion,
  pgQueueEnable,
  pgQueueEnableWorkers,
  pgQueueSchemaV2,
} = getConfig()

/**
 * The app's typed publish surface: produce/invoke plus lifecycle, the same on every node
 * shape (full queue on worker nodes, producer-only on API nodes, sync queue when the queue
 * is env-disabled). Consuming members (`subscribe`/`maintenance`) are deliberately absent: on
 * an API node they do not exist at runtime, so reaching for them is a compile error.
 *
 * The produce context is pg-boss's (a caller-owned transaction). It is honoured only when
 * pg-boss is the running engine; see {@link txQueueCtx}.
 */
export type StorageQueue<M extends QueueMap> = ProducerQueue<M, PgBossCtx>

/** What the events layer supplies at startup. The queue layer knows no concrete events: the
 * middlewares read their per-event config off each queue's bound message class. */
export interface QueueSetup<M extends QueueMap> {
  queues: QueueRegistry<M>
  handlers: ReadonlyArray<HandlerFor<M>>
  /** Optional teardown for handler-owned resources (v1's per-event `onClose`). */
  onStop?: () => Promise<void>
}

const queueStopTimeoutMs = 25_000

/** The held singleton is queue-map-erased (module state cannot be generic). The union, rather
 * than `AnyProducerQueue` alone, lets `'maintenance' in instance` narrow to the full queue. */
let instance: AnyQueue | AnyProducerQueue | undefined
let stopping: Promise<void> | undefined
let unobserveQueueDepth: (() => void) | undefined
// The caller-owned pg-boss instance, when pg-boss is the running engine.
let boss: PgBoss | undefined

function createSyncQueueInstance<M extends QueueMap>(setup: QueueSetup<M>) {
  return createSyncQueue({
    queues: setup.queues,
    handlers: setup.handlers,
    middleware: [tenantDisableEvents(), completionLog(), syncModeGuard()],
  })
}

/** The longest SQS keeps a message: 14 days. */
const SQS_RETENTION_SECONDS = 14 * 86_400

/** The engine behind the queue, one per process (`QUEUE_ADAPTER`). Both are widened to the
 * pg-boss produce context so every node shape shares one typed surface: SQS ignores a
 * context it is handed, and {@link txQueueCtx} only produces one under pg-boss. */
function createAdapter() {
  if (queueAdapter === 'sqs') {
    return sqs({
      client: {
        ...(queueSqsEndpoint ? { endpoint: queueSqsEndpoint } : {}),
        ...(queueSqsRegion ? { region: queueSqsRegion } : {}),
        // The SDK's default of 50 connections makes sends wait for a free one under load.
        requestHandler: new NodeHttpHandler({
          httpAgent: { keepAlive: true, maxSockets: queueSqsMaxSockets },
          httpsAgent: { keepAlive: true, maxSockets: queueSqsMaxSockets },
        }),
      },
      prefix: queueSqsPrefix,
      // SQS keeps a message 4 days unless told otherwise, dead-letter queues included. Ask for
      // the longest it allows, so a dead letter can still be inspected two weeks later. Set
      // when the adapter creates a queue; one that already exists keeps what it has.
      queueAttributes: { MessageRetentionPeriod: String(SQS_RETENTION_SECONDS) },
      provisioning: queueSqsProvisioning,
      events: {
        // v1's `queue_job_error`: a message exhausted its retry budget and dead-lettered.
        deadLettered: ({ queue }) => {
          queueJobError.add(1, { name: queue, adapter: 'sqs' })
        },
      },
    })
  }

  boss = createQueueBoss()
  return pgboss({ boss, schema: pgQueueSchemaV2, queue: queueDefaults() })
}

function createQueueInstance<M extends QueueMap>(
  setup: QueueSetup<M>
): AnyQueue | AnyProducerQueue {
  const { queues, handlers } = setup

  if (!pgQueueEnable) {
    return createSyncQueueInstance(setup)
  }

  // One list for every queue-enabled shape: on a producer-only queue the handlers are
  // dispatch targets (never polled) and the consume stages run around invoke dispatches, so
  // the fallback path behaves identically on API and worker nodes.
  const middleware = [tenantDisableEvents(), schedulingMetrics(), completionLog(), syncFallback()]
  const adapter = createAdapter()

  if (!(pgQueueEnableWorkers ?? true)) {
    return createProducerQueue(adapter, { queues, handlers, middleware })
  }

  return createQueue(adapter, {
    queues,
    handlers,
    middleware,
    events: {
      // How many messages each pull returns: the batch size actually consumed per queue.
      pulled: ({ queue, size }) => {
        queueJobBatchSize.record(size, { name: queue, adapter: queueAdapter })
      },
    },
    pollIdleIntervalMs: 5_000,
    closeTimeout: 20_000,
  })
}

/**
 * Start the queue, idempotently. Three shapes, all behind the same `StorageQueue` surface:
 *
 * - `PG_QUEUE_ENABLE=false`: a sync queue. Every produce runs its handler inline (nested
 *   sends included) and no queue connection exists at all.
 * - queue enabled, workers enabled: the full queue. Producers send, workers consume, and
 *   `invoke` (the produce-failure fallback included) dispatches to the live handlers.
 * - queue enabled, workers disabled (producer-only API nodes): produces send, nothing polls,
 *   and the same handler list attaches as dispatch-only targets for `invoke` and the fallback.
 */
export async function startQueue<M extends QueueMap>(
  setup: QueueSetup<M>,
  opts: { signal?: AbortSignal } = {}
): Promise<StorageQueue<M>> {
  if (instance) {
    return instance as StorageQueue<M>
  }
  if (opts.signal?.aborted) {
    throw ERRORS.Aborted('Cannot start queue with aborted signal')
  }

  // The pg-boss schema is provisioned on a direct connection before the queue starts: the
  // runtime pool may sit behind a transaction pooler, which cannot run migrations.
  if (pgQueueEnable && queueAdapter === 'pgboss') {
    await runPgBossMigrations()
  }

  const queue = createQueueInstance(setup)
  instance = queue

  await queue.start()

  if ('maintenance' in queue) {
    // Maintenance follows the worker role, like v1's sweeps: pg-boss supervision (expiry,
    // retention, partition upkeep; its internal timer is off, see boss.ts) at v1's 5-minute
    // cadence. `queue.close()` stops it. A no-op on the sync queue and on SQS.
    queue.maintenance.start({
      intervalMs: 5 * 60_000,
      onError: (error) => {
        logSchema.error(logger, '[Queue] maintenance error', { type: 'queue', error })
      },
    })
  }

  if (pgQueueEnable && 'maintenance' in queue) {
    // Depth gauges ride `queue.stats()`: one read-only backend sample per metrics collection,
    // already guarded inside the manager (concurrent calls coalesce, the report is cached for
    // its TTL). Worker nodes only, so producer-only API nodes do not double-report the same
    // backend gauges.
    unobserveQueueDepth = registerQueueDepthObserver(async () => {
      const report = await queue.stats()
      return report.queues.map(
        (stats): QueueDepthSample => ({
          queue: stats.queue,
          adapter: queueAdapter,
          ...(stats.backlog ? { pending: stats.backlog.count } : {}),
          ...(stats.inFlight ? { inFlight: stats.inFlight.count } : {}),
          ...(stats.retryParked ? { retryParked: stats.retryParked.total.count } : {}),
          ...(stats.oldestBacklogAgeMs !== undefined
            ? { oldestBacklogAgeSeconds: stats.oldestBacklogAgeMs / 1000 }
            : {}),
        })
      )
    })
  }

  const { signal } = opts
  if (signal) {
    signal.addEventListener(
      'abort',
      () => {
        logSchema.info(logger, '[Queue] Stopping', { type: 'queue' })
        // Returned so an AsyncAbortController holds the next shutdown group (database pools,
        // storage backends) until the queue has drained — and bounded by the deadline the
        // shutdown as a whole was given.
        return stopQueue(setup.onStop, abortDeadline(signal))
          .then(() => {
            logSchema.info(logger, '[Queue] Exited', { type: 'queue' })
          })
          .catch((e) => {
            logSchema.error(logger, '[Queue] Error while stopping queue', {
              error: e,
              type: 'queue',
            })
          })
      },
      { once: true }
    )
  }

  return instance as StorageQueue<M>
}

/** The running queue. Producers reach it through the typed accessor in storage/events (or,
 * from inside the registry's own module graph, this generic accessor with a type-only import
 * of the registry's queue map, avoiding the runtime import cycle a value import would create). */
export function getQueue<M extends QueueMap>(): StorageQueue<M> {
  if (!instance) {
    throw new Error('queue is not started (call startQueue first)')
  }
  return instance as StorageQueue<M>
}

export function isQueueStarted(): boolean {
  return instance !== undefined
}

/** TEST SEAM: install a stub as the running queue. Test stubs are partial by nature, so the
 * widening cast lives here, once. */
export function setQueueForTesting(queue: Partial<AnyQueue>): void {
  instance = queue as AnyQueue
}

/** TEST SEAM: build and install the sync queue (the shape `startQueue` uses when the queue
 * is env-disabled) regardless of `pgQueueEnable`, so a test file that flips the flag on still
 * never constructs a real engine. */
export async function startSyncQueueForTesting<M extends QueueMap>(
  setup: QueueSetup<M>
): Promise<StorageQueue<M>> {
  const queue = createSyncQueueInstance(setup)
  await queue.start()
  instance = queue
  return instance as StorageQueue<M>
}

/** Pending jobs on a queue (v1 `Queue.getQueueSize`): the backend's sampled backlog. Throws
 * when the queue was never started. */
export async function queueSize(queue: string): Promise<number> {
  if (!instance) {
    throw new Error('queueSize requires a started queue in this process')
  }
  const report = await instance.stats({ queues: [queue] })
  return report.queues.reduce((total, stats) => total + (stats.backlog?.count ?? 0), 0)
}

/**
 * Drain and stop: `close()` drains workers under their closeTimeout and detaches the adapter
 * (a caller-owned boss survives it), then the boss stops, gracefully in production. The whole
 * teardown is raced against a hard 25s bound.
 */
export async function stopQueue(
  onStop?: () => Promise<void>,
  deadline = Number.POSITIVE_INFINITY
): Promise<void> {
  if (!instance) return
  if (stopping) return stopping

  const { isProduction } = getConfig()
  const current = instance

  stopping = (async () => {
    try {
      // Stop sampling before the backend goes away.
      unobserveQueueDepth?.()
      unobserveQueueDepth = undefined
      await Promise.race([
        (async () => {
          await current.close()
          if (boss) {
            await boss.stop({ timeout: 20_000, graceful: isProduction, close: true })
            boss = undefined
          }
        })(),
        new Promise((_, reject) => {
          const ms = Math.max(0, Math.min(queueStopTimeoutMs, deadline - Date.now()))
          setTimeout(() => reject(new Error(`Queue stop timed out after ${ms}ms`)), ms).unref()
        }),
      ])
    } finally {
      instance = undefined
      stopping = undefined
      // Handler-owned resources are released even when the drain timed out or failed: a hung
      // queue must not leak the shared backend.
      await onStop?.()
    }
  })()

  return stopping
}
