import { ERRORS } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import {
  type QueueDepthSample,
  queueJobError,
  registerQueueDepthObserver,
} from '@internal/monitoring/metrics'
import type { PgBossCtx } from '@supabase-labs/wave-adapter-pgboss'
import { pgboss } from '@supabase-labs/wave-adapter-pgboss'
import { pgque } from '@supabase-labs/wave-adapter-pgque'
import type {
  AnyProducerWave,
  AnyWave,
  HandlerFor,
  ProducerWave,
  TopicMap,
  TopicRegistry,
} from '@supabase-labs/wave-core'
import { createProducerWave, createSyncWave, createWave } from '@supabase-labs/wave-core'
import type { Pool } from 'pg'
import { getConfig } from '../../config'
import { createQueueBoss, queueDefaults, runPgBossMigrations } from './boss'
import { createQueuePgPool } from './database'
import { schedulingMetrics, syncFallback, syncModeGuard, tenantDisableEvents } from './middleware'
import { runPgqueMigrations } from './pgque'

const {
  pgQueueAdapter,
  pgQueueEnable,
  pgQueueEnableWorkers,
  pgQueueSchemaV2,
  pgQueueTickIntervalMs,
} = getConfig()

/** A storage wave: the app's typed publish surface — produce/invoke + lifecycle — honest on
 * every node shape (full wave on worker nodes, producer-only wave on API nodes, sync wave
 * when the queue is env-disabled; all three carry it). Full-wave-only members
 * (`subscribe`/`consumer`/`reader`/`maintenance`) are deliberately absent: on an API node
 * they don't exist at runtime, so app code reaching for them is a compile error here rather
 * than a runtime throw. */
export type StorageWave<M extends TopicMap> = ProducerWave<M, PgBossCtx>

/**
 * What the events layer supplies at startup — the queue layer knows no concrete events beyond
 * the `meta` each topic declares alongside its pg-boss extension (both merged onto core's
 * `CreateTopicOptions` — see `storage/events/topics.ts`); the middlewares below read it
 * straight off `setup.topics`, so there is no separate lookup to wire.
 */
export interface QueueOptions<M extends TopicMap> {
  topics: TopicRegistry<M>
  handlers: ReadonlyArray<HandlerFor<M>>
  /** Optional teardown for handler-owned resources (v1's per-event `onClose`). */
  onStop?: () => Promise<void>
}

const queueStopTimeoutMs = 25_000

/** The held singleton is topic-erased (module state can't be generic): wave's erased holders
 * — `AnyWave` for the consuming shapes (full wave, sync wave), `AnyProducerWave` for
 * producer-only API nodes — accept every concrete wave cast-free, and the typed surface is
 * restored by the accessors below (`startQueue`'s return, `getWave<M>`). The union (rather
 * than the assignable-from-both `AnyProducerWave` alone) is what lets `'maintenance' in
 * instance` narrow back to the full-wave arm where consumption-side members live. */
let instance: AnyWave | AnyProducerWave | undefined
let stopping: Promise<void> | undefined
let pgquePool: Pool | undefined
let unobserveQueueDepth: (() => void) | undefined

// One engine per process, selected by PG_QUEUE_ADAPTER: the pg-boss instance (default) or a
// plain pg pool for the pgque adapter (which owns its schema/migrations and needs no boss).
const boss = pgQueueAdapter === 'pgboss' ? createQueueBoss() : undefined

/**
 * The queue layer is a singleton, so the app's events layer can call `queue()` from anywhere and
 * get the same instance. The first call must provide the topics and handlers; subsequent calls
 * may omit them (or provide the same values).
 * @param opts
 */
function createSyncWaveInstance<M extends TopicMap>(opts: QueueOptions<M>) {
  return createSyncWave({
    topics: opts.topics,
    handlers: opts.handlers,
    middleware: [tenantDisableEvents(), syncModeGuard()],
  })
}

function createWaveInstance<M extends TopicMap>(opts: QueueOptions<M>): AnyWave | AnyProducerWave {
  const workersEnabled = pgQueueEnableWorkers ?? true
  const { topics, handlers } = opts

  if (!pgQueueEnable) {
    return createSyncWaveInstance(opts)
  }

  // One list for every queue-enabled shape: on a producer-only wave the handlers are
  // dispatch targets (never polled) and the consume stages run around invoke dispatches,
  // so the fallback path behaves identically on API and worker nodes.
  const middleware = [tenantDisableEvents(), schedulingMetrics(), syncFallback()]

  if (pgQueueAdapter === 'pgque') {
    pgquePool = createQueuePgPool()
    // We skip migrations here, but we run them in startQueue() that's because
    // the adapter might use the pgPool constructed with the pgBouncer connection string
    // migrations can only run on a direct connection
    const pgQueAdapter = pgque(pgquePool, {
      queueConfig: { ticker_max_count: '5000', ticker_idle_period: '30 seconds' },
      skipMigrations: true,
      events: {
        // v1's `queue_job_error`: a message exhausted its retry budget and dead-lettered.
        // The verdict is the adapter's (an envelope never carries the budget, only its
        // `attempt`), so the metric rides its dead-letter event rather than middleware
        // arithmetic. Under pgboss the engine verdicts invisibly — no event, no metric.
        deadLettered: ({ topic }) => {
          queueJobError.add(1, { name: topic, adapter: 'pgque' })
        },
      },
      // Local-ticker sweep cadence; unset defers to the adapter default (100ms). Idle sweep
      // cost scales with fleet size × topics, so fleets on DBs without pg_cron may raise it
      // (aggregate cadence stays sub-second: N instances tick independently).
      tickIntervalMs: pgQueueTickIntervalMs,
      // Ticker default is 'auto': ONE database-side pg_cron runner for the whole fleet when
      // the extension is usable (fleet-scale idle sweeps were ~half the queue DB's idle CPU),
      // else every instance self-ticks as before. The resolved mode is worth a log line.
      onTickerMode: (mode) => {
        logSchema.info(logger, `[Queue] pgque ticker mode: ${mode}`, { type: 'queue' })
      },
    })

    if (!workersEnabled) {
      return createProducerWave(pgQueAdapter, { topics, handlers, middleware })
    }

    return createWave(pgQueAdapter, {
      topics,
      handlers,
      middleware,
      pollIdleIntervalMs: 5_000,
      closeTimeout: 20_000,
    })
  }

  const pgBossAdapter = pgboss({
    boss: boss!,
    schema: pgQueueSchemaV2,
    queue: queueDefaults(),
  })

  if (!workersEnabled) {
    return createProducerWave(pgBossAdapter, { topics, handlers, middleware })
  }

  return createWave(pgBossAdapter, {
    topics,
    handlers,
    middleware,
    pollIdleIntervalMs: 5_000,
    closeTimeout: 20_000,
  })
}

/**
 * Start the queue, idempotently. Three shapes, all behind the same `StorageWave` surface:
 *
 * - `PG_QUEUE_ENABLE=false` → a sync wave IS the app's wave: every produce runs its handler
 *   inline (nested sends included), no queue connection exists at all (v1 parity).
 * - queue enabled, workers enabled → full wave: producers append, workers consume, and
 *   `invoke` (the produce-failure fallback included) dispatches to the live handlers.
 * - queue enabled, workers disabled (producer-only API nodes) → producer wave: produces
 *   append, nothing polls, and the same handler list attaches as dispatch-only targets —
 *   so `invoke` and the fallback run them inline exactly as a worker node would.
 */
export async function startQueue<M extends TopicMap>(
  queueOpts: QueueOptions<M>,
  opts: { signal?: AbortSignal } = {}
): Promise<StorageWave<M>> {
  if (instance) {
    return instance as StorageWave<M>
  }
  if (opts.signal?.aborted) {
    throw ERRORS.Aborted('Cannot start queue with aborted signal')
  }

  // Both adapters provision their schema on a direct connection before the wave starts:
  // the runtime pools may sit behind a transaction pooler, which cannot run migrations.
  if (pgQueueEnable) {
    if (pgQueueAdapter === 'pgque') {
      await runPgqueMigrations()
    } else {
      await runPgBossMigrations()
    }
  }

  const wave = createWaveInstance(queueOpts)
  instance = wave

  await wave.start()

  if ('maintenance' in wave) {
    // Maintenance follows the worker role, like v1's sweeps: pg-boss supervision (expiry/
    // retention/partition upkeep — its internal timer is off, see boss.ts) plus each
    // adapter's abandoned-group reaper (on pgque an unreaped memberless group pins window
    // rotation forever). v1's 5-minute sweep cadence; `wave.close()` stops it. Producer
    // waves carry no maintenance member at all; the sync wave's is a no-op, so this
    // presence check starts real maintenance exactly where a real adapter consumes.
    wave.maintenance.start({
      intervalMs: 5 * 60_000,
      onError: (error) => {
        logSchema.error(logger, '[Queue] maintenance error', { type: 'queue', error })
      },
    })
  }

  if (pgQueueEnable && 'maintenance' in wave) {
    // Depth gauges (pending / in-flight / retry-parked / oldest age) ride `wave.stats()`:
    // one read-only backend sample per metrics collection, already guarded inside wave
    // (concurrent calls coalesce, the report is cached for its 5s TTL). Worker nodes only —
    // `stats` now lives on the publish surface too, so the full-wave discriminator
    // (`maintenance`) keeps producer-only API nodes from double-reporting the same backend
    // gauges; the env-disabled sync wave never enters here. Per-subscription rows aggregate
    // to their topic: counts sum (subscriptions hold disjoint work), ages take the max.
    unobserveQueueDepth = registerQueueDepthObserver(async () => {
      const report = await wave.stats()
      const byTopic = new Map<string, QueueDepthSample>()
      for (const sub of report.subscriptions) {
        const sample = byTopic.get(sub.topic) ?? { topic: sub.topic, adapter: pgQueueAdapter }
        if (sub.backlog) sample.pending = (sample.pending ?? 0) + sub.backlog.count
        if (sub.inFlight) sample.inFlight = (sample.inFlight ?? 0) + sub.inFlight.count
        if (sub.retryParked) {
          sample.retryParked = (sample.retryParked ?? 0) + sub.retryParked.total.count
        }
        if (sub.oldestBacklogAgeMs !== undefined) {
          sample.oldestBacklogAgeSeconds = Math.max(
            sample.oldestBacklogAgeSeconds ?? 0,
            sub.oldestBacklogAgeMs / 1000
          )
        }
        byTopic.set(sub.topic, sample)
      }
      return [...byTopic.values()]
    })
  }

  if (opts.signal) {
    opts.signal.addEventListener(
      'abort',
      () => {
        logSchema.info(logger, '[Queue] Stopping', { type: 'queue' })
        stopQueue(queueOpts.onStop)
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

  return instance as StorageWave<M>
}

/** The running wave. Producers reach it through the typed accessor in storage/events (or,
 * from inside the topic registry's own module graph, this generic accessor with a type-only
 * import of the registry's topic map — avoiding the runtime import cycle a value import would
 * create). */
export function getWave<M extends TopicMap>(): StorageWave<M> {
  if (!instance) {
    throw new Error('queue is not started (call startQueue first)')
  }
  return instance as StorageWave<M>
}

export function isQueueStarted(): boolean {
  return instance !== undefined
}

/** TEST SEAM: install a stub wave as the running instance. Test stubs are partial by nature,
 * so the widening cast lives HERE, once — callers pass plain objects with just the members
 * their test touches (`queueSize` reads the stub's own `stats`). */
export function setWaveForTesting(wave: Partial<AnyWave>): void {
  instance = wave as AnyWave
}

/** TEST SEAM: build and install the sync wave — the exact shape `startQueue` uses when the
 * queue is env-disabled — regardless of `pgQueueEnable`, so a test file that flips the flag on
 * (to exercise queue-enabled app branches) still never constructs a real pg-boss. */
export async function startSyncWaveForTesting<M extends TopicMap>(
  opts: QueueOptions<M>
): Promise<StorageWave<M>> {
  const wave = createSyncWaveInstance(opts)
  await wave.start()
  instance = wave
  return instance as StorageWave<M>
}

/** Pending jobs on a topic (v1 `Queue.getQueueSize`): the backend's sampled backlog across
 * the topic's subscriptions, via the adapter-portable `wave.stats()` — pgque and pgboss
 * alike, on every node shape (`stats` lives on the publish surface, so producer-only API
 * nodes carry it too). Throws when the queue was never started. */
export async function queueSize(topic: string): Promise<number> {
  if (!instance) {
    throw new Error('queueSize requires a started queue in this process')
  }
  const report = await instance.stats({ topics: [topic] })
  return report.subscriptions.reduce((total, sub) => total + (sub.backlog?.count ?? 0), 0)
}

/**
 * Drain and stop: wave.close() drains workers under their closeTimeout and detaches the
 * adapter (the caller-owned boss survives it), then the boss stops — gracefully in
 * production, as v1 did — the whole teardown raced against a hard 25s bound.
 */
export async function stopQueue(onStop?: () => Promise<void>): Promise<void> {
  if (!instance) return
  if (stopping) return stopping

  const { isProduction } = getConfig()
  const current = instance

  stopping = (async () => {
    try {
      // Stop sampling before the backend goes away — a collection cycle mid-teardown would
      // race the closing pool (the observer skips failed samples, but why make it).
      unobserveQueueDepth?.()
      unobserveQueueDepth = undefined
      await Promise.race([
        (async () => {
          await current.close()
          if (boss) {
            await boss.stop({ timeout: 20_000, graceful: isProduction, close: true })
          }
          if (pgquePool) {
            await pgquePool.end()
            pgquePool = undefined
          }
          await onStop?.()
        })(),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('Queue stop timeout')), queueStopTimeoutMs)
        ),
      ])
    } finally {
      instance = undefined
      stopping = undefined
    }
  })()

  return stopping
}
