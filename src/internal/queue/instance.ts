import { ERRORS } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import type { PgBossCtx } from '@supabase-labs/wave-adapter-pgboss'
import { pgboss } from '@supabase-labs/wave-adapter-pgboss'
import type { AnyWave, HandlerFor, TopicMap, TopicRegistry, Wave } from '@supabase-labs/wave-core'
import { createSyncWave, createWave } from '@supabase-labs/wave-core'
import type { PgBoss } from 'pg-boss'
import { getConfig } from '../../config'
import { createQueueBoss, queueDefaults } from './boss'
import { schedulingMetrics, syncFallback, syncModeGuard, tenantDisableEvents } from './middleware'

const { pgQueueEnable, pgQueueEnableWorkers, pgQueueSchemaV2 } = getConfig()

/** A storage wave: the app's typed produce/invoke surface over the pgboss adapter. */
export type StorageWave<M extends TopicMap> = Wave<M, PgBossCtx>

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

/** The held singleton is topic-erased (module state can't be generic): `AnyWave` is wave's
 * erased-holder type — every concrete `Wave<...>` assigns here cast-free (ADR-0062), and the
 * typed surface is restored by the accessors below (`startQueue`'s return, `getWave<M>`). */
let instance: AnyWave | undefined
let stopping: Promise<void> | undefined

const boss = createQueueBoss({ enableWorkers: pgQueueEnableWorkers ?? true })

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

function createWaveInstance<M extends TopicMap>(opts: QueueOptions<M>): Wave<TopicRegistry<M>> {
  const workersEnabled = pgQueueEnableWorkers ?? true
  const { topics, handlers } = opts

  if (!pgQueueEnable) {
    return createSyncWaveInstance(opts)
  }

  const pgBossAdapter = pgboss({
    boss,
    schema: pgQueueSchemaV2,
    queue: queueDefaults(),
  })

  return createWave(pgBossAdapter, {
    topics,
    handlers: workersEnabled ? handlers : [],
    middleware: [tenantDisableEvents(), schedulingMetrics(), syncFallback()],
    pollIdleIntervalMs: 5_000,
    closeTimeout: 20_000,
  })
}

/**
 * Start the queue, idempotently. Three shapes, all behind the same `Wave` interface:
 *
 * - `PG_QUEUE_ENABLE=false` → a sync wave IS the app's wave: every produce runs its handler
 *   inline (nested sends included), no queue connection exists at all (v1 parity).
 * - queue enabled, workers enabled → full wave: producers append, workers consume, and a
 *   handler-attached sync wave backs the produce-failure fallback.
 * - queue enabled, workers disabled (producer-only API nodes) → same, minus consuming: the
 *   wave gets no handlers, while the sync wave still attaches them so the fallback can run.
 */
export async function startQueue<M extends TopicMap>(
  queueOpts: QueueOptions<M>,
  opts: { signal?: AbortSignal } = {}
): Promise<StorageWave<M>> {
  const { topics, handlers } = queueOpts

  if (instance) {
    return instance as StorageWave<M>
  }
  if (opts.signal?.aborted) {
    throw ERRORS.Aborted('Cannot start queue with aborted signal')
  }

  instance = createWaveInstance({
    topics,
    handlers,
    onStop: queueOpts.onStop,
  })

  await instance.start()

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

let bossForTesting: Pick<PgBoss, 'getQueueStats'> | undefined

/** TEST SEAM: install a stub wave as the running instance, and optionally a stats backend
 * consulted by `queueSize`. Test stubs are partial by nature, so the widening cast lives
 * HERE, once — callers pass plain objects with just the members their test touches. */
export function setWaveForTesting(
  wave: Partial<AnyWave>,
  boss?: Pick<PgBoss, 'getQueueStats'>
): void {
  instance = wave as AnyWave
  bossForTesting = boss
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

/** Pending jobs on a topic's bare queue (v1 `Queue.getQueueSize`, via pg-boss v12 queue stats).
 * Requires a real queue backend — throws in env-disabled sync mode. */
export async function queueSize(topic: string): Promise<number> {
  const stats = await (bossForTesting ?? boss).getQueueStats(topic)
  return stats[0]?.queuedCount ?? 0
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
      await Promise.race([
        (async () => {
          await current.close()
          if (boss) {
            await boss.stop({ timeout: 20_000, graceful: isProduction, close: true })
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
