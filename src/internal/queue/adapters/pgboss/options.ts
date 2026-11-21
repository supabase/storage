import type { ConstructorOptions, PgBoss, Queue, QueuePolicy } from 'pg-boss'
import type { Codec } from '../../manager/codec'
import type { AdapterEvents } from '../../manager/events'

/** Per-queue config the adapter writes onto every queue it creates (before per-queue options). */
export type QueueDefaults = Omit<Queue, 'name' | 'policy' | 'partition'>

/**
 * The `pgboss` namespace of a queue's options, written onto the queue at every provision.
 * `policy` alone is immutable after the first creation: provisioning reads the queue back and THROWS on a
 * declared-vs-actual mismatch, so config drift is loud.
 *
 * `key_strict_fifo` is rejected (queues here are unordered). The drop-semantics policies
 * (`exclusive`/`short`/`singleton`/`stately`) are an explicit opt-in: the engine silently skips
 * enqueues that conflict on `(queue, singleton_key)`. `exclusive` additionally REQUIRES every
 * send to carry an `idempotencyKey`: keyless jobs all collide on the empty key, so a keyless
 * send throws instead of silently serializing the whole queue.
 */
export interface PgBossQueueExtension extends QueueDefaults {
  readonly policy?: QueuePolicy
}

declare module '../../manager/registry' {
  interface QueueOptions {
    /** pg-boss queue options (see {@link PgBossQueueExtension}). */
    readonly pgboss?: PgBossQueueExtension
  }
}

export interface PgBossOptions<W = unknown> {
  /**
   * A caller-owned PgBoss instance, shareable with the app's own pg-boss usage. `close()`
   * does NOT stop it and its supervision config stays the caller's. Exactly one of
   * `boss` / `connectionString` is required.
   */
  readonly boss?: PgBoss
  /**
   * Adapter-owned construction: the adapter creates a PgBoss with its OWN supervision on
   * (expiry sweeps and retention run on pg-boss's scheduler, so crash redelivery never
   * depends on the manager's maintenance) and `close()` stops it. Cron scheduling stays off.
   */
  readonly connectionString?: string
  /** Extra constructor options for the owned path (schema, intervals, migrate, …). */
  readonly bossOptions?: Omit<ConstructorOptions, 'connectionString' | 'supervise' | 'schedule'>
  /**
   * The pg-boss schema, used by the statements the public API lacks (stats, and `release`).
   * Must match the instance's schema. Default: the owned path's `bossOptions.schema`, else
   * `"pgboss"`.
   */
  readonly schema?: string
  /** Payload codec. Defaults to structured JSON (stored as `jsonb`). */
  readonly codec?: Codec<W>
  /**
   * Defaults for every queue the adapter creates (retention, deletion, expiration, …). A
   * queue's own `pgboss` options win over them, and its declared `retry` and
   * `livenessTimeoutMs` win over both.
   */
  readonly queue?: QueueDefaults
  /**
   * Owned path only: errors pg-boss's own supervisor emits are reported as
   * `maintenanceError({ job: 'supervise' })`, once per emitted error (a caller-owned instance
   * keeps its own `error` listener). `deadLettered` never fires: pg-boss issues that verdict
   * natively.
   */
  readonly events?: AdapterEvents
}

/**
 * The retry posture every created queue starts from, over the caller's defaults: a failed job
 * redelivers at once (`retryDelay 0`), and a handler with no declared budget still stops
 * after 10 redeliveries. A queue's `pgboss` options and its declared `retry` override it.
 */
export const withRetryDefaults = (queueDefaults: QueueDefaults): QueueDefaults => ({
  ...queueDefaults,
  retryLimit: 10,
  retryDelay: 0,
  retryBackoff: false,
})

/** Policies no queue here can carry. */
export const REJECTED_POLICIES: readonly string[] = ['key_strict_fifo']
