import { deleteTenantConfig, getTenantConfig, TenantMigrationStatus } from '@internal/database'
import {
  areMigrationsUpToDate,
  DBMigration,
  runMigrationsOnTenant,
  updateTenantMigrationsState,
} from '@internal/database/migrations'
import { ErrorCode, StorageBackendError } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, WirePayload } from '@internal/queue'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { getConfig } from '../../../config'
import { DEDUP_TTL_1H, storageEvent } from '../base'
import { systemRetry, TOPICS } from '../topics'

const { pgQueueConcurrentTasksPerQueue } = getConfig()

export interface RunMigrationsPayload extends BasePayload {
  tenantId: string
  upToMigration?: keyof typeof DBMigration
}

/**
 * Worst-case wall clock for ONE migration attempt on a large tenant (~12h observed, +1h
 * headroom). Read in two places that must stay in lockstep: the handler's `consumeTimeout`
 * below, and the topic's pgboss `expireInSeconds` (topics.ts) — pgboss expiry is a hard cap
 * from `started_on` that heartbeats do NOT extend, so it must outlast a legitimate run.
 */
export const MIGRATION_MAX_RUNTIME_SECONDS = 13 * 3600

export class RunMigrationsOnTenants extends storageEvent<RunMigrationsPayload>({
  type: 'RunMigrationsOnTenants',
  idempotencyKey: (data) => `migrations_${data.tenantId}`,
  idempotencyTtlMs: DEDUP_TTL_1H,
  // v1: never runs in-process; skipped when the queue is disabled.
  allowSync: false,
}) {}

export class RunMigrationsHandler extends TopicHandler(RunMigrationsOnTenants) {
  // An instance field (not a module-level const): deferred to construction, like `options`
  // below, rather than evaluated at import time — safe regardless of import order relative to
  // `../topics`.
  private readonly retryPolicy = systemRetry(TOPICS.runMigrations)
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    parallelism: pgQueueConcurrentTasksPerQueue,
    retry: this.retryPolicy,
    // A migration may hold its delivery for hours. The batched flow goes silent for the whole
    // settle join — on pgque nothing touches the subconsumer while the job runs, so after
    // livenessTimeoutMs a sibling steals the window and the redelivery acks through the
    // LockTimeout path while the real run continues unsupervised. Streaming keeps the puller
    // (and its subconsumer touch cadence) beating around the busy lane. Also wave's default
    // for shared single-message workers — pinned here because it is load-bearing for
    // liveness, not a throughput choice.
    flow: 'streaming',
    // Short on purpose: it bounds CRASH takeover, not run length. A live worker stays alive
    // via streaming pulls (pgque) / the auto-heartbeat below (pgboss touch); a dead one is
    // taken over in minutes instead of holding its window for the length of a migration.
    livenessTimeoutMs: 10 * 60_000,
    // pgboss: touches active jobs so heartbeat-based expiry never fires on a live run.
    // pgque: advisory no-op (the adapter has no delivery-heartbeat capability).
    heartbeatIntervalMs: 60_000,
    // The hard per-attempt bound heartbeatIntervalMs requires: past it the invocation's
    // signal aborts and the message releases for redelivery (no retry budget consumed).
    consumeTimeout: MIGRATION_MAX_RUNTIME_SECONDS * 1000,
  }

  async handle(ctx: JobContext<WirePayload<RunMigrationsPayload>>): Promise<void> {
    const { data, attempt } = ctx.message
    const tenantId = data.tenant.ref
    const { sbReqId } = data
    deleteTenantConfig(tenantId)
    const tenant = await getTenantConfig(tenantId)

    const migrationsUpToDate = await areMigrationsUpToDate(tenantId)

    if (migrationsUpToDate) {
      return
    }

    try {
      logSchema.info(logger, `[Migrations] running for tenant ${tenantId}`, {
        type: 'migrations',
        project: tenantId,
        sbReqId,
      })
      await runMigrationsOnTenant({
        databaseUrl: tenant.databaseUrl,
        tenantId,
        waitForLock: false,
        upToMigration: data.upToMigration,
      })
      await updateTenantMigrationsState(tenantId, {
        migration: data.upToMigration,
        state: TenantMigrationStatus.COMPLETED,
      })

      logSchema.info(logger, `[Migrations] completed for tenant ${tenantId}`, {
        type: 'migrations',
        project: tenantId,
        sbReqId,
      })
    } catch (e) {
      if (e instanceof StorageBackendError && e.code === ErrorCode.LockTimeout) {
        logSchema.info(logger, `[Migrations] lock timeout for tenant ${tenantId}`, {
          type: 'migrations',
          project: tenantId,
          sbReqId,
        })
        return
      }

      logSchema.error(logger, `[Migrations] failed for tenant ${tenantId}`, {
        type: 'migrations',
        error: e,
        project: tenantId,
        sbReqId,
      })

      // v1: retryCount === retryLimit ⇒ FAILED_STALE. Wave's `attempt` counts deliveries
      // (attempt - 1 === retryCount), so the last budgeted delivery is maxAttempts.
      if (attempt >= this.retryPolicy.maxAttempts) {
        await updateTenantMigrationsState(tenantId, { state: TenantMigrationStatus.FAILED_STALE })
      } else {
        await updateTenantMigrationsState(tenantId, { state: TenantMigrationStatus.FAILED })
      }

      // v1 worked around the fork letting a created + retry job coexist per singleton key
      // (`deleteIfActiveExists`). v12's `exclusive` unique index spans created..active, so
      // that state is unrepresentable and the workaround is gone.
      throw e
    }
  }
}
