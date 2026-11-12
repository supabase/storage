import { deleteTenantConfig, getTenantConfig, TenantMigrationStatus } from '@internal/database'
import {
  areMigrationsUpToDate,
  DBMigration,
  isDBMigrationName,
  runMigrationsOnTenant,
  updateTenantMigrationsState,
} from '@internal/database/migrations'
import { ERRORS, ErrorCode, StorageBackendError } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, StorageJob } from '@internal/queue'
import type { SubscribeOptions } from '@internal/queue/manager'
import { QueueHandler } from '@internal/queue/manager'
import { getConfig } from '../../../config'
import { DEDUP_TTL_1H, storageEvent } from '../base'
import { QUEUES, systemRetry } from '../registry'

const { pgQueueConcurrentTasksPerQueue } = getConfig()

export interface RunMigrationsPayload extends BasePayload {
  tenantId: string
  upToMigration?: keyof typeof DBMigration
}

/**
 * Worst-case wall clock for ONE migration attempt on a large tenant (~12h observed, +1h
 * headroom). Read in two places that must stay in lockstep: the handler's `consumeTimeout`
 * below, and the queue's pgboss `expireInSeconds` (registry.ts) — pgboss expiry is a hard cap
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

export class RunMigrationsHandler extends QueueHandler(RunMigrationsOnTenants) {
  // An instance field (not a module-level const): deferred to construction, like `options`
  // below, rather than evaluated at import time — safe regardless of import order relative to
  // `../registry`.
  private readonly retryPolicy = systemRetry(QUEUES.runMigrations)
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    parallelism: pgQueueConcurrentTasksPerQueue,
    // A migration may hold its delivery for hours. The batched flow would gate every other
    // tenant's settle and the next pull behind the slowest run in its batch; streaming keeps
    // pulling around the busy lane. Also the default for single-message workers, pinned
    // here because it is load-bearing, not a throughput choice.
    flow: 'streaming',
    // Renews the lease of active jobs (pg-boss touch, SQS visibility) so the queue's short
    // liveness window (registry.ts: it bounds crash takeover, not run length) never fires
    // on a live run.
    heartbeatIntervalMs: 60_000,
    // The hard per-attempt bound heartbeatIntervalMs requires: past it the invocation's
    // signal aborts and the message releases for redelivery (no retry budget consumed).
    consumeTimeout: MIGRATION_MAX_RUNTIME_SECONDS * 1000,
  }

  async handle(ctx: StorageJob<RunMigrationsPayload>): Promise<void> {
    const { data, attempt } = ctx.message
    const tenantId = data.tenant.ref
    const { sbReqId, upToMigration } = data

    // During a rollout a worker on an older release can pick up a job from a newer one.
    // Fail it without touching tenant state so a retry can land on a worker that knows the migration.
    if (upToMigration !== undefined && !isDBMigrationName(upToMigration)) {
      logSchema.warning(logger, `[Migrations] job targets an unknown migration, retrying`, {
        type: 'migrations',
        project: tenantId,
        sbReqId,
        metadata: JSON.stringify({ upToMigration }),
      })
      throw ERRORS.InternalError(undefined, `Migration ${upToMigration} is unknown to this release`)
    }

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

      // v1: retryCount === retryLimit ⇒ FAILED_STALE. A message's `attempt` counts deliveries
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
