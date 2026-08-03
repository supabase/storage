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
    retry: this.retryPolicy,
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
