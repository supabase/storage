import { BaseEvent } from './base-event'
import { getTenantConfig, TenantMigrationStatus } from '@internal/database'
import { JobWithMetadata, SendOptions, WorkOptions } from 'pg-boss'
import { logger, logSchema } from '@internal/monitoring'
import { BasePayload } from '@internal/queue'
import {
  areMigrationsUpToDate,
  DBMigration,
  runMigrationsOnTenant,
  updateTenantMigrationsState,
} from '@internal/database/migrations'
import { ErrorCode, StorageBackendError } from '@internal/errors'

interface RunMigrationsPayload extends BasePayload {
  tenantId: string
  upToMigration?: keyof typeof DBMigration
}

export class RunMigrationsOnTenants extends BaseEvent<RunMigrationsPayload> {
  static queueName = 'tenants-migrations'
  static allowSync = false

  static getWorkerOptions(): WorkOptions {
    return {
      teamSize: 200,
      teamConcurrency: 10,
      includeMetadata: true,
      enforceSingletonQueueActiveLimit: true,
    }
  }

  static getQueueOptions(payload: RunMigrationsPayload): SendOptions {
    return {
      expireInHours: 48,
      singletonKey: `migrations_${payload.tenantId}`,
      singletonHours: 1,
      useSingletonQueue: true,
      retryLimit: 3,
      retryDelay: 5,
      priority: 10,
    }
  }

  static async handle(job: JobWithMetadata<RunMigrationsPayload>) {
    const tenantId = job.data.tenant.ref
    const tenant = await getTenantConfig(tenantId)

    const migrationsUpToDate = await areMigrationsUpToDate(tenantId)

    if (migrationsUpToDate) {
      return
    }

    try {
      logSchema.info(logger, `[Migrations] running for tenant ${tenantId}`, {
        type: 'migrations',
        project: tenantId,
      })
      await runMigrationsOnTenant({
        databaseUrl: tenant.databaseUrl,
        tenantId,
        waitForLock: false,
        upToMigration: job.data.upToMigration,
      })
      await updateTenantMigrationsState(tenantId, {
        migration: job.data.upToMigration,
        state: TenantMigrationStatus.COMPLETED,
      })

      logSchema.info(logger, `[Migrations] completed for tenant ${tenantId}`, {
        type: 'migrations',
        project: tenantId,
      })
    } catch (e) {
      if (e instanceof StorageBackendError && e.code === ErrorCode.LockTimeout) {
        logSchema.info(logger, `[Migrations] lock timeout for tenant ${tenantId}`, {
          type: 'migrations',
          project: tenantId,
        })
        return
      }

      logSchema.error(logger, `[Migrations] failed for tenant ${tenantId}`, {
        type: 'migrations',
        error: e,
        project: tenantId,
      })

      if (job.retrycount === job.retrylimit) {
        await updateTenantMigrationsState(tenantId, { state: TenantMigrationStatus.FAILED_STALE })
      } else {
        await updateTenantMigrationsState(tenantId, { state: TenantMigrationStatus.FAILED })
      }
      throw e
    }
  }
}
