import { BaseEvent } from './base-event'
import { getTenantConfig, TenantMigrationStatus } from '@internal/database'
import { JobWithMetadata, SendOptions, WorkOptions } from 'pg-boss'
import { logger, logSchema } from '@internal/monitoring'
import { BasePayload } from '@internal/queue'
import {
  areMigrationsUpToDate,
  runMigrationsOnTenant,
  updateTenantMigrationsState,
} from '@internal/database/migrations'
import { ErrorCode, StorageBackendError } from '@internal/errors'

interface RunMigrationsPayload extends BasePayload {
  tenantId: string
}

export class RunMigrationsOnTenants extends BaseEvent<RunMigrationsPayload> {
  static queueName = 'tenants-migrations'

  static getWorkerOptions(): WorkOptions {
    return {
      teamSize: 200,
      enforceSingletonQueueActiveLimit: true,
      teamConcurrency: 10,
      includeMetadata: true,
    }
  }

  static getQueueOptions(payload: RunMigrationsPayload): SendOptions {
    return {
      expireInHours: 2,
      singletonKey: payload.tenantId,
      useSingletonQueue: true,
      retryLimit: 3,
      retryDelay: 5,
      priority: 10,
    }
  }

  static async handle(job: JobWithMetadata<BasePayload>) {
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
      await runMigrationsOnTenant(tenant.databaseUrl, tenantId, false)
      await updateTenantMigrationsState(tenantId)

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
