import { BaseEvent } from './base-event'
import {
  areMigrationsUpToDate,
  getTenantConfig,
  TenantMigrationStatus,
  updateTenantMigrationsState,
  runMigrationsOnTenant,
} from '@internal/database'
import { JobWithMetadata, SendOptions, WorkOptions } from 'pg-boss'
import { logger, logSchema } from '@internal/monitoring'
import { BasePayload } from '@internal/queue'

interface RunMigrationsPayload extends BasePayload {
  tenantId: string
}

export class RunMigrationsOnTenants extends BaseEvent<RunMigrationsPayload> {
  static queueName = 'tenants-migrations'

  static getWorkerOptions(): WorkOptions {
    return {
      teamSize: 200,
      teamConcurrency: 10,
      includeMetadata: true,
    }
  }

  static getQueueOptions(payload: RunMigrationsPayload): SendOptions {
    return {
      singletonKey: payload.tenantId,
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
      await runMigrationsOnTenant(tenant.databaseUrl, tenantId)
      await updateTenantMigrationsState(tenantId)

      logSchema.info(logger, `[Migrations] completed for tenant ${tenantId}`, {
        type: 'migrations',
        project: tenantId,
      })
    } catch (e) {
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
