import { BaseEvent, BasePayload } from './base-event'
import { getTenantConfig, updateTenantMigrationVersion } from '../../database/tenant'
import { Job, WorkOptions } from 'pg-boss'
import { runMigrationsOnTenant } from '../../database/migrate'
import { logger, logSchema } from '../../monitoring'

interface RunMigrationsPayload extends BasePayload {
  tenantId: string
}

export class RunMigrationsOnTenants extends BaseEvent<RunMigrationsPayload> {
  static queueName = 'tenants-migrations'

  static getWorkerOptions(): WorkOptions {
    return {
      teamSize: 50,
      teamConcurrency: 4,
    }
  }

  static getQueueOptions(payload: RunMigrationsPayload) {
    return {
      singletonKey: payload.tenantId,
    }
  }

  static async handle(job: Job<BasePayload>) {
    const tenant = await getTenantConfig(job.data.tenant.ref)
    try {
      await runMigrationsOnTenant(tenant.databaseUrl, job.data.tenant.ref)
    } catch (e) {
      logSchema.error(logger, `[Migrations] failed for tenant ${job.data.tenant.ref}`, {
        type: 'migrations',
        error: e,
        project: job.data.tenant.ref,
      })
      throw e
    }
  }
}
