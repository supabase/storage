import { BaseEvent, BasePayload } from './base-event'
import { getTenantConfig } from '../../database/tenant'
import { BatchWorkOptions, Job } from 'pg-boss'
import { runMigrationsOnTenant } from '../../database/migrate'
import { knex } from '../../database/multitenant-db'
import { getConfig } from '../../config'

const { dbMigrationHash } = getConfig()

interface RunMigrationsPayload extends BasePayload {
  tenantId: string
}

export class RunMigrationsEvent extends BaseEvent<RunMigrationsPayload> {
  static queueName = 'tenants-migrations'

  static getWorkerOptions(): BatchWorkOptions {
    return {
      batchSize: 100,
      newJobCheckIntervalSeconds: 20,
    }
  }

  static async handle(jobs: Job<BasePayload>[]) {
    const migrations = jobs.map(async (job) => {
      const tenant = await getTenantConfig(job.data.tenant.ref)
      await runMigrationsOnTenant(tenant.databaseUrl)
      return job.data.tenant.ref
    })

    const results = await Promise.allSettled(migrations)
    const successfulTenants = results
      .filter((result) => result.status === 'fulfilled')
      .map((result) => (result as PromiseFulfilledResult<string>).value)
  }

  singletonKey(payload: BasePayload) {
    return payload.tenant.ref
  }
}
