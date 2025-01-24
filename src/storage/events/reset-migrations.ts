import { BaseEvent } from './base-event'
import { getTenantConfig } from '@internal/database'
import { JobWithMetadata, SendOptions, WorkOptions } from 'pg-boss'
import { BasePayload } from '@internal/queue'
import { DBMigration, resetMigration } from '@internal/database/migrations'
import { RunMigrationsOnTenants } from './run-migrations'
import { logger, logSchema } from '@internal/monitoring'

interface ResetMigrationsPayload extends BasePayload {
  tenantId: string
  untilMigration: keyof typeof DBMigration
  markCompletedTillMigration?: keyof typeof DBMigration
}

export class ResetMigrationsOnTenant extends BaseEvent<ResetMigrationsPayload> {
  static queueName = 'tenants-migrations-reset'

  static getWorkerOptions(): WorkOptions {
    return {
      enforceSingletonQueueActiveLimit: true,
      teamSize: 200,
      teamConcurrency: 10,
      includeMetadata: true,
    }
  }

  static getQueueOptions(payload: ResetMigrationsPayload): SendOptions {
    return {
      expireInHours: 2,
      singletonKey: payload.tenantId,
      useSingletonQueue: true,
      retryLimit: 3,
      retryDelay: 5,
      priority: 10,
    }
  }

  static async handle(job: JobWithMetadata<ResetMigrationsPayload>) {
    const tenantId = job.data.tenant.ref
    const tenant = await getTenantConfig(tenantId)

    logSchema.info(logger, `[Migrations] resetting migrations for ${tenantId}`, {
      type: 'migrations',
      project: tenantId,
    })

    const reset = await resetMigration({
      tenantId: tenantId,
      markCompletedTillMigration: job.data.markCompletedTillMigration,
      untilMigration: job.data.untilMigration,
      databaseUrl: tenant.databaseUrl,
    })

    if (reset) {
      await RunMigrationsOnTenants.send({
        tenantId: tenantId,
        tenant: {
          ref: tenantId,
        },
        singletonKey: tenantId,
      })
    }

    logSchema.info(logger, `[Migrations] reset successful for ${tenantId}`, {
      type: 'migrations',
      project: tenantId,
    })
  }
}
