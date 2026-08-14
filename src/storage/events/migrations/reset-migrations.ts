import { getTenantConfig } from '@internal/database'
import { DBMigration, resetMigration } from '@internal/database/migrations'
import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, WirePayload } from '@internal/queue'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { getConfig } from '../../../config'
import { storageEvent } from '../base'
import { getStorageQueue } from '../queue'
import { systemRetry, TOPICS } from '../topics'
import { RunMigrationsOnTenants } from './run-migrations'

const { pgQueueConcurrentTasksPerQueue } = getConfig()

export interface ResetMigrationsPayload extends BasePayload {
  tenantId: string
  untilMigration: keyof typeof DBMigration
  markCompletedTillMigration?: keyof typeof DBMigration
}

export class ResetMigrationsOnTenant extends storageEvent<ResetMigrationsPayload>({
  type: 'ResetMigrationsOnTenant',
  idempotencyKey: (data) => data.tenantId,
}) {}

export class ResetMigrationsHandler extends TopicHandler(ResetMigrationsOnTenant) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    parallelism: pgQueueConcurrentTasksPerQueue,
    retry: systemRetry(TOPICS.resetMigrations),
  }

  async handle(ctx: JobContext<WirePayload<ResetMigrationsPayload>>): Promise<void> {
    const { data } = ctx.message
    const tenantId = data.tenant.ref
    const { sbReqId } = data
    const tenant = await getTenantConfig(tenantId)

    logSchema.info(logger, `[Migrations] resetting migrations for ${tenantId}`, {
      type: 'migrations',
      project: tenantId,
      sbReqId,
    })

    const reset = await resetMigration({
      tenantId,
      markCompletedTillMigration: data.markCompletedTillMigration,
      untilMigration: data.untilMigration,
      databaseUrl: tenant.databaseUrl,
    })

    if (reset) {
      await getStorageQueue().produce(
        new RunMigrationsOnTenants({
          tenantId,
          tenant: {
            ref: tenantId,
            host: '',
          },
          sbReqId,
        })
      )
    }

    logSchema.info(logger, `[Migrations] reset successful for ${tenantId}`, {
      type: 'migrations',
      project: tenantId,
      sbReqId,
    })
  }
}
