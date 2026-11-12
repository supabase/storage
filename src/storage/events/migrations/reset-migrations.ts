import { getTenantConfig } from '@internal/database'
import { DBMigration, isDBMigrationName, resetMigration } from '@internal/database/migrations'
import { ERRORS } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, StorageJob } from '@internal/queue'
import type { SubscribeOptions } from '@internal/queue/manager'
import { QueueHandler } from '@internal/queue/manager'
import { getConfig } from '../../../config'
import { storageEvent } from '../base'
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
  // A fleet-wide fan-out: never gated per tenant, so sending one loads no tenant config.
  disableKeys: () => [],
}) {}

export class ResetMigrationsHandler extends QueueHandler(ResetMigrationsOnTenant) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    parallelism: pgQueueConcurrentTasksPerQueue,
  }

  async handle(ctx: StorageJob<ResetMigrationsPayload>): Promise<void> {
    const { data } = ctx.message
    const tenantId = data.tenant.ref
    const { sbReqId, untilMigration, markCompletedTillMigration } = data

    // During a rollout a worker on an older release can pick up a reset from a newer one.
    // An unknown target would record a rewind that never happened, so fail without touching
    // tenant state and let a retry land on a worker that knows the migration.
    const unknownMigration = [untilMigration, markCompletedTillMigration].find(
      (migration) => migration !== undefined && !isDBMigrationName(migration)
    )
    if (unknownMigration !== undefined) {
      logSchema.warning(logger, `[Migrations] reset job targets an unknown migration, retrying`, {
        type: 'migrations',
        project: tenantId,
        sbReqId,
        metadata: JSON.stringify({ untilMigration, markCompletedTillMigration }),
      })
      throw ERRORS.InternalError(
        undefined,
        `Migration ${unknownMigration} is unknown to this release`
      )
    }

    const tenant = await getTenantConfig(tenantId)

    logSchema.info(logger, `[Migrations] resetting migrations for ${tenantId}`, {
      type: 'migrations',
      project: tenantId,
      sbReqId,
    })

    const reset = await resetMigration({
      tenantId,
      markCompletedTillMigration,
      untilMigration,
      databaseUrl: tenant.databaseUrl,
    })

    if (reset) {
      await RunMigrationsOnTenants.send({
        tenantId,
        tenant: {
          ref: tenantId,
          host: '',
        },
        sbReqId,
      })
    }

    logSchema.info(logger, `[Migrations] reset successful for ${tenantId}`, {
      type: 'migrations',
      project: tenantId,
      sbReqId,
    })
  }
}
