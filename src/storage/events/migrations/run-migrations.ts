import { deleteTenantConfig, getTenantConfig, TenantMigrationStatus } from '@internal/database'
import {
  areMigrationsUpToDate,
  cacheTenantMigration,
  completeTenantMigrations,
  DBMigration,
  failTenantMigrations,
  isDBMigrationName,
  readTenantMigrationVersion,
  runMigrationsOnTenant,
} from '@internal/database/migrations'
import { ErrorCode, StorageBackendError } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import { BasePayload } from '@internal/queue'
import { JobWithMetadata, Queue, SendOptions, WorkOptions } from 'pg-boss'
import { BaseEvent } from '../base-event'

interface RunMigrationsPayload extends BasePayload {
  tenantId: string
  upToMigration?: keyof typeof DBMigration
}

export class RunMigrationsOnTenants extends BaseEvent<RunMigrationsPayload> {
  static queueName = 'tenants-migrations-v2'
  static allowSync = false

  static getQueueOptions(): Queue {
    return {
      name: this.queueName,
      policy: 'exactly_once',
    } as const
  }

  static getWorkerOptions(): WorkOptions {
    return {
      includeMetadata: true,
    }
  }

  static getSendOptions(payload: RunMigrationsPayload): SendOptions {
    return {
      singletonKey: `migrations_${payload.tenantId}`,
      singletonHours: 1,
      expireInMinutes: 10,
      retryLimit: 3,
      retryDelay: 5,
      priority: 10,
    }
  }

  static async handle(job: JobWithMetadata<RunMigrationsPayload>) {
    const tenantId = job.data.tenant.ref
    const { sbReqId } = job.data
    deleteTenantConfig(tenantId)
    const tenant = await getTenantConfig(tenantId)
    const expected = {
      expectedMigrationVersion: tenant.migrationVersion ?? null,
      expectedDatabaseUrl: tenant.databaseUrlEncrypted,
    }
    const expectedMigrationStatus = tenant.migrationStatus ?? null
    let migrationStarted = false

    try {
      let migrationsUpToDate = await areMigrationsUpToDate(tenantId)

      if (
        migrationsUpToDate &&
        isDBMigrationName(tenant.migrationVersion) &&
        tenant.migrationStatus === TenantMigrationStatus.COMPLETED
      ) {
        // A queued repair must verify the physical schema before trusting completed metadata.
        const physicalMigration = await readTenantMigrationVersion({
          tenantId,
          databaseUrl: tenant.databaseUrl,
        })
        cacheTenantMigration(tenant, physicalMigration)
        migrationsUpToDate = await areMigrationsUpToDate(tenantId)
      }

      if (migrationsUpToDate) {
        return
      }

      logSchema.info(logger, `[Migrations] running for tenant ${tenantId}`, {
        type: 'migrations',
        project: tenantId,
        sbReqId,
      })
      migrationStarted = true
      const physicalMigration = await runMigrationsOnTenant({
        databaseUrl: tenant.databaseUrl,
        tenantId,
        waitForLock: false,
        upToMigration: job.data.upToMigration,
      })
      const updated = await completeTenantMigrations(tenantId, {
        ...expected,
        migration: physicalMigration,
      })
      if (updated === 0) {
        logSchema.warning(logger, `[Migrations] completion skipped for tenant ${tenantId}`, {
          type: 'migrations',
          project: tenantId,
          sbReqId,
          metadata: JSON.stringify({ physicalMigration }),
        })
        return
      }

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

      // A failed preflight read does not mean the tenant's migrations failed.
      if (migrationStarted) {
        await failTenantMigrations(tenantId, {
          ...expected,
          expectedMigrationStatus,
          state:
            job.retryCount === job.retryLimit
              ? TenantMigrationStatus.FAILED_STALE
              : TenantMigrationStatus.FAILED,
        })
      }

      try {
        // get around pg-boss not allowing to have a stately queue in a state
        // where there is a job in created state and retry state
        const singletonKey = job.singletonKey || ''
        await this.deleteIfActiveExists(this.getQueueName(), singletonKey, job.id)
      } catch (e) {
        logSchema.error(logger, `[Migrations] Error deleting job ${job.id}`, {
          type: 'migrations',
          error: e,
          project: tenantId,
          sbReqId,
        })
        return
      }

      throw e
    } finally {
      deleteTenantConfig(tenantId)
    }
  }
}
