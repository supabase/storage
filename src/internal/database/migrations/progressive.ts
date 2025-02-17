import { logger, logSchema } from '../../monitoring'
import { getTenantConfig, TenantMigrationStatus } from '../tenant'
import { RunMigrationsOnTenants } from '@storage/events'
import { areMigrationsUpToDate } from '@internal/database/migrations/migrate'
import { getConfig } from '../../../config'
import { DBMigration } from '@internal/database/migrations/types'

const { dbMigrationFeatureFlagsEnabled } = getConfig()

export class ProgressiveMigrations {
  protected tenants: string[] = []
  protected emittingJobs = false
  protected watchInterval: NodeJS.Timeout | undefined

  constructor(protected readonly options: { maxSize: number; interval: number; watch?: boolean }) {
    if (typeof options.watch === 'undefined') {
      this.options.watch = true
    }
  }

  start(signal: AbortSignal) {
    this.watchTenants(signal)

    signal.addEventListener('abort', () => {
      if (this.watchInterval) {
        clearInterval(this.watchInterval)
        logSchema.info(logger, '[Migrations] Stopping', {
          type: 'migrations',
        })
        this.drain().catch((e) => {
          logSchema.error(logger, '[Migrations] Error creating migration jobs', {
            type: 'migrations',
            error: e,
            metadata: JSON.stringify({
              strategy: 'progressive',
            }),
          })
        })
      }
    })
  }

  async drain() {
    return this.createJobs(this.tenants.length).catch((e) => {
      logSchema.error(logger, '[Migrations] Error creating migration jobs', {
        type: 'migrations',
        error: e,
        metadata: JSON.stringify({
          strategy: 'progressive',
        }),
      })
    })
  }

  async addTenant(tenant: string, forceMigrate?: boolean) {
    const tenantIndex = this.tenants.indexOf(tenant)

    if (tenantIndex !== -1) {
      return
    }

    // check feature flags
    if (dbMigrationFeatureFlagsEnabled && !forceMigrate) {
      const { migrationFeatureFlags, migrationVersion } = await getTenantConfig(tenant)
      if (!migrationFeatureFlags || !migrationVersion) {
        return
      }

      // we only want to run migrations for tenants that have the feature flag enabled
      // a feature flag can be any migration version that is greater than the current migration version
      const migrationFeatureFlagsEnabled = migrationFeatureFlags.some(
        (flag) => DBMigration[flag as keyof typeof DBMigration] > DBMigration[migrationVersion]
      )

      if (!migrationFeatureFlagsEnabled) {
        return
      }
    }

    this.tenants.push(tenant)

    if (this.tenants.length < this.options.maxSize || this.emittingJobs) {
      return
    }

    this.createJobs(this.options.maxSize).catch((e) => {
      logSchema.error(logger, '[Migrations] Error creating migration jobs', {
        type: 'migrations',
        error: e,
        metadata: JSON.stringify({
          strategy: 'progressive',
        }),
      })
    })
  }

  protected watchTenants(signal: AbortSignal) {
    if (signal.aborted || !this.options.watch) {
      return
    }
    this.watchInterval = setInterval(() => {
      if (this.emittingJobs) {
        return
      }

      this.createJobs(this.options.maxSize).catch((e) => {
        logSchema.error(logger, '[Migrations] Error creating migration jobs', {
          type: 'migrations',
          error: e,
          metadata: JSON.stringify({
            strategy: 'progressive',
          }),
        })
      })
    }, this.options.interval)
  }

  protected async createJobs(maxJobs: number) {
    this.emittingJobs = true
    try {
      const tenantsBatch = this.tenants.splice(0, maxJobs)
      const jobs = await Promise.allSettled(
        tenantsBatch.map(async (tenant) => {
          const tenantConfig = await getTenantConfig(tenant)
          const migrationsUpToDate = await areMigrationsUpToDate(tenant)

          if (migrationsUpToDate || tenantConfig.syncMigrationsDone) {
            return
          }

          const scheduleAt = new Date()
          scheduleAt.setMinutes(scheduleAt.getMinutes() + 5)
          const scheduleForLater =
            tenantConfig.migrationStatus === TenantMigrationStatus.FAILED_STALE
              ? scheduleAt
              : undefined

          return new RunMigrationsOnTenants({
            tenantId: tenant,
            scheduleAt: scheduleForLater,
            tenant: {
              host: '',
              ref: tenant,
            },
          })
        })
      )

      const validJobs = jobs
        .map((job) => {
          if (job.status === 'fulfilled' && job.value) {
            return job.value
          }
        })
        .filter((job) => job)

      await RunMigrationsOnTenants.batchSend(validJobs as RunMigrationsOnTenants[])
    } finally {
      this.emittingJobs = false
    }
  }
}
