import { logger, logSchema } from '../../monitoring'
import { getTenantConfig, TenantMigrationStatus } from '../tenant'
import { RunMigrationsOnTenants } from '@storage/events'
import { areMigrationsUpToDate } from '@internal/database/migrations/migrate'

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

  addTenant(tenant: string) {
    const tenantIndex = this.tenants.indexOf(tenant)

    if (tenantIndex !== -1) {
      return
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
    const tenantsBatch = this.tenants.splice(0, maxJobs)
    const jobs = await Promise.allSettled(
      tenantsBatch.map(async (tenant) => {
        const tenantConfig = await getTenantConfig(tenant)
        const migrationsUpToDate = await areMigrationsUpToDate(tenant)

        if (
          migrationsUpToDate ||
          tenantConfig.syncMigrationsDone ||
          tenantConfig.migrationStatus === TenantMigrationStatus.FAILED_STALE
        ) {
          return
        }

        return new RunMigrationsOnTenants({
          tenantId: tenant,
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
    this.emittingJobs = false
  }
}
