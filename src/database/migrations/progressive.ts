import { logger, logSchema } from '../../monitoring'
import { areMigrationsUpToDate, getTenantConfig, TenantMigrationStatus } from '../tenant'
import { RunMigrationsOnTenants } from '../../queue'

export class ProgressiveMigrations {
  protected tenants: string[] = []
  protected emittingJobs = false
  protected watchInterval: NodeJS.Timer | undefined

  constructor(protected readonly options: { maxSize: number; interval: number }) {}

  start(signal: AbortSignal) {
    this.watch(signal)

    signal.addEventListener('abort', () => {
      if (this.watchInterval) {
        clearInterval(this.watchInterval)
      }
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

    this.createJobs().catch((e) => {
      logSchema.error(logger, '[Migrations] Error creating migration jobs', {
        type: 'migrations',
        error: e,
        metadata: JSON.stringify({
          strategy: 'progressive',
        }),
      })
    })
  }

  protected watch(signal: AbortSignal) {
    if (signal.aborted) {
      return
    }
    this.watchInterval = setInterval(() => {
      if (this.emittingJobs) {
        return
      }

      this.createJobs().catch((e) => {
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

  protected async createJobs() {
    this.emittingJobs = true
    const tenantsBatch = this.tenants.splice(0, this.options.maxSize)
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
