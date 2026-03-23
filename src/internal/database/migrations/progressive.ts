import { areMigrationsUpToDate } from '@internal/database/migrations/migrate'
import { ErrorCode, isStorageError } from '@internal/errors'
import { RunMigrationsOnTenants } from '@storage/events'
import { getConfig } from '../../../config'
import { logger, logSchema } from '../../monitoring'
import { getTenantConfig, TenantMigrationStatus } from '../tenant'

const { dbMigrationFreezeAt } = getConfig()

export class ProgressiveMigrations {
  protected tenants: string[] = []
  protected emittingJobs = false
  protected inFlightCreateJobs?: Promise<void>
  protected pendingCreateJobsMax = 0
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

  protected createJobs(maxJobs: number) {
    this.pendingCreateJobsMax = Math.max(this.pendingCreateJobsMax, maxJobs)

    if (this.inFlightCreateJobs) {
      return this.inFlightCreateJobs
    }

    this.emittingJobs = true
    this.inFlightCreateJobs = this.runCreateJobs().finally(() => {
      this.emittingJobs = false
      this.inFlightCreateJobs = undefined
      this.pendingCreateJobsMax = 0
    })

    return this.inFlightCreateJobs
  }

  protected async runCreateJobs() {
    while (this.pendingCreateJobsMax > 0) {
      const maxJobs = this.pendingCreateJobsMax
      this.pendingCreateJobsMax = 0
      await this.createJobsBatch(maxJobs)
    }
  }

  protected async createJobsBatch(maxJobs: number) {
    const tenantsBatch = this.tenants.slice(0, maxJobs)
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
          upToMigration: dbMigrationFreezeAt,
          tenant: {
            host: '',
            ref: tenant,
          },
        })
      })
    )

    const completedTenants = new Set<string>()
    const droppedTenants = new Set<string>()
    const retryableFailedTenants = new Set<string>()
    const validJobs = jobs
      .map((job, index) => {
        const tenant = tenantsBatch[index]

        if (job.status === 'rejected') {
          // If there are more terminal errors later, we need to extend this check.
          if (isStorageError(ErrorCode.TenantNotFound, job.reason)) {
            droppedTenants.add(tenant)
            logSchema.warning(
              logger,
              `[Migrations] Failed to prepare migration job for tenant ${tenant}; dropping tenant from queue because it no longer exists`,
              {
                type: 'migrations',
                error: job.reason,
                project: tenant,
                metadata: JSON.stringify({
                  strategy: 'progressive',
                }),
              }
            )
            return
          }

          retryableFailedTenants.add(tenant)
          logSchema.warning(
            logger,
            `[Migrations] Failed to prepare migration job for tenant ${tenant}; keeping tenant queued for retry`,
            {
              type: 'migrations',
              error: job.reason,
              project: tenant,
              metadata: JSON.stringify({
                strategy: 'progressive',
              }),
            }
          )
          return
        }

        completedTenants.add(tenant)
        return job.value
      })
      .filter((job) => job)

    if (validJobs.length > 0) {
      await RunMigrationsOnTenants.batchSend(validJobs as RunMigrationsOnTenants[])
    }

    if (completedTenants.size > 0 || droppedTenants.size > 0 || retryableFailedTenants.size > 0) {
      const remainingTenants = this.tenants.filter(
        (tenant) =>
          !completedTenants.has(tenant) &&
          !droppedTenants.has(tenant) &&
          !retryableFailedTenants.has(tenant)
      )
      const failedTenantsInQueue = this.tenants.filter((tenant) =>
        retryableFailedTenants.has(tenant)
      )
      this.tenants = remainingTenants.concat(failedTenantsInQueue)
    }
  }
}
