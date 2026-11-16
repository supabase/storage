import { setTimeout as delay } from 'node:timers/promises'
import { createLruCache, PROGRESSIVE_MIGRATION_SUBMISSIONS_CACHE_NAME } from '@internal/cache'
import { abortDeadline } from '@internal/concurrency'
import { areMigrationsUpToDate } from '@internal/database/migrations/migrate'
import { ErrorCode, isStorageError } from '@internal/errors'
import { QueueBatchBusyError } from '@internal/queue/errors'
import { RunMigrationsOnTenants } from '@storage/events'
import { getConfig } from '../../../config'
import { logger, logSchema } from '../../monitoring'
import { getTenantConfig, TenantMigrationStatus } from '../tenant'

const { dbMigrationFreezeAt } = getConfig()
const maxBatchSendBackoffMs = 60_000
const progressiveMigrationsMetadata = JSON.stringify({ strategy: 'progressive' })

export class ProgressiveMigrations {
  protected readonly tenants = new Set<string>()
  protected readonly recentSubmissions = createLruCache<string, boolean>(
    PROGRESSIVE_MIGRATION_SUBMISSIONS_CACHE_NAME,
    {
      max: getConfig().caches.progressiveMigrationSubmissions.maxEntries,
      ttlResolution: 0,
      perf: { now: () => this.now() },
    }
  )
  private readonly submissionCooldownMs = getConfig().caches.progressiveMigrationSubmissions.ttlMs
  private readonly maxPendingTenants = getConfig().caches.progressiveMigrationSubmissions.maxEntries
  protected inFlightCreateJobs?: Promise<void>
  protected consecutiveBatchSendFailures = 0
  private lastBatchContended = false
  protected nextBatchSendAttemptAt = 0
  protected batchSendTimer: NodeJS.Timeout | undefined
  private shutdownSignal: AbortSignal | undefined

  constructor(protected readonly options: { maxSize: number; interval: number }) {}

  start(signal: AbortSignal) {
    this.shutdownSignal = signal

    signal.addEventListener('abort', () => {
      logSchema.info(logger, '[Migrations] Stopping', {
        type: 'migrations',
      })
      return this.drain().finally(() => this.recentSubmissions.dispose())
    })
  }

  async drain() {
    const deadline = this.shutdownSignal?.aborted
      ? Math.min(Date.now() + 5_000, abortDeadline(this.shutdownSignal) ?? Infinity)
      : Infinity
    await this.inFlightCreateJobs?.catch(() => {})
    this.resetBatchSendBackoff()
    let remaining = this.tenants.size
    while (remaining > 0 && Date.now() < deadline) {
      await this.createJobsAndLogErrors(Math.min(remaining, this.options.maxSize))
      if (!this.isBatchSendBackoffActive()) {
        remaining -= this.options.maxSize
      } else if (!this.lastBatchContended || deadline === Infinity) {
        break
      } else {
        this.resetBatchSendBackoff()
        await delay(Math.min(50 + Math.random() * 200, Math.max(deadline - Date.now(), 0)))
      }
    }
    if (this.shutdownSignal?.aborted && this.tenants.size > 0) {
      logSchema.warning(logger, '[Migrations] Pending migration submissions remain at shutdown', {
        type: 'migrations',
        metadata: JSON.stringify({ strategy: 'progressive', pendingTenants: this.tenants.size }),
      })
    }
  }

  addTenant(tenant: string, options?: { force?: boolean }) {
    if (options?.force) {
      this.recentSubmissions.delete(tenant)
    } else if (this.recentSubmissions.get(tenant)) {
      return
    }

    if (
      this.tenants.has(tenant) ||
      (!options?.force && this.tenants.size >= this.maxPendingTenants)
    ) {
      return
    }

    this.tenants.add(tenant)

    if (this.inFlightCreateJobs) {
      return
    }

    this.scheduleNextBatch()
  }

  protected createJobs(maxJobs: number) {
    if (this.inFlightCreateJobs) {
      return this.inFlightCreateJobs
    }

    this.inFlightCreateJobs = this.createJobsBatch(maxJobs).finally(() => {
      this.inFlightCreateJobs = undefined
    })

    return this.inFlightCreateJobs
  }

  private createJobsAndLogErrors(maxJobs: number) {
    return this.createJobs(maxJobs).catch((e) => {
      logSchema.error(logger, '[Migrations] Error creating migration jobs', {
        type: 'migrations',
        error: e,
        metadata: progressiveMigrationsMetadata,
      })
      this.scheduleNextBatch()
    })
  }

  protected async createJobsBatch(maxJobs: number) {
    this.lastBatchContended = false
    if (this.isBatchSendBackoffActive()) {
      return
    }

    const tenantsBatch: string[] = []
    for (const tenant of this.tenants) {
      if (tenantsBatch.length >= maxJobs) {
        break
      }
      tenantsBatch.push(tenant)
    }

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

    const { retryableFailedTenants, sendableJobs } = this.classifyPreparedJobs(tenantsBatch, jobs)

    if (sendableJobs.length > 0) {
      try {
        // Match the fleet sender's tenant order to reduce competing insert deadlocks.
        sendableJobs.sort((a, b) => (a.tenant < b.tenant ? -1 : a.tenant > b.tenant ? 1 : 0))
        const submission = RunMigrationsOnTenants.batchSend(sendableJobs.map(({ job }) => job))
        await submission
        if (submission) {
          // Suppress repeated queue inserts from this process after a successful batch.
          // Other processes still coordinate outstanding work through the queue policy.
          for (const { tenant } of sendableJobs) {
            this.recentSubmissions.set(tenant, true, {
              ttl: this.submissionCooldownMs * (1 + this.random()),
            })
          }
        }
        this.resetBatchSendBackoff()
      } catch (e) {
        for (const { tenant } of sendableJobs) {
          retryableFailedTenants.add(tenant)
        }
        const retryDelayMs = this.deferBatchSendRetry()
        const busy = e instanceof QueueBatchBusyError
        this.lastBatchContended = busy
        const log = busy
          ? this.consecutiveBatchSendFailures >= 3
            ? logSchema.warning
            : logSchema.info
          : logSchema.error
        log(
          logger,
          busy
            ? '[Migrations] Deferring migration jobs batch; another producer is active'
            : '[Migrations] Error sending migration jobs batch',
          {
            type: 'migrations',
            ...(busy ? {} : { error: e }),
            metadata: JSON.stringify({
              strategy: 'progressive',
              consecutiveFailures: this.consecutiveBatchSendFailures,
              retryDelayMs,
            }),
          }
        )
      }
    }

    this.reconcileTenants(tenantsBatch, retryableFailedTenants)

    if (this.tenants.size === 0) {
      this.resetBatchSendBackoff()
    } else {
      this.scheduleNextBatch()
    }
  }

  private classifyPreparedJobs(
    tenantsBatch: string[],
    jobs: PromiseSettledResult<RunMigrationsOnTenants | undefined>[]
  ) {
    const retryableFailedTenants = new Set<string>()
    const sendableJobs: Array<{ tenant: string; job: RunMigrationsOnTenants }> = []

    for (const [index, job] of jobs.entries()) {
      const tenant = tenantsBatch[index]

      if (job.status === 'rejected') {
        const dropTenant = isStorageError(ErrorCode.TenantNotFound, job.reason)
        if (!dropTenant) {
          retryableFailedTenants.add(tenant)
        }
        logSchema.warning(
          logger,
          dropTenant
            ? `[Migrations] Failed to prepare migration job for tenant ${tenant}; dropping tenant from queue because it no longer exists`
            : `[Migrations] Failed to prepare migration job for tenant ${tenant}; keeping tenant queued for retry`,
          {
            type: 'migrations',
            error: job.reason,
            project: tenant,
            metadata: progressiveMigrationsMetadata,
          }
        )
        continue
      }

      if (job.value) {
        sendableJobs.push({ tenant, job: job.value })
      }
    }

    return { retryableFailedTenants, sendableJobs }
  }

  private reconcileTenants(tenantsBatch: string[], retryableFailedTenants: Set<string>) {
    for (const tenant of tenantsBatch) {
      this.tenants.delete(tenant)
      if (retryableFailedTenants.has(tenant)) {
        this.tenants.add(tenant)
      }
    }
  }

  protected now() {
    return performance.now()
  }

  protected random() {
    return Math.random()
  }

  private get baseDelayMs() {
    return Math.max(this.options.interval, 1)
  }

  private isBatchSendBackoffActive() {
    return this.now() < this.nextBatchSendAttemptAt
  }

  private deferBatchSendRetry() {
    this.consecutiveBatchSendFailures++

    const maximumDelayMs = Math.max(this.baseDelayMs, maxBatchSendBackoffMs)
    const upperDelayMs = Math.min(
      maximumDelayMs,
      this.baseDelayMs * 2 ** this.consecutiveBatchSendFailures
    )
    const lowerDelayMs = Math.max(this.baseDelayMs, Math.floor(upperDelayMs / 2))
    const retryDelayMs = Math.round(lowerDelayMs + (upperDelayMs - lowerDelayMs) * this.random())

    this.nextBatchSendAttemptAt = this.now() + retryDelayMs
    return retryDelayMs
  }

  private scheduleNextBatch() {
    if (
      this.tenants.size === 0 ||
      !this.shutdownSignal ||
      this.shutdownSignal.aborted ||
      this.batchSendTimer
    ) {
      return
    }

    const delayMs = this.isBatchSendBackoffActive()
      ? Math.max(this.nextBatchSendAttemptAt - this.now(), 1)
      : this.baseDelayMs

    const timer = setTimeout(() => {
      this.clearBatchSendSchedule()
      void this.createJobsAndLogErrors(this.options.maxSize)
    }, delayMs)
    this.batchSendTimer = timer
    timer.unref()
  }

  private clearBatchSendSchedule() {
    this.cancelBatchSendTimer()
    this.nextBatchSendAttemptAt = 0
  }

  private cancelBatchSendTimer() {
    if (this.batchSendTimer) {
      clearTimeout(this.batchSendTimer)
      this.batchSendTimer = undefined
    }
  }

  protected resetBatchSendBackoff() {
    this.clearBatchSendSchedule()
    this.consecutiveBatchSendFailures = 0
  }
}
