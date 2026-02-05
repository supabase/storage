import PgBoss, { Db, Job, JobWithMetadata } from 'pg-boss'
import { ERRORS } from '@internal/errors'
import { QueueDB } from '@internal/queue/database'
import { getConfig } from '../../config'
import { logger, logSchema } from '../monitoring'
import { QueueJobRetryFailed, QueueJobCompleted, QueueJobError } from '../monitoring/metrics'
import { Event } from './event'
import { Semaphore } from '@shopify/semaphore'
import { QueueHealthMonitor } from './health-monitor'

//eslint-disable-next-line @typescript-eslint/no-explicit-any
type SubclassOfBaseClass = (new (payload: any) => Event<any>) & {
  [K in keyof typeof Event]: (typeof Event)[K]
}

export abstract class Queue {
  protected static events: SubclassOfBaseClass[] = []
  private static pgBoss?: PgBoss
  private static pgBossDb?: PgBoss.Db
  private static healthMonitor?: QueueHealthMonitor

  static createPgBoss(opts: { db: Db; enableWorkers: boolean }) {
    const {
      isMultitenant,
      databaseURL,
      multitenantDatabaseUrl,
      pgQueueConnectionURL,
      pgQueueArchiveCompletedAfterSeconds,
      pgQueueDeleteAfterDays,
      pgQueueDeleteAfterHours,
      pgQueueRetentionDays,
    } = getConfig()

    let url = pgQueueConnectionURL ?? databaseURL

    if (isMultitenant && !pgQueueConnectionURL) {
      if (!multitenantDatabaseUrl) {
        throw new Error(
          'running storage in multi-tenant but DB_MULTITENANT_DATABASE_URL is not set'
        )
      }
      url = multitenantDatabaseUrl
    }

    return new PgBoss({
      connectionString: url,
      migrate: true,
      db: opts.db,
      schema: 'pgboss_v10',
      application_name: 'storage-pgboss',
      ...(pgQueueDeleteAfterHours
        ? { deleteAfterHours: pgQueueDeleteAfterHours }
        : { deleteAfterDays: pgQueueDeleteAfterDays }),
      archiveCompletedAfterSeconds: pgQueueArchiveCompletedAfterSeconds,
      retentionDays: pgQueueRetentionDays,
      retryBackoff: true,
      retryLimit: 20,
      expireInHours: 23,
      maintenanceIntervalSeconds: 60 * 5,
      schedule: opts.enableWorkers,
      supervise: opts.enableWorkers,
    })
  }

  static async start(opts: {
    signal?: AbortSignal
    onMessage?: (job: Job) => void
    registerWorkers?: () => void
  }) {
    if (Queue.pgBoss) {
      return Queue.pgBoss
    }

    if (opts.signal?.aborted) {
      throw ERRORS.Aborted('Cannot start queue with aborted signal')
    }

    const {
      isMultitenant,
      databaseURL,
      multitenantDatabaseUrl,
      pgQueueConnectionURL,
      pgQueueEnableWorkers,
      pgQueueReadWriteTimeout,
      pgQueueConcurrentTasksPerQueue,
      pgQueueMaxConnections,
      pgQueueHealthCheckEnabled,
      pgQueueHealthCheckMaxConsecutiveErrors,
      pgQueueHealthCheckUnhealthyTimeoutMs,
    } = getConfig()

    // Initialize health monitor if enabled (only once)
    if (pgQueueHealthCheckEnabled && !Queue.healthMonitor) {
      Queue.healthMonitor = new QueueHealthMonitor({
        maxConsecutiveErrors: pgQueueHealthCheckMaxConsecutiveErrors,
        unhealthyTimeoutMs: pgQueueHealthCheckUnhealthyTimeoutMs,
      })

      logSchema.info(logger, '[Queue Health] Health monitoring enabled', {
        type: 'queue',
        metadata: JSON.stringify({
          maxConsecutiveErrors: pgQueueHealthCheckMaxConsecutiveErrors,
          unhealthyTimeoutMs: pgQueueHealthCheckUnhealthyTimeoutMs,
        }),
      })
    }

    let url = pgQueueConnectionURL || databaseURL

    if (isMultitenant && !pgQueueConnectionURL) {
      if (!multitenantDatabaseUrl) {
        throw new Error(
          'running storage in multi-tenant but DB_MULTITENANT_DATABASE_URL is not set'
        )
      }
      url = multitenantDatabaseUrl
    }

    Queue.pgBossDb = new QueueDB({
      min: 0,
      max: pgQueueMaxConnections,
      connectionString: url,
      statement_timeout: pgQueueReadWriteTimeout > 0 ? pgQueueReadWriteTimeout : undefined,
    })

    Queue.pgBoss = this.createPgBoss({
      db: Queue.pgBossDb,
      enableWorkers: pgQueueEnableWorkers !== false,
    })

    Queue.pgBoss.on('error', (error) => {
      logSchema.error(logger, '[Queue] error', {
        type: 'queue',
        error,
      })
      Queue.healthMonitor?.trackConnectionError(error)
    })

    // Listen for successful database queries to track connection health
    // Only QueueDB instances support the 'success' event
    if (Queue.pgBossDb instanceof QueueDB && Queue.healthMonitor) {
      Queue.pgBossDb.on('success', () => {
        Queue.healthMonitor?.trackSuccessfulOperation()
      })
    }

    await Queue.pgBoss.start()

    if (opts.registerWorkers && pgQueueEnableWorkers) {
      opts.registerWorkers()
    }

    await Queue.callStart()
    await Queue.startWorkers({
      maxConcurrentTasks: pgQueueConcurrentTasksPerQueue,
      onMessage: opts.onMessage,
      signal: opts.signal,
    })

    if (opts.signal) {
      opts.signal.addEventListener(
        'abort',
        async () => {
          logSchema.info(logger, '[Queue] Stopping', {
            type: 'queue',
          })
          return Queue.stop()
            .then(async () => {
              logSchema.info(logger, '[Queue] Exited', {
                type: 'queue',
              })
            })
            .catch((e) => {
              logSchema.error(logger, '[Queue] Error while stopping queue', {
                error: e,
                type: 'queue',
              })
            })
            .finally(async () => {
              await Queue.callClose().catch(() => {
                // no-op
              })
            })
        },
        { once: true }
      )
    }

    return Queue.pgBoss
  }

  static getInstance() {
    if (!this.pgBoss) {
      throw new Error('pg boss not initialised')
    }

    return this.pgBoss
  }

  static getDb() {
    if (!this.pgBossDb) {
      throw new Error('pg boss not initialised')
    }

    return this.pgBossDb
  }

  static register<T extends SubclassOfBaseClass>(event: T) {
    Queue.events.push(event)
  }

  static async stop() {
    if (!this.pgBoss) {
      return
    }

    const boss = this.pgBoss
    const { isProduction } = getConfig()

    await boss.stop({
      timeout: 20 * 1000,
      graceful: isProduction,
      wait: true,
    })

    await new Promise((resolve) => {
      boss.once('stopped', async () => {
        await this.callClose()
        resolve(null)
      })
    })

    Queue.pgBoss = undefined
  }

  protected static async startWorkers(opts: {
    maxConcurrentTasks: number
    signal?: AbortSignal
    onMessage?: (job: Job) => void
  }) {
    for (const event of Queue.events) {
      await Queue.registerTask(event, opts.maxConcurrentTasks, opts.onMessage, opts.signal)
    }
  }

  protected static callStart() {
    const events = Queue.events.map((event) => {
      return event.onStart()
    })

    return Promise.all(events)
  }

  protected static callClose() {
    const events = Queue.events.map((event) => {
      return event.onClose()
    })

    return Promise.all(events)
  }

  protected static async registerTask(
    event: SubclassOfBaseClass,
    maxConcurrentTasks: number,
    onMessage?: (job: Job) => void,
    signal?: AbortSignal
  ) {
    const queueName = event.getQueueName()
    const deadLetterName = event.deadLetterQueueName()

    const concurrentTaskCount = event.getWorkerOptions().concurrentTaskCount || maxConcurrentTasks
    try {
      // Create dead-letter queue and the normal queue
      const queueOptions = {
        policy: 'standard',
        ...event.getQueueOptions(),
      } as const

      // dead-letter
      await this.pgBoss?.createQueue(deadLetterName, {
        ...queueOptions,
        name: deadLetterName,
        retentionDays: 30,
        retryBackoff: true,
      })

      // // normal queue
      await this.pgBoss?.createQueue(queueName, {
        ...queueOptions,
        name: queueName,
        deadLetter: deadLetterName,
      })
    } catch {
      // no-op
    }

    return this.pollQueue(event, {
      concurrentTaskCount,
      onMessage,
      signal,
    })
  }

  protected static pollQueue(
    event: SubclassOfBaseClass,
    queueOpts: {
      concurrentTaskCount: number
      onMessage?: (job: Job) => void
      signal?: AbortSignal
    }
  ) {
    const semaphore = new Semaphore(queueOpts.concurrentTaskCount)
    const pollingInterval = (event.getWorkerOptions().pollingIntervalSeconds || 5) * 1000
    const batchSize =
      event.getWorkerOptions().batchSize ||
      queueOpts.concurrentTaskCount + Math.max(1, Math.floor(queueOpts.concurrentTaskCount * 1.2))

    logSchema.info(logger, `[Queue] Polling queue ${event.getQueueName()}`, {
      type: 'queue',
      metadata: JSON.stringify({
        queueName: event.getQueueName(),
        batchSize: batchSize,
        pollingInterval: pollingInterval,
      }),
    })

    let started = false
    let jobFetched = 0

    const interval = setInterval(async () => {
      if (started) {
        return
      }

      try {
        started = true
        const defaultFetch = {
          includeMetadata: true,
          batchSize,
        }

        const currentBatch = defaultFetch.batchSize - jobFetched

        if (currentBatch <= 0) {
          return
        }

        const jobs = await this.pgBoss?.fetch(event.getQueueName(), {
          ...event.getWorkerOptions(),
          ...defaultFetch,
          batchSize: currentBatch,
        })

        jobFetched += jobs?.length || 0

        if (jobFetched < defaultFetch.batchSize) {
          started = false
        }

        if (queueOpts.signal?.aborted) {
          started = false
          return
        }

        if (!jobs || (jobs && jobs.length === 0)) {
          started = false
          return
        }

        await Promise.allSettled(
          jobs.map(async (job) => {
            const lock = await semaphore.acquire()
            try {
              queueOpts.onMessage?.(job as Job)

              await event.handle(job, { signal: queueOpts.signal })

              await this.pgBoss?.complete(event.getQueueName(), job.id)
              QueueJobCompleted.inc({
                name: event.getQueueName(),
              })
            } catch (e) {
              QueueJobRetryFailed.inc({
                name: event.getQueueName(),
              })

              await this.pgBoss?.fail(event.getQueueName(), job.id)

              try {
                const dbJob: JobWithMetadata | null =
                  (job as JobWithMetadata).priority !== undefined
                    ? (job as JobWithMetadata)
                    : await Queue.getInstance().getJobById(event.getQueueName(), job.id)

                if (!dbJob) {
                  return
                }
                if (dbJob.retryCount >= dbJob.retryLimit) {
                  QueueJobError.inc({
                    name: event.getQueueName(),
                  })
                }
              } catch (e) {
                logSchema.error(logger, `[Queue Handler] fetching job ${event.name}`, {
                  type: 'queue-task',
                  error: e,
                  metadata: JSON.stringify(job),
                })
              }

              logSchema.error(logger, `[Queue Handler] Error while processing job ${event.name}`, {
                type: 'queue-task',
                error: e,
                metadata: JSON.stringify(job),
              })

              throw e
            } finally {
              jobFetched = Math.max(0, jobFetched - 1)
              await lock.release()
            }
          })
        )
      } catch (e) {
        logSchema.error(logger, `[Queue] Error while polling queue ${event.name}`, {
          type: 'queue',
          error: e,
          metadata: JSON.stringify({
            queueName: event.getQueueName(),
            batchSize,
            pollingInterval,
          }),
        })
      } finally {
        started = false
      }
    }, pollingInterval)

    queueOpts.signal?.addEventListener('abort', () => {
      clearInterval(interval)
    })
  }
}
