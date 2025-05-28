import PgBoss, { Job, JobWithMetadata } from 'pg-boss'
import { ERRORS } from '@internal/errors'
import { QueueDB } from '@internal/queue/database'
import { getConfig } from '../../config'
import { logger, logSchema } from '../monitoring'
import { QueueJobRetryFailed, QueueJobCompleted, QueueJobError } from '../monitoring/metrics'
import { Event } from './event'
import { Semaphore } from '@shopify/semaphore'

//eslint-disable-next-line @typescript-eslint/no-explicit-any
type SubclassOfBaseClass = (new (payload: any) => Event<any>) & {
  [K in keyof typeof Event]: (typeof Event)[K]
}

export abstract class Queue {
  protected static events: SubclassOfBaseClass[] = []
  private static pgBoss?: PgBoss

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
      pgQueueArchiveCompletedAfterSeconds,
      pgQueueDeleteAfterDays,
      pgQueueDeleteAfterHours,
      pgQueueRetentionDays,
      pgQueueEnableWorkers,
      pgQueueReadWriteTimeout,
      pgQueueMaxConnections,
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

    Queue.pgBoss = new PgBoss({
      connectionString: url,

      db: new QueueDB({
        min: 0,
        max: pgQueueMaxConnections,
        connectionString: url,
        statement_timeout: pgQueueReadWriteTimeout > 0 ? pgQueueReadWriteTimeout : undefined,
      }),
      schema: 'pgboss_v10',
      application_name: 'storage-pgboss',
      ...(pgQueueDeleteAfterHours ? {} : { deleteAfterDays: pgQueueDeleteAfterDays }),
      ...(pgQueueDeleteAfterHours ? { deleteAfterHours: pgQueueDeleteAfterHours } : {}),
      archiveCompletedAfterSeconds: pgQueueArchiveCompletedAfterSeconds,
      retentionDays: pgQueueRetentionDays,
      retryBackoff: true,
      retryLimit: 20,
      expireInHours: 23,
      maintenanceIntervalSeconds: 60 * 5,
      schedule: pgQueueEnableWorkers !== false,
      supervise: pgQueueEnableWorkers !== false,
    })

    Queue.pgBoss.on('error', (error) => {
      logSchema.error(logger, '[Queue] error', {
        type: 'queue',
        error,
      })
    })

    await Queue.pgBoss.start()

    if (opts.registerWorkers && pgQueueEnableWorkers) {
      opts.registerWorkers()
    }

    await Queue.callStart()
    await Queue.startWorkers({
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

  protected static startWorkers(opts: { signal?: AbortSignal; onMessage?: (job: Job) => void }) {
    const workers: Promise<any>[] = []

    Queue.events.forEach((event) => {
      workers.push(Queue.registerTask(event.getQueueName(), event, opts.onMessage, opts.signal))
    })

    return Promise.all(workers)
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
    queueName: string,
    event: SubclassOfBaseClass,
    onMessage?: (job: Job) => void,
    signal?: AbortSignal
  ) {
    const concurrentTaskCount = event.getWorkerOptions().batchSize || 50
    const pollingInterval = event.getWorkerOptions().pollingIntervalSeconds || 5 * 1000
    const semaphore = new Semaphore(concurrentTaskCount)

    try {
      // Create dead-letter queue and the normal queue
      const queueOptions = {
        name: queueName,
        policy: 'standard',
        deadLetter: queueName + '-dead-letter',
        ...event.getQueueOptions(),
      } as const

      await this.pgBoss?.createQueue(queueName + '-dead-letter', queueOptions)
      await this.pgBoss?.createQueue(queueName, queueOptions)
    } catch {
      // no-op
    }

    let started = false
    const interval = setInterval(async () => {
      if (started) {
        return
      }

      started = true
      const defaultFetch = {
        includeMetadata: true,
        batchSize: concurrentTaskCount * 2,
      }
      const jobs = await this.pgBoss?.fetch(queueName, {
        ...event.getWorkerOptions(),
        ...defaultFetch,
      })

      if (signal?.aborted) {
        started = false
        return
      }

      if (!jobs || (jobs && jobs.length === 0)) {
        started = false
        return
      }

      try {
        await Promise.allSettled(
          jobs.map(async (job) => {
            const lock = await semaphore.acquire()
            try {
              onMessage?.(job as Job)

              await event.handle(job)

              await this.pgBoss?.complete(queueName, job.id)
              QueueJobCompleted.inc({
                name: event.getQueueName(),
              })
            } catch (e) {
              await this.pgBoss?.fail(queueName, job.id)

              QueueJobRetryFailed.inc({
                name: event.getQueueName(),
              })

              try {
                const dbJob: JobWithMetadata | null =
                  (job as JobWithMetadata).priority !== undefined
                    ? (job as JobWithMetadata)
                    : await Queue.getInstance().getJobById(queueName, job.id)

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
              await lock.release()
            }
          })
        )
      } finally {
        started = false
      }
    }, pollingInterval)

    signal?.addEventListener('abort', () => {
      clearInterval(interval)
    })
  }
}
