import PgBoss, { Job, JobWithMetadata } from 'pg-boss'
import { ERRORS } from '@internal/errors'
import { QueueDB } from '@internal/queue/database'
import { getConfig } from '../../config'
import { logger, logSchema } from '../monitoring'
import { QueueJobRetryFailed, QueueJobCompleted, QueueJobError } from '../monitoring/metrics'
import { BasePayload, Event } from './event'

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
      pgQueueDeleteAfterDays,
      pgQueueDeleteAfterHours,
      pgQueueArchiveCompletedAfterSeconds,
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
      application_name: 'storage-pgboss',
      ...(pgQueueDeleteAfterHours ? {} : { deleteAfterDays: pgQueueDeleteAfterDays }),
      ...(pgQueueDeleteAfterHours ? { deleteAfterHours: pgQueueDeleteAfterHours } : {}),
      archiveCompletedAfterSeconds: pgQueueArchiveCompletedAfterSeconds,
      retentionDays: pgQueueRetentionDays,
      retryBackoff: true,
      retryLimit: 20,
      expireInHours: 48,
      noSupervisor: pgQueueEnableWorkers === false,
      noScheduling: pgQueueEnableWorkers === false,
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
    await Queue.startWorkers(opts.onMessage)

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
      destroy: true,
    })

    await new Promise((resolve) => {
      boss.once('stopped', async () => {
        await this.callClose()
        resolve(null)
      })
    })

    Queue.pgBoss = undefined
  }

  protected static startWorkers(onMessage?: (job: Job) => void) {
    const workers: Promise<string>[] = []

    Queue.events.forEach((event) => {
      workers.push(Queue.registerTask(event.getQueueName(), event, true, onMessage))

      const slowRetryQueue = event.withSlowRetryQueue()

      if (slowRetryQueue) {
        workers.push(Queue.registerTask(event.getSlowRetryQueueName(), event, false, onMessage))
      }
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

  protected static registerTask(
    queueName: string,
    event: SubclassOfBaseClass,
    slowRetryQueueOnFail?: boolean,
    onMessage?: (job: Job) => void
  ) {
    const hasSlowRetryQueue = event.withSlowRetryQueue()
    return Queue.getInstance().work(
      queueName,
      event.getWorkerOptions(),
      async (job: Job<BasePayload>) => {
        try {
          if (onMessage) {
            onMessage(job)
          }
          const res = await event.handle(job)

          QueueJobCompleted.inc({
            name: event.getQueueName(),
          })

          return res
        } catch (e) {
          QueueJobRetryFailed.inc({
            name: event.getQueueName(),
          })

          try {
            const dbJob: JobWithMetadata | null =
              (job as JobWithMetadata).priority !== undefined
                ? (job as JobWithMetadata)
                : await Queue.getInstance().getJobById(job.id)

            if (!dbJob) {
              return
            }
            if (dbJob.retrycount >= dbJob.retrylimit) {
              QueueJobError.inc({
                name: event.getQueueName(),
              })

              if (hasSlowRetryQueue && slowRetryQueueOnFail) {
                event.sendSlowRetryQueue(job.data).catch(() => {
                  logSchema.error(
                    logger,
                    `[Queue Handler] Error while sending job to slow retry queue`,
                    {
                      type: 'queue-task',
                      error: e,
                      metadata: JSON.stringify(job),
                    }
                  )
                })
              }
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
        }
      }
    )
  }
}
