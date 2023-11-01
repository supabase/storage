import PgBoss, { Job } from 'pg-boss'
import { getConfig } from '../config'
import { registerWorkers } from './workers'
import { BaseEvent, BasePayload } from './events'
import { QueueJobRetryFailed, QueueJobCompleted, QueueJobError } from '../monitoring/metrics'
import { logger } from '../monitoring'
import { normalizeRawError } from '../storage'

type SubclassOfBaseClass = (new (payload: any) => BaseEvent<any>) & {
  [K in keyof typeof BaseEvent]: typeof BaseEvent[K]
}

export abstract class Queue {
  protected static events: SubclassOfBaseClass[] = []
  private static pgBoss?: PgBoss

  static async init() {
    if (Queue.pgBoss) {
      return Queue.pgBoss
    }

    const {
      isMultitenant,
      multitenantDatabaseUrl,
      pgQueueConnectionURL,
      pgQueueDeleteAfterDays,
      pgQueueArchiveCompletedAfterSeconds,
      pgQueueRetentionDays,
    } = getConfig()

    let url = pgQueueConnectionURL ?? process.env.DATABASE_URL

    if (isMultitenant) {
      url = pgQueueConnectionURL ?? multitenantDatabaseUrl
    }

    Queue.pgBoss = new PgBoss({
      connectionString: url,
      max: 4,
      application_name: 'storage-pgboss',
      deleteAfterDays: pgQueueDeleteAfterDays,
      archiveCompletedAfterSeconds: pgQueueArchiveCompletedAfterSeconds,
      retentionDays: pgQueueRetentionDays,
      retryBackoff: true,
      retryLimit: 20,
      expireInHours: 48,
    })

    registerWorkers()

    await Queue.pgBoss.start()
    await Queue.startWorkers()

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

    await this.pgBoss.stop()
  }

  protected static startWorkers() {
    const workers: Promise<string>[] = []

    Queue.events.forEach((event) => {
      workers.push(
        Queue.getInstance().work(
          event.getQueueName(),
          event.getWorkerOptions(),
          async (job: Job<BasePayload>) => {
            try {
              const res = await event.handle(job)

              QueueJobCompleted.inc({
                name: event.getQueueName(),
              })

              return res
            } catch (e) {
              QueueJobRetryFailed.inc({
                name: event.getQueueName(),
              })

              Queue.getInstance()
                .getJobById(job.id)
                .then((dbJob) => {
                  if (!dbJob) {
                    return
                  }
                  if (dbJob.retrycount === dbJob.retrylimit) {
                    QueueJobError.inc({
                      name: event.getQueueName(),
                    })
                  }
                })

              logger.error(
                {
                  job: JSON.stringify(job),
                  rawError: normalizeRawError(e),
                },
                'Error while processing job'
              )

              throw e
            }
          }
        )
      )
    })

    return Promise.all(workers)
  }
}
