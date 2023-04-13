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

    const { isMultitenant, multitenantDatabaseUrl, pgQueueConnectionURL } = getConfig()

    let url = pgQueueConnectionURL ?? process.env.DATABASE_URL

    if (isMultitenant) {
      url = pgQueueConnectionURL ?? multitenantDatabaseUrl
    }
    Queue.pgBoss = new PgBoss({
      connectionString: url,
      max: 4,
      application_name: 'storage-pgboss',
      deleteAfterDays: 7,
      archiveCompletedAfterSeconds: 14_400,
      retentionDays: 7,
      retryBackoff: true,
      retryLimit: 20,
      expireInHours: 48,
      monitorStateIntervalSeconds: 30,
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
      const options = event.getWorkerOptions()

      workers.push(
        Queue.getInstance().work(
          event.getQueueName(),
          options,
          async (job: Job<BasePayload> | Job<BasePayload>[]) => {
            if (!Array.isArray(job)) {
              job = [job]
            }

            await Promise.all(
              job.map(async (j) => {
                try {
                  const res = await event.handle(j)

                  if (options.batchSize) {
                    await event.ack(j)
                  }

                  QueueJobCompleted.inc({
                    tenant_id: j.data.tenant.ref,
                    name: event.getQueueName(),
                  })

                  return res
                } catch (e) {
                  QueueJobRetryFailed.inc({
                    tenant_id: j.data.tenant.ref,
                    name: event.getQueueName(),
                  })

                  Queue.getInstance()
                    .getJobById(j.id)
                    .then((dbJob) => {
                      if (!dbJob) {
                        return
                      }
                      if (dbJob.retrycount === dbJob.retrylimit) {
                        QueueJobError.inc({
                          tenant_id: j.data.tenant.ref,
                          name: event.getQueueName(),
                        })
                      }
                    })

                  logger.error(
                    {
                      job: JSON.stringify(j),
                      rawError: normalizeRawError(e),
                    },
                    'Error while processing job'
                  )

                  if (!options.batchSize) {
                    throw e
                  }

                  await event.fail(j)
                }
              })
            )
          }
        )
      )
    })

    return Promise.all(workers)
  }
}
