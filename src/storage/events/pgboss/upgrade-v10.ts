import { multitenantPgExecutor, PgTransaction } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { BasePayload, PG_BOSS_SCHEMA, Queue, SYSTEM_TENANT_REF } from '@internal/queue'
import { Job, Queue as PgBossQueue, SendOptions, WorkOptions } from 'pg-boss'
import { BaseEvent } from '../base-event'

type UpgradePgBossV10Payload = BasePayload

export class UpgradePgBossV10 extends BaseEvent<UpgradePgBossV10Payload> {
  static queueName = 'upgrade-pg-boss-v10'

  static getQueueOptions(): PgBossQueue {
    return {
      name: this.queueName,
      policy: 'exactly_once',
    } as const
  }

  static getWorkerOptions(): WorkOptions {
    return {
      includeMetadata: true,
    }
  }

  static getSendOptions(payload: UpgradePgBossV10Payload): SendOptions {
    return {
      expireInHours: 2,
      singletonKey: 'pgboss-upgrade-v10',
      singletonHours: 12,
      retryLimit: 3,
      retryDelay: 5,
      priority: 10,
    }
  }

  static async handle(job: Job<UpgradePgBossV10Payload>) {
    return this.handlePg(job)
  }

  private static async handlePg(job: Job<UpgradePgBossV10Payload>) {
    const { sbReqId } = job.data

    await withPgTransaction(async (tnx) => {
      const resultLock = await tnx.query<{ locked: boolean }>(
        `SELECT pg_try_advisory_xact_lock(-5525285245963000606) AS locked`
      )
      const lockAcquired = resultLock.rows.shift()?.locked || false

      if (!lockAcquired) {
        return
      }

      const targetSchema = PG_BOSS_SCHEMA
      const sourceSchema = 'pgboss'

      const queues = await Queue.getInstance().getQueues()

      for (const queue of queues) {
        try {
          await tnx.query({
            text: `
              INSERT INTO ${targetSchema}.job (
                  id,
                  name,
                  priority,
                  data,
                  retry_limit,
                  retry_count,
                  retry_delay,
                  retry_backoff,
                  start_after,
                  singleton_key,
                  singleton_on,
                  expire_in,
                  created_on,
                  keep_until,
                  output,
                  policy
              )
              SELECT
                  id,
                  name,
                  priority,
                  data,
                  retryLimit,
                  retryCount,
                  retryDelay,
                  retryBackoff,
                  startAfter,
                  singletonKey,
                  singletonOn,
                  expireIn,
                  createdOn,
                  keepUntil,
                  output jsonb,
                  $1 as policy
              FROM ${sourceSchema}.job
              WHERE name = $2
                  AND state = 'created'
              ON CONFLICT DO NOTHING
            `,
            values: [queue.policy, queue.name],
          })
        } catch (error) {
          logSchema.error(logger, '[PgBoss] Error while copying jobs', {
            type: 'pgboss',
            error,
            project: job.data.tenant?.ref || SYSTEM_TENANT_REF,
            sbReqId,
          })
        }
      }
    })
  }
}

async function withPgTransaction<T>(fn: (tnx: PgTransaction) => Promise<T>): Promise<T> {
  const tnx = await multitenantPgExecutor.beginTransaction()

  try {
    const result = await fn(tnx)
    await tnx.commit()
    return result
  } catch (e) {
    try {
      await tnx.rollback()
    } catch (rollbackError) {
      logSchema.warning(logger, '[UpgradePgBossV10] Failed to rollback transaction', {
        type: 'pgboss',
        error: rollbackError,
        metadata: JSON.stringify({ originalError: String(e) }),
      })
    }
    throw e
  }
}
