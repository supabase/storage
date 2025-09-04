import { BaseEvent } from '../base-event'
import { Job, Queue as PgBossQueue, SendOptions, WorkOptions } from 'pg-boss'
import { BasePayload, Queue } from '@internal/queue'
import { multitenantKnex } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'

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
    await multitenantKnex.transaction(async (tnx) => {
      const resultLock = await tnx.raw('SELECT pg_try_advisory_xact_lock(-5525285245963000606)')
      const lockAcquired = resultLock.rows.shift()?.pg_try_advisory_xact_lock || false

      if (!lockAcquired) {
        return
      }

      const targetSchema = 'pgboss_v10'
      const sourceSchema = 'pgboss'

      const queues = await Queue.getInstance().getQueues()

      for (const queue of queues) {
        try {
          const sql = `
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
                '${queue.policy}' as policy
            FROM ${sourceSchema}.job
            WHERE name = '${queue.name}'
                AND state = 'created'
            ON CONFLICT DO NOTHING
        `

          await multitenantKnex.raw(sql)
        } catch (error) {
          logSchema.error(logger, '[PgBoss] Error while copying jobs', {
            type: 'pgboss',
            error,
          })
        }
      }
    })
  }
}
