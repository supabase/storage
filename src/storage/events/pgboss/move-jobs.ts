import { BaseEvent } from '../base-event'
import { Job, Queue as PgBossQueue, SendOptions, WorkOptions } from 'pg-boss'
import { BasePayload, Queue } from '@internal/queue'
import { multitenantKnex } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'

interface MoveJobsPayload extends BasePayload {
  fromQueue: string
  toQueue: string
  deleteJobsFromOriginalQueue?: boolean
}

export class MoveJobs extends BaseEvent<MoveJobsPayload> {
  static queueName = 'move-jobs'

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

  static getSendOptions(payload: MoveJobsPayload): SendOptions {
    return {
      expireInHours: 2,
      singletonKey: `move_${payload.fromQueue}_to_${payload.toQueue}`,
      singletonHours: 12,
      retryLimit: 3,
      retryDelay: 5,
      priority: 10,
    }
  }

  static async handle(job: Job<MoveJobsPayload>) {
    await multitenantKnex.transaction(async (tnx) => {
      const resultLock = await tnx.raw('SELECT pg_try_advisory_xact_lock(-5525285245963000611)')
      const lockAcquired = resultLock.rows.shift()?.pg_try_advisory_xact_lock || false

      if (!lockAcquired) {
        return
      }

      const schema = 'pgboss_v10'
      const fromQueueName = job.data.fromQueue
      const toQueue = await Queue.getInstance().getQueue(job.data.toQueue)

      if (!toQueue) {
        logSchema.error(logger, `[PgBoss] Target queue ${job.data.toQueue} does not exist`, {
          type: 'pgboss',
        })
        return
      }

      try {
        const sql = `
            INSERT INTO ${schema}.job (
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
                policy,
                state
            )
            SELECT 
                id,
                '${toQueue.name}' as name,
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
                '${toQueue.policy}' as policy,
                'created' as state
            FROM ${schema}.job
            WHERE name = '${fromQueueName}'
                AND state IN ('created', 'active', 'retry')
            ON CONFLICT DO NOTHING
        `

        await tnx.raw(sql)

        if (job.data.deleteJobsFromOriginalQueue) {
          const deleteSql = `
                DELETE FROM ${schema}.job
                WHERE name = '${fromQueueName}'
                    AND state IN ('created', 'active', 'retry')
            `
          await tnx.raw(deleteSql)
        }
      } catch (error) {
        logSchema.error(logger, '[PgBoss] Error while copying jobs', {
          type: 'pgboss',
          error,
        })
      }
    })
  }
}
