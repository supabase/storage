import { multitenantPgExecutor, PgTransaction } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { BasePayload, PG_BOSS_SCHEMA, SYSTEM_TENANT_REF } from '@internal/queue'
import { Job, Queue as PgBossQueue, SendOptions, WorkOptions } from 'pg-boss'
import { BaseEvent } from '../base-event'

type UpgradePgBossV12Payload = BasePayload

const SOURCE_SCHEMA = 'pgboss_v10'

/**
 * Copies pending jobs from the pg-boss v10 fork schema (pgboss_v10) into the
 * upstream pg-boss v12 schema. Only jobs whose queue exists in the new schema
 * are copied; the queue's policy in the new schema is applied so jobs queued
 * under the fork-only `exactly_once` policy continue under `exclusive`.
 */
export class UpgradePgBossV12 extends BaseEvent<UpgradePgBossV12Payload> {
  static queueName = 'upgrade-pgboss-v12'

  static getQueueOptions(): PgBossQueue {
    return {
      name: this.queueName,
      policy: 'exclusive',
    } as const
  }

  static getWorkerOptions(): WorkOptions {
    return {
      includeMetadata: true,
    }
  }

  static getSendOptions(): SendOptions {
    return {
      expireInSeconds: 2 * 60 * 60,
      singletonKey: 'pgboss-upgrade-v12',
      singletonSeconds: 12 * 60 * 60,
      retryLimit: 3,
      retryDelay: 5,
      priority: 10,
    }
  }

  static async handle(job: Job<UpgradePgBossV12Payload>) {
    const { sbReqId } = job.data

    await withPgTransaction(async (tnx) => {
      const resultLock = await tnx.query<{ locked: boolean }>(
        `SELECT pg_try_advisory_xact_lock(-5525285245963000612) AS locked`
      )
      const lockAcquired = resultLock.rows.shift()?.locked || false

      if (!lockAcquired) {
        return
      }

      const sourceSchemaExists = await tnx.query<{ exists: boolean }>(
        `SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = $1) AS exists`,
        [SOURCE_SCHEMA]
      )

      if (!sourceSchemaExists.rows.shift()?.exists) {
        logSchema.info(logger, `[PgBoss] Source schema ${SOURCE_SCHEMA} does not exist, skipping`, {
          type: 'pgboss',
          project: job.data.tenant?.ref || SYSTEM_TENANT_REF,
          sbReqId,
        })
        return
      }

      try {
        await tnx.query(`
          INSERT INTO ${PG_BOSS_SCHEMA}.job (
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
              expire_seconds,
              created_on,
              keep_until,
              output,
              policy,
              dead_letter,
              state
          )
          SELECT
              source.id,
              source.name,
              source.priority,
              source.data,
              source.retry_limit,
              source.retry_count,
              source.retry_delay,
              source.retry_backoff,
              source.start_after,
              source.singleton_key,
              source.singleton_on,
              GREATEST(1, EXTRACT(EPOCH FROM source.expire_in))::int,
              source.created_on,
              source.keep_until,
              source.output,
              queue.policy,
              dlq.name,
              'created' as state
          FROM ${SOURCE_SCHEMA}.job AS source
          JOIN ${PG_BOSS_SCHEMA}.queue AS queue ON queue.name = source.name
          LEFT JOIN ${PG_BOSS_SCHEMA}.queue AS dlq ON dlq.name = source.dead_letter
          WHERE source.state IN ('created', 'active', 'retry')
          ON CONFLICT DO NOTHING
        `)
      } catch (error) {
        logSchema.error(logger, '[PgBoss] Error while copying jobs from previous schema', {
          type: 'pgboss',
          error,
          project: job.data.tenant?.ref || SYSTEM_TENANT_REF,
          sbReqId,
        })
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
      logSchema.warning(logger, '[UpgradePgBossV12] Failed to rollback transaction', {
        type: 'pgboss',
        error: rollbackError,
        metadata: JSON.stringify({ originalError: String(e) }),
      })
    }
    throw e
  }
}
