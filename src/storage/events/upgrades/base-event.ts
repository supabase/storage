import { multitenantKnex } from '@internal/database'
import { hashStringToInt } from '@internal/hashing'
import { logger, logSchema } from '@internal/monitoring'
import { BasePayload, Event } from '@internal/queue'
import { BaseEvent } from '@storage/events'
import { Knex } from 'knex'
import { Job, Queue as PgBossQueue, SendOptions } from 'pg-boss'
import { getConfig } from '../../../config'

const { isMultitenant } = getConfig()

export type UpgradeBaseEventPayload = BasePayload

export abstract class UpgradeBaseEvent<T extends UpgradeBaseEventPayload> extends BaseEvent<T> {
  static getQueueOptions(): PgBossQueue {
    return {
      name: this.queueName,
      policy: 'exactly_once',
    } as const
  }

  static getSendOptions(): SendOptions {
    return {
      expireInHours: 2,
      singletonKey: this.getQueueName(),
      singletonHours: 12,
      retryBackoff: false,
      retryLimit: 3,
      retryDelay: 5,
      priority: 10,
    }
  }

  static async handleUpgrade(
    tnx: Knex.Transaction,
    job: Job<UpgradeBaseEventPayload>
  ): Promise<void> {}

  static async handle(job: Job<UpgradeBaseEventPayload>) {
    if (!isMultitenant) {
      return
    }

    await this.runOnce((t: Knex.Transaction) => {
      return this.handleUpgrade(t, job)
    })
  }

  protected static async runOnce(fn: (t: Knex.Transaction) => Promise<unknown> | void) {
    logSchema.info(logger, `[Upgrade] Starting upgrade event: ${this.getQueueName()}`, {
      type: 'upgradeEvent',
    })
    await multitenantKnex.transaction(async (t) => {
      const hash = hashStringToInt('event:upgrade-lock')
      const result = await t.raw<{ rows: { pg_try_advisory_xact_lock: boolean }[] }>(
        `SELECT pg_try_advisory_xact_lock(?);`,
        [hash]
      )
      const lockAcquired = result.rows.shift()?.pg_try_advisory_xact_lock || false

      if (!lockAcquired) {
        logSchema.info(logger, `[Upgrade] Lock already acquired for: ${this.getQueueName()}`, {
          type: 'upgradeEvent',
        })
        return
      }

      const id = await t
        .table('event_upgrades')
        .select('event_id')
        .where('event_id', this.getQueueName())
        .first()

      if (id) {
        return
      }

      await fn(t)

      await t
        .table('event_upgrades')
        .insert({
          event_id: this.getQueueName(),
        })
        .onConflict('event_id')
        .ignore()

      logSchema.info(logger, `[Upgrade] Completed upgrade: ${this.getQueueName()}`, {
        type: 'upgradeEvent',
      })
    })
  }
}
