import { type DatabaseTransaction, multitenantPgExecutor } from '@internal/database'
import { hashStringToInt } from '@internal/hashing'
import { logger, logSchema } from '@internal/monitoring'

export type UpgradeTransaction = DatabaseTransaction

/**
 * Run a one-shot fleet upgrade at most once, ever (v1 `UpgradeBaseEvent.runOnce`): an
 * advisory xact lock serializes concurrent attempts, and the `event_upgrades` ledger makes
 * completion durable — a second run is a no-op commit.
 */
export async function runUpgradeOnce(
  eventId: string,
  fn: (t: UpgradeTransaction) => Promise<unknown> | void
): Promise<void> {
  logSchema.info(logger, `[Upgrade] Starting upgrade event: ${eventId}`, {
    type: 'upgradeEvent',
  })

  const t = await multitenantPgExecutor.beginTransaction()

  try {
    const hash = hashStringToInt('event:upgrade-lock')
    const result = await t.query<{ pg_try_advisory_xact_lock: boolean }>({
      text: `SELECT pg_try_advisory_xact_lock($1);`,
      values: [hash],
    })
    const lockAcquired = result.rows.shift()?.pg_try_advisory_xact_lock || false

    if (!lockAcquired) {
      logSchema.info(logger, `[Upgrade] Lock already acquired for: ${eventId}`, {
        type: 'upgradeEvent',
      })
      await t.commit()
      return
    }

    const id = await t.query<{ event_id: string }>({
      text: `SELECT event_id FROM event_upgrades WHERE event_id = $1 LIMIT 1`,
      values: [eventId],
    })

    if (id.rows.length > 0) {
      await t.commit()
      return
    }

    await fn(t)

    await t.query({
      text: `
        INSERT INTO event_upgrades (event_id)
        VALUES ($1)
        ON CONFLICT (event_id) DO NOTHING
      `,
      values: [eventId],
    })

    logSchema.info(logger, `[Upgrade] Completed upgrade: ${eventId}`, {
      type: 'upgradeEvent',
    })

    await t.commit()
  } catch (e) {
    try {
      await t.rollback()
    } catch (rollbackError) {
      logSchema.warning(logger, '[Upgrade] Failed to rollback transaction', {
        type: 'upgradeEvent',
        error: rollbackError,
        metadata: JSON.stringify({
          queueName: eventId,
          originalError: String(e),
        }),
      })
    }
    throw e
  }
}
