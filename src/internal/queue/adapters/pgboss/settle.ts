import type { ConnectionOptions, Db, PgBoss } from 'pg-boss'
import type { NackEntry } from '../../manager/adapter'
import { deliveriesOf } from './message'
import { hasTransaction } from './pg'

// A receipt names one delivery (job id + attempt + start time). release is fenced to exactly
// that delivery. nack and heartbeat are fenced to the attempt: pg-boss applies them only
// while the job is still active on that attempt, so a stale or repeated one is a no-op. The
// attempt alone cannot tell a delivery from a later one that followed a RELEASE of the same
// attempt, so a nack or heartbeat issued for a delivery this consumer already released could
// land on that later delivery (failing it once, or renewing it). The manager settles each
// delivery exactly once, so that order never occurs.

/**
 * Ack goes by job id, unfenced, with `includeQueued`: the work is done whichever attempt
 * reports it, so an ack completes the job even after a raced release or an expiry re-queued it.
 * Completed rows are unaffected.
 */
export const ack = async (boss: PgBoss, queue: string, receipts: readonly string[]) => {
  const ids = [...new Set(deliveriesOf(receipts).map((delivery) => delivery.id))]
  if (ids.length > 0) await boss.complete(queue, ids, undefined, { includeQueued: true })
}

/**
 * Release: nothing failed, so the job goes back to the queue WITHOUT consuming retry budget
 * and is deliverable at once (`start_after` is untouched and already past). pg-boss has no
 * call for this (`fail()` charges the budget and dead-letters at the limit), hence the raw
 * statement. Fetch advances `retry_count` only when `started_on` is set, so clearing it makes
 * the next delivery the same attempt number.
 *
 * Fenced on the exact delivery: still active, same attempt, same start time. A repeated or
 * stale release, or one for an acked job, matches nothing. `started_on` is compared at the
 * millisecond the driver hands back, so a stale release could only match a later delivery
 * fetched in the very millisecond of the one it names, and would merely release it.
 *
 * On a `stately` queue the move to `retry` can collide with another job of the same key
 * already in `retry`, exactly as pg-boss's own `fail()` does.
 */
export const release = async (
  boss: PgBoss,
  schema: string,
  queue: string,
  receipts: readonly string[]
) => {
  const deliveries = deliveriesOf(receipts)
  if (deliveries.length === 0) return
  await boss.getDb().executeSql(
    `update "${schema}".job as job set state = 'retry', started_on = null
       from unnest($2::uuid[], $3::int[], $4::bigint[]) as delivery(id, retry_count, started_ms)
      where job.name = $1 and job.id = delivery.id and job.state = 'active'
        and job.retry_count = delivery.retry_count
        and floor(extract(epoch from job.started_on) * 1000) = delivery.started_ms`,
    [
      queue,
      deliveries.map((delivery) => delivery.id),
      deliveries.map((delivery) => delivery.retryCount),
      deliveries.map((delivery) => delivery.startedMs),
    ]
  )
}

/**
 * The handler failed. `fail()` runs pg-boss's own verdict per job: re-queue while `retry_count < retry_limit` (the
 * queue's declared budget), dead-letter into the queue's `deadLetter` at it. One call for the
 * whole batch. A delayed entry's delay then rides `start_after` on the re-queued row, per job
 * (pg-boss has no batch update), and only for the jobs this call actually failed; `update()`
 * matches queued jobs only, so a dead-lettered one is left alone. With delays the two steps
 * share one transaction on pg-boss's own pool when available; the fallback's window (a
 * competing fetch between them) degrades to an early redelivery.
 */
export const nack = async (boss: PgBoss, queue: string, entries: readonly NackEntry[]) => {
  const attempts = deliveriesOf(entries.map((entry) => entry.receipt))
  if (attempts.length === 0) return
  const delays = new Map<string, number>()
  for (const entry of entries) {
    const id = entry.receipt.slice(0, entry.receipt.indexOf(':')).toLowerCase()
    if (entry.delayMs > 0 && !delays.has(id)) delays.set(id, entry.delayMs)
  }

  const park = async (db?: Db): Promise<void> => {
    const conn: ConnectionOptions = db !== undefined ? { db } : {}
    const failed = (await boss.fail(queue, attempts, undefined, conn)) as { settled?: string[] }
    for (const id of failed.settled ?? []) {
      const delayMs = delays.get(id)
      if (delayMs === undefined) continue
      await boss.update(queue, undefined, {
        id,
        startAfter: new Date(Date.now() + delayMs),
        ...conn,
      })
    }
  }
  const db = boss.getDb()
  if (delays.size > 0 && hasTransaction(db)) await db.withTransaction((tx) => park(tx))
  else await park()
}

/**
 * `touch()` re-stamps `heartbeat_on`, so a job whose queue declared `heartbeatSeconds`
 * outlives that window while it beats. `expireInSeconds` is NOT extended: it stays the hard
 * cap on one attempt.
 */
export const heartbeat = async (boss: PgBoss, queue: string, receipts: readonly string[]) => {
  const attempts = deliveriesOf(receipts)
  if (attempts.length > 0) await boss.touch(queue, attempts)
}
