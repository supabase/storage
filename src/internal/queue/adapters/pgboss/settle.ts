import type { ConnectionOptions, Db, PgBoss } from 'pg-boss'
import type { RetryEntry } from '../../manager/adapter'
import { attemptsOf } from './message'
import { hasTransaction } from './pg'

// A receipt names one delivery (job id + attempt). nack, retry and heartbeat are FENCED to it:
// pg-boss applies them only while the job is still active on that attempt, so a stale or
// repeated settle is a no-op.

/**
 * Ack goes by job id, unfenced, with `includeQueued`: the work is done whichever attempt
 * reports it, so an ack completes the job even after a raced nack or an expiry re-queued it.
 * Completed rows are unaffected.
 */
export const ack = async (boss: PgBoss, queue: string, receipts: readonly string[]) => {
  const ids = [...new Set(attemptsOf(receipts).map((attempt) => attempt.id))]
  if (ids.length > 0) await boss.complete(queue, ids, undefined, { includeQueued: true })
}

/** Release = `fail()`: the same job goes back to `retry`, due after the queue's `retryDelay`,
 * and consumes one unit of its retry budget. */
export const nack = async (boss: PgBoss, queue: string, receipts: readonly string[]) => {
  const attempts = attemptsOf(receipts)
  if (attempts.length > 0) await boss.fail(queue, attempts)
}

/**
 * `fail()` runs pg-boss's own verdict per job: re-queue while `retry_count < retry_limit` (the
 * budget the subscribe wrote onto the queue), dead-letter into the queue's `deadLetter` at it.
 * One call for the whole batch. A delayed entry's delay then rides `start_after` on the
 * re-queued row, per job (pg-boss has no batch update), and only for the jobs this call
 * actually failed; `update()` matches queued jobs only, so a dead-lettered one is left alone.
 * With delays the two steps share one transaction on pg-boss's own pool when available; the
 * fallback's window (a competing fetch between them) degrades to an early redelivery.
 */
export const retry = async (boss: PgBoss, queue: string, entries: readonly RetryEntry[]) => {
  const attempts = attemptsOf(entries.map((entry) => entry.receipt))
  if (attempts.length === 0) return
  const delays = new Map<string, number>()
  for (const entry of entries) {
    const id = entry.receipt.slice(0, entry.receipt.lastIndexOf(':')).toLowerCase()
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
  const attempts = attemptsOf(receipts)
  if (attempts.length > 0) await boss.touch(queue, attempts)
}
