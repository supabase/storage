import type { JobWithMetadata, PgBoss } from 'pg-boss'
import type { QueueSubscription, RetryPolicy } from '../../manager/adapter'
import { toPullMessage } from './message'
import * as settle from './settle'

/**
 * A subscription over one pg-boss queue: competing consumers arbitrated by pg-boss's
 * `FOR UPDATE SKIP LOCKED` fetch. Pulls and settles are stateless calls on the queue, so the
 * subscription itself is what goes quiet after `close()`: it pulls nothing and settles nothing,
 * and the jobs it still held redeliver when their heartbeat or expiration window lapses.
 */
export function createSubscription<W>(
  boss: PgBoss,
  schema: string,
  queue: string,
  retry: RetryPolicy | undefined
): QueueSubscription<W> {
  let closed = false

  /**
   * pg-boss copies a queue's retry budget and dead-letter target onto each job at INSERT and
   * issues its verdict from the job row. A job sent before this consumer's policy reached the
   * queue (the first subscribe, a changed policy) still carries the old one, so it is
   * re-stamped when it is pulled. Jobs that already match, the steady state, cost nothing.
   */
  const applyRetryPolicy = async (jobs: readonly JobWithMetadata<unknown>[]): Promise<void> => {
    if (retry === undefined) return
    const retryLimit = Math.max(0, retry.maxAttempts - 1)
    const deadLetter = retry.deadLetter ?? null
    const stale = jobs.filter(
      (job) => job.retryLimit !== retryLimit || (job.deadLetter ?? null) !== deadLetter
    )
    if (stale.length === 0) return
    await boss.getDb().executeSql(
      `update "${schema}".job set retry_limit = $3, dead_letter = $4
          where name = $1 and id = any($2::uuid[]) and state = 'active'`,
      [queue, stale.map((job) => job.id), retryLimit, deadLetter]
    )
  }

  return {
    async pull(max) {
      if (closed) return []
      const jobs = await boss.fetch<unknown>(queue, {
        batchSize: Math.max(1, max),
        includeMetadata: true,
      })
      await applyRetryPolicy(jobs)
      return jobs.map((job) => toPullMessage<W>(job))
    },
    ack: async (receipts) => (closed ? undefined : settle.ack(boss, queue, receipts)),
    nack: async (receipts) => (closed ? undefined : settle.nack(boss, queue, receipts)),
    retry: async (entries) => (closed ? undefined : settle.retry(boss, queue, entries)),
    heartbeat: async (receipts) => (closed ? undefined : settle.heartbeat(boss, queue, receipts)),
    async close() {
      closed = true
    },
  }
}
