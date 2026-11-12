import type { PgBoss } from 'pg-boss'
import type { QueueSubscription } from '../../manager/adapter'
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
  queue: string
): QueueSubscription<W> {
  let closed = false
  return {
    async pull(max) {
      if (closed) return []
      const jobs = await boss.fetch<unknown>(queue, {
        batchSize: Math.max(1, max),
        includeMetadata: true,
      })
      return jobs.map((job) => toPullMessage<W>(job))
    },
    ack: async (receipts) => (closed ? undefined : settle.ack(boss, queue, receipts)),
    nack: async (entries) => (closed ? undefined : settle.nack(boss, queue, entries)),
    release: async (receipts) =>
      closed ? undefined : settle.release(boss, schema, queue, receipts),
    heartbeat: async (receipts) => (closed ? undefined : settle.heartbeat(boss, queue, receipts)),
    async close() {
      closed = true
    },
  }
}
