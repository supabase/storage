import type { PgBoss } from 'pg-boss'
import type { QueueSession } from '../../manager/adapter'
import { toPullMessage } from './message'
import * as settle from './settle'

/**
 * A session over one pg-boss queue: competing consumers arbitrated by pg-boss's
 * `FOR UPDATE SKIP LOCKED` fetch. Pulls and settles are stateless calls on the queue, so the
 * session itself is what goes quiet after `close()`: it pulls nothing and settles nothing,
 * and the jobs it still held redeliver when their heartbeat or expiration window lapses.
 */
export function queueSession<W>(boss: PgBoss, queue: string): QueueSession<W> {
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
    nack: async (receipts) => (closed ? undefined : settle.nack(boss, queue, receipts)),
    retry: async (entries) => (closed ? undefined : settle.retry(boss, queue, entries)),
    heartbeat: async (receipts) => (closed ? undefined : settle.heartbeat(boss, queue, receipts)),
    async close() {
      closed = true
    },
  }
}
