import type { JobInsert, PgBoss } from 'pg-boss'
import type { SendContext } from '../../manager/adapter'
import type { ProduceMessage } from '../../manager/message'
import { wrap } from './message'
import { type PgBossCtx, toDb } from './pg'

/**
 * One batch insert into the queue, inside the caller's transaction when `ctx.ctx` is given.
 *
 * `idempotencyKey` becomes the job's `singleton_key` on every queue policy, and
 * `idempotencyTtlMs` its `singletonSeconds` window: pg-boss's window-dedup unique index has no
 * policy filter, so a `standard` queue dedupes like any other once the window is present.
 * Without `idempotencyTtlMs` NO window dedup engages (pg-boss has no queue-level default).
 *
 * An `exclusive` queue requires a key on every message whatever the window: keyless jobs all
 * collide on the empty singleton key and the engine silently drops the collision. The policy
 * is read from the declared registry (`ctx.queues`), so the guard holds on every producing
 * instance; a send driven without a registry has no declaration to check against.
 *
 * `delayMs` becomes `start_after` (whole seconds, rounded up).
 */
export function sender<W>(boss: PgBoss) {
  return async (
    queue: string,
    messages: readonly ProduceMessage<W>[],
    ctx: SendContext<PgBossCtx>,
    signal?: AbortSignal
  ): Promise<void> => {
    if (messages.length === 0) return
    const exclusive = ctx.queues?.get(queue)?.options?.pgboss?.policy === 'exclusive'
    const jobs = messages.map((message): JobInsert => {
      const { idempotencyKey, idempotencyTtlMs, delayMs } = message
      if (exclusive && idempotencyKey === undefined) {
        throw new Error(
          `queue pgboss: sending to "${queue}" (policy "exclusive") requires an ` +
            `idempotencyKey; keyless jobs all collide on the empty singleton key`
        )
      }
      return {
        data: wrap(message),
        ...(idempotencyKey !== undefined ? { singletonKey: idempotencyKey } : {}),
        ...(idempotencyKey !== undefined && idempotencyTtlMs !== undefined
          ? { singletonSeconds: Math.max(1, Math.ceil(idempotencyTtlMs / 1000)) }
          : {}),
        ...(delayMs !== undefined ? { startAfter: Math.ceil(delayMs / 1000) } : {}),
      }
    })
    signal?.throwIfAborted()
    await boss.insert(queue, jobs, ctx.ctx !== undefined ? { db: toDb(ctx.ctx) } : {})
  }
}
