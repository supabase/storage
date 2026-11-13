import type { PgBoss } from 'pg-boss'
import { livenessOf, type QueueOptions } from '../../manager/registry'
import { type QueueDefaults, REJECTED_POLICIES, withRetryDefaults } from './options'

/**
 * A queue's declared options as pg-boss queue columns: the adapter defaults, then the
 * `pgboss` extension, then the declared policy.
 *
 * Liveness becomes `heartbeatSeconds` (floored at 10s by the engine): fetch stamps
 * `heartbeat_on`, so a consumer that never beats fails on that schedule while a beating one
 * is renewed by `touch()`. Expiration keeps its native meaning, the hard cap on one attempt,
 * lifted to pg-boss's 24h maximum when nothing declares it so a heartbeating long job
 * outlives the engine's 15-minute default.
 *
 * pg-boss copies these columns onto each job at INSERT and issues its verdicts from the job
 * row, so a job queued under an older policy keeps that policy.
 */
const columnsOf = (queueDefaults: QueueDefaults, options: QueueOptions | undefined) => {
  const { policy: _immutable, ...ext } = options?.pgboss ?? {}
  const retry = options?.retry
  return {
    ...withRetryDefaults(queueDefaults),
    ...ext,
    heartbeatSeconds: Math.max(10, Math.ceil(livenessOf(options) / 1000)),
    expireInSeconds: ext.expireInSeconds ?? queueDefaults.expireInSeconds ?? 86_399,
    ...(retry !== undefined
      ? {
          retryLimit: Math.max(0, retry.maxAttempts - 1),
          retryDelay: Math.ceil((retry.backoffMs ?? 0) / 1000),
          retryBackoff: false,
          ...(retry.deadLetter !== undefined ? { deadLetter: retry.deadLetter } : {}),
        }
      : {}),
  } satisfies QueueDefaults
}

export interface Provisioner {
  /**
   * Create the queue with its declared options, or bring an existing one in line with them
   * (how a changed policy rolls out). An options-less call only makes sure the queue exists.
   * A declared dead-letter queue must already exist. The pg-boss `policy` is immutable: a
   * declared one differing from the stored one throws.
   */
  provision(queue: string, options?: QueueOptions): Promise<void>
  /** Provision the queue only if it does not exist yet; an existing one is not rewritten. */
  ensure(queue: string, options?: QueueOptions): Promise<void>
}

export function provisioner(boss: PgBoss, queueDefaults: QueueDefaults): Provisioner {
  const provision = async (queue: string, options?: QueueOptions): Promise<void> => {
    const policy = options?.pgboss?.policy
    if (policy !== undefined && REJECTED_POLICIES.includes(policy)) {
      throw new Error(`queue pgboss: policy "${policy}" is not supported (queues are unordered)`)
    }
    const columns = columnsOf(queueDefaults, options)
    // A no-op when the queue exists, which is why the declared options are then written again.
    await boss.createQueue(queue, { ...columns, ...(policy !== undefined ? { policy } : {}) })
    if (options === undefined) return
    if (policy !== undefined) {
      const existing = await boss.getQueue(queue)
      if (existing !== null && existing.policy !== policy) {
        throw new Error(
          `queue pgboss: queue "${queue}" exists with policy "${existing.policy}"; a queue's ` +
            `policy is immutable after its first creation (declared "${policy}")`
        )
      }
    }
    // `null` clears a dead-letter target the declaration no longer names.
    await boss.updateQueue(queue, { ...columns, deadLetter: columns.deadLetter ?? null })
  }

  return {
    provision,
    async ensure(queue, options) {
      if ((await boss.getQueue(queue)) === null) await provision(queue, options)
    },
  }
}
