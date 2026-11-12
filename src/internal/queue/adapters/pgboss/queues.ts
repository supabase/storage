import type { PgBoss } from 'pg-boss'
import type { SubscribePolicy } from '../../manager/adapter'
import type { QueueOptions } from '../../manager/registry'
import { type QueueDefaults, REJECTED_POLICIES, withRetryDefaults } from './options'

export interface Provisioner {
  /** Create the queue with its declared options. Idempotent: an existing queue is left as it
   * is, except that a declared policy differing from the stored one throws. */
  provision(queue: string, options?: QueueOptions): Promise<void>
  /** What a subscribe needs before it may pull: the queue, its dead-letter queue, and the
   * consumer's liveness/retry policy written onto the queue. */
  prepare(queue: string, policy: SubscribePolicy): Promise<void>
}

export function provisioner(boss: PgBoss, queueDefaults: QueueDefaults): Provisioner {
  const provision = async (queue: string, options?: QueueOptions): Promise<void> => {
    const { policy, ...ext } = options?.pgboss ?? {}
    if (policy !== undefined && REJECTED_POLICIES.includes(policy)) {
      throw new Error(`queue pgboss: policy "${policy}" is not supported (queues are unordered)`)
    }
    await boss.createQueue(queue, {
      ...withRetryDefaults(queueDefaults),
      ...ext,
      ...(policy !== undefined ? { policy } : {}),
    })
    // createQueue is a no-op on conflict, so a policy changed in code would silently never
    // apply: read it back and throw on drift.
    if (policy !== undefined) {
      const existing = await boss.getQueue(queue)
      if (existing !== null && existing.policy !== policy) {
        throw new Error(
          `queue pgboss: queue "${queue}" exists with policy "${existing.policy}"; a queue's ` +
            `policy is immutable after its first creation (declared "${policy}")`
        )
      }
    }
  }

  const prepare = async (queue: string, policy: SubscribePolicy): Promise<void> => {
    const { retry, livenessTimeoutMs, queues } = policy
    const declared = (name: string): QueueOptions | undefined => queues?.get(name)?.options
    // The dead-letter queue first: pg-boss requires it to exist before a queue references it.
    if (retry?.deadLetter !== undefined) {
      await provision(retry.deadLetter, declared(retry.deadLetter))
    }
    await provision(queue, declared(queue))

    const { policy: _immutable, ...ext } = declared(queue)?.pgboss ?? {}
    // The consumer's policy in pg-boss's own queue columns. Liveness becomes
    // `heartbeatSeconds` (floored at 10s by the engine): fetch stamps `heartbeat_on`, so a
    // consumer that never beats fails on that schedule while a beating one is renewed by
    // `touch()`. Expiration keeps its native meaning, the hard cap on one attempt. These
    // columns are copied onto each job row at INSERT, so they govern jobs sent from now on.
    const overrides: QueueDefaults = {
      ...(livenessTimeoutMs !== undefined
        ? {
            heartbeatSeconds: Math.max(10, Math.ceil(livenessTimeoutMs / 1000)),
            ...((ext.expireInSeconds ?? queueDefaults.expireInSeconds) === undefined
              ? { expireInSeconds: 86_399 }
              : {}),
          }
        : {}),
      ...(retry !== undefined
        ? {
            retryLimit: Math.max(0, retry.maxAttempts - 1),
            retryDelay: Math.ceil((retry.backoffMs ?? 0) / 1000),
            retryBackoff: false,
            ...(retry.deadLetter !== undefined ? { deadLetter: retry.deadLetter } : {}),
          }
        : {}),
    }
    if (Object.keys(overrides).length > 0) {
      // Re-layer the defaults and the queue's declared options under the overrides so this
      // update cannot clobber per-queue config.
      await boss.updateQueue(queue, { ...queueDefaults, ...ext, ...overrides })
    }
  }

  return { provision, prepare }
}
