import type { QueueSubscription } from '../adapter'
import type { PullMessage } from '../message'
import { retryDelayOf } from '../registry'
import type { Slot } from './invocation'
import type { WorkerRuntime } from './types'

/** Settles waiting to land, sorted by kind. */
export interface Settles {
  readonly acks: string[]
  readonly nacks: PullMessage[]
  readonly releases: string[]
}

/** Whether the handler failed this delivery. Running out of `consumeTimeout` is a failure
 * like a throw: the handler was given its time and did not finish. A shutdown is not: the
 * worker took the delivery away. */
const failed = (slot: Slot): boolean =>
  slot?.status === 'failed' || (slot?.status === 'released' && slot.reason === 'timeout')

/** Sort one delivery into its settle: ok acks, a failure (a throw or a timeout) under a retry
 * policy nacks, everything else (a failure without a policy, a shutdown, never started)
 * releases. */
export const sortSettle = (
  into: Settles,
  leased: PullMessage,
  slot: Slot,
  hasRetry: boolean
): void => {
  if (slot?.status === 'ok') into.acks.push(leased.receipt)
  else if (failed(slot) && hasRetry) into.nacks.push(leased)
  else into.releases.push(leased.receipt)
}

/**
 * Settle one set of deliveries: acks, then nacks, then releases. The whole failed set goes
 * to the adapter in ONE `nack` call; the adapter counts each entry's budget and dead-letters
 * exhausted messages itself.
 *
 * An infrastructure failure mid-settle best-effort releases what may still be released, then
 * rethrows. A call that threw may nonetheless have landed, which bounds what is safe:
 *
 * - acks and nacks that threw are released: an ack that landed is terminal (a later release
 *   is a no-op, ack wins), and one that did not holds a lease only this worker can free
 *   promptly. The price is that a nack which did land degrades to budget-free redelivery.
 * - a release that threw is never re-issued: if it landed, the adapter may already have
 *   handed the next delivery out under the SAME receipt, and releasing again would release
 *   that new delivery mid-flight. If it did not land, the lease expires and the message
 *   redelivers.
 */
export async function settle(
  rt: Pick<WorkerRuntime, 'queue' | 'retry' | 'events'>,
  subscription: QueueSubscription,
  { acks, nacks, releases }: Settles
): Promise<void> {
  let stage: 'acks' | 'nacks' | 'releases' = 'acks'
  try {
    if (acks.length > 0) await subscription.ack(acks)
    stage = 'nacks'
    if (nacks.length > 0) {
      // Each entry's delay follows the queue's backoff curve at the attempt that failed.
      const entries = nacks.map((r) => ({
        receipt: r.receipt,
        attempt: r.attempt,
        delayMs: retryDelayOf(rt.retry, r.attempt),
      }))
      await subscription.nack(entries.map(({ receipt, delayMs }) => ({ receipt, delayMs })))
      for (const { attempt, delayMs } of entries) {
        rt.events?.retryScheduled?.({ queue: rt.queue, attempt, delayMs })
      }
    }
    stage = 'releases'
    if (releases.length > 0) await subscription.release(releases)
  } catch (error) {
    const unsettled =
      stage === 'releases'
        ? []
        : [...(stage === 'acks' ? acks : []), ...nacks.map((r) => r.receipt), ...releases]
    try {
      if (unsettled.length > 0) await subscription.release(unsettled)
    } catch {
      /* best-effort */
    }
    throw error
  }
}
