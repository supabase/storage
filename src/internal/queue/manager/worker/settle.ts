import type { QueueSubscription } from '../adapter'
import type { PullMessage } from '../message'
import type { Slot } from './invocation'
import type { WorkerRuntime } from './types'

/** Settles waiting to land, sorted by kind. */
export interface Settles {
  readonly acks: string[]
  readonly retries: PullMessage[]
  readonly nacks: string[]
}

/** Sort one delivery into its settle: ok acks, a failure under a retry policy retries,
 * everything else (a failure without a policy, a release, never started) nacks. */
export const sortSettle = (
  into: Settles,
  leased: PullMessage,
  slot: Slot,
  hasRetry: boolean
): void => {
  if (slot?.status === 'ok') into.acks.push(leased.receipt)
  else if (slot?.status === 'failed' && hasRetry) into.retries.push(leased)
  else into.nacks.push(leased.receipt)
}

/**
 * Settle one set of deliveries: acks, then retries, then releases. The whole failed set goes
 * to the adapter's retry machinery in ONE call; the adapter counts each entry's budget and
 * dead-letters exhausted messages itself.
 *
 * An infrastructure failure mid-settle best-effort releases what may still be released, then
 * rethrows. A call that threw may nonetheless have landed, which bounds what is safe:
 *
 * - acks and retries that threw are released: an ack that landed is terminal (a later nack
 *   is a no-op, ack wins), and one that did not holds a lease only this worker can free
 *   promptly. The price is that a retry which did land degrades to budget-free redelivery.
 * - a nack that threw is never re-issued: if it landed, the adapter may already have handed
 *   the next delivery out under the SAME receipt, and nacking again would release that new
 *   delivery mid-flight. If it did not land, the lease expires and the message redelivers.
 */
export async function settle(
  rt: Pick<WorkerRuntime, 'queue' | 'retry' | 'events'>,
  subscription: QueueSubscription,
  { acks, retries, nacks }: Settles
): Promise<void> {
  let stage: 'acks' | 'retries' | 'nacks' = 'acks'
  try {
    if (acks.length > 0) await subscription.ack(acks)
    stage = 'retries'
    if (retries.length > 0) {
      const delayMs = rt.retry?.backoffMs ?? 0
      await subscription.retry(retries.map((r) => ({ receipt: r.receipt, delayMs })))
      for (const r of retries) {
        rt.events?.retryScheduled?.({ queue: rt.queue, attempt: r.attempt, delayMs })
      }
    }
    stage = 'nacks'
    if (nacks.length > 0) await subscription.nack(nacks)
  } catch (error) {
    const unsettled =
      stage === 'nacks'
        ? []
        : [...(stage === 'acks' ? acks : []), ...retries.map((r) => r.receipt), ...nacks]
    try {
      if (unsettled.length > 0) await subscription.nack(unsettled)
    } catch {
      /* best-effort */
    }
    throw error
  }
}
