import type { SQSClient } from '@aws-sdk/client-sqs'
import type { NackEntry } from '../../manager/adapter'
import type { AdapterEvents } from '../../manager/events'
import type { PullMessage } from '../../manager/message'
import { MAX_BATCH_ENTRIES, mapWithConcurrency, splitIntoChunks } from './client/batching'
import { describeFailedEntries } from './client/errors'
import {
  createSettleCalls,
  type OutgoingMessage,
  sendMessageBatch,
  type VisibilityChange,
} from './client/messages'
import {
  fairGroupOf,
  fifoIdsOf,
  messageAttributesOf,
  parseReceipt,
  parseReceipts,
  type Receipt,
} from './message'
import type { QueueInfo } from './queues'

// Settling deliveries. The subscription's verbs map onto two SQS calls:
//
// - `ack` deletes the message.
// - `release` sets the visibility timeout to zero, so the message is deliverable again at once.
// - `nack` sets the visibility timeout to the retry delay, so the message stays parked in the
//   queue until then. A delivery whose attempts are spent is dead-lettered instead.
// - `heartbeat` restarts the visibility timeout, extending the consumer's lease.

/**
 * The longest a nacked delivery is parked. A visibility timeout is at most twelve hours
 * counted from the receive, so the park leaves an hour for the time the handler already spent
 * on the delivery.
 */
const MAX_RETRY_DELAY_SECONDS = 39_600
/** How long an exhausted delivery is held while it is copied to the dead-letter queue. */
const DEAD_LETTER_HOLD_SECONDS = 60
/** Batch sends of one dead-lettering in flight at once. */
const DEAD_LETTER_SEND_CONCURRENCY = 4
/** Deliveries tracked before the oldest is forgotten, so a subscription's memory stays bounded. */
const MAX_TRACKED_DELIVERIES = 10_000

const dropOldestBeyondLimit = (tracked: Map<string, unknown> | Set<string>): void => {
  for (const oldest of tracked.keys()) {
    if (tracked.size <= MAX_TRACKED_DELIVERIES) return
    tracked.delete(oldest)
  }
}

export interface SettlerConfig {
  readonly sqs: SQSClient
  readonly events: AdapterEvents | undefined
  readonly queue: QueueInfo
  /** The visibility timeout the subscription receives with: the queue's liveness window. */
  readonly visibilityTimeoutSeconds: number
  /** Deliveries allowed per message before it is dead-lettered. Absent means no limit. */
  readonly maxAttempts: number | undefined
  /** Where exhausted messages go. Absent means they are dropped. */
  readonly deadLetter: QueueInfo | undefined
}

/** The settle half of a subscription, plus what `pull` needs from it. */
export interface Settler {
  ack(receipts: readonly string[]): Promise<void>
  release(receipts: readonly string[]): Promise<void>
  nack(entries: readonly NackEntry[]): Promise<void>
  heartbeat(receipts: readonly string[]): Promise<void>
  /**
   * Restart the lease of messages received a while ago, before they are handed out. Resolves
   * with the ones SQS renewed: a message whose lease ran out, or whose renewal SQS did not
   * accept, is left out and redelivers on its own.
   */
  renew(messages: readonly PullMessage<string>[]): Promise<PullMessage<string>[]>
  /** Note the messages a pull is about to hand out. */
  trackDelivered(messages: readonly PullMessage<string>[]): void
  /** Take messages that arrived already past their attempt limit out of the queue. */
  deadLetterExhausted(exhausted: readonly PullMessage<string>[]): Promise<void>
  /** Drop everything tracked, when the subscription closes. */
  clear(): void
}

export function createSettler(config: SettlerConfig): Settler {
  const { sqs, events, queue, visibilityTimeoutSeconds, maxAttempts, deadLetter } = config
  const settleCalls = createSettleCalls({ sqs, queueUrl: queue.url, queue: queue.queue })

  // What the subscription knows about deliveries it handed out and has not seen settled, keyed by
  // receipt handle. A delivery that never settles is dropped once a map is full.
  //
  // `nack` gets receipts and dead-lettering needs the message, so a subscription with an attempt
  // limit keeps the messages it handed out (the same objects, not copies).
  const messages = new Map<string, PullMessage<string>>()
  // A receipt carries the deadline the delivery was received with. A heartbeat moves it, and
  // the new deadline is kept here until the delivery settles.
  const extendedDeadlines = new Map<string, number>()
  // Deliveries this subscription gave back with a release or a parked nack. SQS goes on
  // accepting their receipt handles while the message waits, so without this a repeated
  // settle would make a parked delivery visible early, and a late heartbeat or ack would hide
  // or delete a message the subscription no longer holds.
  const givenBack = new Set<string>()
  // The lease check runs before the request is sent, and the request takes time to arrive, so
  // the lease is treated as lost a little early: a tenth of the timeout, at most a second.
  const safetyMarginMs = Math.min(1_000, visibilityTimeoutSeconds * 100)

  /**
   * Whether this subscription still holds the delivery, and so may change its visibility.
   *
   * SQS accepts a visibility change through a receipt handle whose timeout already ran out,
   * even after the message was handed to another consumer, where it would cut short that
   * consumer's lease. So the subscription fences itself: a delivery past its own lease deadline is
   * no longer released, parked or renewed, it is left to whoever holds it now.
   */
  const stillHolds = (receipt: Receipt): boolean => {
    if (givenBack.has(receipt.handle)) return false
    // A receipt that names no deadline gives nothing to measure against.
    if (receipt.leaseDeadline === 0) return true
    const leaseDeadline = extendedDeadlines.get(receipt.handle) ?? receipt.leaseDeadline
    return Date.now() < leaseDeadline - safetyMarginMs
  }

  const forget = (handles: readonly string[]): void => {
    for (const handle of handles) {
      messages.delete(handle)
      extendedDeadlines.delete(handle)
    }
  }

  const giveBack = (handles: readonly string[]): void => {
    forget(handles)
    for (const handle of handles) givenBack.add(handle)
    dropOldestBeyondLimit(givenBack)
  }

  /** The copy of an exhausted message: same body, headers and key, as a fresh message. */
  const toDeadLetterCopy = (message: PullMessage<string>): OutgoingMessage => {
    const copy = { body: message.data, attributes: messageAttributesOf(message) }
    if (deadLetter?.fifo !== true) return { ...copy, ...fairGroupOf(message.key) }
    // The deduplication id is derived from the delivery, so a second copy of the same
    // delivery is recognized.
    return { ...copy, ...fifoIdsOf(message.key, `${message.id}:${message.attempt}`) }
  }

  /**
   * Take exhausted deliveries out of the queue: copy each to the dead-letter queue when there
   * is one, then delete the original. A crash between the two leaves the original to be
   * dead-lettered again, which duplicates rather than loses.
   */
  const deadLetterExhausted = async (exhausted: readonly PullMessage<string>[]): Promise<void> => {
    if (exhausted.length === 0) return
    if (deadLetter !== undefined) {
      const batches = splitIntoChunks(exhausted.map(toDeadLetterCopy), MAX_BATCH_ENTRIES)
      await mapWithConcurrency(batches, DEAD_LETTER_SEND_CONCURRENCY, async (batch) => {
        const failed = await sendMessageBatch(sqs, deadLetter.url, batch)
        if (failed.length > 0) {
          throw new Error(
            `queue sqs: sending ${failed.length} of ${batch.length} dead letters to ` +
              `"${deadLetter.queue}" failed: ${describeFailedEntries(failed)}`
          )
        }
      })
    }
    const handles = parseReceipts(exhausted.map((message) => message.receipt)).map(
      (receipt) => receipt.handle
    )
    await settleCalls.deleteMessages(handles)
    forget(handles)
    for (const message of exhausted) {
      events?.deadLettered?.({
        queue: queue.queue,
        deadLetter: deadLetter?.queue,
        attempt: message.attempt,
      })
    }
  }

  /**
   * Dead-letter deliveries that `nack` found exhausted, given only their receipt handles.
   *
   * Each is first held with a visibility change. That makes the verdict idempotent: a delivery
   * that was already dead-lettered and deleted no longer accepts the change, and is skipped.
   */
  const deadLetterExhaustedHandles = async (handles: readonly string[]): Promise<void> => {
    if (handles.length === 0) return
    const stillInQueue = await settleCalls.changeVisibility(
      handles.map((handle) => ({ handle, seconds: DEAD_LETTER_HOLD_SECONDS })),
      'a nack'
    )
    const remembered: PullMessage<string>[] = []
    const notRemembered: VisibilityChange[] = []
    for (const handle of stillInQueue) {
      const message = messages.get(handle)
      if (message !== undefined) remembered.push(message)
      else notRemembered.push({ handle, seconds: 0 })
    }
    await deadLetterExhausted(remembered)
    // Without the message there is nothing to copy. The delivery is released instead: the
    // next receive finds it past its attempt limit, where SQS's redrive policy or the pull
    // dead-letters it.
    await settleCalls.changeVisibility(notRemembered, 'a nack')
    giveBack(notRemembered.map((change) => change.handle))
  }

  return {
    async ack(receipts) {
      // A lapsed lease does not stop an ack: the work is done, and deleting the message is
      // right whoever holds it now.
      const handles = parseReceipts(receipts)
        .map((receipt) => receipt.handle)
        .filter((handle) => !givenBack.has(handle))
      await settleCalls.deleteMessages(handles)
      forget(handles)
    },

    // A release makes the message deliverable again, but SQS has already counted the receive
    // and nothing takes that back. So unlike on pg-boss, a released delivery uses one of the
    // queue's attempts: a message handed back often enough (deploys) is
    // dead-lettered without having failed. Queues with a small retry budget feel it first.
    async release(receipts) {
      const parsed = parseReceipts(receipts)
      // A delivery whose lease ran out is already deliverable again; it needs no release.
      const releases = parsed
        .filter(stillHolds)
        .map((receipt) => ({ handle: receipt.handle, seconds: 0 }))
      // On a message that was already deleted the change matches nothing: an ack wins.
      await settleCalls.changeVisibility(releases, 'a release')
      giveBack(parsed.map((receipt) => receipt.handle))
    },

    async nack(entries) {
      // The park is the message's own visibility timeout: nothing is copied and the receive
      // count supplies the next attempt.
      const parks: VisibilityChange[] = []
      const exhaustedHandles = new Set<string>()
      for (const entry of entries) {
        const receipt = parseReceipt(entry.receipt)
        if (receipt === undefined) continue
        if (!stillHolds(receipt)) {
          // Already given back, or its lease ran out: the message is (or will be) deliverable
          // again, and the next receive counts the attempt and decides exhaustion on its own.
          forget([receipt.handle])
        } else if (maxAttempts !== undefined && receipt.attempt >= maxAttempts) {
          exhaustedHandles.add(receipt.handle)
        } else {
          const seconds = Math.max(0, Math.ceil(entry.delayMs / 1000))
          parks.push({
            handle: receipt.handle,
            seconds: Math.min(MAX_RETRY_DELAY_SECONDS, seconds),
          })
        }
      }
      await settleCalls.changeVisibility(parks, 'a nack')
      giveBack(parks.map((park) => park.handle))
      await deadLetterExhaustedHandles([...exhaustedHandles])
    },

    async heartbeat(receipts) {
      const seconds = visibilityTimeoutSeconds
      // A delivery whose lease ran out is not revived: someone else may hold it by now.
      const extensions = parseReceipts(receipts)
        .filter(stillHolds)
        .map((receipt) => ({ handle: receipt.handle, seconds }))
      // The timeout restarts from when SQS applies the change, which is no earlier than now.
      const requestedAt = Date.now()
      const extended = await settleCalls.changeVisibility(extensions, 'a heartbeat')
      for (const handle of extended) extendedDeadlines.set(handle, requestedAt + seconds * 1000)
      dropOldestBeyondLimit(extendedDeadlines)
    },

    async renew(pulled) {
      const held = pulled.flatMap((message) => {
        const receipt = parseReceipt(message.receipt)
        return receipt !== undefined && stillHolds(receipt) ? [{ message, receipt }] : []
      })
      if (held.length === 0) return []
      const requestedAt = Date.now()
      let renewed: readonly string[]
      try {
        renewed = await settleCalls.changeVisibility(
          held.map(({ receipt }) => ({
            handle: receipt.handle,
            seconds: visibilityTimeoutSeconds,
          })),
          'a renewal'
        )
      } catch {
        return []
      }
      const accepted = new Set(renewed)
      for (const handle of accepted) {
        extendedDeadlines.set(handle, requestedAt + visibilityTimeoutSeconds * 1000)
      }
      dropOldestBeyondLimit(extendedDeadlines)
      return held
        .filter(({ receipt }) => accepted.has(receipt.handle))
        .map(({ message }) => message)
    },

    trackDelivered(pulled) {
      if (maxAttempts === undefined) return
      for (const message of pulled) {
        const receipt = parseReceipt(message.receipt)
        if (receipt !== undefined) messages.set(receipt.handle, message)
      }
      dropOldestBeyondLimit(messages)
    },

    deadLetterExhausted,

    clear() {
      messages.clear()
      extendedDeadlines.clear()
      givenBack.clear()
    },
  }
}
