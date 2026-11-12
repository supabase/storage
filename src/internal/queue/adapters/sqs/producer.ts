import type { SQSClient } from '@aws-sdk/client-sqs'
import { NotSupportedError, type SendContext } from '../../manager/adapter'
import type { ProduceMessage } from '../../manager/message'
import { MAX_BATCH_ENTRIES, mapWithConcurrency, sleep } from './client/batching'
import { describeFailedEntries, type FailedBatchEntry } from './client/errors'
import { type OutgoingMessage, sendMessageBatch } from './client/messages'
import { fairGroupOf, fifoIdsOf, messageAttributesOf, messageByteSize } from './message'
import type { QueueInfo, Queues } from './queues'

/**
 * The entries of one batch call may total this many bytes. SQS's long-standing limit; where
 * the service accepts more, staying under it only costs an extra call. A single message
 * larger than this goes alone, and the service decides whether it is too large at all.
 */
const MAX_BATCH_BYTES = 262_144
/** The longest per-message delay SQS accepts. */
const MAX_DELAY_MS = 15 * 60_000
/** How many times entries SQS failed on its own side are sent again. */
const SERVICE_FAULT_RETRIES = 3
/** The wait before the first resend; it doubles with each further one. */
const SERVICE_FAULT_BACKOFF_MS = 50
/** Batch calls of one send in flight at once. */
const BATCH_CONCURRENCY = 4

/** A validated message with the bytes it counts toward a batch call's size limit. */
interface SizedMessage extends OutgoingMessage {
  readonly bytes: number
}

/** The delay of a message in whole seconds, or nothing when it has none. */
const delaySecondsOf = (info: QueueInfo, message: ProduceMessage<string>): number | undefined => {
  if (message.delayMs === undefined || message.delayMs <= 0) return undefined
  // A FIFO queue only delays as a whole (its `DelaySeconds` attribute), never per message.
  if (info.fifo) throw new NotSupportedError('delay')
  if (message.delayMs > MAX_DELAY_MS) {
    throw new Error(
      `queue sqs: delayMs ${message.delayMs} on queue "${info.queue}" is longer than the ` +
        `15 minutes SQS can delay a message`
    )
  }
  return Math.ceil(message.delayMs / 1000)
}

const toSizedMessage = (info: QueueInfo, message: ProduceMessage<string>): SizedMessage => {
  if (typeof message.data !== 'string') {
    throw new Error(
      `queue sqs: queue "${info.queue}" got a non-string payload. The SQS body is text, so the ` +
        `adapter's codec must encode to a string`
    )
  }
  const attributes = messageAttributesOf(message)
  return {
    body: message.data,
    attributes,
    // A standard queue cannot deduplicate. Its key (else its idempotency key) is still
    // sent, as the tenant id of SQS fair queues.
    ...(info.fifo
      ? fifoIdsOf(message.key, message.idempotencyKey)
      : fairGroupOf(message.key, message.idempotencyKey)),
    delaySeconds: delaySecondsOf(info, message),
    bytes: messageByteSize(message.data, attributes),
  }
}

/** Split messages into batch calls of at most ten entries and {@link MAX_BATCH_BYTES} each. */
const splitIntoBatchCalls = (messages: readonly SizedMessage[]): SizedMessage[][] => {
  const batches: SizedMessage[][] = []
  let current: SizedMessage[] = []
  let currentBytes = 0
  for (const message of messages) {
    const currentIsFull =
      current.length === MAX_BATCH_ENTRIES || currentBytes + message.bytes > MAX_BATCH_BYTES
    if (current.length > 0 && currentIsFull) {
      batches.push(current)
      current = []
      currentBytes = 0
    }
    current.push(message)
    currentBytes += message.bytes
  }
  if (current.length > 0) batches.push(current)
  return batches
}

export type Send = (
  queue: string,
  messages: readonly ProduceMessage<string>[],
  ctx: SendContext<never>,
  signal?: AbortSignal
) => Promise<void>

/**
 * `send`: one call becomes `SendMessageBatch` calls of up to ten entries. One batch call is
 * all-or-reported, but a send larger than one call is several calls and is not atomic: when
 * it rejects, other batches may have landed.
 */
export function createSend(deps: { readonly sqs: SQSClient; readonly queues: Queues }): Send {
  const { sqs, queues } = deps

  /**
   * Send one batch until every message is accepted. Messages SQS failed for its own reasons
   * are sent again; one it rejected as the sender's fault fails the send at once. A FIFO
   * resend carries the same deduplication id, so it cannot duplicate; a standard one can.
   */
  const sendBatch = async (
    info: QueueInfo,
    batch: readonly OutgoingMessage[],
    signal: AbortSignal | undefined
  ): Promise<void> => {
    let unsent = batch
    for (let resends = 0; ; resends++) {
      const failures: readonly FailedBatchEntry[] = await sendMessageBatch(
        sqs,
        info.url,
        unsent,
        signal
      )
      if (failures.length === 0) return
      const senderIsAtFault = failures.some((failure) => failure.SenderFault === true)
      if (senderIsAtFault || resends === SERVICE_FAULT_RETRIES) {
        throw new Error(
          `queue sqs: ${failures.length} of ${batch.length} messages were not accepted on ` +
            `queue "${info.queue}" (${describeFailedEntries(failures)})`
        )
      }
      // A failed entry's id is the message's position in the batch it was sent in.
      const sent = unsent
      unsent = failures.map((failure) => sent[Number(failure.Id)] as OutgoingMessage)
      await sleep(SERVICE_FAULT_BACKOFF_MS * 2 ** resends)
    }
  }

  return async (queue, messages, ctx, signal) => {
    if (messages.length === 0) return
    const info = await queues.resolve(queue, ctx.queues)
    // Validate and shape everything before the first call, so a bad message rejects the send
    // with nothing sent.
    const outgoing = messages.map((message) => toSizedMessage(info, message))
    await mapWithConcurrency(splitIntoBatchCalls(outgoing), BATCH_CONCURRENCY, (batch) =>
      sendBatch(info, batch, signal)
    )
  }
}
