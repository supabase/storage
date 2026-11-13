import {
  ChangeMessageVisibilityBatchCommand,
  DeleteMessageBatchCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageBatchCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs'
import { MAX_BATCH_ENTRIES, mapWithConcurrency, splitIntoChunks } from './batching'
import {
  describeFailedEntries,
  errorCode,
  type FailedBatchEntry,
  isAbortError,
  isStaleReceiptHandleCode,
} from './errors'

// ── Sending ────────────────────────────────────────────────────────────────────────────────

export interface MessageAttribute {
  DataType: 'String'
  StringValue: string
}
export type MessageAttributes = Record<string, MessageAttribute>

export interface OutgoingMessage {
  readonly body: string
  readonly attributes?: MessageAttributes | undefined
  /** FIFO only: SQS holds a group's other messages back while one of them is in flight. */
  readonly messageGroupId?: string | undefined
  /** FIFO only: a second message with the same id inside five minutes is dropped. */
  readonly deduplicationId?: string | undefined
  /** Standard queues only. */
  readonly delaySeconds?: number | undefined
}

/**
 * One `SendMessageBatch` of at most ten messages. Resolves with the entries SQS failed; a
 * failed entry's `Id` is the message's position in the batch.
 */
export async function sendMessageBatch(
  sqs: SQSClient,
  queueUrl: string,
  batch: readonly OutgoingMessage[],
  signal?: AbortSignal
): Promise<readonly FailedBatchEntry[]> {
  const response = await sqs.send(
    new SendMessageBatchCommand({
      QueueUrl: queueUrl,
      Entries: batch.map((message, position) => ({
        Id: String(position),
        MessageBody: message.body,
        ...(message.attributes !== undefined ? { MessageAttributes: message.attributes } : {}),
        ...(message.messageGroupId !== undefined ? { MessageGroupId: message.messageGroupId } : {}),
        ...(message.deduplicationId !== undefined
          ? { MessageDeduplicationId: message.deduplicationId }
          : {}),
        ...(message.delaySeconds !== undefined ? { DelaySeconds: message.delaySeconds } : {}),
      })),
    }),
    signal !== undefined ? { abortSignal: signal } : {}
  )
  return response.Failed ?? []
}

// ── Receiving ──────────────────────────────────────────────────────────────────────────────

export interface ReceiveRequest {
  readonly queueUrl: string
  /** Messages wanted. One call returns at most ten. */
  readonly max: number
  /** How long the call waits for a message before returning empty (long polling). */
  readonly waitTimeSeconds: number
  /** How long the received messages stay invisible to other consumers. Absent: the queue's. */
  readonly visibilityTimeoutSeconds: number | undefined
  /** Cancels the wait. */
  readonly signal: AbortSignal
}

/**
 * One `ReceiveMessage` call, with every message attribute and the receive count.
 *
 * A cancelled wait resolves empty. A receive SQS had already answered when the cancel landed
 * is not lost: its messages are delivered again after their visibility timeout.
 */
export async function receiveMessages(sqs: SQSClient, request: ReceiveRequest): Promise<Message[]> {
  try {
    const response = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: request.queueUrl,
        MaxNumberOfMessages: Math.min(MAX_BATCH_ENTRIES, Math.max(1, request.max)),
        WaitTimeSeconds: request.waitTimeSeconds,
        ...(request.visibilityTimeoutSeconds !== undefined
          ? { VisibilityTimeout: request.visibilityTimeoutSeconds }
          : {}),
        MessageAttributeNames: ['All'],
        MessageSystemAttributeNames: ['ApproximateReceiveCount', 'SentTimestamp'],
      }),
      { abortSignal: request.signal }
    )
    return response.Messages ?? []
  } catch (error) {
    if (request.signal.aborted || isAbortError(error)) return []
    throw error
  }
}

// ── Settling ───────────────────────────────────────────────────────────────────────────────
//
// The two calls that settle a delivery: deleting the message, and changing its visibility
// timeout. Both take receipt handles in batches of ten and report failures per entry.
//
// Each resolves with the receipt handles SQS accepted. A stale handle (the message was already
// deleted, or is no longer in flight) is left out of the result without an error, because
// settling a delivery the consumer no longer holds is a no-op. Any other failure rejects, so
// the caller's settle retry runs the call again.

/** Batch calls of one settle in flight at once. */
const SETTLE_CONCURRENCY = 4

export interface VisibilityChange {
  readonly handle: string
  /** The new visibility timeout, counted from now. Zero makes the message deliverable at once. */
  readonly seconds: number
}

export interface SettleCalls {
  deleteMessages(handles: readonly string[]): Promise<string[]>
  /** `operation` names what the change is for ("a release", "a nack"…) in an error message. */
  changeVisibility(changes: readonly VisibilityChange[], operation: string): Promise<string[]>
}

/** Sends one batch of at most ten handles and resolves with the entries SQS failed. Entry ids
 * are the handles' positions in the batch. */
type SendBatch = (handles: readonly string[]) => Promise<readonly FailedBatchEntry[]>

export function createSettleCalls(deps: {
  readonly sqs: SQSClient
  readonly queueUrl: string
  /** Named in error messages. */
  readonly queue: string
}): SettleCalls {
  const { sqs, queueUrl, queue } = deps

  /** Settle one batch; resolves with the handles SQS accepted. */
  const settleBatch = async (
    batch: readonly string[],
    sendBatch: SendBatch,
    operation: string
  ): Promise<string[]> => {
    let failedEntries: readonly FailedBatchEntry[]
    try {
      failedEntries = await sendBatch(batch)
    } catch (error) {
      if (!isStaleReceiptHandleCode(errorCode(error))) throw error
      // Some endpoints reject the whole call for one stale handle. A lone handle is that stale
      // one; several are settled one by one so the others still land.
      if (batch.length === 1) return []
      const accepted = await mapWithConcurrency(batch, SETTLE_CONCURRENCY, (handle) =>
        settleBatch([handle], sendBatch, operation)
      )
      return accepted.flat()
    }
    const unexpected = failedEntries.filter((entry) => !isStaleReceiptHandleCode(entry.Code))
    if (unexpected.length > 0) {
      throw new Error(
        `queue sqs: ${operation} failed on "${queue}": ${describeFailedEntries(unexpected)}`
      )
    }
    const stalePositions = new Set(failedEntries.map((entry) => Number(entry.Id)))
    return batch.filter((_, position) => !stalePositions.has(position))
  }

  const settleInBatches = async (
    handles: readonly string[],
    sendBatch: SendBatch,
    operation: string
  ): Promise<string[]> => {
    if (handles.length === 0) return []
    const accepted = await mapWithConcurrency(
      splitIntoChunks(handles, MAX_BATCH_ENTRIES),
      SETTLE_CONCURRENCY,
      (batch) => settleBatch(batch, sendBatch, operation)
    )
    return accepted.flat()
  }

  return {
    deleteMessages(handles) {
      const sendBatch: SendBatch = async (batch) => {
        const response = await sqs.send(
          new DeleteMessageBatchCommand({
            QueueUrl: queueUrl,
            Entries: batch.map((handle, position) => ({
              Id: String(position),
              ReceiptHandle: handle,
            })),
          })
        )
        return response.Failed ?? []
      }
      // A batch may not name one handle twice.
      return settleInBatches([...new Set(handles)], sendBatch, 'an ack')
    },

    changeVisibility(changes, operation) {
      // One change per handle: a handle listed twice keeps its last timeout.
      const secondsByHandle = new Map<string, number>()
      for (const change of changes) secondsByHandle.set(change.handle, change.seconds)
      const sendBatch: SendBatch = async (batch) => {
        const response = await sqs.send(
          new ChangeMessageVisibilityBatchCommand({
            QueueUrl: queueUrl,
            Entries: batch.map((handle, position) => ({
              Id: String(position),
              ReceiptHandle: handle,
              VisibilityTimeout: secondsByHandle.get(handle) ?? 0,
            })),
          })
        )
        return response.Failed ?? []
      }
      return settleInBatches([...secondsByHandle.keys()], sendBatch, operation)
    },
  }
}
