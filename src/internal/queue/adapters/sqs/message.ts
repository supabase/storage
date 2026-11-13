import { createHash, randomUUID } from 'node:crypto'
import type { Message as SqsMessage } from '@aws-sdk/client-sqs'
import type { ProduceMessage, PullMessage } from '../../manager/message'
import type { MessageAttributes } from './client/messages'

// The wire format. The body is the codec's text, verbatim, so anything else reading the queue
// sees the payload and nothing adapter-specific. What the adapter adds rides in two message
// attributes:
//
// - `queue.headers`: the whole header map as one JSON string. SQS allows ten attributes per
//   message, far fewer than middleware may add headers, so they share one.
// - `queue.key`: the message key.
//
// A message carrying neither came from another producer. It delivers as a bare payload, with
// its own string attributes as headers.

export const HEADERS_ATTRIBUTE = 'queue.headers'
export const KEY_ATTRIBUTE = 'queue.key'
const RESERVED_ATTRIBUTE_PREFIX = 'queue.'

const isEmptyObject = (value: object): boolean => {
  for (const _ in value) return false
  return true
}

/** The attributes of a message being sent, or `undefined` when it needs none. */
export const messageAttributesOf = (
  message: Pick<ProduceMessage<string>, 'headers' | 'key'>
): MessageAttributes | undefined => {
  const attributes: MessageAttributes = {}
  // An attribute value may not be empty, so an empty header map or key is left off.
  if (message.headers !== undefined && !isEmptyObject(message.headers)) {
    attributes[HEADERS_ATTRIBUTE] = {
      DataType: 'String',
      StringValue: JSON.stringify(message.headers),
    }
  }
  if (message.key !== undefined && message.key !== '') {
    attributes[KEY_ATTRIBUTE] = { DataType: 'String', StringValue: message.key }
  }
  return isEmptyObject(attributes) ? undefined : attributes
}

/** Bytes a message counts toward SQS's size limits: its body plus every attribute's name,
 * type and value. */
export const messageByteSize = (
  body: string,
  attributes: MessageAttributes | undefined
): number => {
  let bytes = Buffer.byteLength(body)
  for (const name in attributes) {
    const attribute = attributes[name] as MessageAttributes[string]
    bytes +=
      Buffer.byteLength(name) + attribute.DataType.length + Buffer.byteLength(attribute.StringValue)
  }
  return bytes
}

// ── FIFO identifiers ───────────────────────────────────────────────────────────────────────

// A message group id and a deduplication id are at most 128 characters of printable ASCII
// without spaces.
const VALID_FIFO_IDENTIFIER = /^[\x21-\x7e]{1,128}$/

/**
 * Turn any string into a valid FIFO identifier. A value that already fits is used as is, so
 * it stays readable in the AWS console; anything else is hashed.
 */
export const toFifoIdentifier = (value: string): string =>
  VALID_FIFO_IDENTIFIER.test(value)
    ? value
    : `sha256:${createHash('sha256').update(value).digest('hex')}`

/**
 * The two ids every message on a FIFO queue carries. A keyless message's group is its own
 * deduplication id: a group to itself, and the same one when the message is sent again, so
 * the resend is recognised whether the queue deduplicates queue-wide or per message group.
 */
/**
 * The group of a message on a STANDARD queue: its key, else its idempotency key. SQS treats
 * it as a tenant id there (fair queues): when one group builds a backlog or is slow to
 * process, the other groups' messages are delivered first. Nothing is ordered or held back,
 * and a message with neither counts as a tenant of its own.
 */
export const fairGroupOf = (
  key: string | undefined,
  idempotencyKey?: string
): { messageGroupId?: string } => {
  const group = key !== undefined && key !== '' ? key : idempotencyKey
  return group !== undefined && group !== '' ? { messageGroupId: toFifoIdentifier(group) } : {}
}

/**
 * The two ids every message on a FIFO queue carries. SQS holds a group back while one of its
 * messages is in flight, so the group is the narrowest identity the message has: its
 * idempotency key (also its deduplication id), else its key, else an id of its own. A resend
 * lands in the same group, so it is recognised whether the queue deduplicates queue-wide or
 * per message group.
 */
export const fifoIdsOf = (
  key: string | undefined,
  idempotencyKey: string | undefined
): { messageGroupId: string; deduplicationId: string } => {
  const deduplicationId =
    idempotencyKey !== undefined ? toFifoIdentifier(idempotencyKey) : randomUUID()
  const keyed = idempotencyKey === undefined && key !== undefined && key !== ''
  return { messageGroupId: keyed ? toFifoIdentifier(key) : deduplicationId, deduplicationId }
}

// ── Receiving ──────────────────────────────────────────────────────────────────────────────

type ReceivedAttributes = NonNullable<SqsMessage['MessageAttributes']>

/** Decode the packed headers. Anything that is not a JSON object of strings yields no
 * headers: the payload is delivered rather than the whole receive failed. */
const parsePackedHeaders = (packed: string): Record<string, string> => {
  let parsed: unknown
  try {
    parsed = JSON.parse(packed)
  } catch {
    return {}
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return {}
  const headers: Record<string, string> = {}
  for (const [name, value] of Object.entries(parsed)) {
    if (typeof value === 'string') headers[name] = value
  }
  return headers
}

const headersOf = (attributes: ReceivedAttributes): Record<string, string> => {
  const packed = attributes[HEADERS_ATTRIBUTE]?.StringValue
  if (packed !== undefined) return parsePackedHeaders(packed)
  // Another producer's message: its own text attributes are the closest thing to headers.
  const headers: Record<string, string> = {}
  for (const name in attributes) {
    const attribute = attributes[name]
    if (name.startsWith(RESERVED_ATTRIBUTE_PREFIX) || attribute?.StringValue === undefined) continue
    if (attribute.DataType?.startsWith('Binary')) continue
    headers[name] = attribute.StringValue
  }
  return headers
}

/**
 * A received SQS message as a subscription hands it out. The attempt is SQS's receive count, so a
 * release, a crash and a nack all advance it. `leaseDeadline` is when the visibility timeout
 * runs out by this process's clock (0 when unknown).
 */
export const toPullMessage = (received: SqsMessage, leaseDeadline = 0): PullMessage<string> => {
  const attributes = received.MessageAttributes ?? {}
  const system = received.Attributes ?? {}
  const attempt = Math.max(1, Number(system.ApproximateReceiveCount ?? 1) || 1)
  const key = attributes[KEY_ATTRIBUTE]?.StringValue
  return {
    id: received.MessageId ?? '',
    data: received.Body ?? '',
    headers: headersOf(attributes),
    timestamp: Number(system.SentTimestamp ?? Date.now()),
    attempt,
    receipt: formatReceipt({ attempt, leaseDeadline, handle: received.ReceiptHandle ?? '' }),
    ...(key !== undefined ? { key } : {}),
  }
}

// ── Receipts ───────────────────────────────────────────────────────────────────────────────

/**
 * The receipt a delivery is settled with: `<attempt>:<lease deadline>:<receipt handle>`.
 *
 * - The attempt lets `nack` tell an exhausted delivery from the receipt alone.
 * - The lease deadline is when the delivery's visibility timeout runs out by this process's
 *   clock (0 when unknown). Past it SQS may have handed the message to someone else, and the
 *   subscription stops changing its visibility.
 * - The receipt handle is what SQS itself settles the delivery by.
 */
export interface Receipt {
  readonly attempt: number
  readonly leaseDeadline: number
  readonly handle: string
}

export const formatReceipt = (receipt: Receipt): string =>
  `${receipt.attempt}:${receipt.leaseDeadline}:${receipt.handle}`

/** Parse a receipt, or `undefined` when the string is not one this adapter issued. */
export const parseReceipt = (receipt: string): Receipt | undefined => {
  const firstColon = receipt.indexOf(':')
  const secondColon = firstColon === -1 ? -1 : receipt.indexOf(':', firstColon + 1)
  if (firstColon <= 0 || secondColon === -1) return undefined
  const deadlineText = receipt.slice(firstColon + 1, secondColon)
  const handle = receipt.slice(secondColon + 1)
  if (deadlineText === '' || handle === '') return undefined
  const attempt = Number(receipt.slice(0, firstColon))
  const leaseDeadline = Number(deadlineText)
  if (!Number.isInteger(attempt) || attempt < 1) return undefined
  if (!Number.isInteger(leaseDeadline) || leaseDeadline < 0) return undefined
  return { attempt, leaseDeadline, handle }
}

/** Parse many receipts, leaving out any this adapter did not issue: settling those is a no-op. */
export const parseReceipts = (receipts: readonly string[]): Receipt[] => {
  const parsed: Receipt[] = []
  for (const receipt of receipts) {
    const one = parseReceipt(receipt)
    if (one !== undefined) parsed.push(one)
  }
  return parsed
}
