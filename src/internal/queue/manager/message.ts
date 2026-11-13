import type { Codec } from './codec'

/**
 * The header carrying a message's type identifier on the wire. Stamped when a message class
 * declares a `type` and surfaced back on `Message.type` at delivery. An explicitly set header
 * of this name is never overwritten.
 */
export const MESSAGE_TYPE_HEADER = 'message-type'

/**
 * A message to send. One shape on both sides of the manager: a caller produces a
 * `ProduceMessage<T>` holding the decoded payload, and an adapter is asked to send a
 * `ProduceMessage<W>` holding the payload as its codec encoded it.
 */
export interface ProduceMessage<B = unknown> {
  readonly data: B
  /** Grouping key. Adapters with a native notion of it (SQS FIFO message groups) use it. */
  readonly key?: string
  /** Headers delivered with the message. Always present when an adapter sees the message. */
  readonly headers?: Readonly<Record<string, string>>
  /**
   * Producer dedup identity: a second send with the same key within `idempotencyTtlMs` is
   * dropped (the call still succeeds). How far an engine can honour it is the adapter's to
   * document (pg-boss: singleton key and window; SQS: FIFO queues only, fixed five minutes).
   */
  readonly idempotencyKey?: string
  /** The dedup window (ms) `idempotencyKey` applies over. */
  readonly idempotencyTtlMs?: number
  /**
   * Delivery delay (ms). A declared adapter capability (`delayedDelivery`): producing with a
   * delay on an adapter without it throws `NotSupportedError('delay')`.
   */
  readonly delayMs?: number
}

/**
 * A message as the backend delivers it: what was produced plus what the backend stamped on
 * it. Carries no send options, so it is safe to send again as it is (a dead letter).
 */
export interface Message<B = unknown> {
  readonly id: string
  readonly data: B
  readonly key?: string
  readonly headers: Readonly<Record<string, string>>
  /** Backend-assigned creation time, epoch milliseconds. */
  readonly timestamp: number
  /** Delivery attempt: 1 on first delivery, incremented on each redelivery. */
  readonly attempt: number
  /** The message-type identifier (the `message-type` header). The manager derives it; an
   * adapter leaves it unset. */
  readonly type?: string
}

/** What a subscription's `pull` returns: a message leased to this consumer, with the handle that
 * settles it. */
export interface PullMessage<W = unknown> extends Message<W> {
  /** Opaque per-delivery handle used to settle this delivery. */
  readonly receipt: string
}

/** Build the decoded {@link Message} handed to handlers from the encoded one an adapter
 * returned. `T` is the queue's asserted payload type (the single boundary cast). */
export const toMessage = <T = unknown>(pulled: Message<unknown>, codec: Codec): Message<T> => ({
  id: pulled.id,
  data: codec.decode(pulled.data) as T,
  headers: pulled.headers,
  timestamp: pulled.timestamp,
  attempt: pulled.attempt,
  ...(pulled.key !== undefined ? { key: pulled.key } : {}),
  ...(pulled.headers[MESSAGE_TYPE_HEADER] !== undefined
    ? { type: pulled.headers[MESSAGE_TYPE_HEADER] }
    : {}),
})
