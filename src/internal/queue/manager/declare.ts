import { MESSAGE_TYPE_HEADER, type ProduceMessage } from './message'
import type { AnyMessageClass, MessagePayload } from './registry'
import type { BatchContext, JobContext, SubscribeOptions } from './worker/types'

/** Marks instances built by `defineMessage` classes, so produce can recognise them by shape. */
const MESSAGE_BRAND = Symbol.for('queue.message')

/**
 * A produce-ready message instance: the payload plus its per-message options. Which queue it
 * goes to is NOT on the instance: the registry binds each message class to a queue.
 */
export interface MessageInstance<T = unknown> extends ProduceMessage<T> {
  /**
   * Message-type identifier (e.g. `"order.placed"`), declared as a literal-typed member on
   * the message class. Optional, but it makes same-shaped classes distinguishable at compile
   * time and is stamped into the `message-type` header, surfacing as `Message.type`.
   */
  readonly type?: string
}

/** Per-instance overrides accepted by a message-class constructor. Each wins over the class's
 * own derivation or default. */
export type MessageInit = Omit<ProduceMessage<unknown>, 'data'>

/** Class-level defaults declared once in {@link defineMessage}, applied to every instance. */
export interface MessageDefaults<T> {
  /** Derive the grouping key from the payload (an explicit `init.key` wins). */
  readonly key?: (data: T) => string
  /** Headers stamped on every instance (instance headers merge over them). */
  readonly headers?: Readonly<Record<string, string>>
  /** Derive the producer dedup identity from the payload (an explicit `init` value wins). */
  readonly idempotencyKey?: (data: T) => string
  /** The dedup window (ms) `idempotencyKey` applies over. */
  readonly idempotencyTtlMs?: number
}

/** The constructor {@link defineMessage} returns. */
export interface MessageClass<T = unknown> {
  new (data: T, init?: MessageInit): MessageInstance<T>
}

/**
 * Declare a message class: a payload type with optional class-level defaults. The class
 * carries NO queue name; the registry binds it to one (`queue(OrderPlaced)`).
 *
 * ```ts
 * class OrderPlaced extends defineMessage<Order>({ key: (o) => o.orderId }) {
 *   readonly type = 'order.placed'
 * }
 * await queue.produce(new OrderPlaced(order))
 * ```
 */
export function defineMessage<T>(defaults?: MessageDefaults<T>): MessageClass<T> {
  class Defined {
    readonly type?: string
    readonly data: T
    readonly key?: string
    readonly headers?: Readonly<Record<string, string>>
    readonly idempotencyKey?: string
    readonly idempotencyTtlMs?: number
    readonly delayMs?: number
    constructor(data: T, init?: MessageInit) {
      this.data = data
      const key = init?.key ?? defaults?.key?.(data)
      if (key !== undefined) this.key = key
      if (defaults?.headers !== undefined || init?.headers !== undefined) {
        this.headers = { ...defaults?.headers, ...init?.headers }
      }
      const idempotencyKey = init?.idempotencyKey ?? defaults?.idempotencyKey?.(data)
      if (idempotencyKey !== undefined) this.idempotencyKey = idempotencyKey
      const idempotencyTtlMs = init?.idempotencyTtlMs ?? defaults?.idempotencyTtlMs
      if (idempotencyTtlMs !== undefined) this.idempotencyTtlMs = idempotencyTtlMs
      if (init?.delayMs !== undefined) this.delayMs = init.delayMs
    }
  }
  Object.defineProperty(Defined.prototype, MESSAGE_BRAND, { value: true })
  return Defined as MessageClass<T>
}

/** Runtime discriminator: a `defineMessage` instance vs `produce`'s other argument shapes. */
export const isMessageInstance = (value: unknown): value is MessageInstance =>
  typeof value === 'object' && value !== null && MESSAGE_BRAND in value

/** Everything a message instance sends, with the `message-type` header stamped when the class
 * declares a `type` (an explicitly set header wins). */
export const toProduceMessage = (m: MessageInstance): ProduceMessage<unknown> => {
  const headers =
    m.type === undefined || m.headers?.[MESSAGE_TYPE_HEADER] !== undefined
      ? m.headers
      : { ...m.headers, [MESSAGE_TYPE_HEADER]: m.type }
  return pickMessage({ ...m, headers })
}

/** Copy a produce message's defined fields (and nothing else) into a fresh object. */
export const pickMessage = <B>(m: ProduceMessage<B>): ProduceMessage<B> => ({
  data: m.data,
  ...(m.key !== undefined ? { key: m.key } : {}),
  ...(m.headers !== undefined ? { headers: m.headers } : {}),
  ...(m.idempotencyKey !== undefined ? { idempotencyKey: m.idempotencyKey } : {}),
  ...(m.idempotencyTtlMs !== undefined ? { idempotencyTtlMs: m.idempotencyTtlMs } : {}),
  ...(m.delayMs !== undefined ? { delayMs: m.delayMs } : {}),
})

/** A handler object naming its queue directly: `{ queue, options?, handle }`. */
export interface QueueHandlerInstance<K extends string = string, T = unknown> {
  readonly queue: K
  readonly options?: SubscribeOptions
  handle(ctx: JobContext<T>): void | Promise<void>
}

/** A handler bound to message classes; the queue resolves through the registry at attach
 * time (every listed class must be bound to the SAME queue). */
export interface MessageHandlerInstance<
  Cs extends readonly AnyMessageClass[] = readonly AnyMessageClass[],
> {
  readonly messages: Cs
  readonly options?: SubscribeOptions
  handle(ctx: JobContext<MessagePayload<Cs[number]>>): void | Promise<void>
}

/** The batch sibling of {@link QueueHandlerInstance}: `handleBatch` receives one pulled batch
 * per call. The method name is the opt-in. */
export interface QueueBatchHandlerInstance<K extends string = string, T = unknown> {
  readonly queue: K
  readonly options?: SubscribeOptions
  handleBatch(ctx: BatchContext<T>): void | Promise<void>
}

/** The batch sibling of {@link MessageHandlerInstance}. */
export interface BatchMessageHandlerInstance<
  Cs extends readonly AnyMessageClass[] = readonly AnyMessageClass[],
> {
  readonly messages: Cs
  readonly options?: SubscribeOptions
  handleBatch(ctx: BatchContext<MessagePayload<Cs[number]>>): void | Promise<void>
}

/**
 * Base-class factory for a handler bound to message classes. The subclass supplies `handle`
 * and (optionally) worker `options`; its constructor arguments are its own, a natural seam
 * for injecting dependencies. List several classes for a polymorphic queue and switch on
 * `ctx.message.type`.
 *
 * ```ts
 * class FulfillOrder extends QueueHandler(OrderPlaced) {
 *   override readonly options: SubscribeOptions = { prefetch: 32 }
 *   async handle(ctx: JobContext<Order>): Promise<void> { ... }
 * }
 * ```
 */
export function QueueHandler<const Cs extends readonly AnyMessageClass[]>(...messages: Cs) {
  // The return type is inferred so `handle` stays ABSTRACT: forgetting it is a compile error.
  abstract class Base implements MessageHandlerInstance<Cs> {
    readonly messages: Cs = messages
    readonly options?: SubscribeOptions
    abstract handle(ctx: JobContext<MessagePayload<Cs[number]>>): void | Promise<void>
  }
  return Base
}

/**
 * The batch sibling of {@link QueueHandler}: `prefetch` caps the batch (one pull = one batch)
 * and `consumeTimeout` bounds the whole call. Return acks all; `throw new BatchFailure(failed)`
 * acks the rest and fails only the subset; any other throw fails the whole batch.
 */
export function QueueBatchHandler<const Cs extends readonly AnyMessageClass[]>(...messages: Cs) {
  abstract class Base implements BatchMessageHandlerInstance<Cs> {
    readonly messages: Cs = messages
    readonly options?: SubscribeOptions
    abstract handleBatch(ctx: BatchContext<MessagePayload<Cs[number]>>): void | Promise<void>
  }
  return Base
}

/** Every handler-object shape `subscribe` and config `handlers` accept. */
export type AnyHandlerInstance =
  | QueueHandlerInstance
  | MessageHandlerInstance
  | QueueBatchHandlerInstance
  | BatchMessageHandlerInstance

/** Runtime discriminator: a handler object (vs a queue name or the inline options form). */
export const isHandlerInstance = (value: unknown): value is AnyHandlerInstance => {
  if (typeof value !== 'object' || value === null) return false
  const v = value as {
    handle?: unknown
    handleBatch?: unknown
    queue?: unknown
    messages?: unknown
  }
  return (
    (typeof v.handle === 'function' || typeof v.handleBatch === 'function') &&
    (typeof v.queue === 'string' || Array.isArray(v.messages))
  )
}
