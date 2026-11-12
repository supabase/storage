import type { Codec } from './codec'
import type { ProduceMessage, PullMessage } from './message'
import type { QueueOptions, QueuesView } from './queues'
import type { StatsQuery, StatsReport } from './stats'

/**
 * The retry/dead-letter policy a handler declares. Enforcement is the ADAPTER's: the backend
 * counts the budget in its own unit (explicit retries, or deliveries where nack and crash
 * redelivery also consume) and dead-letters exhausted messages itself. The manager only
 * forwards this policy and passes `backoffMs` as each `retry` call's delay.
 */
export interface RetryPolicy {
  /** Max delivery attempts before the message is dead-lettered (or dropped, sans `deadLetter`). */
  readonly maxAttempts: number
  /** Queue that receives exhausted messages. Declaring it provisions it on subscribe; the
   * adapter maps it to its native dead-letter mechanism. Omit to drop exhausted messages. */
  readonly deadLetter?: string
  /** Delay (ms) before a failed message is redelivered. Defaults to 0. */
  readonly backoffMs?: number
}

/** One failed delivery for `retry`: the receipt and the delay before its redelivery. */
export interface RetryEntry {
  readonly receipt: string
  readonly delayMs: number
}

/** What one consumer declares when it attaches to a queue. */
export interface SubscribePolicy {
  /** Stable id of this consumer instance, for adapters that track consumers. */
  readonly consumerId?: string
  /**
   * How long this consumer may legitimately go without pulling or heartbeating before the
   * backend may treat it as hung and redeliver its in-flight work. Advisory: adapters without
   * a matching concept ignore it.
   */
  readonly livenessTimeoutMs?: number
  /** The handler's retry/dead-letter policy, for the adapter to enforce. Absent means no
   * budget: failures are released and redelivered indefinitely. */
  readonly retry?: RetryPolicy
  /** The manager's declared queue registry, so the adapter can read a queue's declared options
   * without having provisioned it itself. Absent when the adapter is driven directly. */
  readonly queues?: QueuesView
}

/**
 * One consumer's live attachment to a queue: what it pulls from and settles through. A
 * receipt belongs to the session that issued it: settling one this session did not issue (or
 * no longer owns) is a harmless no-op, and every method is a no-op after {@link close}.
 */
export interface QueueSession<W = unknown> {
  /** Lease up to `max` undelivered messages to this consumer. Empty means nothing is
   * deliverable right now; the manager backs off and pulls again. */
  pull(max: number): Promise<PullMessage<W>[]>
  /**
   * Settle leased deliveries. Batch-shaped: one call, one round trip. IDEMPOTENT and atomic
   * per call: re-settling a receipt and settling a stale or unknown one are harmless no-ops.
   */
  ack(receipts: readonly string[]): Promise<void>
  /** Release leased deliveries so they are redelivered. Idempotent like ack, and a nack of an
   * already-acked receipt must not un-settle it (ack wins). */
  nack(receipts: readonly string[]): Promise<void>
  /**
   * Durable retry, batch-shaped: each entry becomes deliverable again after its `delayMs`
   * with `attempt + 1`. Idempotent per entry (a receipt named twice counts once; an unknown
   * one is skipped). The exhaustion verdict is here, per entry: when the subscribe declared a
   * {@link RetryPolicy} and an entry's budget is spent, the adapter dead-letters it to the
   * policy's `deadLetter` queue (or drops it) instead of redelivering.
   */
  retry(entries: readonly RetryEntry[]): Promise<void>
  /**
   * OPTIONAL delivery heartbeat: assert this consumer is alive mid-delivery without pulling,
   * renewing the given in-flight receipts' leases. Absent means the handler-facing
   * `heartbeat()` is an advisory no-op. Idempotent; never revives a delivery the caller no
   * longer owns.
   */
  heartbeat?(receipts: readonly string[]): Promise<void>
  /** Detach this consumer and release what the subscribe set up. Deliveries it still holds
   * redeliver. Retriable: a `close()` that threw may be called again. */
  close(): Promise<void>
}

/**
 * Per-call context for {@link ProducerAdapter.send}. Always present (`{}` when there is
 * nothing to carry).
 */
export interface SendContext<Ctx = never> {
  /** The caller's adapter-specific produce context, verbatim from `ProduceOptions.ctx`:
   * typically the database transaction this send should join. */
  readonly ctx?: Ctx
  /** The manager's declared queue registry (see {@link SubscribePolicy.queues}). */
  readonly queues?: QueuesView
}

/**
 * The produce side of a backend: identity, lifecycle, provisioning and sending. Everything an
 * adapter is except consuming, so it is all a producer-only manager needs.
 *
 * Optional members are capabilities. An adapter declares one by implementing it; the manager
 * throws {@link NotSupportedError} when a caller asks for one that is absent.
 *
 * `Ctx` is the adapter-specific produce context. It defaults to `never`, so code that passes
 * one fails to compile against a backend that cannot join a caller's transaction.
 */
export interface ProducerAdapter<W = unknown, Ctx = never> {
  readonly name: string
  /** The codec the manager uses to encode/decode payloads. */
  readonly codec: Codec<W>
  /** Optional one-time startup. Called by `start()` before provisioning; idempotent. */
  start?(): Promise<void>
  /** Release resources (connections, timers, background tasks). */
  close(): Promise<void>
  /** Provision a queue. Idempotent; an options-less call only makes sure the queue exists. */
  provision(queue: string, options?: QueueOptions): Promise<void>
  /** Declares that this backend honours {@link ProduceMessage.delayMs}. */
  readonly delayedDelivery?: true
  /**
   * Send a batch of messages in one round trip. `signal` is the produce call's bound;
   * honouring it is best-effort (it frees the connection early) and never tightens the
   * outcome: an aborted send's outcome stays unknown unless the backend confirms it.
   */
  send(
    queue: string,
    messages: readonly ProduceMessage<W>[],
    ctx: SendContext<Ctx>,
    signal?: AbortSignal
  ): Promise<void>
  /** Sample the backend's gauges in one read-only invocation. */
  stats?(query?: StatsQuery, signal?: AbortSignal): Promise<StatsReport>
  /** One background-maintenance pass over the whole backend, scheduled by the manager. */
  maintain?(): Promise<void>
}

/** The full backend contract: a {@link ProducerAdapter} that can also be consumed from. */
export interface QueueAdapter<W = unknown, Ctx = never> extends ProducerAdapter<W, Ctx> {
  /**
   * Attach one consumer to `queue`: provision what it needs on first use (the queue, its
   * dead-letter queue) and return once it may pull. Competing consumers, no order.
   */
  subscribe(queue: string, policy: SubscribePolicy): Promise<QueueSession<W>>
}

/** A caller asked for a capability the adapter does not implement. */
export class NotSupportedError extends Error {
  constructor(readonly capability: 'delay' | 'stats') {
    super(
      capability === 'delay'
        ? 'this adapter does not support delayed delivery (missing delayedDelivery)'
        : 'this adapter does not expose queue stats (missing stats)'
    )
    this.name = 'NotSupportedError'
  }
}
