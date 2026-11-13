import type { QueueAdapter, QueueSubscription, RetryPolicy } from '../adapter'
import type { Codec } from '../codec'
import type { QueueEvents } from '../events'
import type { Message } from '../message'
import type { ConsumeNext, QueueMiddleware } from '../middleware'
import type { QueuesView } from '../registry'

export interface SubscribeOptions {
  /**
   * Max messages to fetch per round trip and, for a batch handler, the cap on one call's
   * batch (one pull = one batch). Under the streaming flow it is the sliding window: the most
   * messages this worker holds pulled-but-unsettled at once. Fetched-but-unhandled messages
   * are released for redelivery on shutdown. Defaults to 16.
   */
  readonly prefetch?: number
  /** Max concurrent handler invocations inside this worker (for a batch handler: concurrent
   * slices of one pulled batch). Default 1. Under the streaming flow at most
   * `min(parallelism, prefetch)` invocations run at once. */
  readonly parallelism?: number
  /**
   * The worker's pull/settle flow. `"batched"`: one pull, the whole batch handled, one settle
   * join, then the next pull, so the slowest invocation gates its batch-mates' settles AND
   * the next pull. `"streaming"`: `prefetch` is a sliding window refilled in chunks as
   * settles land, and every message settles as it finishes (settles coalesce into amortized
   * round trips). Streaming is the default for single-message handlers and not applicable to
   * batch handlers (a batch handler's pull IS its batch).
   */
  readonly flow?: 'batched' | 'streaming'
  /** How long to wait (ms) before re-polling after an empty pull. Defaults to the manager's
   * `pollIdleIntervalMs` (1s). */
  readonly pollIdleIntervalMs?: number
  /** How long to linger (ms) before the next pull after a non-empty one, a throughput ceiling
   * of about `prefetch / interval`. Defaults to the manager's `pollBatchIntervalMs` (100). */
  readonly pollBatchIntervalMs?: number
  /**
   * Per-invocation timeout (ms): bounds each handler call (a whole `handleBatch` call for a
   * batch handler), middleware stages included. When it elapses the invocation's `signal`
   * aborts and its messages count as failed: under the queue's retry policy each uses an
   * attempt and is redelivered after the policy's backoff, or dead-lettered when its attempts
   * are spent. On a queue with no retry policy they are released and redelivered at once.
   * The handler is not stopped, only signalled: one that ignores `signal` keeps running while
   * the message is delivered again. Omit for no timeout.
   */
  readonly consumeTimeout?: number
  /**
   * Auto-heartbeat cadence (ms) while pulled work is in flight. REQUIRES `consumeTimeout`,
   * the bound that keeps a wedged handler from being kept alive forever, and must be shorter
   * than the queue's liveness window (`QueueOptions.livenessTimeoutMs`). A handler whose
   * `consumeTimeout` exceeds that window must set it, or its work would be redelivered while
   * still running. Advisory on adapters whose subscriptions have no `heartbeat`.
   */
  readonly heartbeatIntervalMs?: number
  /** How long `stop()` waits for in-flight handlers before aborting their `signal`. Defaults
   * to the manager's `closeTimeout`; when both are unset, drains to natural completion. */
  readonly closeTimeout?: number
  /**
   * Called on infrastructure errors (a failed pull/settle/subscribe; handler failures belong
   * to the retry policy). The worker survives them: it releases unsettled work, backs off one
   * idle interval and resumes. Defaults to `console.error`.
   */
  readonly onError?: (error: unknown) => void
  /** Middleware local to this worker, composed INSIDE the manager-level entries. */
  readonly middleware?: readonly QueueMiddleware[]
}

export interface JobContext<T> {
  /** The queue this delivery came from. */
  readonly queue: string
  readonly message: Message<T>
  /** Aborts when `consumeTimeout` elapses or `stop()` forces shutdown after `closeTimeout`. */
  readonly signal: AbortSignal
  /** Explicit delivery heartbeat for long jobs: renews this worker's in-flight leases through
   * the subscription's `heartbeat` (an advisory no-op where there is none). Best-effort: a failed
   * beat is reported via `onError`, never thrown into the handler. */
  readonly heartbeat: () => Promise<void>
}

export type Handler<T> = (ctx: JobContext<T>) => void | Promise<void>

/** The batch sibling of {@link JobContext}: one pulled batch (or slice) per call. */
export interface BatchContext<T> {
  readonly queue: string
  readonly messages: readonly Message<T>[]
  /** One attempt bound for the whole call. */
  readonly signal: AbortSignal
  readonly heartbeat: () => Promise<void>
}

export type BatchHandler<T> = (ctx: BatchContext<T>) => void | Promise<void>

/**
 * Thrown from a batch handler to fail ONLY these messages while the rest of the batch acks.
 * Entries are matched by object identity against `ctx.messages`; an entry that is not from
 * this batch is a handler bug and fails the whole batch. Any OTHER throw fails the whole batch.
 */
export class BatchFailure extends Error {
  readonly failed: readonly Message<unknown>[]

  constructor(failed: Iterable<Message<unknown>>, options?: { cause?: unknown }) {
    super('queue: batch handler failed a subset of its messages', options)
    this.name = 'BatchFailure'
    this.failed = [...failed]
  }
}

/** What the {@link Worker} is constructed with: the public options plus what the manager
 * injects. */
export interface WorkerOptions extends SubscribeOptions {
  readonly queue: string
  readonly adapter: Pick<QueueAdapter, 'subscribe'>
  readonly codec: Codec
  /** True when the handler is batch-shaped. */
  readonly batch?: boolean
  readonly defaultCloseTimeout: number | undefined
  readonly defaultPollIdleIntervalMs: number
  readonly defaultPollBatchIntervalMs: number
  /** The consume chain, composed once at attachment (manager-level outside worker-level). */
  readonly chain: (terminal: ConsumeNext) => ConsumeNext
  readonly events?: QueueEvents
  /** Called when the worker has stopped, so the manager can drop it from its registry. */
  readonly onStop: () => void
  /** The declared registry: the source of the queue's policy (retry, liveness), and
   * forwarded to the adapter on subscribe. */
  readonly queues?: QueuesView
}

/** What a running worker's loops share: its resolved configuration and the live state
 * `stop()` reaches into. */
export interface WorkerRuntime {
  readonly codec: Codec
  readonly queue: string
  /** The queue's declared retry policy. */
  readonly retry: RetryPolicy | undefined
  readonly consumeTimeout: number | undefined
  readonly prefetch: number
  readonly parallelism: number
  readonly pollIdleIntervalMs: number
  readonly pollBatchIntervalMs: number
  readonly heartbeatIntervalMs: number | undefined
  /** Reports an infrastructure error (`onError` + the `workerError` event). Never throws. */
  readonly report: (error: unknown) => void
  readonly events: QueueEvents | undefined
  readonly chain: (terminal: ConsumeNext) => ConsumeNext
  readonly handler: Handler<unknown> | BatchHandler<unknown>
  readonly batch: boolean
  /** Aborts on `stop()`, to interrupt a poll-sleep. */
  readonly wake: AbortSignal
  /** In-flight invocations' controllers; `stop()` aborts them once `closeTimeout` elapses. */
  readonly inFlight: Set<AbortController>
  /** False once `stop()` was called: no new pull, no new invocation. */
  readonly running: boolean
  /** This worker's subscription, opened on first use. A failed subscribe leaves none, so the
   * next loop iteration retries. */
  subscription(): Promise<QueueSubscription>
}
