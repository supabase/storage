import type { QueueSession, SubscribePolicy } from './adapter'
import { shutdownReason } from './signals'
import { validateSubscribeOptions } from './validate'
import { runBatchedLoop } from './worker/batched'
import { runStreamingLoop } from './worker/streaming'
import type { BatchHandler, Handler, WorkerOptions, WorkerRuntime } from './worker/types'

export type {
  BatchContext,
  BatchHandler,
  Handler,
  JobContext,
  SubscribeOptions,
  WorkerOptions,
} from './worker/types'
export { BatchFailure } from './worker/types'

/** Default idle poll interval (ms): how long a caught-up worker waits before re-polling. */
export const DEFAULT_POLL_IDLE_INTERVAL_MS = 1000
/** Default linger (ms) between non-empty pulls: a protective throughput ceiling of about
 * `prefetch × 10` messages per second per worker. */
export const DEFAULT_POLL_BATCH_INTERVAL_MS = 100
/** Default liveness contract (ms) when nothing better can be derived (no `consumeTimeout`). */
const DEFAULT_LIVENESS_TIMEOUT_MS = 300_000

/**
 * A self-driving consumer: pull → handle → ack/nack/retry on one queue. The caller never
 * holds an ack; a handler returns (ack) or throws (retry/nack). The two flows live beside it:
 * `worker/batched.ts` and `worker/streaming.ts`.
 */
export class Worker {
  readonly #runtime: WorkerRuntime
  #session: QueueSession | undefined
  readonly #closeTimeout: number | undefined
  readonly #onStop: () => void
  readonly #wake = new AbortController() // aborted on stop to interrupt the poll-sleep
  readonly #done: Promise<void>
  #running = true
  #closed = false

  /** True once the worker has fully stopped. */
  get closed(): boolean {
    return this.#closed
  }

  constructor(options: WorkerOptions, handler: Handler<unknown> | BatchHandler<unknown>) {
    validateSubscribeOptions(options)
    const { queue, adapter, consumeTimeout } = options
    const prefetch = Math.max(1, Math.floor(options.prefetch ?? 16))
    const batch = options.batch === true
    this.#onStop = options.onStop
    this.#closeTimeout = options.closeTimeout ?? options.defaultCloseTimeout
    const policy: SubscribePolicy = {
      consumerId: crypto.randomUUID(),
      // A worker handles up to `prefetch` messages between pulls and consumeTimeout bounds
      // each, so the derived contract is the sequential worst case, doubled for slack and
      // floored at 60s.
      livenessTimeoutMs:
        options.livenessTimeoutMs ??
        (consumeTimeout !== undefined
          ? Math.max(60_000, 2 * prefetch * consumeTimeout)
          : DEFAULT_LIVENESS_TIMEOUT_MS),
      ...(options.retry !== undefined ? { retry: options.retry } : {}),
      ...(options.queues !== undefined ? { queues: options.queues } : {}),
    }
    const onError =
      options.onError ??
      ((error) => console.error(`queue: worker on "${queue}" — retrying after`, error))
    const worker = this
    this.#runtime = {
      codec: options.codec,
      queue,
      retry: options.retry,
      consumeTimeout,
      prefetch,
      parallelism: Math.max(1, Math.floor(options.parallelism ?? 1)),
      pollIdleIntervalMs: options.pollIdleIntervalMs ?? options.defaultPollIdleIntervalMs,
      pollBatchIntervalMs: options.pollBatchIntervalMs ?? options.defaultPollBatchIntervalMs,
      heartbeatIntervalMs: options.heartbeatIntervalMs,
      // A reporter that throws must not take the worker's loops down with it.
      report: (error) => {
        try {
          onError(error)
        } catch {
          /* contained */
        }
        options.events?.workerError?.({ queue, error })
      },
      events: options.events,
      chain: options.chain,
      handler,
      batch,
      wake: this.#wake.signal,
      inFlight: new Set<AbortController>(),
      get running(): boolean {
        return worker.#running
      },
      async session(): Promise<QueueSession> {
        // Kept on success only, so a failed subscribe retries and stop() closes what opened.
        worker.#session ??= await adapter.subscribe(queue, policy)
        return worker.#session
      },
    }
    // Streaming wherever it applies: the batched settle join lets one slow invocation gate
    // its batch-mates AND the next pull. Batch handlers need the join.
    const flow = options.flow ?? (batch ? 'batched' : 'streaming')
    const loop = flow === 'streaming' ? runStreamingLoop : runBatchedLoop
    this.#done = loop(this.#runtime).finally(() => {
      this.#closed = true
    })
  }

  /**
   * Graceful shutdown: stop pulling, wake any poll-sleep, drain in-flight handlers (up to
   * `closeTimeout`, then abort their signals; to natural completion when unset), then close
   * the session. Idempotent; a `stop()` whose session close threw may be called again.
   */
  async stop(): Promise<void> {
    this.#running = false
    this.#wake.abort()
    const timer =
      this.#closeTimeout === undefined
        ? undefined
        : setTimeout(() => {
            for (const ac of this.#runtime.inFlight) ac.abort(shutdownReason())
          }, this.#closeTimeout)
    try {
      await this.#done
    } finally {
      clearTimeout(timer)
      this.#onStop()
      const session = this.#session
      if (session !== undefined) {
        await session.close()
        this.#session = undefined
      }
    }
  }
}
