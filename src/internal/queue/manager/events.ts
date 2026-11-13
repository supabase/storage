/**
 * Point-fact observability callbacks for the manager's pipeline. All optional, synchronous
 * and fire-and-forget: a sink must never throw or block. Around-semantics (spans, fallbacks)
 * belong in middleware.
 */
export interface QueueEvents {
  /** A message is about to be sent. `headers` is MUTABLE: the trace-context injection point. */
  produce?(ctx: {
    readonly queue: string
    readonly key?: string
    headers: Record<string, string>
  }): void
  /**
   * A worker finished one invocation, whatever the outcome: one handler call (`size: 1`) or
   * one `handleBatch` call. `failed` counts the invocation's failures, middleware failures
   * included; `durationMs` spans the whole chain; `attempt` is present when the invocation
   * carried exactly one message.
   */
  handled?(ctx: {
    readonly queue: string
    readonly outcome: 'ok' | 'error' | 'timeout' | 'shutdown'
    readonly size: number
    readonly failed: number
    readonly attempt?: number
    readonly durationMs: number
  }): void
  /** A failed delivery was handed to the adapter's retry machinery. Whether it was parked or
   * dead-lettered is the adapter's verdict (see {@link AdapterEvents.deadLettered}). */
  retryScheduled?(ctx: {
    readonly queue: string
    readonly attempt: number
    readonly delayMs: number
  }): void
  /**
   * A worker pulled messages from the engine: `size` is how many one pull returned. Empty
   * pulls are not reported. For a batch handler it is the batch (before `parallelism` slices
   * it); for a streaming worker it is one refill of its window.
   */
  pulled?(ctx: { readonly queue: string; readonly size: number }): void
  /** A worker survived an infrastructure error (failed pull/ack/subscribe) and is retrying. */
  workerError?(ctx: { readonly queue: string; readonly error: unknown }): void
}

/**
 * Facts only an adapter can see. Adapters accept a sink in their own options. All optional,
 * synchronous, fire-and-forget; sinks must never throw.
 */
export interface AdapterEvents {
  /** A self-scheduled background job started failing (edge-triggered, not once per beat). */
  maintenanceError?(ctx: { readonly job: string; readonly error: unknown }): void
  /** The failing background job's next pass succeeded. */
  maintenanceRecovered?(ctx: { readonly job: string }): void
  /** A message exhausted its retry budget and was dead-lettered (or dropped). Only adapters
   * that issue the verdict themselves can emit it; an engine that verdicts natively cannot. */
  deadLettered?(ctx: {
    readonly queue: string
    readonly deadLetter: string | undefined
    readonly attempt: number
  }): void
}
