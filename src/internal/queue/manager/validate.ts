/**
 * Refuse a numeric option that is not a finite number of at least `min`. Node collapses an
 * `Infinity`, `NaN` or negative timer delay to 1 ms with only a warning, so a limit that
 * should not exist is omitted, never set to `Infinity` (`hint` says so where that is the
 * likely intent).
 */
export const requireFinite = (
  name: string,
  value: number | undefined,
  min: number,
  hint?: string
): void => {
  if (value === undefined) return
  if (typeof value !== 'number' || !Number.isFinite(value) || value < min) {
    throw new Error(
      `queue: ${name} must be a finite number of at least ${min}, got ${String(value)}` +
        (hint !== undefined ? ` (${hint})` : '')
    )
  }
}

/**
 * The subscribe-option checks, against the liveness window of the handler's queue. Applied
 * when a Worker is built and again when a handler is registered as an inline dispatch
 * target, so one handler list is accepted or refused the same way on every manager shape.
 */
export const validateSubscribeOptions = (
  options: {
    readonly prefetch?: number
    readonly parallelism?: number
    readonly flow?: 'batched' | 'streaming'
    readonly batch?: boolean
    readonly pollIdleIntervalMs?: number
    readonly pollBatchIntervalMs?: number
    readonly consumeTimeout?: number
    readonly heartbeatIntervalMs?: number
    readonly closeTimeout?: number
  },
  queue: string,
  livenessMs: number
): void => {
  requireFinite('prefetch', options.prefetch, 1)
  requireFinite('parallelism', options.parallelism, 1)
  requireFinite('pollIdleIntervalMs', options.pollIdleIntervalMs, 0)
  requireFinite('pollBatchIntervalMs', options.pollBatchIntervalMs, 0)
  requireFinite('consumeTimeout', options.consumeTimeout, 1, 'omit it for no timeout')
  requireFinite('heartbeatIntervalMs', options.heartbeatIntervalMs, 1)
  requireFinite('closeTimeout', options.closeTimeout, 0)
  if (options.flow === 'streaming' && options.batch === true) {
    throw new Error(
      'queue: flow "streaming" is not applicable to a batch handler — one pull IS its batch'
    )
  }
  // The auto-heartbeat must be bounded by a hard progress limit: without one, a wedged
  // invocation would be kept alive forever.
  if (options.heartbeatIntervalMs !== undefined && options.consumeTimeout === undefined) {
    throw new Error(
      'queue: heartbeatIntervalMs requires consumeTimeout (the per-invocation progress bound)'
    )
  }
  // The queue's liveness window is how long a delivery may go unsettled and un-heartbeated
  // before the backend redelivers it: a handler allowed to run longer must heartbeat, and a
  // heartbeat slower than the window renews nothing in time.
  const { consumeTimeout, heartbeatIntervalMs } = options
  if (heartbeatIntervalMs !== undefined && heartbeatIntervalMs >= livenessMs) {
    throw new Error(
      `queue: heartbeatIntervalMs (${heartbeatIntervalMs}) on queue "${queue}" must be shorter than the queue's livenessTimeoutMs (${livenessMs})`
    )
  }
  if (
    consumeTimeout !== undefined &&
    consumeTimeout > livenessMs &&
    heartbeatIntervalMs === undefined
  ) {
    throw new Error(
      `queue: consumeTimeout (${consumeTimeout}) on queue "${queue}" exceeds the queue's livenessTimeoutMs (${livenessMs}) — its work would be redelivered while still running; set heartbeatIntervalMs or raise the queue's livenessTimeoutMs`
    )
  }
}
