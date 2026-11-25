// Small async helpers shared across the manager: waiting and cancellation built on
// AbortSignal, and the run-once memo.

/** Sleep `ms`, or wake early if `signal` aborts (so shutdown needn't wait out a poll interval). */
export const sleep = (ms: number, signal?: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    if (signal?.aborted) {
      resolve()
      return
    }
    const done = (): void => {
      clearTimeout(timer)
      signal?.removeEventListener('abort', done)
      resolve()
    }
    const timer = setTimeout(done, ms)
    signal?.addEventListener('abort', done)
  })

const TIMEOUT_REASON_NAME = 'TimeoutError'
const SHUTDOWN_REASON_NAME = 'AbortError'

/** Abort reason for a `consumeTimeout` expiry. */
export const timeoutReason = (): DOMException =>
  new DOMException('queue: consume timeout exceeded', TIMEOUT_REASON_NAME)

/** Abort reason for a forced shutdown (`stop()`/`close()` past its `closeTimeout`). */
export const shutdownReason = (): DOMException =>
  new DOMException('queue: shutting down', SHUTDOWN_REASON_NAME)

/** Whether `signal` was aborted due to a `consumeTimeout` expiry. */
export const isTimeoutSignal = (signal: AbortSignal): boolean =>
  signal.aborted &&
  signal.reason instanceof DOMException &&
  signal.reason.name === TIMEOUT_REASON_NAME

/** Whether `signal` was aborted due to a forced queue shutdown. */
export const isShutdownSignal = (signal: AbortSignal): boolean =>
  signal.aborted &&
  signal.reason instanceof DOMException &&
  signal.reason.name === SHUTDOWN_REASON_NAME

/** A promise that resolves (with `value`) once `signal` aborts, at once if it already has,
 * plus the listener's teardown for a race that did not end on it. */
export function whenAborted<T = void>(
  signal: AbortSignal,
  value?: T
): { readonly aborted: Promise<T>; readonly dispose: () => void } {
  let onAbort: (() => void) | undefined
  const aborted = new Promise<T>((resolve) => {
    if (signal.aborted) resolve(value as T)
    else {
      onAbort = () => resolve(value as T)
      signal.addEventListener('abort', onAbort, { once: true })
    }
  })
  return {
    aborted,
    dispose: () => {
      if (onAbort !== undefined) signal.removeEventListener('abort', onAbort)
    },
  }
}

/**
 * A memoised async run. `fn` runs at most once successfully: concurrent callers share the
 * in-flight promise and later calls return the settled value. A rejection is NOT remembered,
 * so the next call runs `fn` again; `reset()` forgets a success too.
 */
export interface RunOnce<T> {
  (): Promise<T>
  reset(): void
}

export function runOnce<T>(fn: () => Promise<T>): RunOnce<T> {
  let current: Promise<T> | undefined
  const run = (): Promise<T> => {
    if (current === undefined) {
      // The executor form turns a synchronous throw inside `fn` into a rejection.
      const attempt: Promise<T> = new Promise<T>((resolve) => resolve(fn())).catch(
        (error: unknown) => {
          // Forget only OUR attempt: a reset() mid-flight may have let a newer run start.
          if (current === attempt) current = undefined
          throw error
        }
      )
      current = attempt
    }
    return current
  }
  run.reset = (): void => {
    current = undefined
  }
  return run
}
