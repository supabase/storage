import type { ProducerAdapter } from './adapter'

export interface MaintenanceOptions {
  /** How often (ms) to run a maintenance pass. Defaults to 60_000. */
  readonly intervalMs?: number
  /** Called if a scheduled pass throws; the loop keeps running. */
  readonly onError?: (err: unknown) => void
}

/**
 * Periodic background maintenance the adapter needs to stay healthy. Opt-in: nothing runs
 * until `start()`. A no-op when the adapter exposes no `maintain()`.
 */
export interface Maintenance {
  /** Begin running `adapter.maintain()` every `intervalMs`. Idempotent. */
  start(options?: MaintenanceOptions): void
  /** Run one maintenance pass now (throws on failure). */
  runOnce(): Promise<void>
  /** Stop the timer and await any in-flight pass. */
  stop(): Promise<void>
}

export function createMaintenance(adapter: Pick<ProducerAdapter, 'maintain'>): Maintenance {
  let timer: ReturnType<typeof setInterval> | undefined
  let running = false
  let inFlight: Promise<void> = Promise.resolve()
  return {
    start(options) {
      if (adapter.maintain === undefined || timer !== undefined) return
      timer = setInterval(() => {
        if (running) return // a slow pass must never overlap itself
        running = true
        inFlight = (async () => {
          try {
            await adapter.maintain?.()
          } catch (err) {
            try {
              options?.onError?.(err)
            } catch {
              // an error handler must never break the loop
            }
          } finally {
            running = false
          }
        })()
      }, options?.intervalMs ?? 60_000)
      timer.unref?.()
    },
    async runOnce() {
      await adapter.maintain?.()
    },
    async stop() {
      if (timer !== undefined) {
        clearInterval(timer)
        timer = undefined
      }
      await inFlight
    },
  }
}
