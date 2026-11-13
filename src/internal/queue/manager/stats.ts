import { NotSupportedError, type ProducerAdapter } from './adapter'

/**
 * Sampled gauges, read on the caller's cadence (a Prometheus collector, a health endpoint).
 * Point-in-time and momentarily stale by nature; rate-shaped numbers derive from
 * `QueueEvents`/middleware on the caller's side.
 */
export interface StatsQuery {
  /** Narrow the report to these queues; omitted means every queue in the backend. */
  readonly queues?: readonly string[]
  /** Cap on every {@link BoundedCount} read (default {@link DEFAULT_STATS_COUNT_CAP}): counts
   * are exact up to it and saturate at it, bounding the scan cost under a deep backlog. */
  readonly countCap?: number
}

export const DEFAULT_STATS_COUNT_CAP = 250_000

/** A count read with bounded cost: exact up to the query's cap, saturating at it. */
export interface BoundedCount {
  readonly count: number
  /** True when the read hit the cap: the true number is at least `count`. */
  readonly saturated: boolean
}

/**
 * One queue's gauges. Every gauge is optional: `undefined` means this engine cannot compute
 * it, never 0. A dead-letter target is a queue, so its depth is that queue's `backlog`.
 */
export interface QueueStats {
  readonly queue: string
  /** Undelivered messages. May be engine-approximate. */
  readonly backlog?: BoundedCount
  /** Delivered but unsettled. */
  readonly inFlight?: BoundedCount
  /** Parked awaiting a retry slot: everything parked / the subset due now. */
  readonly retryParked?: { readonly total: BoundedCount; readonly due: BoundedCount }
  /** Age (ms at sample time) of the oldest undelivered message; `0` when nothing is waiting. */
  readonly oldestBacklogAgeMs?: number
}

export interface StatsReport {
  readonly queues: readonly QueueStats[]
}

/** Default TTL (ms) of the manager's cached stats report. */
export const DEFAULT_STATS_TTL_MS = 5_000

/**
 * Wrap an adapter's raw `stats` so a hot caller can never hammer the backend: concurrent
 * calls with the same query coalesce onto one round trip, and the most recent settled report
 * is reused for `ttlMs` (`0` disables the reuse). A rejected round trip is never cached.
 * An adapter without stats yields an accessor that rejects with {@link NotSupportedError}.
 */
export function createStatsAccessor(
  adapter: Pick<ProducerAdapter, 'stats'>,
  ttlMs: number = DEFAULT_STATS_TTL_MS
): (query?: StatsQuery) => Promise<StatsReport> {
  const raw = adapter.stats?.bind(adapter)
  if (raw === undefined) {
    return () => Promise.reject(new NotSupportedError('stats'))
  }

  const inFlight = new Map<string, Promise<StatsReport>>()
  let memoKey: string | undefined
  let memo: Promise<StatsReport> | undefined
  let memoAt = 0

  const keyOf = (query?: StatsQuery): string =>
    JSON.stringify([query?.countCap ?? null, query?.queues ?? null])

  return (query?: StatsQuery): Promise<StatsReport> => {
    const cap = query?.countCap
    if (cap !== undefined && !(Number.isSafeInteger(cap) && cap > 0)) {
      return Promise.reject(
        new RangeError(`queue: stats countCap must be a positive integer, got ${String(cap)}`)
      )
    }
    const key = keyOf(query)
    const running = inFlight.get(key)
    if (running !== undefined) return running
    if (memo !== undefined && memoKey === key && ttlMs > 0 && Date.now() < memoAt + ttlMs) {
      return memo
    }
    const run = raw(query)
    inFlight.set(key, run)
    run.then(
      () => {
        inFlight.delete(key)
        memoKey = key
        memo = run
        memoAt = Date.now()
      },
      () => {
        inFlight.delete(key)
      }
    )
    return run
  }
}
