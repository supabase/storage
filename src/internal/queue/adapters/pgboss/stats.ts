import type { PgBoss } from 'pg-boss'
import {
  type BoundedCount,
  DEFAULT_STATS_COUNT_CAP,
  type StatsQuery,
  type StatsReport,
} from '../../manager/stats'

interface GaugeRow {
  readonly queue: string
  /** Rows the live scan read, at most `cap + 1`. */
  readonly scanned: number
  readonly backlog: number
  readonly retry_total: number
  readonly retry_due: number
  readonly oldest_ms: number
  readonly active: number
}

/**
 * Sampled gauges from pg-boss's own job states: `created` jobs are the backlog (delayed ones
 * included, from the moment they are sent), `active` jobs are in flight, `retry` jobs are the
 * parked set (due once `start_after` has passed).
 *
 * One statement, two capped reads per queue. The backlog, both retry figures and the oldest
 * age come from ONE `limit cap + 1` scan over the queue's live jobs through pg-boss's partial
 * index on queued jobs (`where state < 'active' and not blocked`; `not blocked` changes no
 * count here but is what makes that index eligible). The index is not keyed by state, so a
 * read per state would walk every live row of the other states to find its own; one shared
 * scan costs at most `cap + 1` rows whatever the mix. Its price is a shared cap: past it all
 * of those figures are floors, a retry count that is truly zero included. Nothing indexes
 * active jobs by state: that read walks the queue's rows through the primary key, retained
 * completed history included.
 *
 * `oldestBacklogAgeMs` is the age of the oldest `created` job that is already due, so a
 * deliberate delay never reads as lag.
 */
export async function statsReport(
  boss: PgBoss,
  schema: string,
  query?: StatsQuery,
  signal?: AbortSignal
): Promise<StatsReport> {
  signal?.throwIfAborted()
  const cap = query?.countCap ?? DEFAULT_STATS_COUNT_CAP
  const { rows } = await boss.getDb().executeSql(
    `select q.name as queue, live.scanned, live.backlog, live.retry_total, live.retry_due,
            live.oldest_ms,
            (select count(*) from (select 1 from "${schema}".job j
               where j.name = q.name and j.state = 'active'
               limit $2::int + 1) a)::int as active
       from "${schema}".queue q
       cross join lateral (
         select count(*)::int as scanned,
                (count(*) filter (where c.state = 'created'))::int as backlog,
                (count(*) filter (where c.state = 'retry'))::int as retry_total,
                (count(*) filter (where c.state = 'retry' and c.start_after <= now()))::int
                  as retry_due,
                coalesce(greatest(0, extract(epoch from now() - min(c.created_on)
                  filter (where c.state = 'created' and c.start_after <= now())) * 1000), 0)::float8
                  as oldest_ms
           from (select j.state, j.start_after, j.created_on from "${schema}".job j
                  where j.name = q.name and j.state < 'active' and not j.blocked
                  limit $2::int + 1) c
       ) live
      where $1::text[] is null or q.name = any($1)
      order by q.name`,
    [query?.queues === undefined ? null : [...query.queues], cap]
  )

  const bounded = (n: number, floor = false): BoundedCount =>
    n > cap || floor ? { count: Math.min(cap, n), saturated: true } : { count: n, saturated: false }
  return {
    queues: (rows as GaugeRow[]).map((row) => {
      const floor = row.scanned > cap
      return {
        queue: row.queue,
        backlog: bounded(row.backlog, floor),
        inFlight: bounded(row.active),
        retryParked: {
          total: bounded(row.retry_total, floor),
          due: bounded(row.retry_due, floor),
        },
        oldestBacklogAgeMs: Math.round(row.oldest_ms),
      }
    }),
  }
}
