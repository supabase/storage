import { memory } from '../adapters/memory'
import {
  createProducerQueue,
  createQueue,
  createStatsAccessor,
  DEFAULT_STATS_TTL_MS,
  defineQueues,
  NotSupportedError,
  type ProducerAdapter,
  queue,
  type StatsQuery,
  type StatsReport,
} from './index'
import { deferred, recordingAdapter, rejection } from './test-support'

const report = (): StatsReport => ({ queues: [] })

/** A counting stats source whose round trips can be held open through `gate`. */
const provider = (gate?: Promise<void>) => {
  const queries: (StatsQuery | undefined)[] = []
  const adapter: Pick<ProducerAdapter, 'stats'> = {
    async stats(query) {
      queries.push(query)
      if (gate !== undefined) await gate
      return report()
    },
  }
  return { adapter, calls: () => queries.length, queries }
}

describe('createStatsAccessor', () => {
  beforeEach(() => {
    vi.useFakeTimers({ now: 0 })
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it('rejects (never throws) with NotSupportedError when the adapter has no stats', async () => {
    const accessor = createStatsAccessor({})
    const pending = accessor()
    expect(pending).toBeInstanceOf(Promise) // a health endpoint's .catch sees it
    const err = await rejection(pending)
    expect(err).toBeInstanceOf(NotSupportedError)
    expect((err as NotSupportedError).capability).toBe('stats')
    expect((err as Error).name).toBe('NotSupportedError')
  })

  it("calls the adapter's stats bound to the adapter, forwarding the query", async () => {
    const source = {
      marker: report(),
      async stats(this: { marker: StatsReport }, _query?: StatsQuery): Promise<StatsReport> {
        return this.marker
      },
    }
    const spy = vi.spyOn(source, 'stats')
    const accessor = createStatsAccessor(source)
    const query = { queues: ['a'], countCap: 10 }

    expect(await accessor(query)).toBe(source.marker)
    expect(spy).toHaveBeenCalledWith(query)
  })

  it('coalesces concurrent identical calls onto one round trip', async () => {
    const gate = deferred()
    const { adapter, calls } = provider(gate.promise)
    const accessor = createStatsAccessor(adapter)

    const [a, b] = [accessor(), accessor()]
    expect(b).toBe(a)
    // Same query, differently built: still one round trip.
    const [c, d] = [
      accessor({ queues: ['x'], countCap: 5 }),
      accessor({ countCap: 5, queues: ['x'] }),
    ]
    gate.resolve()

    expect(await a).toBe(await b)
    expect(await c).toBe(await d)
    expect(calls()).toBe(2)
  })

  it('reuses the settled report within the TTL and refreshes after it', async () => {
    const { adapter, calls } = provider()
    const accessor = createStatsAccessor(adapter, 5_000)

    const first = await accessor()
    vi.setSystemTime(4_999)
    expect(await accessor()).toBe(first) // fresh: served from the slot
    expect(calls()).toBe(1)
    vi.setSystemTime(5_001)
    expect(await accessor()).not.toBe(first) // stale: a new round trip
    expect(calls()).toBe(2)
  })

  it(`defaults the TTL to ${DEFAULT_STATS_TTL_MS}ms`, async () => {
    const { adapter, calls } = provider()
    const accessor = createStatsAccessor(adapter)
    await accessor()
    vi.setSystemTime(DEFAULT_STATS_TTL_MS - 1)
    await accessor()
    expect(calls()).toBe(1)
    vi.setSystemTime(DEFAULT_STATS_TTL_MS)
    await accessor()
    expect(calls()).toBe(2)
  })

  it('ttl 0 disables the reuse but keeps the coalescing', async () => {
    const sequential = provider()
    const accessor = createStatsAccessor(sequential.adapter, 0)
    await accessor()
    await accessor()
    expect(sequential.calls()).toBe(2) // every sequential call hits the backend

    const gate = deferred()
    const concurrent = provider(gate.promise)
    const coalescing = createStatsAccessor(concurrent.adapter, 0)
    const [a, b] = [coalescing(), coalescing()]
    gate.resolve()
    expect(await a).toBe(await b) // a stampede still collapses to one query
    expect(concurrent.calls()).toBe(1)
  })

  it('holds one slot: a different query bypasses it and takes it over', async () => {
    const { adapter, calls } = provider()
    const accessor = createStatsAccessor(adapter, 5_000)

    await accessor()
    await accessor({ queues: ['a'] }) // different key: raw call, claims the slot
    expect(calls()).toBe(2)
    await accessor({ queues: ['a'] }) // the latest query is what is memoised
    expect(calls()).toBe(2)
    await accessor({ queues: ['a'], countCap: 10 }) // the cap is part of the key
    expect(calls()).toBe(3)
    await accessor() // the original query lost the slot: raw again
    expect(calls()).toBe(4)
  })

  it('coalesces per query: A, B, A in flight is two round trips', async () => {
    const gate = deferred()
    const { adapter, calls } = provider(gate.promise)
    const accessor = createStatsAccessor(adapter)

    const a1 = accessor()
    const b = accessor({ queues: ['b'] }) // a different query must not evict A's running call
    const a2 = accessor()
    gate.resolve()

    expect(await a1).toBe(await a2)
    expect(await b).not.toBe(await a1)
    expect(calls()).toBe(2)
  })

  it('rejects a countCap that is not a positive integer before touching the backend', async () => {
    const { adapter, calls, queries } = provider()
    const accessor = createStatsAccessor(adapter)

    for (const countCap of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
      const err = await rejection(accessor({ countCap }))
      expect(err).toBeInstanceOf(RangeError)
      expect((err as Error).message).toBe(
        `queue: stats countCap must be a positive integer, got ${String(countCap)}`
      )
    }
    expect(calls()).toBe(0)
    await expect(accessor({ countCap: 1 })).resolves.toEqual(report())
    expect(queries).toEqual([{ countCap: 1 }])
  })

  it('never caches a rejection, and concurrent callers share the one that failed', async () => {
    let fail = true
    let calls = 0
    const adapter: Pick<ProducerAdapter, 'stats'> = {
      async stats() {
        calls += 1
        if (fail) throw new Error('backend down')
        return report()
      },
    }
    const accessor = createStatsAccessor(adapter, 5_000)

    const [a, b] = [rejection(accessor()), rejection(accessor())]
    expect(((await a) as Error).message).toBe('backend down')
    expect(await b).toBe(await a)
    expect(calls).toBe(1)

    fail = false
    await expect(accessor()).resolves.toEqual(report()) // retried, not replayed
    expect(calls).toBe(2)
  })
})

describe('manager stats()', () => {
  const queues = defineQueues({ jobs: queue<{ n: number }>(), idle: queue<{ n: number }>() })

  it('full and producer managers serve the adapter report through the guard', async () => {
    for (const create of [createQueue, createProducerQueue] as const) {
      const stats = vi.fn(async () => report())
      const q = create({ ...memory(), stats }, { queues })

      const first = await q.stats()
      expect(await q.stats()).toBe(first) // memoised within the default TTL
      expect(stats).toHaveBeenCalledTimes(1)
      await q.stats({ queues: ['jobs'], countCap: 3 })
      expect(stats).toHaveBeenLastCalledWith({ queues: ['jobs'], countCap: 3 })
      await q.close()
    }
  })

  it('statsTtlMs: 0 disables the reuse', async () => {
    const stats = vi.fn(async () => report())
    const { adapter } = recordingAdapter({ stats })
    const q = createProducerQueue(adapter, { queues, statsTtlMs: 0 })
    await q.stats()
    await q.stats()
    expect(stats).toHaveBeenCalledTimes(2)
  })

  it('rejects with NotSupportedError when the adapter does not expose stats', async () => {
    const { adapter } = recordingAdapter() // no stats member
    await expect(createProducerQueue(adapter, { queues }).stats()).rejects.toThrow(
      NotSupportedError
    )
    const { stats: _omitted, ...bare } = memory()
    void _omitted
    await expect(createQueue(bare, { queues }).stats()).rejects.toThrow(
      'this adapter does not expose queue stats (missing stats)'
    )
  })

  it('reports the memory adapter gauges: narrowed by queue, saturating at the cap', async () => {
    const q = createProducerQueue(memory(), { queues, statsTtlMs: 0 })
    await q.start()
    await q.produce('jobs', [{ data: { n: 1 } }, { data: { n: 2 } }, { data: { n: 3 } }])

    expect((await q.stats()).queues.map((s) => s.queue)).toEqual(['jobs', 'idle'])
    expect(await q.stats({ queues: ['jobs'] })).toEqual({
      queues: [
        {
          queue: 'jobs',
          backlog: { count: 3, saturated: false },
          inFlight: { count: 0, saturated: false },
          retryParked: {
            total: { count: 0, saturated: false },
            due: { count: 0, saturated: false },
          },
          oldestBacklogAgeMs: expect.any(Number),
        },
      ],
    })
    const capped = await q.stats({ queues: ['jobs'], countCap: 2 })
    expect(capped.queues[0].backlog).toEqual({ count: 2, saturated: true })
    await q.close()
  })
})
