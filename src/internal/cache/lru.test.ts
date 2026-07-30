import { createLruCache } from '@internal/cache'
import { vi } from 'vitest'

describe('lru cache wrapper', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  test('plain get returns hits misses and stale values according to allowStale', () => {
    const staleCache = createLruCache<string, { bytes: number }>({
      max: 2,
      ttl: 10,
      allowStale: true,
      perf: {
        now: () => Date.now(),
      },
    })

    staleCache.set('entry', { bytes: 1 })

    expect(staleCache.get('entry')).toEqual({ bytes: 1 })
    expect(staleCache.get('missing')).toBeUndefined()

    vi.advanceTimersByTime(11)

    expect(staleCache.get('entry')).toEqual({ bytes: 1 })

    const expiringCache = createLruCache<string, { bytes: number }>({
      max: 2,
      ttl: 10,
      allowStale: false,
      perf: {
        now: () => Date.now(),
      },
    })

    expiringCache.set('entry', { bytes: 1 })

    vi.advanceTimersByTime(11)

    expect(expiringCache.get('entry')).toBeUndefined()
  })

  test('purges timer-driven stale entries from raw cache stats', () => {
    const cache = createLruCache<string, { bytes: number }>({
      max: 2,
      ttl: 10,
      purgeStaleIntervalMs: 20,
      perf: {
        now: () => Date.now(),
      },
    })

    cache.set('stale', { bytes: 1 })

    expect(cache.getStats()).toEqual({ entries: 1 })

    vi.advanceTimersByTime(20)

    expect(cache.getStats()).toEqual({ entries: 0 })
    expect(cache.get('stale')).toBeUndefined()
  })

  test('tracks entries as values are replaced deleted and expired', () => {
    const cache = createLruCache<string, { bytes: number }>({
      max: 2,
      ttl: 15,
      perf: {
        now: () => Date.now(),
      },
    })

    cache.set('a', { bytes: 3 })
    cache.set('b', { bytes: 5 })

    expect(cache.getStats()).toEqual({ entries: 2 })

    cache.set('a', { bytes: 7 })

    expect(cache.getStats()).toEqual({ entries: 2 })

    cache.delete('b')

    expect(cache.getStats()).toEqual({ entries: 1 })

    vi.advanceTimersByTime(16)

    expect(cache.get('a')).toBeUndefined()
    expect(cache.getStats()).toEqual({ entries: 0 })
  })

  test('clears the stale purge timer on dispose', () => {
    const cache = createLruCache<string, { bytes: number }>({
      max: 2,
      ttl: 10,
      purgeStaleIntervalMs: 20,
      perf: {
        now: () => Date.now(),
      },
    })

    cache.set('stale', { bytes: 1 })
    cache.dispose()

    vi.advanceTimersByTime(20)

    expect(cache.getStats()).toEqual({ entries: 1 })

    cache.dispose()
  })
})
