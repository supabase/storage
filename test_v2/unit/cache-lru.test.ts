import { afterEach, beforeEach, describe, expect, test, vi } from 'vitest'
import { createLruCache } from '@internal/cache'

describe('lru cache wrapper', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    vi.useRealTimers()
  })

  test('reports hit miss and stale outcomes', () => {
    const cache = createLruCache<string, { bytes: number }>({
      max: 2,
      ttl: 10,
      allowStale: true,
      perf: {
        now: () => Date.now(),
      },
    })

    expect(cache.getWithOutcome('missing')).toEqual({
      value: undefined,
      outcome: 'miss',
    })

    cache.set('entry', { bytes: 1 })

    expect(cache.getWithOutcome('entry')).toEqual({
      value: { bytes: 1 },
      outcome: 'hit',
    })

    vi.advanceTimersByTime(11)

    expect(cache.getWithOutcome('entry')).toEqual({
      value: { bytes: 1 },
      outcome: 'stale',
    })
  })

  test('purges timer-driven stale entries from raw cache stats', () => {
    const cache = createLruCache<string, { bytes: number }>({
      max: 2,
      maxSize: 2,
      ttl: 10,
      purgeStaleIntervalMs: 20,
      sizeCalculation: (value) => value.bytes,
      perf: {
        now: () => Date.now(),
      },
    })

    cache.set('stale', { bytes: 1 })

    expect(cache.getStats()).toEqual({ entries: 1, sizeBytes: 1 })

    vi.advanceTimersByTime(20)

    expect(cache.getStats()).toEqual({ entries: 0, sizeBytes: 0 })
    expect(cache.get('stale')).toBeUndefined()
  })

  test('tracks calculated size as entries are replaced deleted and expired', () => {
    const cache = createLruCache<string, { bytes: number }>({
      max: 2,
      maxSize: 20,
      ttl: 15,
      sizeCalculation: (value) => value.bytes,
      perf: {
        now: () => Date.now(),
      },
    })

    cache.set('a', { bytes: 3 })
    cache.set('b', { bytes: 5 })

    expect(cache.getStats()).toEqual({ entries: 2, sizeBytes: 8 })

    cache.set('a', { bytes: 7 })

    expect(cache.getStats()).toEqual({ entries: 2, sizeBytes: 12 })

    cache.delete('b')

    expect(cache.getStats()).toEqual({ entries: 1, sizeBytes: 7 })

    vi.advanceTimersByTime(16)

    expect(cache.get('a')).toBeUndefined()
    expect(cache.getStats()).toEqual({ entries: 0, sizeBytes: 0 })
  })

  test('clears the stale purge timer on dispose', () => {
    const cache = createLruCache<string, { bytes: number }>({
      max: 2,
      maxSize: 2,
      ttl: 10,
      purgeStaleIntervalMs: 20,
      sizeCalculation: (value) => value.bytes,
      perf: {
        now: () => Date.now(),
      },
    })

    cache.set('stale', { bytes: 1 })
    cache.dispose()

    vi.advanceTimersByTime(20)

    expect(cache.getStats()).toEqual({ entries: 1, sizeBytes: 1 })

    cache.dispose()
  })
})
