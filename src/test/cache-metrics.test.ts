'use strict'

import {
  createLruCache,
  createTtlCache,
  DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS,
  TENANT_CONFIG_CACHE_NAME,
} from '@internal/cache'
import {
  cacheEntries,
  cacheEvictionsTotal,
  cacheRequestsTotal,
  cacheSizeBytes,
  meter,
  setMetricsEnabled,
} from '@internal/monitoring/metrics'
import { monitorCache } from '../internal/cache/monitoring'

function busyWaitMs(ms: number) {
  const end = Date.now() + ms
  while (Date.now() < end) {
    // Block the event loop so TTL expires before timer-driven cleanup runs.
  }
}

describe('cache telemetry helpers', () => {
  beforeEach(() => {
    jest.useFakeTimers()
  })

  afterEach(() => {
    jest.restoreAllMocks()
    jest.useRealTimers()
  })

  test('records cache hits and misses', () => {
    const addSpy = jest.spyOn(cacheRequestsTotal, 'add')
    const cache = createLruCache(TENANT_CONFIG_CACHE_NAME, {
      max: 2,
    })

    cache.set('hit', { ok: true })

    expect(cache.get('hit')).toEqual({ ok: true })
    expect(cache.get('miss')).toBeUndefined()

    expect(addSpy).toHaveBeenNthCalledWith(1, 1, {
      cache: TENANT_CONFIG_CACHE_NAME,
      outcome: 'hit',
    })
    expect(addSpy).toHaveBeenNthCalledWith(2, 1, {
      cache: TENANT_CONFIG_CACHE_NAME,
      outcome: 'miss',
    })
  })

  test('can read without recording cache request metrics', () => {
    const addSpy = jest.spyOn(cacheRequestsTotal, 'add')
    const cache = createLruCache(TENANT_CONFIG_CACHE_NAME, {
      max: 2,
    })

    cache.set('hit', { ok: true })

    expect(cache.get('hit', { recordMetrics: false })).toEqual({ ok: true })
    expect(cache.get('miss', { recordMetrics: false })).toBeUndefined()

    expect(addSpy).not.toHaveBeenCalled()
  })

  test('records stale cache reads when allowStale is enabled', () => {
    const addSpy = jest.spyOn(cacheRequestsTotal, 'add')
    const cache = createLruCache(TENANT_CONFIG_CACHE_NAME, {
      max: 2,
      ttl: 10,
      allowStale: true,
      perf: {
        now: () => Date.now(),
      },
    })

    cache.set('stale', { ok: true })
    jest.advanceTimersByTime(11)

    expect(cache.get('stale')).toEqual({ ok: true })
    expect(addSpy).toHaveBeenCalledWith(1, {
      cache: TENANT_CONFIG_CACHE_NAME,
      outcome: 'stale',
    })
  })

  test('records evictions', () => {
    const evictionSpy = jest.spyOn(cacheEvictionsTotal, 'add')
    const cache = createLruCache(TENANT_CONFIG_CACHE_NAME, {
      max: 1,
    })

    cache.set('first', { ok: true })
    cache.set('second', { ok: false })

    expect(evictionSpy.mock.calls).toContainEqual([
      1,
      {
        cache: TENANT_CONFIG_CACHE_NAME,
      },
    ])
  })

  test('records ttl cache hits and misses', () => {
    const addSpy = jest.spyOn(cacheRequestsTotal, 'add')
    const cache = createTtlCache(TENANT_CONFIG_CACHE_NAME, {
      max: 2,
      ttl: 1000,
    })

    cache.set('hit', { ok: true })

    expect(cache.get('hit')).toEqual({ ok: true })
    expect(cache.get('miss')).toBeUndefined()

    expect(addSpy).toHaveBeenNthCalledWith(1, 1, {
      cache: TENANT_CONFIG_CACHE_NAME,
      outcome: 'hit',
    })
    expect(addSpy).toHaveBeenNthCalledWith(2, 1, {
      cache: TENANT_CONFIG_CACHE_NAME,
      outcome: 'miss',
    })
  })

  test('records ttl cache evictions', () => {
    const evictionSpy = jest.spyOn(cacheEvictionsTotal, 'add')
    const cache = createTtlCache(TENANT_CONFIG_CACHE_NAME, {
      max: 1,
      ttl: 1000,
    })

    cache.set('first', { ok: true })
    cache.set('second', { ok: false })

    expect(evictionSpy).toHaveBeenCalledWith(1, {
      cache: TENANT_CONFIG_CACHE_NAME,
    })
  })

  test('chains caller disposeAfter after recording evictions', () => {
    const evictionSpy = jest.spyOn(cacheEvictionsTotal, 'add')
    const disposeAfter = jest.fn()
    const cache = createLruCache(TENANT_CONFIG_CACHE_NAME, {
      max: 1,
      disposeAfter,
    })

    cache.set('first', { ok: true })
    cache.set('second', { ok: false })

    expect(evictionSpy).toHaveBeenCalledWith(1, {
      cache: TENANT_CONFIG_CACHE_NAME,
    })
    expect(disposeAfter).toHaveBeenCalledWith({ ok: true }, 'first', 'evict')
    expect(evictionSpy.mock.invocationCallOrder[0]).toBeLessThan(
      disposeAfter.mock.invocationCallOrder[0]
    )
  })

  test('purges stale entries on the background interval', async () => {
    const cache = createLruCache(TENANT_CONFIG_CACHE_NAME, {
      max: 2,
      maxSize: 2,
      ttl: DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS - 1,
      purgeStaleIntervalMs: DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS,
      sizeCalculation: () => 1,
      perf: {
        now: () => Date.now(),
      },
    })

    cache.set('stale', { ok: true })

    expect(cache.getStats()).toEqual({ entries: 1, sizeBytes: 1 })

    jest.advanceTimersByTime(DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS)

    expect(cache.getStats()).toEqual({ entries: 0, sizeBytes: 0 })
    expect(cache.get('stale')).toBeUndefined()

    cache.set('fresh', { ok: false })

    expect(cache.getStats()).toEqual({ entries: 1, sizeBytes: 1 })
    expect(cache.get('fresh')).toEqual({ ok: false })

    jest.advanceTimersByTime(DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS)

    expect(cache.getStats()).toEqual({ entries: 0, sizeBytes: 0 })
    expect(cache.get('fresh')).toBeUndefined()
  })

  test('purges stale entries before reporting occupancy metrics', () => {
    const addBatchObservableCallbackSpy = jest.spyOn(meter, 'addBatchObservableCallback')
    let batchObserver: ((observer: { observe: (...args: unknown[]) => void }) => void) | undefined

    addBatchObservableCallbackSpy.mockImplementation((callback) => {
      batchObserver = callback as typeof batchObserver
      return undefined as never
    })

    const cache = createLruCache(TENANT_CONFIG_CACHE_NAME, {
      max: 2,
      maxSize: 2,
      ttl: 10,
      sizeCalculation: () => 1,
      perf: {
        now: () => Date.now(),
      },
    })

    cache.set('stale', { ok: true })

    jest.advanceTimersByTime(11)

    expect(cache.getStats()).toEqual({ entries: 1, sizeBytes: 1 })

    const observeSpy = jest.fn()
    batchObserver?.({ observe: observeSpy })

    expect(cache.getStats()).toEqual({ entries: 0, sizeBytes: 0 })
    expect(observeSpy).toHaveBeenCalledWith(cacheEntries, 0, {
      cache: TENANT_CONFIG_CACHE_NAME,
    })
    expect(observeSpy).toHaveBeenCalledWith(cacheSizeBytes, 0, {
      cache: TENANT_CONFIG_CACHE_NAME,
    })
  })

  test('skips stale purges when occupancy gauges are disabled', () => {
    const addBatchObservableCallbackSpy = jest.spyOn(meter, 'addBatchObservableCallback')
    let batchObserver: ((observer: { observe: (...args: unknown[]) => void }) => void) | undefined

    addBatchObservableCallbackSpy.mockImplementation((callback) => {
      batchObserver = callback as typeof batchObserver
      return undefined as never
    })

    const purgeStale = jest.fn()
    const cache = {
      delete: jest.fn().mockReturnValue(false),
      get: jest.fn(),
      getStats: jest.fn().mockReturnValue({ entries: 1, sizeBytes: 1 }),
      getWithOutcome: jest.fn().mockReturnValue({ value: undefined, outcome: 'miss' }),
      set: jest.fn(),
    }

    monitorCache(TENANT_CONFIG_CACHE_NAME, cache, { purgeStale })

    try {
      setMetricsEnabled([
        { name: 'cache_entries', enabled: false },
        { name: 'cache_size_bytes', enabled: false },
      ])

      const observeSpy = jest.fn()
      batchObserver?.({ observe: observeSpy })

      expect(purgeStale).not.toHaveBeenCalled()
      expect(cache.getStats).not.toHaveBeenCalled()
      expect(observeSpy).not.toHaveBeenCalled()
    } finally {
      setMetricsEnabled([
        { name: 'cache_entries', enabled: true },
        { name: 'cache_size_bytes', enabled: true },
      ])
    }
  })

  test('records stale ttl cache reads before timer cleanup', () => {
    jest.useRealTimers()

    const addSpy = jest.spyOn(cacheRequestsTotal, 'add')
    const cache = createTtlCache(TENANT_CONFIG_CACHE_NAME, {
      max: 2,
      ttl: 10,
    })

    cache.set('stale', { ok: true })
    busyWaitMs(20)

    expect(cache.get('stale')).toEqual({ ok: true })
    expect(addSpy).toHaveBeenCalledWith(1, {
      cache: TENANT_CONFIG_CACHE_NAME,
      outcome: 'stale',
    })
  })

  test('purges stale ttl entries before reporting occupancy metrics', () => {
    jest.useRealTimers()

    const addBatchObservableCallbackSpy = jest.spyOn(meter, 'addBatchObservableCallback')
    let batchObserver: ((observer: { observe: (...args: unknown[]) => void }) => void) | undefined

    addBatchObservableCallbackSpy.mockImplementation((callback) => {
      batchObserver = callback as typeof batchObserver
      return undefined as never
    })

    const cache = createTtlCache(TENANT_CONFIG_CACHE_NAME, {
      max: 2,
      ttl: 10,
      sizeCalculation: () => 1,
    })

    cache.set('stale', { ok: true })
    busyWaitMs(20)

    const observeSpy = jest.fn()
    batchObserver?.({ observe: observeSpy })

    expect(cache.getStats()).toEqual({ entries: 0, sizeBytes: 0 })
    expect(observeSpy).toHaveBeenCalledWith(cacheEntries, 0, {
      cache: TENANT_CONFIG_CACHE_NAME,
    })
    expect(observeSpy).toHaveBeenCalledWith(cacheSizeBytes, 0, {
      cache: TENANT_CONFIG_CACHE_NAME,
    })
  })

  test('dispose unregisters occupancy callbacks and tears down wrapped caches', () => {
    const addBatchObservableCallbackSpy = jest.spyOn(meter, 'addBatchObservableCallback')
    const removeBatchObservableCallbackSpy = jest.spyOn(meter, 'removeBatchObservableCallback')
    const cache = {
      delete: jest.fn().mockReturnValue(false),
      dispose: jest.fn(),
      get: jest.fn(),
      getStats: jest.fn().mockReturnValue({ entries: 1, sizeBytes: 1 }),
      getWithOutcome: jest.fn().mockReturnValue({ value: undefined, outcome: 'miss' }),
      set: jest.fn(),
    }

    const monitoredCache = monitorCache(TENANT_CONFIG_CACHE_NAME, cache)
    const [callback, observables] = addBatchObservableCallbackSpy.mock.calls.at(-1) as [
      Parameters<typeof meter.addBatchObservableCallback>[0],
      Parameters<typeof meter.addBatchObservableCallback>[1],
    ]

    monitoredCache.dispose()
    monitoredCache.dispose()

    expect(removeBatchObservableCallbackSpy).toHaveBeenCalledTimes(1)
    expect(removeBatchObservableCallbackSpy).toHaveBeenCalledWith(callback, observables)
    expect(cache.dispose).toHaveBeenCalledTimes(1)
  })
})
