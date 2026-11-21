import { vi } from 'vitest'
import { createMetricCache } from './metric-cache'

describe('pgvector metric cache', () => {
  it('uses environment-configured capacity and TTL', async () => {
    vi.useFakeTimers()
    vi.resetModules()
    vi.stubEnv('PGVECTOR_METRIC_CACHE_MAX_ENTRIES', '1')
    vi.stubEnv('PGVECTOR_METRIC_CACHE_TTL_MS', '10')
    const { getConfig, setEnvPaths } = await import('../../../../../config')
    setEnvPaths([])
    getConfig({ reload: true })
    const { createMetricCache: createConfiguredCache } = await import('./metric-cache')
    let now = 1
    const cache = createConfiguredCache({ ttlJitterRatio: 0, perf: { now: () => now } })
    cache.set('a', 'euclidean')
    cache.set('b', 'cosine')
    expect(cache.get('a')).toBeUndefined()
    expect(cache.get('b')).toBe('cosine')
    now = 12
    expect(cache.get('b')).toBe('cosine')
    vi.advanceTimersByTime(30_000)
    expect(cache.get('b')).toBeUndefined()
    cache.dispose()
    vi.useRealTimers()
  })
  it('expires entries at an absolute ttl even when read continuously', () => {
    let now = 1
    const cache = createMetricCache({
      ttl: 10,
      ttlJitterRatio: 0,
      ttlResolution: 0,
      perf: { now: () => now },
    })

    cache.set('index', 'euclidean')
    now = 5
    expect(cache.get('index')).toBe('euclidean')

    now = 9
    expect(cache.get('index')).toBe('euclidean')

    now = 12
    expect(cache.get('index')).toBeUndefined()
    cache.dispose()
  })

  it('applies per-set ttl jitter', () => {
    const random = vi.spyOn(Math, 'random')
    let now = 1
    const cache = createMetricCache({
      ttl: 10,
      ttlJitterRatio: 0.5,
      ttlResolution: 0,
      perf: { now: () => now },
    })

    random.mockReturnValue(1)
    cache.set('full-jitter', 'euclidean')
    random.mockReturnValue(0)
    cache.set('no-jitter', 'cosine')

    now = 7
    expect(cache.get('full-jitter')).toBeUndefined()
    expect(cache.get('no-jitter')).toBe('cosine')

    now = 12
    expect(cache.get('no-jitter')).toBeUndefined()
    cache.dispose()
  })
})
