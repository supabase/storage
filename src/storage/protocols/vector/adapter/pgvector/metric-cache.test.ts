import { createMetricCache } from './metric-cache'

describe('pgvector metric cache', () => {
  it('renews the TTL on hot reads', () => {
    let now = 1
    const cache = createMetricCache({
      ttl: 10,
      ttlResolution: 0,
      perf: { now: () => now },
    })

    cache.set('index', 'euclidean')
    now = 10
    expect(cache.get('index')).toBe('euclidean')

    now = 12
    expect(cache.get('index')).toBe('euclidean')

    now = 23
    expect(cache.get('index')).toBeUndefined()
    cache.dispose()
  })
})
