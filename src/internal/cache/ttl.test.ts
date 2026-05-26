import { vi } from 'vitest'

describe('ttl cache wrapper', () => {
  let createTtlCache: typeof import('./ttl').createTtlCache

  beforeAll(async () => {
    vi.useFakeTimers()
    ;({ createTtlCache } = await import('./ttl'))
  })

  beforeEach(() => {
    vi.clearAllTimers()
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  afterAll(() => {
    vi.useRealTimers()
  })

  test('purges stale entries from stats and iteration after ttl elapses', async () => {
    const cache = createTtlCache<string, { bytes: number }>({
      max: 10,
      ttl: 20,
      sizeCalculation: (value) => value.bytes,
    })

    cache.set('stale', { bytes: 4 })

    expect(cache.getStats()).toEqual({ entries: 1, sizeBytes: 4 })

    await vi.advanceTimersByTimeAsync(40)

    expect(cache.getStats()).toEqual({ entries: 0, sizeBytes: 0 })
    expect([...cache.entries()]).toEqual([])
    expect([...cache.values()]).toEqual([])
  })

  test('keeps replaced keys visible in iteration and size tracking', () => {
    const cache = createTtlCache<string, { bytes: number }>({
      max: 10,
      ttl: Infinity,
      sizeCalculation: (value) => value.bytes,
    })

    cache.set('a', { bytes: 3 })
    cache.set('a', { bytes: 7 })

    expect(cache.getStats()).toEqual({ entries: 1, sizeBytes: 7 })
    expect([...cache.entries()]).toEqual([['a', { bytes: 7 }]])
    expect([...cache.keys()]).toEqual(['a'])
    expect([...cache.values()]).toEqual([{ bytes: 7 }])
  })

  test('tracks iteration and calculated size', () => {
    const cache = createTtlCache<string, { bytes: number }>({
      max: 10,
      ttl: Infinity,
      sizeCalculation: (value) => value.bytes,
    })

    cache.set('a', { bytes: 3 })
    cache.set('b', { bytes: 5 })

    expect(cache.getStats()).toEqual({ entries: 2, sizeBytes: 8 })
    expect([...cache.entries()]).toEqual([
      ['a', { bytes: 3 }],
      ['b', { bytes: 5 }],
    ])
    expect([...cache.values()]).toEqual([{ bytes: 3 }, { bytes: 5 }])

    cache.set('a', { bytes: 7 })

    expect(cache.getStats()).toEqual({ entries: 2, sizeBytes: 12 })
    expect(cache.get('a')).toEqual({ bytes: 7 })

    cache.delete('b')

    expect(cache.getStats()).toEqual({ entries: 1, sizeBytes: 7 })

    cache.clear()

    expect(cache.getStats()).toEqual({ entries: 0, sizeBytes: 0 })
    expect([...cache.entries()]).toEqual([])

    cache.set('c', { bytes: 2 })

    expect(cache.getStats()).toEqual({ entries: 1, sizeBytes: 2 })
    expect([...cache.entries()]).toEqual([['c', { bytes: 2 }]])
  })

  test('keeps stats and iteration in sync when max capacity evicts entries', () => {
    const cache = createTtlCache<string, { bytes: number }>({
      max: 1,
      ttl: 1000,
      sizeCalculation: (value) => value.bytes,
    })

    cache.set('a', { bytes: 3 })
    cache.set('b', { bytes: 5 })

    expect(cache.getStats()).toEqual({ entries: 1, sizeBytes: 5 })
    expect([...cache.entries()]).toEqual([['b', { bytes: 5 }]])
    expect([...cache.keys()]).toEqual(['b'])
    expect([...cache.values()]).toEqual([{ bytes: 5 }])
    expect(cache.get('a')).toBeUndefined()
    expect(cache.get('b')).toEqual({ bytes: 5 })
  })

  test('does not extend ttl when iterating entries keys or values', async () => {
    const cache = createTtlCache<string, { bytes: number }>({
      max: 10,
      ttl: 40,
      updateAgeOnGet: true,
      checkAgeOnGet: true,
      sizeCalculation: (value) => value.bytes,
    })

    cache.set('a', { bytes: 3 })

    await vi.advanceTimersByTimeAsync(20)

    expect([...cache.entries()]).toEqual([['a', { bytes: 3 }]])
    expect([...cache.keys()]).toEqual(['a'])
    expect([...cache.values()]).toEqual([{ bytes: 3 }])

    await vi.advanceTimersByTimeAsync(30)

    expect(cache.get('a')).toBeUndefined()
    expect(cache.getStats()).toEqual({ entries: 0, sizeBytes: 0 })
  })

  test('cancels the ttl timer on dispose', async () => {
    const cache = createTtlCache<string, { bytes: number }>({
      max: 10,
      ttl: 20,
      sizeCalculation: (value) => value.bytes,
    })

    cache.set('stale', { bytes: 4 })
    cache.dispose()

    await vi.advanceTimersByTimeAsync(40)

    expect(cache.getStats()).toEqual({ entries: 1, sizeBytes: 4 })

    cache.dispose()
  })
})
