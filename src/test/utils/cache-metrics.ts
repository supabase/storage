type CacheMetricAttributes = {
  cache?: string
  outcome?: string
}

type CacheRequestMetricCall = [number, { cache: string; outcome: string }]

type AssertLogicalLookupMetricsOptions<T> = {
  addSpy: jest.SpyInstance
  backendCallSpy: jest.Mock | jest.SpyInstance
  cacheName: string
  startLookups: () => [Promise<T>, Promise<T>, Promise<T>]
  resolveBackend: () => void
  assertConcurrentResults?: (results: [T, T, T]) => void | Promise<void>
  assertCachedHit: () => Promise<void>
}

async function waitForImmediate(): Promise<void> {
  await new Promise((resolve) => setImmediate(resolve))
}

export function getCacheRequestCalls(
  addSpy: jest.SpyInstance,
  cacheName: string
): CacheRequestMetricCall[] {
  return addSpy.mock.calls.filter(([, attrs]) => {
    const metricAttrs = attrs as CacheMetricAttributes | undefined

    return Boolean(
      metricAttrs &&
        typeof metricAttrs === 'object' &&
        metricAttrs.outcome &&
        metricAttrs.cache === cacheName
    )
  }) as CacheRequestMetricCall[]
}

export async function assertLogicalLookupMetrics<T>({
  addSpy,
  backendCallSpy,
  cacheName,
  startLookups,
  resolveBackend,
  assertConcurrentResults,
  assertCachedHit,
}: AssertLogicalLookupMetricsOptions<T>): Promise<void> {
  await waitForImmediate()
  addSpy.mockClear()

  const lookups = startLookups()

  await waitForImmediate()

  // Each caller records its initial logical miss.
  // Waiters re-check the cache inside
  // the mutex with recordMetrics: false
  // so only these three outer misses are emitted.
  const misses = [
    [1, { cache: cacheName, outcome: 'miss' }],
    [1, { cache: cacheName, outcome: 'miss' }],
    [1, { cache: cacheName, outcome: 'miss' }],
  ]
  expect(getCacheRequestCalls(addSpy, cacheName)).toEqual(misses)
  expect(backendCallSpy).toHaveBeenCalledTimes(1)

  resolveBackend()

  const results = (await Promise.all(lookups)) as [T, T, T]
  await assertConcurrentResults?.(results)

  expect(getCacheRequestCalls(addSpy, cacheName)).toEqual(misses)

  await assertCachedHit()

  expect(getCacheRequestCalls(addSpy, cacheName)).toEqual([
    ...misses,
    [1, { cache: cacheName, outcome: 'hit' }],
  ])
}
