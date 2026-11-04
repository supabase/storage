import { vi } from 'vitest'

test('configured JWT cache size and TTL bound reuse without extending token expiration', async () => {
  vi.useFakeTimers()
  vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
  vi.resetModules()
  vi.stubEnv('MULTI_TENANT', 'true')
  vi.stubEnv('JWT_CACHE_MAX_ENTRIES', '1')
  vi.stubEnv('JWT_CACHE_TTL_MS', '100')
  let monotonicNow = 1
  vi.doMock('@internal/cache', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@internal/cache')>()
    return {
      ...actual,
      createLruCache: (name: string, options: Record<string, unknown>) =>
        actual.createLruCache(
          name as never,
          {
            ...options,
            ...(name === 'jwt' ? { perf: { now: () => monotonicNow } } : {}),
          } as never
        ),
    }
  })
  const verify = vi.fn(async () => ({ payload: { exp: Date.now() / 1000 + 10 } }))
  vi.doMock('jose', async (importOriginal) => ({
    ...(await importOriginal<typeof import('jose')>()),
    jwtVerify: verify,
  }))

  try {
    const { getConfig, setEnvPaths } = await import('../../config')
    setEnvPaths([])
    getConfig({ reload: true })
    const { verifyJWTWithCache } = await import('./jwt')
    await verifyJWTWithCache('first', 'secret')
    await verifyJWTWithCache('first', 'secret')
    expect(verify).toHaveBeenCalledTimes(1)
    monotonicNow += 101
    vi.advanceTimersByTime(101)
    await verifyJWTWithCache('first', 'secret')
    expect(verify).toHaveBeenCalledTimes(1)
    monotonicNow += 4900
    vi.advanceTimersByTime(4900)
    await verifyJWTWithCache('first', 'secret')
    expect(verify).toHaveBeenCalledTimes(2)
    await verifyJWTWithCache('second', 'secret')
    await verifyJWTWithCache('first', 'secret')
    expect(verify).toHaveBeenCalledTimes(4)

    const now = Date.now()
    verify.mockImplementationOnce(async () => ({ payload: { exp: now / 1000 + 0.05 } }))
    await verifyJWTWithCache('short-lived', 'secret')
    monotonicNow += 51
    vi.advanceTimersByTime(51)
    await verifyJWTWithCache('short-lived', 'secret')
    expect(verify).toHaveBeenCalledTimes(6)
  } finally {
    vi.doUnmock('jose')
    vi.doUnmock('@internal/cache')
    vi.useRealTimers()
    vi.resetModules()
  }
})
