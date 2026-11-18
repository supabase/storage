import { SignJWT } from 'jose'
import { vi } from 'vitest'

const SECRET = 'a-secret-that-is-long-enough-for-hs256-signing'
const key = new TextEncoder().encode(SECRET)

const sign = (claims: Record<string, unknown>) =>
  new SignJWT(claims).setProtectedHeader({ alg: 'HS256' }).sign(key)

async function load() {
  const verify = vi.fn()
  vi.doMock('jose', async (importOriginal) => {
    const actual = await importOriginal<typeof import('jose')>()
    verify.mockImplementation(actual.jwtVerify)
    return { ...actual, jwtVerify: verify }
  })
  const { getConfig, setEnvPaths } = await import('../../config')
  setEnvPaths([])
  getConfig({ reload: true })
  const { verifyJWTWithCache } = await import('./jwt')
  return { verify, verifyJWTWithCache }
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
  vi.resetModules()
  vi.stubEnv('MULTI_TENANT', 'true')
})

afterEach(() => {
  vi.doUnmock('jose')
  vi.doUnmock('@internal/cache')
  vi.useRealTimers()
  vi.resetModules()
})

test('configured JWT cache size and TTL bound reuse without extending token expiration', async () => {
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
  const { verify, verifyJWTWithCache } = await load()
  const nowSec = () => Math.floor(Date.now() / 1000)
  const first = await sign({ exp: nowSec() + 10, n: 1 })
  const second = await sign({ exp: nowSec() + 10, n: 2 })

  await verifyJWTWithCache(first, SECRET)
  await verifyJWTWithCache(first, SECRET)
  expect(verify).toHaveBeenCalledTimes(1)
  monotonicNow += 101
  vi.advanceTimersByTime(101)
  await verifyJWTWithCache(first, SECRET)
  expect(verify).toHaveBeenCalledTimes(1)
  monotonicNow += 4900
  vi.advanceTimersByTime(4900)
  await verifyJWTWithCache(first, SECRET)
  expect(verify).toHaveBeenCalledTimes(2)
  await verifyJWTWithCache(second, SECRET)
  await verifyJWTWithCache(first, SECRET)
  expect(verify).toHaveBeenCalledTimes(4)

  const shortLived = await sign({ exp: nowSec() + 1 })
  await verifyJWTWithCache(shortLived, SECRET)
  monotonicNow += 1001
  vi.advanceTimersByTime(1001)
  await expect(verifyJWTWithCache(shortLived, SECRET)).rejects.toThrow()
  expect(verify).toHaveBeenCalledTimes(6)
})

test('JWT cache hits return fresh decoded payloads and recheck nbf', async () => {
  vi.stubEnv('JWT_CACHE_MAX_ENTRIES', '8')
  const { verify, verifyJWTWithCache } = await load()
  const calls = () => verify.mock.calls.length
  const nowSec = () => Math.floor(Date.now() / 1000)

  const fresh = await sign({ exp: nowSec() + 60 })
  const miss = await verifyJWTWithCache(fresh, SECRET)
  const hit = await verifyJWTWithCache(fresh, SECRET)
  expect(hit).toEqual(miss)
  expect(hit).not.toBe(miss)
  hit.role = 'service_role'
  const next = await verifyJWTWithCache(fresh, SECRET)
  expect(next.role).toBeUndefined()
  expect(next).not.toBe(hit)
  expect(calls()).toBe(1)

  const nbf = await sign({ nbf: nowSec(), exp: nowSec() + 60 })
  await verifyJWTWithCache(nbf, SECRET)
  await verifyJWTWithCache(nbf, SECRET)
  expect(calls()).toBe(2)
  vi.setSystemTime(new Date('2025-12-31T23:59:00Z'))
  await expect(verifyJWTWithCache(nbf, SECRET)).rejects.toThrow()
  expect(calls()).toBe(3)
  vi.setSystemTime(new Date('2026-01-01T00:00:00Z'))
  await verifyJWTWithCache(nbf, SECRET)
  expect(calls()).toBe(4)

  const t = nowSec()
  vi.setSystemTime((t + 1) * 1000)
  const frac = await sign({ nbf: t + 0.5, exp: t + 60 })
  await verifyJWTWithCache(frac, SECRET)
  expect(calls()).toBe(5)
  vi.setSystemTime(t * 1000 + 750)
  await expect(verifyJWTWithCache(frac, SECRET)).rejects.toThrow()
  expect(calls()).toBe(6)

  const large = await sign({ exp: nowSec() + 60, pad: 'p'.repeat(4000) })
  expect(large.length).toBeGreaterThan(4096)
  await verifyJWTWithCache(large, SECRET)
  await verifyJWTWithCache(large, SECRET)
  expect(calls()).toBe(7)
})
