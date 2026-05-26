import { vi } from 'vitest'

const CONFIG_ENV_KEYS = [
  'MULTI_TENANT',
  'IS_MULTITENANT',
  'TENANT_POOL_CACHE_TTL_MS',
  'TENANT_POOL_CACHE_HIT_LOG_SAMPLE_RATE',
  'TENANT_POOL_CACHE_MISS_LOG_SAMPLE_RATE',
] as const

type ConfigEnvKey = (typeof CONFIG_ENV_KEYS)[number]

const originalEnv = new Map<ConfigEnvKey, string | undefined>()

function setConfigEnv(env: Partial<Record<ConfigEnvKey, string>>) {
  for (const key of CONFIG_ENV_KEYS) {
    delete process.env[key]
  }

  process.env.MULTI_TENANT = 'true'

  for (const [key, value] of Object.entries(env)) {
    process.env[key] = value
  }
}

describe('tenant pool cache config parsing', () => {
  beforeAll(() => {
    for (const key of CONFIG_ENV_KEYS) {
      originalEnv.set(key, process.env[key])
    }
  })

  afterEach(() => {
    for (const key of CONFIG_ENV_KEYS) {
      const value = originalEnv.get(key)

      if (value === undefined) {
        delete process.env[key]
      } else {
        process.env[key] = value
      }
    }

    vi.resetModules()
  })

  test('defaults tenant pool cache settings', async () => {
    setConfigEnv({})

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.tenantPoolCacheTtlMs).toBe(1000 * 10)
    expect(config.tenantPoolCacheHitLogSampleRate).toBe(0)
    expect(config.tenantPoolCacheMissLogSampleRate).toBe(0)
  })

  test('parses tenant pool cache ttl in milliseconds', async () => {
    setConfigEnv({
      TENANT_POOL_CACHE_TTL_MS: '30000',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.tenantPoolCacheTtlMs).toBe(30_000)
  })

  test.each([
    '0',
    '-1',
    'nope',
  ])('falls back to the default tenant pool cache ttl for %s', async (ttl) => {
    setConfigEnv({
      TENANT_POOL_CACHE_TTL_MS: ttl,
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.tenantPoolCacheTtlMs).toBe(1000 * 10)
  })

  test('parses fractional tenant pool cache log sample rates', async () => {
    setConfigEnv({
      TENANT_POOL_CACHE_HIT_LOG_SAMPLE_RATE: '0.25',
      TENANT_POOL_CACHE_MISS_LOG_SAMPLE_RATE: '0.75',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.tenantPoolCacheHitLogSampleRate).toBe(0.25)
    expect(config.tenantPoolCacheMissLogSampleRate).toBe(0.75)
  })

  test('clamps tenant pool cache log sample rates to zero and one', async () => {
    setConfigEnv({
      TENANT_POOL_CACHE_HIT_LOG_SAMPLE_RATE: '-0.5',
      TENANT_POOL_CACHE_MISS_LOG_SAMPLE_RATE: '1.5',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.tenantPoolCacheHitLogSampleRate).toBe(0)
    expect(config.tenantPoolCacheMissLogSampleRate).toBe(1)
  })

  test('falls back to default tenant pool cache log sample rates for invalid values', async () => {
    setConfigEnv({
      TENANT_POOL_CACHE_HIT_LOG_SAMPLE_RATE: 'nope',
      TENANT_POOL_CACHE_MISS_LOG_SAMPLE_RATE: 'Infinity',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.tenantPoolCacheHitLogSampleRate).toBe(0)
    expect(config.tenantPoolCacheMissLogSampleRate).toBe(0)
  })
})
