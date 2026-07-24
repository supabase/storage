import { vi } from 'vitest'

const CONFIG_ENV_KEYS = [
  'MULTI_TENANT',
  'IS_MULTITENANT',
  'TENANT_POOL_CACHE_TTL_MS',
  'TENANT_POOL_CACHE_HIT_LOG_SAMPLE_RATE',
  'TENANT_POOL_CACHE_MISS_LOG_SAMPLE_RATE',
  'DATABASE_POOL_DRAIN_TIMEOUT',
  'DATABASE_FREE_POOL_AFTER_INACTIVITY',
  'DATABASE_HEALTHCHECK_UNSCOPED',
  'DATABASE_MAX_CONNECTIONS',
  'DATABASE_WATT_APPLICATION_ENABLED',
  'DATABASE_WATT_ACQUIRE_TIMEOUT',
  'DATABASE_WATT_DESTINATION_ACQUIRE_QUEUE_LIMIT',
  'DATABASE_WATT_DESTINATION_MAX_CONNECTIONS',
  'DATABASE_WATT_GLOBAL_ACQUIRE_QUEUE_LIMIT',
  'DATABASE_WATT_GLOBAL_MAX_CONNECTIONS',
  'DATABASE_WATT_LOCK_IDLE_TIMEOUT',
  'DATABASE_WATT_LOCK_MAX_LIFETIME',
  'DATABASE_WATT_MAX_ACTIVE_POOLS',
  'DATABASE_WATT_POOL_IDLE_TIMEOUT',
  'DATABASE_WATT_SHUTDOWN_TIMEOUT',
  'REQUEST_HARD_LIMITS_ENABLED',
  'STORAGE_S3_REQUEST_CHECKSUM_CALCULATION',
  'STORAGE_S3_RESPONSE_CHECKSUM_VALIDATION',
] as const

type ConfigEnvKey = (typeof CONFIG_ENV_KEYS)[number]

const originalEnv = new Map<ConfigEnvKey, string | undefined>()

function setConfigEnv(env: Partial<Record<ConfigEnvKey, string>>) {
  for (const key of CONFIG_ENV_KEYS) {
    delete process.env[key]
  }

  process.env.MULTI_TENANT = 'true'
  process.env.DATABASE_FREE_POOL_AFTER_INACTIVITY = '60000'
  process.env.DATABASE_MAX_CONNECTIONS = '20'

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
    expect(config.databasePoolDrainTimeout).toBe(30_000)
    expect(config.requestHardLimitsEnabled).toBe(false)
  })

  test('parses request hard limits as disabled by default', async () => {
    setConfigEnv({})

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.requestHardLimitsEnabled).toBe(false)
  })

  test('enables request hard limits from env', async () => {
    setConfigEnv({
      REQUEST_HARD_LIMITS_ENABLED: 'true',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.requestHardLimitsEnabled).toBe(true)
  })

  test('does not force S3 checksum config by default', async () => {
    setConfigEnv({})

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.storageS3RequestChecksumCalculation).toBeUndefined()
    expect(config.storageS3ResponseChecksumValidation).toBeUndefined()
  })

  test('parses split S3 checksum config independently', async () => {
    setConfigEnv({
      STORAGE_S3_REQUEST_CHECKSUM_CALCULATION: 'WHEN_SUPPORTED',
      STORAGE_S3_RESPONSE_CHECKSUM_VALIDATION: 'WHEN_REQUIRED',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.storageS3RequestChecksumCalculation).toBe('WHEN_SUPPORTED')
    expect(config.storageS3ResponseChecksumValidation).toBe('WHEN_REQUIRED')
  })

  test('parses database pool drain timeout in milliseconds', async () => {
    setConfigEnv({
      DATABASE_POOL_DRAIN_TIMEOUT: '45000',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.databasePoolDrainTimeout).toBe(45_000)
  })

  test('disables unscoped database healthchecks by default', async () => {
    setConfigEnv({})

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.databaseHealthcheckUnscoped).toBe(false)
  })

  test('enables unscoped database healthchecks from env', async () => {
    setConfigEnv({ DATABASE_HEALTHCHECK_UNSCOPED: 'true' })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.databaseHealthcheckUnscoped).toBe(true)
  })

  test('disables the Database Watt application by default', async () => {
    setConfigEnv({})

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.databaseWattApplicationEnabled).toBe(false)
  })

  test('enables the Database Watt application from env', async () => {
    setConfigEnv({ DATABASE_WATT_APPLICATION_ENABLED: 'true' })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.databaseWattApplicationEnabled).toBe(true)
  })

  test('defaults Database Watt physical pool management settings', async () => {
    setConfigEnv({})

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config).toMatchObject({
      databaseWattAcquireTimeout: 3_000,
      databaseWattDestinationAcquireQueueLimit: 100,
      databaseWattDestinationMaxConnections: 20,
      databaseWattGlobalAcquireQueueLimit: 500,
      databaseWattGlobalMaxConnections: 20,
      databaseWattLockIdleTimeout: 30_000,
      databaseWattLockMaxLifetime: 120_000,
      databaseWattMaxActivePools: 1_000,
      databaseWattPoolIdleTimeout: 60_000,
      databaseWattShutdownTimeout: 10_000,
    })
  })

  test('parses Database Watt physical pool management settings', async () => {
    setConfigEnv({
      DATABASE_WATT_ACQUIRE_TIMEOUT: '1001',
      DATABASE_WATT_DESTINATION_ACQUIRE_QUEUE_LIMIT: '101',
      DATABASE_WATT_DESTINATION_MAX_CONNECTIONS: '11',
      DATABASE_WATT_GLOBAL_ACQUIRE_QUEUE_LIMIT: '501',
      DATABASE_WATT_GLOBAL_MAX_CONNECTIONS: '51',
      DATABASE_WATT_LOCK_IDLE_TIMEOUT: '30001',
      DATABASE_WATT_LOCK_MAX_LIFETIME: '120001',
      DATABASE_WATT_MAX_ACTIVE_POOLS: '1001',
      DATABASE_WATT_POOL_IDLE_TIMEOUT: '60001',
      DATABASE_WATT_SHUTDOWN_TIMEOUT: '10001',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config).toMatchObject({
      databaseWattAcquireTimeout: 1_001,
      databaseWattDestinationAcquireQueueLimit: 101,
      databaseWattDestinationMaxConnections: 11,
      databaseWattGlobalAcquireQueueLimit: 501,
      databaseWattGlobalMaxConnections: 51,
      databaseWattLockIdleTimeout: 30_001,
      databaseWattLockMaxLifetime: 120_001,
      databaseWattMaxActivePools: 1_001,
      databaseWattPoolIdleTimeout: 60_001,
      databaseWattShutdownTimeout: 10_001,
    })
  })

  test.each([
    '0',
    '-1',
    'nope',
  ])('falls back to the default database pool drain timeout for %s', async (timeout) => {
    setConfigEnv({
      DATABASE_POOL_DRAIN_TIMEOUT: timeout,
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.databasePoolDrainTimeout).toBe(30_000)
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

describe('vectorS3Buckets config parsing', () => {
  const originalValue = process.env.VECTOR_S3_BUCKETS

  afterEach(() => {
    if (originalValue === undefined) {
      delete process.env.VECTOR_S3_BUCKETS
    } else {
      process.env.VECTOR_S3_BUCKETS = originalValue
    }

    vi.resetModules()
  })

  test('defaults to an empty array when VECTOR_S3_BUCKETS is unset', async () => {
    delete process.env.VECTOR_S3_BUCKETS

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.vectorS3Buckets).toEqual([])
  })

  test('defaults to an empty array when VECTOR_S3_BUCKETS is an empty string', async () => {
    process.env.VECTOR_S3_BUCKETS = ''

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.vectorS3Buckets).toEqual([])
  })

  test('parses a comma-separated list of bucket names', async () => {
    process.env.VECTOR_S3_BUCKETS = 'bucket-0,bucket-1,bucket-2'

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.vectorS3Buckets).toEqual(['bucket-0', 'bucket-1', 'bucket-2'])
  })

  test('ignores a trailing comma', async () => {
    process.env.VECTOR_S3_BUCKETS = 'bucket-0, bucket-1,'

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.vectorS3Buckets).toEqual(['bucket-0', 'bucket-1'])
  })
})
