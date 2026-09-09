import { vi } from 'vitest'

const CONFIG_ENV_KEYS = [
  'MULTI_TENANT',
  'IS_MULTITENANT',
  'JWT_JWKS',
  'TENANT_POOL_CACHE_MAX_ENTRIES',
  'DATABASE_POOL_DRAIN_TIMEOUT',
  'DATABASE_HEALTHCHECK_UNSCOPED',
  'OTEL_EXPORTER_OTLP_ENDPOINT',
  'OTEL_EXPORTER_OTLP_METRICS_ENDPOINT',
  'STORAGE_LIFECYCLE_OBJECT_LOCK_TIMEOUT_MS',
  'STORAGE_LIFECYCLE_PAGE_SIZE',
  'STORAGE_LIFECYCLE_JOB_BUDGET_MS',
  'STORAGE_LIFECYCLE_CLAIM_LEASE_MS',
  'STORAGE_LIFECYCLE_RECOVERY_GRACE_MS',
  'STORAGE_LIFECYCLE_FAIRNESS_DELAY_MS',
  'STORAGE_LIFECYCLE_EVALUATION_INTERVAL_MS',
  'STORAGE_LIFECYCLE_SCHEDULER_CRON',
  'STORAGE_LIFECYCLE_TENANT_CONCURRENCY',
  'STORAGE_LIFECYCLE_SHARD_CONCURRENCY',
  'STORAGE_LIFECYCLE_DISPATCH_LIMIT',
  'STORAGE_LIFECYCLE_CONFIG_TRANSACTION_TIMEOUT_MS',
  'STORAGE_S3_CLIENT_TIMEOUT',
  'STORAGE_BACKEND',
  'DATABASE_MULTITENANT_QUERY_TIMEOUT',
  'DATABASE_STATEMENT_TIMEOUT',
  'REQUEST_HARD_LIMITS_ENABLED',
  'STORAGE_VERSIONING_ENABLED',
  'STORAGE_S3_REQUEST_CHECKSUM_CALCULATION',
  'STORAGE_S3_RESPONSE_CHECKSUM_VALIDATION',
  'GLOBAL_S3_BUCKET',
  'GLOBAL_S3_ENDPOINT',
  'GLOBAL_S3_FORCE_PATH_STYLE',
  'REGION',
  'STORAGE_S3_BUCKET',
  'STORAGE_S3_ENDPOINT',
  'STORAGE_S3_FORCE_PATH_STYLE',
  'STORAGE_S3_REGION',
  'PROFILING_AUTOMATIC_ENABLED',
  'PROFILING_S3_BUCKET',
  'PROFILING_S3_REGION',
  'PROFILING_S3_ENDPOINT',
  'PROFILING_S3_FORCE_PATH_STYLE',
  'PROFILING_CAPTURE_SECONDS',
  'PROFILING_CPU_INTERVAL_MICROS',
  'PROFILING_TRIGGER_ELU',
  'PROFILING_MAX_ELU',
  'PROFILING_TRIGGER_DELAY_P99_MS',
  'PROFILING_SEVERE_DELAY_P99_MS',
  'PROFILING_COOLDOWN_SECONDS',
  'PROFILING_MAX_CAPTURES_PER_HOUR',
  'AUTH_URL_SIGNING_JWK_TYPE',
  'AUTH_JWT_ALGORITHM',
  'AUTH_JWT_SECRET',
  'DATABASE_URL',
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

    expect(config.tenantPoolCacheMaxEntries).toBe(16_384)
    expect(config.databasePoolDrainTimeout).toBe(30_000)
    expect(config.requestHardLimitsEnabled).toBe(false)
    expect(config.versioningEnabled).toBe(false)
    expect(config.storageLifecycleObjectLockTimeoutMs).toBe(5000)
    expect(config.storageLifecyclePageSize).toBe(500)
    expect(config.storageLifecycleJobBudgetMs).toBe(20_000)
    expect(config.storageLifecycleClaimLeaseMs).toBe(120_000)
    expect(config.storageLifecycleRecoveryGraceMs).toBe(30_000)
    expect(config.storageLifecycleFairnessDelayMs).toBe(5000)
    expect(config.storageLifecycleEvaluationIntervalMs).toBe(86_400_000)
    expect(config.storageLifecycleConfigurationTransactionTimeoutMs).toBe(30_000)
  })

  test('requires explicit opt-in for lifecycle execution', async () => {
    setConfigEnv({
      STORAGE_VERSIONING_ENABLED: 'true',
      STORAGE_BACKEND: 's3',
      STORAGE_S3_CLIENT_TIMEOUT: '60000',
      STORAGE_LIFECYCLE_OBJECT_LOCK_TIMEOUT_MS: '2500',
      STORAGE_LIFECYCLE_PAGE_SIZE: '750',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.versioningEnabled).toBe(true)
    expect(config.storageLifecycleObjectLockTimeoutMs).toBe(2500)
    expect(config.storageLifecyclePageSize).toBe(500)
  })

  test('enables multitenant lifecycle APIs with the shared versioning flag', async () => {
    setConfigEnv({
      STORAGE_VERSIONING_ENABLED: 'true',
      STORAGE_BACKEND: 's3',
      STORAGE_S3_CLIENT_TIMEOUT: '5000',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.isMultitenant).toBe(true)
    expect(config.versioningEnabled).toBe(true)
  })

  test('allows the shared versioning flag on the file backend', async () => {
    setConfigEnv({
      STORAGE_VERSIONING_ENABLED: 'true',
      STORAGE_BACKEND: 'file',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.versioningEnabled).toBe(true)
    expect(config.storageBackendType).toBe('file')
  })

  test('allows unbounded database statements while multitenant lifecycle is disabled', async () => {
    setConfigEnv({
      STORAGE_VERSIONING_ENABLED: 'false',
      DATABASE_STATEMENT_TIMEOUT: '0',
    })

    const { getConfig } = await import('./config')

    expect(getConfig({ reload: true }).databaseStatementTimeout).toBe(0)
  })

  test('uses the general OTLP endpoint as the metrics endpoint fallback', async () => {
    setConfigEnv({
      OTEL_EXPORTER_OTLP_ENDPOINT: 'http://otel-collector:4317',
      OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: '',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.otlpMetricsEndpoint).toBe('http://otel-collector:4317')
  })

  test('prefers the metrics-specific OTLP endpoint', async () => {
    setConfigEnv({
      OTEL_EXPORTER_OTLP_ENDPOINT: 'http://otel-collector:4317',
      OTEL_EXPORTER_OTLP_METRICS_ENDPOINT: 'http://metrics-collector:4317',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.otlpMetricsEndpoint).toBe('http://metrics-collector:4317')
  })

  test.each([
    [
      'unbounded destructive S3 request',
      {
        STORAGE_BACKEND: 's3',
        STORAGE_VERSIONING_ENABLED: 'true',
        STORAGE_S3_CLIENT_TIMEOUT: '0',
      },
      'Destructive lifecycle on S3 requires a positive STORAGE_S3_CLIENT_TIMEOUT',
    ],
    [
      'malformed destructive S3 timeout',
      {
        STORAGE_BACKEND: 's3',
        STORAGE_VERSIONING_ENABLED: 'true',
        STORAGE_S3_CLIENT_TIMEOUT: 'not-a-number',
      },
      'Destructive lifecycle on S3 requires a positive STORAGE_S3_CLIENT_TIMEOUT',
    ],
    [
      'destructive S3 request outliving its claim lease',
      {
        STORAGE_BACKEND: 's3',
        STORAGE_VERSIONING_ENABLED: 'true',
        STORAGE_S3_CLIENT_TIMEOUT: '60000',
        STORAGE_LIFECYCLE_CLAIM_LEASE_MS: '80000',
        STORAGE_LIFECYCLE_RECOVERY_GRACE_MS: '30000',
      },
      'Destructive lifecycle on S3 requires a positive STORAGE_S3_CLIENT_TIMEOUT',
    ],
    [
      'lifecycle job budget outliving its claim',
      {
        STORAGE_LIFECYCLE_JOB_BUDGET_MS: '120000',
        STORAGE_LIFECYCLE_CLAIM_LEASE_MS: '120000',
      },
      'STORAGE_LIFECYCLE_JOB_BUDGET_MS must be shorter than STORAGE_LIFECYCLE_CLAIM_LEASE_MS',
    ],
    [
      'unbounded tenant lifecycle configuration statement',
      {
        STORAGE_VERSIONING_ENABLED: 'true',
        STORAGE_BACKEND: 's3',
        STORAGE_S3_CLIENT_TIMEOUT: '5000',
        DATABASE_STATEMENT_TIMEOUT: '0',
      },
      'DATABASE_STATEMENT_TIMEOUT must be positive',
    ],
  ])('rejects %s at startup', async (_label, env, message) => {
    setConfigEnv(env)

    const { getConfig } = await import('./config')

    expect(() => getConfig({ reload: true })).toThrow(message)
    expect(() => getConfig()).toThrow(message)
  })

  test('retains the valid cached configuration after a rejected reload', async () => {
    setConfigEnv({})
    const { getConfig } = await import('./config')
    const valid = getConfig({ reload: true })
    process.env.STORAGE_LIFECYCLE_JOB_BUDGET_MS = '120000'

    expect(() => getConfig({ reload: true })).toThrow('must be shorter')
    expect(getConfig()).toBe(valid)
  })

  test.each([
    '2147483648',
    '9007199254740992',
  ])('bounds configuration timers: %s', async (value) => {
    setConfigEnv({ STORAGE_LIFECYCLE_CONFIG_TRANSACTION_TIMEOUT_MS: value })
    const { getConfig } = await import('./config')

    expect(getConfig({ reload: true }).storageLifecycleConfigurationTransactionTimeoutMs).toBe(
      30_000
    )
  })

  test.each([
    ['2147483647', 2147483647],
    ['2147483648', 5000],
    ['9007199254740991', 5000],
  ])('bounds the PostgreSQL lifecycle lock timeout %s', async (value, expected) => {
    setConfigEnv({ STORAGE_LIFECYCLE_OBJECT_LOCK_TIMEOUT_MS: value })
    const { getConfig } = await import('./config')
    expect(getConfig({ reload: true }).storageLifecycleObjectLockTimeoutMs).toBe(expected)
  })

  test('rejects unsafe lifecycle integers before constructing an executor', async () => {
    setConfigEnv({
      STORAGE_LIFECYCLE_OBJECT_LOCK_TIMEOUT_MS: '9007199254740992',
      STORAGE_LIFECYCLE_CLAIM_LEASE_MS: '9007199254740992',
      STORAGE_LIFECYCLE_RECOVERY_GRACE_MS: '9007199254740992',
      STORAGE_LIFECYCLE_FAIRNESS_DELAY_MS: '9007199254740992',
      STORAGE_LIFECYCLE_EVALUATION_INTERVAL_MS: '9007199254740992',
    })
    const { getConfig } = await import('./config')

    expect(getConfig({ reload: true })).toMatchObject({
      storageLifecycleObjectLockTimeoutMs: 5000,
      storageLifecycleClaimLeaseMs: 120_000,
      storageLifecycleRecoveryGraceMs: 30_000,
      storageLifecycleFairnessDelayMs: 5000,
      storageLifecycleEvaluationIntervalMs: 86_400_000,
    })
  })

  test('freezes JWT JWKS configuration and its keys', async () => {
    setConfigEnv({
      JWT_JWKS: JSON.stringify({ keys: [{ kty: 'oct', k: 'secret' }] }),
    })

    const { getConfig } = await import('./config')
    const jwks = getConfig({ reload: true }).jwtJWKS!

    expect(Object.isFrozen(jwks)).toBe(true)
    expect(Object.isFrozen(jwks.keys)).toBe(true)
    expect(Reflect.set(jwks, 'keys', [])).toBe(false)
    const otherKey = { kty: 'oct', k: 'other-secret' }
    const didAppendKey = Reflect.set(jwks.keys, jwks.keys.length, otherKey)
    expect(didAppendKey).toBe(false)
  })

  test('defaults automatic profiling to off with incident-safe thresholds', async () => {
    setConfigEnv({})

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.profilingAutomaticEnabled).toBe(false)
    expect(config.profilingTriggerElu).toBe(0.55)
    expect(config.profilingMaxElu).toBe(0.8)
    expect(config.profilingTriggerDelayP99Ms).toBe(150)
    expect(config.profilingSevereDelayP99Ms).toBe(1_000)
    expect(config.profilingCaptureSeconds).toBe(30)
  })

  test.each([
    ['PROFILING_CAPTURE_SECONDS', '0'],
    ['PROFILING_CAPTURE_SECONDS', '301'],
    ['PROFILING_CAPTURE_SECONDS', '1.5'],
    ['PROFILING_CPU_INTERVAL_MICROS', '999'],
    ['PROFILING_CPU_INTERVAL_MICROS', '1000001'],
    ['PROFILING_CPU_INTERVAL_MICROS', '1.5'],
  ] as const)('rejects unsafe profiling setting %s=%s', async (key, value) => {
    setConfigEnv({ [key]: value })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.profilingCaptureSeconds).toBe(30)
    expect(config.profilingCpuIntervalMicros).toBe(33_000)
  })

  test('accepts bounded profiling capture and sampling settings', async () => {
    setConfigEnv({
      PROFILING_CAPTURE_SECONDS: '300',
      PROFILING_CPU_INTERVAL_MICROS: '1000',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.profilingCaptureSeconds).toBe(300)
    expect(config.profilingCpuIntervalMicros).toBe(1_000)
  })

  test('preserves explicit zero profiling cooldown and capture budget', async () => {
    setConfigEnv({
      PROFILING_COOLDOWN_SECONDS: '0',
      PROFILING_MAX_CAPTURES_PER_HOUR: '0',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.profilingCooldownSeconds).toBe(0)
    expect(config.profilingMaxCapturesPerHour).toBe(0)
  })

  test('inherits the storage S3 transport for profiles', async () => {
    setConfigEnv({
      STORAGE_S3_REGION: 'storage-region',
      STORAGE_S3_ENDPOINT: 'http://storage-s3:9000',
      STORAGE_S3_FORCE_PATH_STYLE: 'true',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.profilingS3Region).toBe('storage-region')
    expect(config.profilingS3Endpoint).toBe('http://storage-s3:9000')
    expect(config.profilingS3ForcePathStyle).toBe(true)
  })

  test('prefers the explicit profiling S3 transport', async () => {
    setConfigEnv({
      STORAGE_S3_REGION: 'storage-region',
      STORAGE_S3_ENDPOINT: 'http://storage-s3:9000',
      STORAGE_S3_FORCE_PATH_STYLE: 'true',
      PROFILING_S3_REGION: 'profile-region',
      PROFILING_S3_ENDPOINT: 'http://profile-s3:9000',
      PROFILING_S3_FORCE_PATH_STYLE: 'false',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.profilingS3Region).toBe('profile-region')
    expect(config.profilingS3Endpoint).toBe('http://profile-s3:9000')
    expect(config.profilingS3ForcePathStyle).toBe(false)
  })

  test('rejects using the normal data bucket for profiles', async () => {
    setConfigEnv({ STORAGE_S3_BUCKET: 'data', PROFILING_S3_BUCKET: 'data' })

    const { getConfig } = await import('./config')
    expect(() => getConfig({ reload: true })).toThrow(
      'PROFILING_S3_BUCKET must be different from the normal storage data bucket'
    )
  })

  test('rejects a profiling overload cutoff at or below the trigger threshold', async () => {
    setConfigEnv({ PROFILING_TRIGGER_ELU: '0.55', PROFILING_MAX_ELU: '0.55' })

    const { getConfig } = await import('./config')
    expect(() => getConfig({ reload: true })).toThrow(
      'PROFILING_MAX_ELU must be greater than PROFILING_TRIGGER_ELU'
    )
  })

  test.each([
    '149',
    '150',
  ])('rejects a severe delay threshold at or below the trigger threshold: %s', async (severeDelay) => {
    setConfigEnv({
      PROFILING_TRIGGER_DELAY_P99_MS: '150',
      PROFILING_SEVERE_DELAY_P99_MS: severeDelay,
    })

    const { getConfig } = await import('./config')
    expect(() => getConfig({ reload: true })).toThrow(
      'PROFILING_SEVERE_DELAY_P99_MS must be greater than PROFILING_TRIGGER_DELAY_P99_MS'
    )
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

  test('defaults the url signing key type to HS512', async () => {
    setConfigEnv({})

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.urlSigningJwkType).toBe('HS512')
  })

  test('parses the url signing key type from env', async () => {
    setConfigEnv({
      AUTH_URL_SIGNING_JWK_TYPE: 'ES256',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.urlSigningJwkType).toBe('ES256')
  })

  test('rejects an unrecognized url signing key type', async () => {
    setConfigEnv({
      AUTH_URL_SIGNING_JWK_TYPE: 'RS256',
    })

    const { getConfig } = await import('./config')
    expect(() => getConfig({ reload: true })).toThrow(
      'Invalid url signing key type "RS256". Expected one of: HS512, ES256.'
    )
  })

  test('defaults the jwt algorithm to HS256', async () => {
    setConfigEnv({})

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.jwtAlgorithm).toBe('HS256')
  })

  test('parses the jwt algorithm from env', async () => {
    setConfigEnv({
      AUTH_JWT_ALGORITHM: 'HS384',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.jwtAlgorithm).toBe('HS384')
  })

  test('rejects an unsupported jwt algorithm', async () => {
    setConfigEnv({
      AUTH_JWT_ALGORITHM: 'ES256',
    })

    const { getConfig } = await import('./config')
    expect(() => getConfig({ reload: true })).toThrow(
      'Invalid jwt algorithm "ES256". Expected one of: HS256, HS384, HS512.'
    )
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

  test.each([
    '0',
    '-1',
    'nope',
    '1.5',
    '1000ms',
    '2147483648',
  ])('falls back to the default database pool drain timeout for %s', async (timeout) => {
    setConfigEnv({
      DATABASE_POOL_DRAIN_TIMEOUT: timeout,
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.databasePoolDrainTimeout).toBe(30_000)
  })

  test('parses tenant pool cache maximum entries', async () => {
    setConfigEnv({
      TENANT_POOL_CACHE_MAX_ENTRIES: '24576',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.tenantPoolCacheMaxEntries).toBe(24_576)
  })

  test('accepts the tenant pool cache maximum entry ceiling', async () => {
    setConfigEnv({
      TENANT_POOL_CACHE_MAX_ENTRIES: '65536',
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.tenantPoolCacheMaxEntries).toBe(65_536)
  })

  test.each([
    '0',
    '-1',
    'nope',
    '1.5',
    '1e3',
    '0x100',
    '+123',
    '0123',
    ' 123',
    '123 ',
    '16384oops',
    '65537',
  ])('falls back to the default tenant pool cache maximum for %s', async (maximum) => {
    setConfigEnv({
      TENANT_POOL_CACHE_MAX_ENTRIES: maximum,
    })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.tenantPoolCacheMaxEntries).toBe(16_384)
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
