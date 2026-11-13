import { vi } from 'vitest'

const CONFIG_ENV_KEYS = [
  'MULTI_TENANT',
  'IS_MULTITENANT',
  'JWT_JWKS',
  'TENANT_POOL_CACHE_MAX_ENTRIES',
  ...[
    'TENANT_CONFIG',
    'TENANT_JWKS',
    'TENANT_S3_CREDENTIALS',
    'JWT',
    'JWT_SIGNING_KEY',
    'JWT_VERIFICATION_KEY',
    'PGVECTOR_METRIC',
    'PROGRESSIVE_MIGRATION_SUBMISSIONS',
  ].flatMap((name) => [`${name}_CACHE_MAX_ENTRIES`, `${name}_CACHE_TTL_MS`]),
  'DATABASE_POOL_DRAIN_TIMEOUT',
  'DATABASE_ENABLE_PIPELINING',
  'DATABASE_HEALTHCHECK_UNSCOPED',
  'DB_SERVICE_ROLE',
  'DB_SUPER_USER',
  'DB_REJECT_SUPER_USER_ROLE',
  'OTEL_EXPORTER_OTLP_ENDPOINT',
  'OTEL_EXPORTER_OTLP_METRICS_ENDPOINT',
  'REQUEST_HARD_LIMITS_ENABLED',
  'STORAGE_LIFECYCLE_PAGE_SIZE',
  'STORAGE_LIFECYCLE_DELETE_BATCH_SIZE',
  'STORAGE_LIFECYCLE_JOB_BUDGET_MS',
  'STORAGE_LIFECYCLE_CLAIM_LEASE_MS',
  'STORAGE_S3_REQUEST_CHECKSUM_CALCULATION',
  'STORAGE_S3_RESPONSE_CHECKSUM_VALIDATION',
  'STORAGE_S3_CLIENT_TIMEOUT',
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
  'CLUSTER_DISCOVERY_TIMEOUT_MS',
  'CLUSTER_DISCOVERY_POLL_INTERVAL_MS',
  'CLUSTER_DISCOVERY_ECS_MAX_RPS',
  'DATABASE_APPLICATION_NAME',
  'VERSION',
] as const

type ConfigEnvKey = (typeof CONFIG_ENV_KEYS)[number]

function setConfigEnv(env: Partial<Record<ConfigEnvKey, string>>) {
  for (const key of CONFIG_ENV_KEYS) {
    vi.stubEnv(key, undefined)
  }

  vi.stubEnv('MULTI_TENANT', 'true')

  for (const [key, value] of Object.entries(env)) {
    vi.stubEnv(key, value)
  }
}

describe('configuration parsing', () => {
  test.each([
    ['TENANT_CONFIG', 'tenantConfig', 16384, 3600000],
    ['TENANT_JWKS', 'tenantJwks', 16384, 3600000],
    ['TENANT_S3_CREDENTIALS', 'tenantS3Credentials', 16384, 3600000],
    ['JWT', 'jwt', 65536, 0],
    ['JWT_SIGNING_KEY', 'jwtSigningKey', 16384, 0],
    ['JWT_VERIFICATION_KEY', 'jwtVerificationKey', 16384, 0],
    ['PGVECTOR_METRIC', 'pgvectorMetric', 1000, 300000],
    ['PROGRESSIVE_MIGRATION_SUBMISSIONS', 'progressiveMigrationSubmissions', 4096, 60000],
  ] as const)(
    'parses %s cache limits and preserves defaults for invalid values',
    async (prefix, key, max, ttl) => {
      setConfigEnv({})
      const { getConfig, setEnvPaths } = await import('./config')
      setEnvPaths([])
      expect(getConfig({ reload: true }).caches[key]).toEqual({ maxEntries: max, ttlMs: ttl })
      vi.stubEnv(`${prefix}_CACHE_MAX_ENTRIES`, '2')
      vi.stubEnv(`${prefix}_CACHE_TTL_MS`, '123')
      expect(getConfig({ reload: true }).caches[key]).toEqual({
        maxEntries: 2,
        ttlMs: 123,
      })
      const maximum = prefix === 'JWT' ? 262144 : 65536
      vi.stubEnv(`${prefix}_CACHE_MAX_ENTRIES`, String(maximum))
      vi.stubEnv(`${prefix}_CACHE_TTL_MS`, '2147483647')
      expect(getConfig({ reload: true }).caches[key]).toEqual({
        maxEntries: maximum,
        ttlMs: 2147483647,
      })
      for (const invalid of ['0', '-1', '1e3', '1.5', ' 2', '999999999999999999999']) {
        vi.stubEnv(`${prefix}_CACHE_MAX_ENTRIES`, invalid)
        vi.stubEnv(`${prefix}_CACHE_TTL_MS`, invalid)
        expect(getConfig({ reload: true }).caches[key]).toEqual({ maxEntries: max, ttlMs: ttl })
      }
      vi.stubEnv(`${prefix}_CACHE_MAX_ENTRIES`, prefix === 'JWT' ? '262145' : '65537')
      vi.stubEnv(`${prefix}_CACHE_TTL_MS`, '2147483648')
      expect(getConfig({ reload: true }).caches[key]).toEqual({ maxEntries: max, ttlMs: ttl })
    }
  )
  test.each([
    ['true', 'true', 'admin', 'admin', true],
    ['true', 'true', 'postgres', undefined, true],
    ['true', 'true', 'service_role', 'postgres', false],
    ['true', 'false', 'admin', 'admin', false],
    ['false', 'true', 'admin', 'admin', false],
  ])(
    'validates service role with multitenant=%s, rejection=%s, service=%s, superuser=%s',
    async (multitenant, reject, serviceRole, superUser, invalid) => {
      setConfigEnv({ MULTI_TENANT: multitenant, DB_REJECT_SUPER_USER_ROLE: reject })
      vi.stubEnv('DB_SERVICE_ROLE', serviceRole)
      vi.stubEnv('DB_SUPER_USER', superUser)
      vi.stubEnv('AUTH_JWT_SECRET', 'test-secret')
      vi.stubEnv('DATABASE_URL', 'postgresql://localhost/test')
      const { getConfig, setEnvPaths } = await import('./config')
      setEnvPaths([])
      const load = () => getConfig({ reload: true })
      if (invalid) {
        expect(load).toThrow('DB_SERVICE_ROLE must differ from DB_SUPER_USER')
      } else {
        expect(load).not.toThrow()
      }
    }
  )

  test.each([
    [undefined, false],
    ['false', false],
    ['true', true],
  ])('opts in to admin role rejection with %s', async (value, expected) => {
    setConfigEnv({})
    vi.stubEnv('DB_REJECT_SUPER_USER_ROLE', value)
    const { getConfig, setEnvPaths } = await import('./config')
    setEnvPaths([])
    expect(getConfig({ reload: true }).dbRejectSuperUserRole).toBe(expected)
  })

  afterEach(() => {
    vi.resetModules()
  })

  test.each([
    [undefined, 'Supabase Storage API Worker 1.2.3'],
    ['custom', 'custom'],
  ])('worker application name with override %s', async (override, expected) => {
    setConfigEnv({})
    vi.stubEnv('DATABASE_APPLICATION_NAME', override)
    vi.stubEnv('VERSION', '1.2.3')
    const { getConfig, markWorker, setEnvPaths } = await import('./config')
    setEnvPaths([])
    markWorker()
    expect(getConfig({ reload: true }).databaseApplicationName).toBe(expected)
  })

  test.each([
    [undefined, 0, 60_000],
    ['0', 0, 0],
    ['15000', 15_000, 15_000],
    ['2147483647', 2_147_483_647, 2_147_483_647],
    ['-1', 0, 60_000],
    ['not-a-number', 0, 60_000],
    ['Infinity', 0, 60_000],
    ['1.5', 0, 60_000],
    ['1000ms', 0, 60_000],
    ['2147483648', 0, 60_000],
  ])('parses S3 inactivity timeout %s for the backend and TUS', async (value, backend, tus) => {
    setConfigEnv({})
    vi.stubEnv('STORAGE_S3_CLIENT_TIMEOUT', value)
    const { getConfig, setEnvPaths } = await import('./config')
    setEnvPaths([])

    expect(getConfig({ reload: true })).toMatchObject({
      storageS3ClientTimeout: backend,
      tusS3ClientTimeout: tus,
    })
  })

  test('defaults tenant pool cache settings', async () => {
    setConfigEnv({})

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.tenantPoolCacheMaxEntries).toBe(16_384)
    expect(config.databasePoolDrainTimeout).toBe(30_000)
    expect(config.requestHardLimitsEnabled).toBe(false)
  })

  test('configures the cluster discovery deadline, cadence and ECS family budget', async () => {
    setConfigEnv({
      CLUSTER_DISCOVERY_TIMEOUT_MS: '45000',
      CLUSTER_DISCOVERY_POLL_INTERVAL_MS: '60000',
      CLUSTER_DISCOVERY_ECS_MAX_RPS: '5',
    })
    const { getConfig } = await import('./config')
    expect(getConfig({ reload: true })).toMatchObject({
      clusterDiscoveryTimeoutMs: 45_000,
      clusterDiscoveryPollIntervalMs: 60_000,
      clusterDiscoveryEcsMaxRps: 5,
    })
  })

  test.each(['0', '9007199254740992'])('uses safe discovery defaults for RPS=%s', async (rps) => {
    setConfigEnv({
      CLUSTER_DISCOVERY_TIMEOUT_MS: '2147483648',
      CLUSTER_DISCOVERY_POLL_INTERVAL_MS: '2147483648',
      CLUSTER_DISCOVERY_ECS_MAX_RPS: rps,
    })
    const { getConfig } = await import('./config')
    expect(getConfig({ reload: true })).toMatchObject({
      clusterDiscoveryTimeoutMs: 30_000,
      clusterDiscoveryPollIntervalMs: 20_000,
      clusterDiscoveryEcsMaxRps: 10,
    })
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
      'lifecycle job budget and claim 1 ms above the queue expiry limit',
      {
        STORAGE_LIFECYCLE_JOB_BUDGET_MS: '100000',
        STORAGE_LIFECYCLE_CLAIM_LEASE_MS: '86240001',
      },
      'must not exceed 23 hours 59 minutes',
    ],
    [
      'lifecycle job budget at the largest accepted knob value',
      { STORAGE_LIFECYCLE_JOB_BUDGET_MS: '2147483647' },
      'STORAGE_LIFECYCLE_JOB_BUDGET_MS must be shorter than STORAGE_LIFECYCLE_CLAIM_LEASE_MS',
    ],
    [
      'lifecycle job budget outliving its claim',
      { STORAGE_LIFECYCLE_JOB_BUDGET_MS: '120000', STORAGE_LIFECYCLE_CLAIM_LEASE_MS: '120000' },
      'STORAGE_LIFECYCLE_JOB_BUDGET_MS must be shorter than STORAGE_LIFECYCLE_CLAIM_LEASE_MS',
    ],
  ])('rejects %s at startup', async (_label, env, message) => {
    setConfigEnv(env)

    const { getConfig } = await import('./config')

    expect(() => getConfig({ reload: true })).toThrow(message)
  })

  test('accepts lifecycle timers at the maximum budget and claim total', async () => {
    setConfigEnv({
      STORAGE_LIFECYCLE_JOB_BUDGET_MS: '100000',
      STORAGE_LIFECYCLE_CLAIM_LEASE_MS: '86240000',
    })
    const { getConfig } = await import('./config')

    expect(() => getConfig({ reload: true })).not.toThrow()
  })

  test.each([
    ['STORAGE_LIFECYCLE_PAGE_SIZE', 480],
    ['STORAGE_LIFECYCLE_DELETE_BATCH_SIZE', 48],
    ['STORAGE_LIFECYCLE_JOB_BUDGET_MS', 2147483647],
    ['STORAGE_LIFECYCLE_CLAIM_LEASE_MS', 2147483647],
  ] as const)('rejects invalid values of %s naming the variable', async (name, maximum) => {
    const { getConfig } = await import('./config')

    for (const value of [
      '120s',
      '0',
      '-1',
      '04',
      '1.5',
      'NaN',
      '5e4',
      String(maximum + 1),
      '9007199254740992',
      '050000',
      '50000.0',
      '+50000',
      '0x10',
    ]) {
      setConfigEnv({ [name]: value })
      expect(() => getConfig({ reload: true })).toThrow(
        `${name} must be a positive integer of at most ${maximum}`
      )
    }
  })

  test('defaults unset lifecycle settings and parses valid ones', async () => {
    const { getConfig, setEnvPaths } = await import('./config')

    setEnvPaths([])
    setConfigEnv({})
    expect(getConfig({ reload: true })).toMatchObject({
      storageLifecyclePageSize: 480,
      storageLifecycleDeleteBatchSize: 48,
      storageLifecycleJobBudgetMs: 50_000,
      storageLifecycleClaimLeaseMs: 120_000,
    })

    setConfigEnv({
      STORAGE_LIFECYCLE_PAGE_SIZE: '',
      STORAGE_LIFECYCLE_DELETE_BATCH_SIZE: '',
      STORAGE_LIFECYCLE_JOB_BUDGET_MS: '',
      STORAGE_LIFECYCLE_CLAIM_LEASE_MS: '',
    })
    expect(getConfig({ reload: true })).toMatchObject({
      storageLifecyclePageSize: 480,
      storageLifecycleDeleteBatchSize: 48,
      storageLifecycleJobBudgetMs: 50_000,
      storageLifecycleClaimLeaseMs: 120_000,
    })

    setConfigEnv({
      STORAGE_LIFECYCLE_PAGE_SIZE: '200',
      STORAGE_LIFECYCLE_DELETE_BATCH_SIZE: '12',
      STORAGE_LIFECYCLE_JOB_BUDGET_MS: '10000',
      STORAGE_LIFECYCLE_CLAIM_LEASE_MS: '90000',
    })
    expect(getConfig({ reload: true })).toMatchObject({
      storageLifecyclePageSize: 200,
      storageLifecycleDeleteBatchSize: 12,
      storageLifecycleJobBudgetMs: 10_000,
      storageLifecycleClaimLeaseMs: 90_000,
    })

    setConfigEnv({
      STORAGE_LIFECYCLE_PAGE_SIZE: ' 200\n',
      STORAGE_LIFECYCLE_DELETE_BATCH_SIZE: ' \t',
    })
    expect(getConfig({ reload: true })).toMatchObject({
      storageLifecyclePageSize: 200,
      storageLifecycleDeleteBatchSize: 48,
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

  test.each(['149', '150'])(
    'rejects a severe delay threshold at or below the trigger threshold: %s',
    async (severeDelay) => {
      setConfigEnv({
        PROFILING_TRIGGER_DELAY_P99_MS: '150',
        PROFILING_SEVERE_DELAY_P99_MS: severeDelay,
      })

      const { getConfig } = await import('./config')
      expect(() => getConfig({ reload: true })).toThrow(
        'PROFILING_SEVERE_DELAY_P99_MS must be greater than PROFILING_TRIGGER_DELAY_P99_MS'
      )
    }
  )

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

  test.each([
    [undefined, false],
    ['false', false],
    ['true', true],
    ['1', false],
    ['TRUE', false],
  ])('parses DATABASE_ENABLE_PIPELINING=%s as %s', async (value, enabled) => {
    setConfigEnv({ DATABASE_ENABLE_PIPELINING: value })

    const { getConfig, setEnvPaths } = await import('./config')
    setEnvPaths([])

    expect(getConfig({ reload: true }).databaseEnablePipelining).toBe(enabled)
  })

  test('enables unscoped database healthchecks from env', async () => {
    setConfigEnv({ DATABASE_HEALTHCHECK_UNSCOPED: 'true' })

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.databaseHealthcheckUnscoped).toBe(true)
  })

  test.each(['0', '-1', 'nope', '1.5', '1000ms', '2147483648'])(
    'falls back to the default database pool drain timeout for %s',
    async (timeout) => {
      setConfigEnv({
        DATABASE_POOL_DRAIN_TIMEOUT: timeout,
      })

      const { getConfig } = await import('./config')
      const config = getConfig({ reload: true })

      expect(config.databasePoolDrainTimeout).toBe(30_000)
    }
  )

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

  test('retains the valid cached configuration after a rejected reload', async () => {
    setConfigEnv({})
    const { getConfig, setEnvPaths } = await import('./config')
    setEnvPaths([])
    const valid = getConfig({ reload: true })

    setConfigEnv({ PROFILING_TRIGGER_ELU: '0.9', PROFILING_MAX_ELU: '0.5' })

    expect(() => getConfig({ reload: true })).toThrow(
      'PROFILING_MAX_ELU must be greater than PROFILING_TRIGGER_ELU'
    )
    expect(getConfig()).toBe(valid)
  })
})

describe('vectorS3Buckets config parsing', () => {
  afterEach(() => {
    vi.resetModules()
  })

  test('defaults to an empty array when VECTOR_S3_BUCKETS is unset', async () => {
    vi.stubEnv('VECTOR_S3_BUCKETS', undefined)

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.vectorS3Buckets).toEqual([])
  })

  test('defaults to an empty array when VECTOR_S3_BUCKETS is an empty string', async () => {
    vi.stubEnv('VECTOR_S3_BUCKETS', '')

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.vectorS3Buckets).toEqual([])
  })

  test('parses a comma-separated list of bucket names', async () => {
    vi.stubEnv('VECTOR_S3_BUCKETS', 'bucket-0,bucket-1,bucket-2')

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.vectorS3Buckets).toEqual(['bucket-0', 'bucket-1', 'bucket-2'])
  })

  test('ignores a trailing comma', async () => {
    vi.stubEnv('VECTOR_S3_BUCKETS', 'bucket-0, bucket-1,')

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.vectorS3Buckets).toEqual(['bucket-0', 'bucket-1'])
  })
})
