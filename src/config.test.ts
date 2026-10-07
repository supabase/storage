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
  'REQUEST_HARD_LIMITS_ENABLED',
  'STORAGE_LIFECYCLE_ENABLED',
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
  'CLUSTER_DISCOVERY_TIMEOUT_MS',
  'CLUSTER_DISCOVERY_POLL_INTERVAL_MS',
  'CLUSTER_DISCOVERY_ECS_MAX_RPS',
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
  afterEach(() => {
    vi.resetModules()
  })

  test('defaults tenant pool cache settings', async () => {
    setConfigEnv({})

    const { getConfig } = await import('./config')
    const config = getConfig({ reload: true })

    expect(config.tenantPoolCacheMaxEntries).toBe(16_384)
    expect(config.databasePoolDrainTimeout).toBe(30_000)
    expect(config.requestHardLimitsEnabled).toBe(false)
    expect(config.storageLifecycleEnabled).toBe(false)
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

  test('requires explicit opt-in for lifecycle configuration routes', async () => {
    setConfigEnv({ STORAGE_LIFECYCLE_ENABLED: 'true' })

    const { getConfig } = await import('./config')

    expect(getConfig({ reload: true }).storageLifecycleEnabled).toBe(true)
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

  // github issue #629 — self-hosted deployments that supply only JWT_JWKS must
  // be able to sign URLs with an asymmetric EC key, instead of silently
  // falling back to the HMAC jwtSecret. The parser auto-selects the first
  // signing-capable key when the env JSON omits an explicit urlSigningKey.
  test('auto-populates urlSigningKey with the first signing-capable EC key in JWT_JWKS', async () => {
    const ecKey = {
      kty: 'EC',
      crv: 'P-256',
      x: 'x',
      y: 'y',
      d: 'private',
      kid: 'ec-signing',
    }
    setConfigEnv({ JWT_JWKS: JSON.stringify({ keys: [ecKey] }) })

    const { getConfig } = await import('./config')
    const jwks = getConfig({ reload: true }).jwtJWKS!

    expect(jwks.urlSigningKey).toEqual(ecKey)
  })

  test('auto-populates urlSigningKey with the first signing-capable oct key in JWT_JWKS', async () => {
    const octKey = { kty: 'oct', k: 'secret-material', kid: 'oct-signing' }
    setConfigEnv({ JWT_JWKS: JSON.stringify({ keys: [octKey] }) })

    const { getConfig } = await import('./config')
    const jwks = getConfig({ reload: true }).jwtJWKS!

    expect(jwks.urlSigningKey).toEqual(octKey)
  })

  test('leaves urlSigningKey unset when JWT_JWKS has only RSA (RSA cannot sign storage URLs)', async () => {
    setConfigEnv({
      JWT_JWKS: JSON.stringify({
        keys: [{ kty: 'RSA', n: 'n', e: 'e', kid: 'rsa-verify-only' }],
      }),
    })

    const { getConfig } = await import('./config')
    const jwks = getConfig({ reload: true }).jwtJWKS!

    expect(jwks.urlSigningKey).toBeUndefined()
  })

  test('leaves urlSigningKey unset when an EC key omits private material (d)', async () => {
    setConfigEnv({
      JWT_JWKS: JSON.stringify({
        keys: [{ kty: 'EC', crv: 'P-256', x: 'x', y: 'y', kid: 'ec-public-only' }],
      }),
    })

    const { getConfig } = await import('./config')
    const jwks = getConfig({ reload: true }).jwtJWKS!

    expect(jwks.urlSigningKey).toBeUndefined()
  })

  test('skips verification-only keys and selects the first signing-capable one', async () => {
    const rsaVerify = { kty: 'RSA', n: 'n', e: 'e', kid: 'rsa-verify' }
    const ecPublic = { kty: 'EC', crv: 'P-256', x: 'x', y: 'y', kid: 'ec-public' }
    const ecSign = {
      kty: 'EC',
      crv: 'P-256',
      x: 'x2',
      y: 'y2',
      d: 'priv',
      kid: 'ec-sign',
    }
    setConfigEnv({
      JWT_JWKS: JSON.stringify({ keys: [rsaVerify, ecPublic, ecSign] }),
    })

    const { getConfig } = await import('./config')
    const jwks = getConfig({ reload: true }).jwtJWKS!

    expect(jwks.urlSigningKey).toEqual(ecSign)
  })

  test('preserves an explicitly-provided urlSigningKey in JWT_JWKS', async () => {
    const explicit = {
      kty: 'EC',
      crv: 'P-256',
      x: 'xe',
      y: 'ye',
      d: 'de',
      kid: 'ec-explicit',
    }
    const other = { kty: 'oct', k: 'secret', kid: 'oct-first' }
    setConfigEnv({
      JWT_JWKS: JSON.stringify({ keys: [other, explicit], urlSigningKey: explicit }),
    })

    const { getConfig } = await import('./config')
    const jwks = getConfig({ reload: true }).jwtJWKS!

    expect(jwks.urlSigningKey).toEqual(explicit)
  })

  test('rejects invalid JWT_JWKS JSON with the documented error', async () => {
    setConfigEnv({ JWT_JWKS: 'not-json' })

    const { getConfig } = await import('./config')

    expect(() => getConfig({ reload: true })).toThrow('Unable to parse JWT_JWKS value to JSON')
  })

  test('skips a signing-capable key explicitly marked use="enc" and selects the next capable key', async () => {
    const encOnly = {
      kty: 'EC',
      crv: 'P-256',
      x: 'xe',
      y: 'ye',
      d: 'de',
      use: 'enc',
      kid: 'ec-encrypt',
    }
    const sign = {
      kty: 'EC',
      crv: 'P-256',
      x: 'xs',
      y: 'ys',
      d: 'ds',
      kid: 'ec-sign',
    }
    setConfigEnv({ JWT_JWKS: JSON.stringify({ keys: [encOnly, sign] }) })

    const { getConfig } = await import('./config')
    const jwks = getConfig({ reload: true }).jwtJWKS!

    expect(jwks.urlSigningKey).toEqual(sign)
  })

  // The round-trip proof for github issue #629: an operator who supplies only
  // an EC signing key via JWT_JWKS gets URLs actually signed with ES256
  // (asymmetric), not with the HMAC jwtSecret fallback. Done at unit level
  // using a real jose signing round-trip so the behavior is proven without
  // the multi-tenant postgres fixture.
  test('round-trip: auto-selected EC key from JWT_JWKS signs tokens with ES256 (not HS256)', async () => {
    const { generateES256JWK, signJWT } = await import('./internal/auth/jwt')
    const ecKey = await generateES256JWK()
    ecKey.kid = 'ec-signing-roundtrip'

    setConfigEnv({ JWT_JWKS: JSON.stringify({ keys: [ecKey] }) })
    const { getConfig } = await import('./config')
    const urlSigningKey = getConfig({ reload: true }).jwtJWKS?.urlSigningKey

    // Precondition: auto-selection picked the EC key out of JWT_JWKS.
    expect(urlSigningKey).toEqual(ecKey)
    expect(typeof urlSigningKey).toBe('object')

    const token = await signJWT({ sub: 'storage-url-sign-629' }, urlSigningKey!, 60)
    const header = JSON.parse(Buffer.from(token.split('.')[0], 'base64url').toString('utf8'))

    // The signing path actually used the EC key — the alg is the key's ES256,
    // not the HS256 HMAC default. This is the behavior change #629 asked for.
    expect(header.alg).toBe('ES256')
    expect(header.kid).toBe('ec-signing-roundtrip')
  })

  // Negative control: without any signing-capable key, the regression that
  // #629 reported stays — urlSigningKey resolves to the HMAC secret string.
  // Guards against a future refactor that would silently reintroduce the
  // reverse direction (asymmetric key loaded, symmetric fallback picked).
  test('round-trip: RSA-only JWT_JWKS leaves urlSigningKey unset so getJwtSecret can fall back to the HMAC jwtSecret', async () => {
    setConfigEnv({
      JWT_JWKS: JSON.stringify({
        keys: [{ kty: 'RSA', n: 'n', e: 'e', kid: 'rsa-verify-only' }],
      }),
    })
    const { getConfig } = await import('./config')
    const jwks = getConfig({ reload: true }).jwtJWKS!

    expect(jwks.urlSigningKey).toBeUndefined()
    // keys still parsed and preserved for verification purposes
    expect(jwks.keys).toHaveLength(1)
    expect(jwks.keys[0].kid).toBe('rsa-verify-only')
  })

  // --- describeJwtJwksMisconfiguration (observability for #629) ---------
  // The reporter spent debugging time tracing a "storage outage" to a
  // mis-configured JWT_JWKS. These tests pin the invariant that the
  // mis-configuration detector returns an actionable description for the
  // one dangerous shape and `undefined` for every well-formed configuration.
  // The actual warning emission lives in getSingleTenantJwtConfig; this
  // helper stays pure so it can be unit-tested without touching the logger
  // import graph (@internal/monitoring itself depends on getConfig).

  test('describeJwtJwksMisconfiguration reports an actionable description when keys exist but none can sign URLs', async () => {
    const { describeJwtJwksMisconfiguration, freezeJwksConfig } = await import('./config')
    const rsaKey = {
      kty: 'RSA',
      n: 'public-modulus-material',
      e: 'public-exponent-material',
      kid: 'rsa-verify-only',
    }
    const result = describeJwtJwksMisconfiguration(
      freezeJwksConfig({ keys: [rsaKey as never] }),
      'ES256'
    )

    expect(result).toBeDefined()
    expect(result?.message).toContain('JWT_JWKS has no URL-signing-capable key')
    expect(result?.message).toContain('HMAC jwtSecret')
    expect(result?.metadata).toEqual({
      keyCount: 1,
      keyTypes: ['RSA'],
      urlSigningJwkType: 'ES256',
    })

    // Red-team guard: the serialized description must never carry the JWK's
    // own key material (`n`, `e`, `k`, `d`, kid) — only non-sensitive counts
    // and types the operator can act on.
    const serialized = JSON.stringify(result)
    expect(serialized).not.toContain('public-modulus-material')
    expect(serialized).not.toContain('public-exponent-material')
    expect(serialized).not.toContain('rsa-verify-only')
    expect(serialized).not.toMatch(/"[kdne]":\s*"/)
  })

  test('describeJwtJwksMisconfiguration returns undefined when JWT_JWKS is absent', async () => {
    const { describeJwtJwksMisconfiguration } = await import('./config')
    expect(describeJwtJwksMisconfiguration(undefined, 'HS512')).toBeUndefined()
  })

  test('describeJwtJwksMisconfiguration returns undefined when the JWKS has an EC signing key', async () => {
    const { describeJwtJwksMisconfiguration, freezeJwksConfig, pickUrlSigningKey } = await import(
      './config'
    )
    const ec = {
      kty: 'EC' as const,
      crv: 'P-256',
      x: 'x',
      y: 'y',
      d: 'd',
      k: '',
      kid: 'ec-sign',
    }
    const jwks = freezeJwksConfig({ keys: [ec], urlSigningKey: pickUrlSigningKey([ec])! })
    expect(describeJwtJwksMisconfiguration(jwks, 'ES256')).toBeUndefined()
  })

  test('describeJwtJwksMisconfiguration returns undefined when an explicit urlSigningKey is supplied', async () => {
    const { describeJwtJwksMisconfiguration, freezeJwksConfig } = await import('./config')
    const explicit = {
      kty: 'oct' as const,
      k: 'secret',
      kid: 'oct-explicit',
    }
    const jwks = freezeJwksConfig({
      keys: [{ kty: 'RSA', n: 'n', e: 'e', kid: 'rsa' } as never, explicit],
      urlSigningKey: explicit,
    })
    expect(describeJwtJwksMisconfiguration(jwks, 'HS512')).toBeUndefined()
  })

  test('describeJwtJwksMisconfiguration returns undefined for an empty keys array', async () => {
    const { describeJwtJwksMisconfiguration, freezeJwksConfig } = await import('./config')
    expect(describeJwtJwksMisconfiguration(freezeJwksConfig({ keys: [] }), 'ES256')).toBeUndefined()
  })

  test('leaves urlSigningKey unset when every capable-shaped key is marked use="enc"', async () => {
    setConfigEnv({
      JWT_JWKS: JSON.stringify({
        keys: [
          { kty: 'oct', k: 'symmetric-encryption', use: 'enc', kid: 'oct-enc' },
          {
            kty: 'EC',
            crv: 'P-256',
            x: 'x',
            y: 'y',
            d: 'd',
            use: 'enc',
            kid: 'ec-enc',
          },
        ],
      }),
    })

    const { getConfig } = await import('./config')
    const jwks = getConfig({ reload: true }).jwtJWKS!

    expect(jwks.urlSigningKey).toBeUndefined()
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
