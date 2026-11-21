import { vi } from 'vitest'

test('tenant and prepared-key caches receive their individual environment limits', async () => {
  vi.resetModules()
  vi.stubEnv('MULTI_TENANT', 'true')
  const prefixes = [
    'TENANT_CONFIG',
    'TENANT_JWKS',
    'TENANT_S3_CREDENTIALS',
    'JWT_SIGNING_KEY',
    'JWT_VERIFICATION_KEY',
  ]
  for (const [index, prefix] of prefixes.entries()) {
    vi.stubEnv(`${prefix}_CACHE_MAX_ENTRIES`, String(index + 1))
    vi.stubEnv(`${prefix}_CACHE_TTL_MS`, String(1000 + index))
  }

  const optionsByName = new Map<string, Record<string, unknown>>()
  vi.doMock('@internal/cache', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@internal/cache')>()
    return {
      ...actual,
      createLruCache: (name: string, options: Record<string, unknown>) => {
        optionsByName.set(name, options)
        return actual.createLruCache(name as never, options as never)
      },
    }
  })

  try {
    const { getConfig, setEnvPaths } = await import('../../config')
    setEnvPaths([])
    getConfig({ reload: true })
    await import('../database/tenant')
    for (const [index, prefix] of prefixes.entries()) {
      expect(optionsByName.get(prefix.toLowerCase())).toMatchObject({
        max: index + 1,
        ttl: 1000 + index,
      })
    }
    expect(optionsByName.get('jwt_signing_key')).not.toHaveProperty('purgeStaleIntervalMs')
    expect(optionsByName.get('jwt_verification_key')).not.toHaveProperty('purgeStaleIntervalMs')
  } finally {
    vi.doUnmock('@internal/cache')
    vi.resetModules()
  }
})
