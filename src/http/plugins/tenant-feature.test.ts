import { ErrorCode } from '@internal/errors'
import { vi } from 'vitest'

describe('requireTenantFeature', () => {
  afterEach(() => {
    vi.doUnmock('../../config')
    vi.doUnmock('@internal/database')
    vi.resetModules()
  })

  it('returns the default JSON error when no formatter is provided', async () => {
    const { app, tenantHasFeatureMock } = await buildApp({
      hasFeature: false,
      isMultitenant: true,
    })

    try {
      const response = await app.inject({ method: 'GET', url: '/probe' })

      expect(response.statusCode).toBe(403)
      expect(response.json()).toEqual({
        error: 'FeatureNotEnabled',
        statusCode: '403',
        message: 'feature not enabled for this tenant',
        code: ErrorCode.FeatureNotEnabled,
      })
      expect(tenantHasFeatureMock).toHaveBeenCalledWith('tenant-a', 's3Protocol')
    } finally {
      await app.close()
    }
  })

  it('continues to the route when the tenant feature is enabled', async () => {
    const { app, tenantHasFeatureMock } = await buildApp({
      hasFeature: true,
      isMultitenant: true,
    })

    try {
      const response = await app.inject({ method: 'GET', url: '/probe' })

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ ok: true })
      expect(tenantHasFeatureMock).toHaveBeenCalledWith('tenant-a', 's3Protocol')
    } finally {
      await app.close()
    }
  })

  it('skips the tenant feature lookup in single-tenant mode', async () => {
    const { app, tenantHasFeatureMock } = await buildApp({
      hasFeature: false,
      isMultitenant: false,
    })

    try {
      const response = await app.inject({ method: 'GET', url: '/probe' })

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ ok: true })
      expect(tenantHasFeatureMock).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })
})

async function buildApp({
  hasFeature,
  isMultitenant,
}: {
  hasFeature: boolean
  isMultitenant: boolean
}) {
  const tenantHasFeatureMock = vi.fn().mockResolvedValue(hasFeature)

  vi.doMock('../../config', async (importOriginal) => {
    const actual = await importOriginal<typeof import('../../config')>()

    return {
      ...actual,
      getConfig: (options?: Parameters<typeof actual.getConfig>[0]) => ({
        ...actual.getConfig(options),
        isMultitenant,
      }),
    }
  })
  vi.doMock('@internal/database', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@internal/database')>()

    return {
      ...actual,
      tenantHasFeature: tenantHasFeatureMock,
    }
  })

  const { default: fastify } = await import('fastify')
  const { requireTenantFeature } = await import('./tenant-feature')
  const app = fastify()

  app.addHook('onRequest', (request, _reply, done) => {
    request.tenantId = 'tenant-a'
    done()
  })
  await app.register(requireTenantFeature('s3Protocol'))
  app.get('/probe', async () => ({ ok: true }))
  await app.ready()

  return { app, tenantHasFeatureMock }
}
