import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

function mockAdminAppDependencies() {
  vi.doMock('./config', () => ({
    getConfig: () => ({
      prometheusMetricsEnabled: false,
      version: 'test',
    }),
  }))
  vi.doMock('@internal/monitoring/otel-metrics', () => ({
    handleMetricsRequest: vi.fn(),
  }))
  vi.doMock('./http', () => ({
    plugins: {
      adminTenantId: async () => {},
      logRequest: () => async () => {},
      requestContext: async () => {},
      signals: async () => {},
    },
    routes: {
      jwks: async () => {},
      metricsConfig: async () => {},
      migrations: async () => {},
      objects: async () => {},
      pprof: async (fastify: { get: Function }) => {
        fastify.get('/profile', async (_request: unknown, reply: { status: Function }) => {
          return reply.status(401).send()
        })
      },
      queue: async () => {},
      s3Credentials: async () => {},
      tenants: async () => {},
    },
    setErrorHandler: vi.fn(),
  }))
}

async function buildAdminApp() {
  mockAdminAppDependencies()
  const { default: buildAdmin } = await import('./admin-app')
  const app = buildAdmin({})
  await app.ready()
  return app
}

async function clearWattGlobals() {
  const { removeGlobals } = await import('@platformatic/globals')
  removeGlobals(['applicationId', 'workerId', 'messaging'])
}

async function setWattGlobals() {
  const { updateGlobals } = await import('@platformatic/globals')
  updateGlobals({
    applicationId: 'storage',
    workerId: 0,
  })
}

describe('admin app pprof registration', () => {
  beforeEach(async () => {
    vi.useRealTimers()
    vi.resetModules()
    await clearWattGlobals()
  })

  afterEach(async () => {
    vi.useRealTimers()
    await clearWattGlobals()
    vi.doUnmock('./config')
    vi.doUnmock('@internal/monitoring/otel-metrics')
    vi.doUnmock('./http')
    vi.resetModules()
  })

  it('does not register pprof endpoints outside Watt', async () => {
    const app = await buildAdminApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/debug/pprof/profile',
      })

      expect(response.statusCode).toBe(404)
    } finally {
      await app.close()
    }
  })

  it('registers pprof endpoints under Watt', async () => {
    await setWattGlobals()

    const app = await buildAdminApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/debug/pprof/profile',
      })

      expect(response.statusCode).toBe(401)
    } finally {
      await app.close()
    }
  })
})
