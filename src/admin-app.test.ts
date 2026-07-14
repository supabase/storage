import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const lastLocalMigrationName = vi.hoisted(() => vi.fn())
const adminApiKey = 'test-admin-api-key'
const originalServerAdminApiKeys = process.env.SERVER_ADMIN_API_KEYS

function mockAdminAppDependencies() {
  vi.doMock('./config', () => ({
    getConfig: () => ({
      adminApiKeys: adminApiKey,
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
      registerApiKeyAuth: (fastify: { addHook: (name: string, hook: Function) => void }) => {
        fastify.addHook(
          'onRequest',
          async (request: { headers: Record<string, unknown> }, reply: { status: Function }) => {
            if (request.headers.apikey !== adminApiKey) {
              return reply.status(401).send()
            }
          }
        )
      },
      requestContext: async () => {},
      signals: async () => {},
    },
    routes: {
      jwks: async () => {},
      icebergAdmin: async () => {},
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

vi.mock('@internal/database/migrations', async () => {
  const actual = await vi.importActual<typeof import('@internal/database/migrations')>(
    '@internal/database/migrations'
  )
  return {
    ...actual,
    lastLocalMigrationName,
  }
})

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

describe('admin app', () => {
  beforeEach(async () => {
    vi.useRealTimers()
    vi.resetModules()
    await clearWattGlobals()
    process.env.SERVER_ADMIN_API_KEYS = adminApiKey
    lastLocalMigrationName.mockResolvedValue('storage-schema')
  })

  afterEach(async () => {
    vi.useRealTimers()
    await clearWattGlobals()
    vi.doUnmock('./config')
    vi.doUnmock('@internal/monitoring/otel-metrics')
    vi.doUnmock('./http')
    vi.resetModules()
  })

  afterAll(() => {
    if (originalServerAdminApiKeys === undefined) {
      delete process.env.SERVER_ADMIN_API_KEYS
    } else {
      process.env.SERVER_ADMIN_API_KEYS = originalServerAdminApiKeys
    }
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

  it('returns the stack migration version', async () => {
    lastLocalMigrationName.mockResolvedValue('create-migrations-table')

    const app = await buildAdminApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/migration-version',
        headers: {
          apikey: adminApiKey,
        },
      })

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({
        migrationVersion: 'create-migrations-table',
      })
    } finally {
      await app.close()
    }
  })

  it('requires the admin API key for the stack migration version', async () => {
    const app = await buildAdminApp()

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/migration-version',
      })

      expect(response.statusCode).toBe(401)
    } finally {
      await app.close()
    }
  })
})
