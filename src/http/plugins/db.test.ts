import Fastify, { type FastifyRequest } from 'fastify'
import { vi } from 'vitest'

async function loadDbPlugins({
  databaseEnableQueryCancellation = false,
}: {
  databaseEnableQueryCancellation?: boolean
} = {}) {
  vi.resetModules()

  const requestDb = {
    dispose: vi.fn().mockResolvedValue(undefined),
    setAbortSignal: vi.fn(),
  }
  const getPostgresConnection = vi.fn().mockResolvedValue(requestDb)
  const getServiceKeyUser = vi.fn().mockResolvedValue({
    jwt: 'service-jwt',
    payload: {
      role: 'service_role',
    },
  })

  vi.doMock('@internal/database', () => {
    return {
      getPostgresConnection,
      getServiceKeyUser,
      getTenantConfig: vi.fn(),
      PgTenantConnection: class {},
    }
  })

  vi.doMock('@internal/database/migrations', () => {
    return {
      areMigrationsUpToDate: vi.fn(),
      DBMigration: {},
      lastLocalMigrationName: vi.fn().mockResolvedValue('initialmigration'),
      progressiveMigrations: {
        addTenant: vi.fn(),
      },
      runMigrationsOnTenant: vi.fn(),
      updateTenantMigrationsState: vi.fn(),
    }
  })

  const configModule = await import('../../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig({
    databaseEnableQueryCancellation,
    isMultitenant: false,
  })

  const { db, dbSuperUser } = await import('./db')

  return {
    db,
    dbSuperUser,
    getPostgresConnection,
    requestDb,
  }
}

describe('dbSuperUser plugin', () => {
  afterEach(() => {
    vi.doUnmock('@internal/database')
    vi.doUnmock('@internal/database/migrations')
    vi.restoreAllMocks()
    vi.resetModules()
  })

  it('does not forward route-level maxConnections into shared tenant pool settings', async () => {
    const { dbSuperUser, getPostgresConnection } = await loadDbPlugins()
    const app = Fastify()

    app.decorateRequest('tenantId')
    app.addHook('onRequest', async (request) => {
      request.tenantId = 'tenant-id'
    })
    const legacyOptions = { disableHostCheck: true, maxConnections: 5 } as unknown as {
      disableHostCheck: boolean
    }
    await app.register(dbSuperUser, legacyOptions)
    app.get('/test', async () => ({ ok: true }))

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/test',
        headers: {
          'x-forwarded-host': 'tenant.local.test',
        },
      })

      expect(response.statusCode).toBe(200)
      expect(getPostgresConnection).toHaveBeenCalledTimes(1)
      expect(getPostgresConnection.mock.calls[0][0]).not.toHaveProperty('maxConnections')
    } finally {
      await app.close()
    }
  })
})

describe.each([
  {
    name: 'db plugin',
    register: async (
      app: ReturnType<typeof Fastify>,
      plugins: Awaited<ReturnType<typeof loadDbPlugins>>
    ) => {
      app.addHook('onRequest', async (request: FastifyRequest) => {
        request.jwt = 'user-jwt'
        request.jwtPayload = {
          role: 'authenticated',
        }
      })
      await app.register(plugins.db)
    },
  },
  {
    name: 'dbSuperUser plugin',
    register: async (
      app: ReturnType<typeof Fastify>,
      plugins: Awaited<ReturnType<typeof loadDbPlugins>>
    ) => {
      await app.register(plugins.dbSuperUser)
    },
  },
])('$name query cancellation signal wiring', ({ register }) => {
  afterEach(() => {
    vi.doUnmock('@internal/database')
    vi.doUnmock('@internal/database/migrations')
    vi.restoreAllMocks()
    vi.resetModules()
  })

  it('does not materialize the disconnect signal when query cancellation is disabled', async () => {
    const plugins = await loadDbPlugins({ databaseEnableQueryCancellation: false })
    const app = Fastify()

    app.decorateRequest('tenantId')
    app.decorateRequest('signals')
    app.addHook('onRequest', async (request) => {
      request.tenantId = 'tenant-id'
      request.signals = {
        get disconnect(): AbortController {
          throw new Error('disconnect signal should stay lazy')
        },
      } as typeof request.signals
    })
    await register(app, plugins)
    app.get('/test', async () => ({ ok: true }))

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/test',
        headers: {
          'x-forwarded-host': 'tenant.local.test',
        },
      })

      expect(response.statusCode).toBe(200)
      expect(plugins.requestDb.setAbortSignal).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })

  it('materializes the disconnect signal when query cancellation is enabled', async () => {
    const plugins = await loadDbPlugins({ databaseEnableQueryCancellation: true })
    const app = Fastify()
    const disconnectController = new AbortController()
    const disconnectGetter = vi.fn(() => disconnectController)

    app.decorateRequest('tenantId')
    app.decorateRequest('signals')
    app.addHook('onRequest', async (request) => {
      request.tenantId = 'tenant-id'
      request.signals = Object.defineProperty({}, 'disconnect', {
        get: disconnectGetter,
      }) as typeof request.signals
    })
    await register(app, plugins)
    app.get('/test', async () => ({ ok: true }))

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/test',
        headers: {
          'x-forwarded-host': 'tenant.local.test',
        },
      })

      expect(response.statusCode).toBe(200)
      expect(disconnectGetter).toHaveBeenCalledTimes(1)
      expect(plugins.requestDb.setAbortSignal).toHaveBeenCalledWith(disconnectController.signal)
    } finally {
      await app.close()
    }
  })

  it('does not require request signals when query cancellation is enabled', async () => {
    const plugins = await loadDbPlugins({ databaseEnableQueryCancellation: true })
    const app = Fastify()

    app.decorateRequest('tenantId')
    app.addHook('onRequest', async (request) => {
      request.tenantId = 'tenant-id'
    })
    await register(app, plugins)
    app.get('/test', async () => ({ ok: true }))

    try {
      const response = await app.inject({
        method: 'GET',
        url: '/test',
        headers: {
          'x-forwarded-host': 'tenant.local.test',
        },
      })

      expect(response.statusCode).toBe(200)
      expect(plugins.requestDb.setAbortSignal).not.toHaveBeenCalled()
    } finally {
      await app.close()
    }
  })
})
