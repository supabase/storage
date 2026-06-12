import Fastify from 'fastify'
import { vi } from 'vitest'

async function loadDbSuperUserPlugin() {
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
    isMultitenant: false,
  })

  const { dbSuperUser } = await import('./db')

  return {
    dbSuperUser,
    getPostgresConnection,
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
    const { dbSuperUser, getPostgresConnection } = await loadDbSuperUserPlugin()
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
