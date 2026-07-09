import { vi } from 'vitest'

const getGlobal = vi.hoisted(() => vi.fn())
const lastLocalMigrationName = vi.hoisted(() => vi.fn())
const adminApiKey = 'test-admin-api-key'
const originalServerAdminApiKeys = process.env.SERVER_ADMIN_API_KEYS

vi.mock('@platformatic/globals', () => ({
  getGlobal,
}))

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
  vi.resetModules()
  const { default: buildAdmin } = await import('./admin-app')
  const app = buildAdmin({})
  await app.ready()
  return app
}

describe('admin app', () => {
  beforeEach(() => {
    process.env.SERVER_ADMIN_API_KEYS = adminApiKey
    lastLocalMigrationName.mockResolvedValue('storage-schema')
  })

  afterAll(() => {
    if (originalServerAdminApiKeys === undefined) {
      delete process.env.SERVER_ADMIN_API_KEYS
    } else {
      process.env.SERVER_ADMIN_API_KEYS = originalServerAdminApiKeys
    }
  })

  it('does not register pprof endpoints outside Watt', async () => {
    getGlobal.mockReturnValue(undefined)

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
    getGlobal.mockReturnValue({
      applicationId: 'storage',
      workerId: 0,
    })

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
    getGlobal.mockReturnValue(undefined)
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
    getGlobal.mockReturnValue(undefined)

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
