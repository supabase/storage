import { vi } from 'vitest'

const getGlobal = vi.hoisted(() => vi.fn())
const lastLocalMigrationName = vi.hoisted(() => vi.fn())

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
    lastLocalMigrationName.mockResolvedValue('storage-schema')
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
      })

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({
        migrationVersion: 'create-migrations-table',
      })
    } finally {
      await app.close()
    }
  })
})
