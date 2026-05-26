import { vi } from 'vitest'

const getGlobal = vi.hoisted(() => vi.fn())

vi.mock('@platformatic/globals', () => ({
  getGlobal,
}))

async function buildAdminApp() {
  vi.resetModules()
  const { default: buildAdmin } = await import('./admin-app')
  const app = buildAdmin({})
  await app.ready()
  return app
}

describe('admin app pprof registration', () => {
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
})
