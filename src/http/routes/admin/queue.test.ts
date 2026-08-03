import { vi } from 'vitest'

describe('admin queue routes', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('responds 501: MoveJobs is a v1-only maintenance tool', async () => {
    vi.resetModules()

    const { mergeConfig } = await import('../../../config')
    mergeConfig({
      pgQueueEnable: true,
      adminApiKeys: 'test-admin-key',
    })

    const fastify = (await import('fastify')).default
    const { default: routes } = await import('./queue')

    const app = fastify()
    app.register(routes, { prefix: '/queue' })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/queue/move',
        headers: {
          apikey: 'test-admin-key',
        },
        payload: {
          fromQueue: 'source-queue',
          toQueue: 'target-queue',
          deleteJobsFromOriginalQueue: true,
        },
      })

      expect(response.statusCode).toBe(501)
    } finally {
      await app.close()
    }
  })

  it('rejects move jobs requests without queue names', async () => {
    vi.resetModules()

    const { mergeConfig } = await import('../../../config')
    mergeConfig({
      pgQueueEnable: true,
      adminApiKeys: 'test-admin-key',
    })

    const fastify = (await import('fastify')).default
    const { default: routes } = await import('./queue')

    const app = fastify()
    app.register(routes, { prefix: '/queue' })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/queue/move',
        headers: {
          apikey: 'test-admin-key',
        },
        payload: {
          fromQueue: 'source-queue',
        },
      })

      expect(response.statusCode).toBe(400)
    } finally {
      await app.close()
    }
  })
})
