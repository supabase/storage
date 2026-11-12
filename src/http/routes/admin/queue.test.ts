import { vi } from 'vitest'

describe('admin queue routes', () => {
  async function buildApp(config: Record<string, unknown>) {
    vi.resetModules()

    const { getConfig, mergeConfig } = await import('../../../config')
    // Hydrate the fresh module's config from env first — mergeConfig alone would leave every
    // unlisted key undefined, and the route's module graph (events → monitoring) reads them.
    getConfig()
    mergeConfig({
      adminApiKeys: 'test-admin-key',
      ...config,
    })

    const fastify = (await import('fastify')).default
    const { default: routes } = await import('./queue')

    const app = fastify()
    app.register(routes, { prefix: '/queue' })
    return app
  }

  it('schedules a move between the pg-boss schema generations', async () => {
    const app = await buildApp({ pgQueueEnable: true, queueAdapter: 'pgboss' })

    const { setQueueForTesting } = await import('@internal/queue')
    const produce = vi.fn().mockResolvedValue(undefined)
    setQueueForTesting({ produce, invoke: vi.fn() })

    try {
      for (const [to, type] of [
        ['pgboss-v12', 'MoveJobsV10ToV12'],
        ['pgboss-v10', 'MoveJobsV12ToV10'],
      ]) {
        produce.mockClear()
        const response = await app.inject({
          method: 'POST',
          url: '/queue/move',
          headers: {
            apikey: 'test-admin-key',
          },
          payload: { to },
        })

        expect(response.statusCode).toBe(200)
        expect(produce).toHaveBeenCalledTimes(1)
        const [message] = produce.mock.calls[0]
        expect(message.type).toBe(type)
        expect(message.data.tenant.ref).toBe('SYSTEM_TENANT')
      }
    } finally {
      await app.close()
    }
  })

  it('rejects a move when pg-boss is not the running adapter', async () => {
    const app = await buildApp({ pgQueueEnable: true, queueAdapter: 'sqs' })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/queue/move',
        headers: {
          apikey: 'test-admin-key',
        },
        payload: { to: 'pgboss-v12' },
      })

      expect(response.statusCode).toBe(400)
      expect(JSON.parse(response.body).message).toContain("running queue adapter is 'sqs'")
    } finally {
      await app.close()
    }
  })

  it('rejects an unknown destination engine', async () => {
    const app = await buildApp({ pgQueueEnable: true, queueAdapter: 'pgboss' })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/queue/move',
        headers: {
          apikey: 'test-admin-key',
        },
        payload: { to: 'rabbitmq' },
      })

      expect(response.statusCode).toBe(400)
    } finally {
      await app.close()
    }
  })

  it('rejects a move when the queue is disabled', async () => {
    const app = await buildApp({ pgQueueEnable: false, queueAdapter: 'pgboss' })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/queue/move',
        headers: {
          apikey: 'test-admin-key',
        },
        payload: { to: 'pgboss-v12' },
      })

      expect(response.statusCode).toBe(400)
      expect(JSON.parse(response.body)).toEqual({ message: 'Queue is not enabled' })
    } finally {
      await app.close()
    }
  })
})
