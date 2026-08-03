import { vi } from 'vitest'

describe('admin queue routes', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

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

  it('schedules a move into the running engine', async () => {
    const app = await buildApp({ pgQueueEnable: true, pgQueueAdapter: 'pgque' })

    const { setWaveForTesting } = await import('@internal/queue')
    const produce = vi.fn().mockResolvedValue(undefined)
    setWaveForTesting({ produce, invoke: vi.fn() })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/queue/move',
        headers: {
          apikey: 'test-admin-key',
        },
        payload: { to: 'pgque' },
      })

      expect(response.statusCode).toBe(200)
      expect(produce).toHaveBeenCalledTimes(1)
      const [message] = produce.mock.calls[0]
      expect(message.type).toBe('MoveJobsToPgque')
      expect(message.data.tenant.ref).toBe('SYSTEM_TENANT')
    } finally {
      await app.close()
    }
  })

  it('rejects a move whose destination is not the running adapter', async () => {
    const app = await buildApp({ pgQueueEnable: true, pgQueueAdapter: 'pgque' })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/queue/move',
        headers: {
          apikey: 'test-admin-key',
        },
        payload: { to: 'pgboss' },
      })

      expect(response.statusCode).toBe(400)
      expect(JSON.parse(response.body).message).toContain("running queue adapter is 'pgque'")
    } finally {
      await app.close()
    }
  })

  it('schedules a schema-generation move regardless of the running adapter', async () => {
    const app = await buildApp({ pgQueueEnable: true, pgQueueAdapter: 'pgque' })

    const { setWaveForTesting } = await import('@internal/queue')
    const produce = vi.fn().mockResolvedValue(undefined)
    setWaveForTesting({ produce, invoke: vi.fn() })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/queue/move',
        headers: {
          apikey: 'test-admin-key',
        },
        payload: { to: 'pgboss-v12' },
      })

      expect(response.statusCode).toBe(200)
      expect(produce).toHaveBeenCalledTimes(1)
      expect(produce.mock.calls[0][0].type).toBe('MoveJobsV10ToV12')
    } finally {
      await app.close()
    }
  })

  it('rejects an unknown destination engine', async () => {
    const app = await buildApp({ pgQueueEnable: true, pgQueueAdapter: 'pgque' })

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
    const app = await buildApp({ pgQueueEnable: false, pgQueueAdapter: 'pgque' })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/queue/move',
        headers: {
          apikey: 'test-admin-key',
        },
        payload: { to: 'pgque' },
      })

      expect(response.statusCode).toBe(400)
      expect(JSON.parse(response.body)).toEqual({ message: 'Queue is not enabled' })
    } finally {
      await app.close()
    }
  })
})
