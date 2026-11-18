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

  it('schedules a move to SQS as lanes, each carrying its batch size and pace', async () => {
    const app = await buildApp({ pgQueueEnable: true, queueAdapter: 'sqs' })

    const { setQueueForTesting } = await import('@internal/queue')
    const produce = vi.fn().mockResolvedValue(undefined)
    setQueueForTesting({ produce, invoke: vi.fn() })

    try {
      const before = Date.now()
      const response = await app.inject({
        method: 'POST',
        url: '/queue/move',
        headers: { apikey: 'test-admin-key' },
        payload: { to: 'sqs', parallelism: 2, batchSize: 100, delaySeconds: 1.5, maxMinutes: 10 },
      })

      expect(response.statusCode).toBe(200)
      // One send for all lanes.
      expect(produce).toHaveBeenCalledTimes(1)
      const [lanes] = produce.mock.calls[0]
      expect(lanes.map((lane: { type: string }) => lane.type)).toEqual([
        'MoveJobsV10ToSqs',
        'MoveJobsV10ToSqs',
      ])
      expect(lanes.map((lane: { data: unknown }) => lane.data)).toEqual([
        expect.objectContaining({ lane: 0, round: 0, batchSize: 100, delayMs: 1_500 }),
        expect.objectContaining({ lane: 1, round: 1, batchSize: 100, delayMs: 1_500 }),
      ])
      // The lanes stop at the time limit, whatever is left.
      expect(lanes[0].data.expiresAt).toBeGreaterThanOrEqual(before + 10 * 60_000)
      expect(lanes[0].data.expiresAt).toBeLessThanOrEqual(Date.now() + 10 * 60_000)
      expect(lanes[0].data.tenant.ref).toBe('SYSTEM_TENANT')

      // Defaults: 3 lanes of 500 jobs every 5 seconds, for at most 30 minutes.
      produce.mockClear()
      const defaultsFrom = Date.now()
      await app.inject({
        method: 'POST',
        url: '/queue/move',
        headers: { apikey: 'test-admin-key' },
        payload: { to: 'sqs' },
      })
      const [defaults] = produce.mock.calls[0]
      expect(defaults).toHaveLength(3)
      expect(defaults[0].data).toMatchObject({ batchSize: 500, delayMs: 5_000 })
      expect(defaults[0].data.expiresAt).toBeGreaterThanOrEqual(defaultsFrom + 30 * 60_000)
    } finally {
      await app.close()
    }
  })

  it('rejects a move to SQS when SQS is not the running adapter, or with too many lanes', async () => {
    const pgbossApp = await buildApp({ pgQueueEnable: true, queueAdapter: 'pgboss' })
    try {
      const response = await pgbossApp.inject({
        method: 'POST',
        url: '/queue/move',
        headers: { apikey: 'test-admin-key' },
        payload: { to: 'sqs' },
      })
      expect(response.statusCode).toBe(400)
      expect(JSON.parse(response.body).message).toContain("running queue adapter is 'pgboss'")
    } finally {
      await pgbossApp.close()
    }

    const sqsApp = await buildApp({ pgQueueEnable: true, queueAdapter: 'sqs' })
    try {
      const response = await sqsApp.inject({
        method: 'POST',
        url: '/queue/move',
        headers: { apikey: 'test-admin-key' },
        payload: { to: 'sqs', parallelism: 9 },
      })
      expect(response.statusCode).toBe(400)
    } finally {
      await sqsApp.close()
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
