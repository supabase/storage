import { vi } from 'vitest'

describe('admin migrations queue gauges', () => {
  async function buildApp(pgQueueEnable: boolean) {
    vi.resetModules()

    const { getConfig, mergeConfig } = await import('../../../config')
    getConfig()
    mergeConfig({ adminApiKeys: 'test-admin-key', pgQueueEnable })

    const fastify = (await import('fastify')).default
    const { default: routes } = await import('./migrations')

    const app = fastify()
    app.register(routes, { prefix: '/migrations' })
    return app
  }

  const get = (app: Awaited<ReturnType<typeof buildApp>>, path: string) =>
    app.inject({ method: 'GET', url: `/migrations${path}`, headers: { apikey: 'test-admin-key' } })

  it('reports in-flight and waiting migration jobs from the queue stats', async () => {
    const app = await buildApp(true)
    const { setQueueForTesting } = await import('@internal/queue')
    const { QUEUES } = await import('@storage/events')
    const stats = vi.fn().mockResolvedValue({
      queues: [
        {
          queue: QUEUES.runMigrations,
          backlog: { count: 7, saturated: false },
          inFlight: { count: 2, saturated: false },
          retryParked: {
            total: { count: 3, saturated: false },
            due: { count: 1, saturated: false },
          },
        },
      ],
    })
    setQueueForTesting({ stats })

    try {
      const active = await get(app, '/active')
      const progress = await get(app, '/progress')

      expect(active.statusCode).toBe(200)
      expect(JSON.parse(active.body)).toEqual({ active: 2 })
      expect(progress.statusCode).toBe(200)
      expect(JSON.parse(progress.body)).toEqual({ remaining: 10 })
      expect(stats).toHaveBeenCalledWith({ queues: [QUEUES.runMigrations] })
    } finally {
      await app.close()
    }
  })

  it('reports zero for a queue the engine has no row for yet', async () => {
    const app = await buildApp(true)
    const { setQueueForTesting } = await import('@internal/queue')
    setQueueForTesting({ stats: vi.fn().mockResolvedValue({ queues: [] }) })

    try {
      expect(JSON.parse((await get(app, '/active')).body)).toEqual({ active: 0 })
      expect(JSON.parse((await get(app, '/progress')).body)).toEqual({ remaining: 0 })
    } finally {
      await app.close()
    }
  })

  it('rejects both when the queue is disabled', async () => {
    const app = await buildApp(false)

    try {
      for (const path of ['/active', '/progress']) {
        const response = await get(app, path)
        expect(response.statusCode).toBe(400)
        expect(JSON.parse(response.body)).toEqual({ message: 'Queue is not enabled' })
      }
    } finally {
      await app.close()
    }
  })
})
