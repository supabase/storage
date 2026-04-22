import { SYSTEM_TENANT } from '@internal/queue'
import { vi } from 'vitest'

const { mockUpgradeSend, mockMoveJobsSend } = vi.hoisted(() => ({
  mockUpgradeSend: vi.fn(),
  mockMoveJobsSend: vi.fn(),
}))

vi.mock('@storage/events', () => ({
  UpgradePgBossV10: {
    send: mockUpgradeSend,
  },
  MoveJobs: {
    send: mockMoveJobsSend,
  },
}))

describe('admin queue routes', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('passes sbReqId to the pg-boss upgrade job', async () => {
    vi.resetModules()

    const { mergeConfig } = await import('../../../config')
    mergeConfig({
      pgQueueEnable: true,
      adminApiKeys: 'test-admin-key',
    })

    const fastify = (await import('fastify')).default
    const { default: routes } = await import('./queue')

    const app = fastify()
    app.decorateRequest('sbReqId', undefined)
    app.addHook('onRequest', (request, _reply, done) => {
      request.sbReqId =
        typeof request.headers['sb-request-id'] === 'string'
          ? request.headers['sb-request-id']
          : undefined
      done()
    })
    app.register(routes, { prefix: '/queue' })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/queue/migrate/pgboss-v10',
        headers: {
          apikey: 'test-admin-key',
          'sb-request-id': 'sb-req-123',
        },
      })

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ message: 'Migration scheduled' })
      expect(mockUpgradeSend).toHaveBeenCalledWith({
        sbReqId: 'sb-req-123',
        tenant: SYSTEM_TENANT,
      })
    } finally {
      await app.close()
    }
  })

  it('passes sbReqId to the move jobs task', async () => {
    vi.resetModules()

    const { mergeConfig } = await import('../../../config')
    mergeConfig({
      pgQueueEnable: true,
      adminApiKeys: 'test-admin-key',
    })

    const fastify = (await import('fastify')).default
    const { default: routes } = await import('./queue')

    const app = fastify()
    app.decorateRequest('sbReqId', undefined)
    app.addHook('onRequest', (request, _reply, done) => {
      request.sbReqId =
        typeof request.headers['sb-request-id'] === 'string'
          ? request.headers['sb-request-id']
          : undefined
      done()
    })
    app.register(routes, { prefix: '/queue' })

    try {
      const response = await app.inject({
        method: 'POST',
        url: '/queue/move',
        headers: {
          apikey: 'test-admin-key',
          'sb-request-id': 'sb-req-123',
        },
        payload: {
          fromQueue: 'source-queue',
          toQueue: 'target-queue',
          deleteJobsFromOriginalQueue: true,
        },
      })

      expect(response.statusCode).toBe(200)
      expect(response.json()).toEqual({ message: 'Move jobs scheduled' })
      expect(mockMoveJobsSend).toHaveBeenCalledWith({
        fromQueue: 'source-queue',
        toQueue: 'target-queue',
        deleteJobsFromOriginalQueue: true,
        sbReqId: 'sb-req-123',
        tenant: {
          ref: '',
          host: '',
        },
      })
    } finally {
      await app.close()
    }
  })
})
