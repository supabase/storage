import type { Server } from '@tus/server'
import Fastify, { FastifyInstance } from 'fastify'
import { requestContext } from '../../plugins/request-context'
import { publicRoutes } from './index'
import type { MultiPartRequest } from './lifecycle'

describe('public tus route request context', () => {
  let app: FastifyInstance
  let observedUpload: MultiPartRequest['upload'] | undefined

  beforeEach(async () => {
    observedUpload = undefined

    app = Fastify()
    app.decorateRequest('tenantId')
    app.decorateRequest('owner')
    app.decorateRequest('db')
    app.decorateRequest('storage')

    await app.register(requestContext)

    app.addHook('onRequest', async (request) => {
      request.tenantId = 'tenant-123'
      request.owner = 'owner-123'
      request.db = { dispose: vi.fn() } as never
      request.storage = {
        backend: {},
        db: {},
        location: {},
      } as never
    })

    await app.register(publicRoutes, {
      tusServer: {
        handle: vi.fn(async (rawReq, rawRes) => {
          observedUpload = (rawReq as MultiPartRequest).upload
          rawRes.statusCode = 204
          rawRes.end()
        }),
      } as unknown as Server,
    })
  })

  afterEach(async () => {
    await app.close()
  })

  it('threads sbReqId onto the public route raw upload context', async () => {
    const response = await app.inject({
      method: 'OPTIONS',
      url: '/public/object',
      headers: {
        'sb-request-id': 'sb-req-123',
        'x-upsert': 'true',
      },
    })

    expect(response.statusCode).toBe(204)
    expect(observedUpload).toMatchObject({
      owner: 'owner-123',
      tenantId: 'tenant-123',
      isUpsert: true,
      sbReqId: 'sb-req-123',
    })
  })
})
