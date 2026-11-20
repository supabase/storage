import fastify, { FastifyInstance } from 'fastify'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { setErrorHandler } from '../../error-handler'
import { withFiniteAjv } from '../../finite'
import { errorSchema } from '../../schemas/error'
import renderAuthenticatedImage from './renderAuthenticatedImage'
import renderPublicImage from './renderPublicImage'
import renderSignedImage from './renderSignedImage'

describe('image render routes', () => {
  let app: FastifyInstance
  const obj = { id: 'object', version: 'v1', metadata: {} }
  const renderer = {
    setLimits: vi.fn(() => renderer),
    setTransformations: vi.fn(() => renderer),
    setTransformationsFromString: vi.fn(() => renderer),
    render: vi.fn((_req, reply, _options: { object: typeof obj }) => reply.send('ok')),
  }

  beforeEach(async () => {
    const storage = {
      asSuperUser: () => storage,
      findBucket: async () => ({ id: 'bucket', public: true }),
      from: () => ({
        findObject: async () => obj,
        verifyObjectSignature: async () => ({
          url: 'bucket/cat.png',
          transformations: '',
          format: undefined,
          exp: Math.floor(Date.now() / 1000) + 60,
          versionId: undefined,
        }),
      }),
      location: { getKeyLocation: () => 'tenant/bucket/cat.png' },
      renderer: () => renderer,
    }
    app = fastify(withFiniteAjv({}))
    app.addSchema(errorSchema)
    setErrorHandler(app)
    app.decorateRequest('storage')
    app.decorateRequest('tenantId', 'tenant')
    app.decorateRequest('signals')
    app.addHook('onRequest', async (request) => {
      request.storage = storage as never
      request.signals = { disconnect: new AbortController() } as never
    })
    await app.register(renderPublicImage, { prefix: '/render/image' })
    await app.register(renderSignedImage, { prefix: '/render/image' })
    await app.register(renderAuthenticatedImage, { prefix: '/render/image' })
  })

  afterEach(async () => {
    await app.close()
    vi.clearAllMocks()
  })

  it.each([
    ['public', '/render/image/public/bucket/cat.png'],
    ['authenticated', '/render/image/authenticated/bucket/cat.png'],
    ['signed', '/render/image/sign/bucket/cat.png?token=t'],
  ])('%s route passes the DB row to the renderer as object', async (_name, url) => {
    const response = await app.inject(url)

    expect(response.statusCode).toBe(200)
    expect(renderer.render).toHaveBeenCalledOnce()
    expect(renderer.render.mock.calls[0]?.[2].object).toBe(obj)
  })
})
