import { ErrorCode } from '@internal/errors'
import fastify from 'fastify'
import { setErrorHandler } from '../../error-handler'
import { withFiniteAjv } from '../../finite'
import { authSchema } from '../../schemas/auth'
import { errorSchema } from '../../schemas/error'

describe('REST bucket lifecycle feature flag', () => {
  it('keeps generic writes closed and rejects dedicated lifecycle routes when disabled', async () => {
    const config = await import('../../../config')
    const configured = config.getConfig()
    vi.resetModules()
    vi.doMock('../../../config', () => ({
      ...config,
      getConfig: () => ({
        ...configured,
        storageVersioningEnabled: false,
        storageLifecycleEnabled: false,
        isMultitenant: false,
      }),
    }))

    const [
      { default: createBucket },
      { default: lifecycle },
      { default: updateBucket },
      { Storage },
    ] = await Promise.all([
      import('./createBucket'),
      import('./lifecycle'),
      import('./updateBucket'),
      import('../../../storage/storage'),
    ])
    const database = {
      tenant: () => ({ ref: 'tenant' }),
      hasMigration: vi.fn().mockResolvedValue(true),
      findLifecycleBucket: vi.fn(),
      withTransaction: vi.fn(),
    }
    const storage = new Storage({} as never, database as never, {} as never)
    const create = vi.spyOn(storage, 'createBucket')
    const update = vi.spyOn(storage, 'updateBucket')
    const app = fastify(withFiniteAjv({}))
    app.decorateRequest('owner')
    app.decorateRequest('storage')
    app.addHook('preHandler', async (request) => {
      request.owner = 'owner-id'
      request.storage = storage as never
    })
    app.addSchema(authSchema)
    app.addSchema(errorSchema)
    app.register(createBucket)
    app.register(lifecycle)
    app.register(updateBucket)
    setErrorHandler(app)

    const lifecycleConfiguration = {
      rules: [
        {
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 30 },
        },
      ],
    }

    try {
      const createResponse = await app.inject({
        method: 'POST',
        url: '/',
        headers: { authorization: 'Bearer test' },
        payload: {
          name: 'avatars',
          lifecycle_configuration: lifecycleConfiguration,
        },
      })
      const updateResponse = await app.inject({
        method: 'PUT',
        url: '/avatars',
        headers: { authorization: 'Bearer test' },
        payload: { public: true, lifecycle_configuration: lifecycleConfiguration },
      })

      expect(createResponse.statusCode).toBe(400)
      expect(updateResponse.statusCode).toBe(400)

      for (const method of ['GET', 'PUT', 'DELETE'] as const) {
        const response = await app.inject({
          method,
          url: '/avatars/lifecycle',
          headers: { authorization: 'Bearer test' },
          ...(method === 'PUT' ? { payload: lifecycleConfiguration } : {}),
        })
        expect(response.statusCode).toBe(400)
        expect(response.json()).toMatchObject({ code: ErrorCode.FeatureNotEnabled })
      }

      const invalidPut = await app.inject({
        method: 'PUT',
        url: '/avatars/lifecycle',
        headers: { authorization: 'Bearer test' },
        payload: {
          rules: [{ ...lifecycleConfiguration.rules[0], filter: { prefix: 'unsupported' } }],
        },
      })
      expect(invalidPut.statusCode).toBe(400)
      expect(invalidPut.json()).toMatchObject({ code: ErrorCode.InvalidParameter })
      expect(database.hasMigration).toHaveBeenCalledWith('bucket-lifecycle-configuration')
      expect(create).not.toHaveBeenCalled()
      expect(update).not.toHaveBeenCalled()
      expect(database.findLifecycleBucket).not.toHaveBeenCalled()
      expect(database.withTransaction).not.toHaveBeenCalled()
    } finally {
      await app.close()
      vi.doUnmock('../../../config')
      vi.resetModules()
    }
  })
})
