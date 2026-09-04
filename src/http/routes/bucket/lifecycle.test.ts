import fastify, { type FastifyInstance } from 'fastify'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { setErrorHandler } from '../../error-handler'
import { withFiniteAjv } from '../../finite'
import { authSchema } from '../../schemas/auth'
import { errorSchema } from '../../schemas/error'
import createBucket from './createBucket'
import getBucket from './getBucket'
import lifecycle from './lifecycle'
import updateBucket from './updateBucket'

vi.mock('../../../config', async (importOriginal) => {
  const config = await importOriginal<typeof import('../../../config')>()
  const configured = config.getConfig()
  return {
    ...config,
    getConfig: () => ({
      ...configured,
      storageLifecycleEnabled: true,
    }),
  }
})

const lifecycleConfiguration = {
  rules: [
    {
      id: 'expire-history',
      status: 'Enabled' as const,
      filter: {},
      noncurrentVersionExpiration: {
        noncurrentDays: 30,
        newerNoncurrentVersions: 2,
      },
    },
  ],
}

const bucket = {
  id: 'avatars',
  name: 'avatars',
  owner: 'owner-id',
  public: false,
  type: 'STANDARD' as const,
  created_at: '2026-08-18T00:00:00.000Z',
  updated_at: '2026-08-18T00:00:00.000Z',
  file_size_limit: null,
  allowed_mime_types: null,
}

describe('REST bucket lifecycle configuration routes', () => {
  let app: FastifyInstance
  let storage: {
    createBucket: ReturnType<typeof vi.fn>
    deleteBucketLifecycle: ReturnType<typeof vi.fn>
    db: {
      hasMigration: ReturnType<typeof vi.fn>
    }
    findBucket: ReturnType<typeof vi.fn>
    getBucketLifecycle: ReturnType<typeof vi.fn>
    putBucketLifecycle: ReturnType<typeof vi.fn>
    updateBucket: ReturnType<typeof vi.fn>
  }

  beforeEach(async () => {
    storage = {
      createBucket: vi.fn().mockResolvedValue(undefined),
      deleteBucketLifecycle: vi.fn().mockResolvedValue(undefined),
      db: {
        hasMigration: vi.fn().mockResolvedValue(true),
      },
      findBucket: vi.fn().mockResolvedValue(bucket),
      getBucketLifecycle: vi.fn().mockResolvedValue(lifecycleConfiguration),
      putBucketLifecycle: vi.fn().mockResolvedValue(lifecycleConfiguration),
      updateBucket: vi.fn().mockResolvedValue(undefined),
    }
    app = fastify(withFiniteAjv({}))
    app.addSchema(authSchema)
    app.addSchema(errorSchema)
    app.decorateRequest('owner')
    app.decorateRequest('storage')
    app.addHook('preHandler', async (request) => {
      request.owner = 'owner-id'
      request.storage = storage as never
    })
    app.register(createBucket)
    app.register(getBucket)
    app.register(lifecycle)
    app.register(updateBucket)
    setErrorHandler(app)
    await app.ready()
  })

  afterEach(async () => {
    await app.close()
  })

  it('rejects lifecycle configuration on generic bucket create and update', async () => {
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
      payload: {
        public: true,
        lifecycle_configuration: lifecycleConfiguration,
      },
    })

    expect(createResponse.statusCode).toBe(400)
    expect(updateResponse.statusCode).toBe(400)
    expect(storage.createBucket).not.toHaveBeenCalled()
    expect(storage.updateBucket).not.toHaveBeenCalled()
  })

  it('fully replaces lifecycle configuration through the dedicated PUT route', async () => {
    const response = await app.inject({
      method: 'PUT',
      url: '/avatars/lifecycle',
      headers: { authorization: 'Bearer test' },
      payload: lifecycleConfiguration,
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual(lifecycleConfiguration)
    const put = storage.putBucketLifecycle
    expect(put).toHaveBeenCalledWith('avatars', lifecycleConfiguration)
  })

  it('checks migration readiness before semantic PUT validation', async () => {
    storage.db.hasMigration.mockResolvedValueOnce(false)

    const response = await app.inject({
      method: 'PUT',
      url: '/avatars/lifecycle',
      headers: { authorization: 'Bearer test' },
      payload: {
        rules: [
          {
            ...lifecycleConfiguration.rules[0],
            filter: { prefix: 'unsupported' },
          },
        ],
      },
    })

    expect(response.statusCode).toBe(400)
    expect(response.json()).toMatchObject({ code: 'FeatureNotEnabled' })
    expect(storage.db.hasMigration).toHaveBeenCalledWith('bucket-lifecycle-configuration')
    expect(storage.putBucketLifecycle).not.toHaveBeenCalled()
  })

  it('returns a stored rule that omits noncurrentVersionExpiration', async () => {
    storage.getBucketLifecycle.mockResolvedValueOnce({
      rules: [
        {
          id: 'expire-history',
          status: 'Enabled',
          filter: {},
        },
      ],
    })

    const response = await app.inject({
      method: 'GET',
      url: '/avatars/lifecycle',
      headers: { authorization: 'Bearer test' },
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual({
      rules: [
        {
          id: 'expire-history',
          status: 'Enabled',
          filter: {},
        },
      ],
    })
  })

  it('returns lifecycle configuration without exposing its generation', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/avatars/lifecycle',
      headers: { authorization: 'Bearer test' },
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).toEqual(lifecycleConfiguration)
    expect(response.json()).not.toHaveProperty('lifecycle_configuration_generation')
    expect(storage.getBucketLifecycle).toHaveBeenCalledWith('avatars')
  })

  it('returns NoSuchLifecycleConfiguration when the dedicated resource is absent', async () => {
    storage.getBucketLifecycle.mockResolvedValueOnce(null)

    const response = await app.inject({
      method: 'GET',
      url: '/avatars/lifecycle',
      headers: { authorization: 'Bearer test' },
    })

    expect(response.statusCode).toBe(400)
    expect(response.json()).toMatchObject({ code: 'NoSuchLifecycleConfiguration' })
  })

  it('deletes lifecycle configuration idempotently', async () => {
    const first = await app.inject({
      method: 'DELETE',
      url: '/avatars/lifecycle',
      headers: { authorization: 'Bearer test' },
    })
    const retry = await app.inject({
      method: 'DELETE',
      url: '/avatars/lifecycle',
      headers: { authorization: 'Bearer test' },
    })

    expect(first.statusCode).toBe(200)
    expect(retry.statusCode).toBe(200)
    expect(first.json()).toEqual({ message: 'Successfully deleted' })
    expect(retry.json()).toEqual({ message: 'Successfully deleted' })
    expect(storage.deleteBucketLifecycle).toHaveBeenCalledTimes(2)
    expect(storage.deleteBucketLifecycle).toHaveBeenNthCalledWith(1, 'avatars')
    expect(storage.deleteBucketLifecycle).toHaveBeenNthCalledWith(2, 'avatars')
  })

  it.each([
    '/avatars',
    '/avatars?include=lifecycle',
  ])('does not fetch lifecycle JSON on a generic bucket read: %s', async (url) => {
    const response = await app.inject({
      method: 'GET',
      url,
      headers: { authorization: 'Bearer test' },
    })

    expect(response.statusCode).toBe(200)
    expect(response.json()).not.toHaveProperty('lifecycle_configuration')
    expect(storage.db.hasMigration).not.toHaveBeenCalled()
    expect(storage.findBucket).toHaveBeenCalledWith(
      'avatars',
      expect.not.stringContaining('lifecycle_configuration')
    )
  })

  it.each([
    [
      'MALFORMED_XML',
      {
        rules: [
          {
            ...lifecycleConfiguration.rules[0],
            expiration: { days: 1 },
          },
        ],
      },
      'Rule 1 contains unsupported field expiration',
    ],
    [
      'INVALID_ARGUMENT',
      {
        rules: [
          { ...lifecycleConfiguration.rules[0], id: 'duplicate' },
          { ...lifecycleConfiguration.rules[0], id: 'duplicate' },
        ],
      },
      'Rule ID must be unique. Found same ID for more than one rule',
    ],
    [
      'INVALID_ARGUMENT',
      {
        rules: [
          {
            ...lifecycleConfiguration.rules[0],
            id: '😀'.repeat(128),
          },
        ],
      },
      'Rule 1 ID must be 255 characters or fewer',
    ],
    [
      'INVALID_ARGUMENT',
      {
        rules: [
          {
            ...lifecycleConfiguration.rules[0],
            noncurrentVersionExpiration: { noncurrentDays: 0 },
          },
        ],
      },
      "'NoncurrentDays' for NoncurrentVersionExpiration action must be a positive integer",
    ],
    [
      'INVALID_ARGUMENT',
      {
        rules: [
          {
            ...lifecycleConfiguration.rules[0],
            noncurrentVersionExpiration: {
              noncurrentDays: 1,
              newerNoncurrentVersions: 101,
            },
          },
        ],
      },
      "'NewerNoncurrentVersions' for NoncurrentVersionExpiration action must be an integer between 1 and 100",
    ],
    [
      'INVALID_REQUEST',
      {
        rules: [
          {
            id: 'legacy-with-count',
            status: 'Enabled' as const,
            legacyPrefix: '',
            noncurrentVersionExpiration: {
              noncurrentDays: 30,
              newerNoncurrentVersions: 2,
            },
          },
        ],
      },
      'NewerNoncurrentVersions element can only be used in Lifecycle V2.',
    ],
  ])('maps the %s semantic category to REST InvalidParameter', async (_, configuration, message) => {
    const response = await app.inject({
      method: 'PUT',
      url: '/avatars/lifecycle',
      headers: { authorization: 'Bearer test' },
      payload: configuration,
    })

    expect(response.statusCode).toBe(400)
    expect(response.json()).toMatchObject({ code: 'InvalidParameter', message })
    expect(storage.putBucketLifecycle).not.toHaveBeenCalled()
  })

  it('maps a 256-unit ASCII rule ID to REST InvalidParameter', async () => {
    const response = await app.inject({
      method: 'PUT',
      url: '/avatars/lifecycle',
      headers: { authorization: 'Bearer test' },
      payload: {
        rules: [
          {
            ...lifecycleConfiguration.rules[0],
            id: 'a'.repeat(256),
          },
        ],
      },
    })

    expect(response.statusCode).toBe(400)
    expect(response.json()).toMatchObject({
      code: 'InvalidParameter',
      message: 'Rule 1 ID must be 255 characters or fewer',
    })
    expect(storage.putBucketLifecycle).not.toHaveBeenCalled()
  })

  it.each([
    ['null character', 'before\u0000after'],
    ['low control character', 'before\u0001after'],
    ['unpaired high surrogate', 'before\ud800after'],
    ['unpaired low surrogate', 'before\udc00after'],
    ['U+FFFE', 'before\ufffeafter'],
    ['U+FFFF', 'before\uffffafter'],
  ])('rejects a rule ID containing %s before persistence', async (_label, id) => {
    const response = await app.inject({
      method: 'PUT',
      url: '/avatars/lifecycle',
      headers: { authorization: 'Bearer test' },
      payload: {
        rules: [{ ...lifecycleConfiguration.rules[0], id }],
      },
    })

    expect(response.statusCode).toBe(400)
    expect(response.json()).toMatchObject({ code: 'InvalidParameter' })
    expect(storage.putBucketLifecycle).not.toHaveBeenCalled()
  })

  it('rejects a mixed canonical and S3 lifecycle body', async () => {
    const response = await app.inject({
      method: 'PUT',
      url: '/avatars/lifecycle',
      headers: { authorization: 'Bearer test' },
      payload: {
        ...lifecycleConfiguration,
        LifecycleConfiguration: {
          Rule: {
            ID: 'other',
            Status: 'Disabled',
            Filter: {},
            NoncurrentVersionExpiration: { NoncurrentDays: 1 },
          },
        },
      },
    })

    expect(response.statusCode).toBe(400)
    expect(response.json()).toMatchObject({
      code: 'InvalidParameter',
      message: 'Lifecycle configuration must not mix rules with LifecycleConfiguration',
    })
    expect(storage.putBucketLifecycle).not.toHaveBeenCalled()
  })

  it('rejects an empty lifecycle rule set before invoking the database', async () => {
    const response = await app.inject({
      method: 'PUT',
      url: '/avatars/lifecycle',
      headers: { authorization: 'Bearer test' },
      payload: { rules: [] },
    })

    expect(response.statusCode).toBe(400)
    expect(storage.putBucketLifecycle).not.toHaveBeenCalled()
  })
})
