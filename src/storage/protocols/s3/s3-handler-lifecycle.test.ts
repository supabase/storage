import { ErrorCode } from '@internal/errors'
import * as config from '../../../config'

describe('S3ProtocolHandler lifecycle configuration', () => {
  let Handler: typeof import('./s3-handler').S3ProtocolHandler
  let Storage: typeof import('../../storage').Storage
  let versioningEnabled = true
  let lifecycleEnabled = true
  let backendType = 's3'

  beforeAll(async () => {
    const configured = config.getConfig()
    vi.resetModules()
    vi.doMock('../../../config', () => ({
      ...config,
      getConfig: () => ({
        ...configured,
        storageVersioningEnabled: versioningEnabled,
        storageLifecycleEnabled: lifecycleEnabled,
        storageBackendType: backendType,
        isMultitenant: false,
      }),
    }))
    Handler = (await import('./s3-handler')).S3ProtocolHandler
    Storage = (await import('../../storage')).Storage
  })

  afterEach(() => {
    versioningEnabled = true
    lifecycleEnabled = true
    backendType = 's3'
  })

  afterAll(() => {
    vi.doUnmock('../../../config')
    vi.resetModules()
  })

  function createHandler(bucketOverrides: Record<string, unknown> = {}) {
    const bucket = {
      id: 'bucket',
      name: 'bucket',
      type: 'STANDARD',
      lifecycle_configuration: null,
      lifecycle_configuration_generation: null,
      ...bucketOverrides,
    }
    const transactionDatabase = {
      deleteLifecycleConfiguration: vi.fn().mockResolvedValue(bucket),
      findLifecycleBucket: vi.fn().mockResolvedValue(bucket),
      hasMigration: vi.fn().mockResolvedValue(true),
      putLifecycleConfiguration: vi.fn().mockResolvedValue(bucket),
    }

    const db = {
      ...transactionDatabase,
      tenantId: 'tenant-id',
      tenant: () => ({ ref: 'tenant-id', host: '' }),
      connection: {
        getAbortSignal: vi.fn().mockReturnValue(undefined),
        setAbortSignal: vi.fn(),
      },
      withTransaction: vi.fn((fn: (database: typeof transactionDatabase) => unknown) =>
        fn(transactionDatabase)
      ),
    }
    return {
      db,
      handler: new Handler(
        new Storage({} as never, db as never, {} as never),
        'tenant-id',
        'owner-id'
      ),
    }
  }

  it('returns NoSuchLifecycleConfiguration for an accessible bucket without a policy', async () => {
    const { handler } = createHandler()

    await expect(handler.getBucketLifecycle('bucket')).rejects.toMatchObject({
      code: ErrorCode.NoSuchLifecycleConfiguration,
      httpStatusCode: 404,
    })
  })

  it.each(['s3', 'file'])('normalizes lifecycle PUT on %s before persistence', async (backend) => {
    backendType = backend
    const { db, handler } = createHandler()

    await expect(
      handler.putBucketLifecycle('bucket', {
        LifecycleConfiguration: {
          Rule: [
            {
              ID: 'expire',
              Status: 'Enabled',
              Filter: '',
              NoncurrentVersionExpiration: {
                NoncurrentDays: '30',
                NewerNoncurrentVersions: '2',
              },
            },
          ],
        },
      })
    ).resolves.toEqual({ statusCode: 200 })
    expect(db.putLifecycleConfiguration).toHaveBeenCalledWith('bucket', {
      rules: [
        {
          id: 'expire',
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: {
            noncurrentDays: 30,
            newerNoncurrentVersions: 2,
          },
        },
      ],
    })
  })

  it.each([
    {
      rules: [{ Status: 'Enabled', NoncurrentVersionExpiration: { NoncurrentDays: '1' } }],
      code: ErrorCode.MalformedXML,
      message: 'Rule 1 must contain Filter',
    },
    {
      rules: [
        {
          ID: 'duplicate',
          Status: 'Enabled',
          Filter: {},
          NoncurrentVersionExpiration: { NoncurrentDays: '1' },
        },
        {
          ID: 'duplicate',
          Status: 'Disabled',
          Filter: {},
          NoncurrentVersionExpiration: { NoncurrentDays: '2' },
        },
      ],
      code: ErrorCode.InvalidArgument,
      message: 'Rule ID must be unique. Found same ID for more than one rule',
    },
    {
      rules: [
        { Status: 'Enabled', Prefix: '', NoncurrentVersionExpiration: { NoncurrentDays: '1' } },
      ],
      code: ErrorCode.InvalidRequest,
      message: 'Rule 1 contains unsupported element Prefix; use Filter instead',
    },
  ])('maps lifecycle validation to $code without persistence', async ({ rules, code, message }) => {
    const { db, handler } = createHandler()
    await expect(
      handler.putBucketLifecycle('bucket', {
        LifecycleConfiguration: { Rule: rules },
      })
    ).rejects.toMatchObject({ code, message })
    expect(db.putLifecycleConfiguration).not.toHaveBeenCalled()
  })

  it('checks migration readiness before validating or persisting a PUT', async () => {
    const { db, handler } = createHandler()
    db.hasMigration.mockResolvedValue(false)

    await expect(handler.putBucketLifecycle('bucket', null)).rejects.toMatchObject({
      code: ErrorCode.FeatureNotEnabled,
    })
    expect(db.hasMigration).toHaveBeenCalledWith('bucket-lifecycle-configuration')
    expect(db.putLifecycleConfiguration).not.toHaveBeenCalled()
  })

  it('deletes lifecycle configuration idempotently through the database contract', async () => {
    const { db, handler } = createHandler()

    await expect(handler.deleteBucketLifecycle('bucket')).resolves.toEqual({ statusCode: 204 })
    expect(db.deleteLifecycleConfiguration).toHaveBeenCalledWith('bucket')
    expect(db.hasMigration).not.toHaveBeenCalled()
  })

  it.each(['s3', 'file'])('rejects lifecycle operations on %s when disabled', async (backend) => {
    backendType = backend
    lifecycleEnabled = false
    const { db, handler } = createHandler()

    await expect(handler.getBucketLifecycle('bucket')).rejects.toMatchObject({
      code: ErrorCode.FeatureNotEnabled,
    })
    await expect(
      handler.putBucketLifecycle('bucket', {
        LifecycleConfiguration: {
          Rule: [
            { Status: 'Enabled', Filter: {}, NoncurrentVersionExpiration: { NoncurrentDays: '1' } },
          ],
        },
      })
    ).rejects.toMatchObject({ code: ErrorCode.FeatureNotEnabled })
    await expect(handler.putBucketLifecycle('bucket', null)).rejects.toMatchObject({
      httpStatusCode: 400,
      code: ErrorCode.MalformedXML,
    })
    await expect(handler.deleteBucketLifecycle('bucket')).rejects.toMatchObject({
      code: ErrorCode.FeatureNotEnabled,
    })
    expect(db.findLifecycleBucket).not.toHaveBeenCalled()
    expect(db.putLifecycleConfiguration).not.toHaveBeenCalled()
    expect(db.deleteLifecycleConfiguration).not.toHaveBeenCalled()
  })
})
