import { ErrorCode } from '@internal/errors'
import * as config from '../../../config'
import { Storage } from '../../storage'

describe('S3ProtocolHandler lifecycle configuration', () => {
  let Handler: typeof import('./s3-handler').S3ProtocolHandler

  beforeAll(async () => {
    const configured = config.getConfig()
    vi.resetModules()
    vi.doMock('../../../config', () => ({
      ...config,
      getConfig: () => ({
        ...configured,
        storageLifecycleEnabled: true,
      }),
    }))
    Handler = (await import('./s3-handler')).S3ProtocolHandler
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
    const db = {
      deleteLifecycleConfiguration: vi.fn().mockResolvedValue({ bucket, changed: true }),
      findLifecycleBucket: vi.fn().mockResolvedValue(bucket),
      hasMigration: vi.fn().mockResolvedValue(true),
      putLifecycleConfiguration: vi.fn().mockResolvedValue({ bucket, changed: true }),
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

  it('normalizes a lifecycle PUT before persistence', async () => {
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

  it('preserves an empty legacy Prefix on PUT and GET', async () => {
    const configuration = {
      rules: [
        {
          id: 'legacy',
          status: 'Enabled' as const,
          legacyPrefix: '' as const,
          noncurrentVersionExpiration: { noncurrentDays: 30 },
        },
      ],
    }
    const writer = createHandler()

    await expect(
      writer.handler.putBucketLifecycle('bucket', {
        LifecycleConfiguration: {
          Rule: {
            ID: 'legacy',
            Status: 'Enabled',
            Prefix: '',
            NoncurrentVersionExpiration: { NoncurrentDays: '30' },
          },
        },
      })
    ).resolves.toEqual({ statusCode: 200 })
    expect(writer.db.putLifecycleConfiguration).toHaveBeenCalledWith('bucket', configuration)

    const reader = createHandler({ lifecycle_configuration: configuration })
    await expect(reader.handler.getBucketLifecycle('bucket')).resolves.toEqual({
      responseBody: {
        LifecycleConfiguration: {
          Rule: [
            {
              ID: 'legacy',
              Status: 'Enabled',
              Prefix: '',
              NoncurrentVersionExpiration: { NoncurrentDays: 30 },
            },
          ],
        },
      },
    })
  })

  it.each([
    [
      {
        Status: 'Enabled',
        Filter: {},
        NoncurrentVersionExpiration: { NoncurrentDays: '0' },
      },
      ErrorCode.InvalidArgument,
    ],
    [
      {
        Status: 'Enabled',
        Prefix: '',
        NoncurrentVersionExpiration: {
          NoncurrentDays: '1',
          NewerNoncurrentVersions: '2',
        },
      },
      ErrorCode.InvalidRequest,
    ],
    [
      {
        Status: 'Enabled',
        Filter: { Prefix: 'logs/' },
        NoncurrentVersionExpiration: { NoncurrentDays: '1' },
      },
      ErrorCode.InvalidRequest,
    ],
    [
      {
        Status: 'Enabled',
        Filter: { Tag: { Key: 'retention', Value: 'short' } },
        NoncurrentVersionExpiration: { NoncurrentDays: '1' },
      },
      ErrorCode.InvalidRequest,
    ],
    [
      {
        Status: 'Enabled',
        Filter: {},
        Expiration: { Days: '1' },
      },
      ErrorCode.InvalidRequest,
    ],
    [
      {
        Status: 'Enabled',
        Filter: { FuturePredicate: 'value' },
        NoncurrentVersionExpiration: { NoncurrentDays: '1' },
      },
      ErrorCode.MalformedXML,
    ],
    [
      {
        Status: 'Enabled',
        Filter: {},
      },
      ErrorCode.InvalidRequest,
    ],
  ])('maps lifecycle validation failures to the S3 error contract', async (rule, code) => {
    const { db, handler } = createHandler()

    await expect(
      handler.putBucketLifecycle('bucket', {
        LifecycleConfiguration: { Rule: [rule] },
      })
    ).rejects.toMatchObject({ code })
    expect(db.putLifecycleConfiguration).not.toHaveBeenCalled()
  })

  it('maps duplicate lifecycle rule IDs to InvalidArgument', async () => {
    const { db, handler } = createHandler()
    const rule = {
      ID: 'duplicate',
      Status: 'Enabled',
      Filter: {},
      NoncurrentVersionExpiration: { NoncurrentDays: '1' },
    }

    await expect(
      handler.putBucketLifecycle('bucket', {
        LifecycleConfiguration: { Rule: [rule, rule] },
      })
    ).rejects.toMatchObject({
      code: ErrorCode.InvalidArgument,
      message: 'Rule ID must be unique. Found same ID for more than one rule',
    })
    expect(db.putLifecycleConfiguration).not.toHaveBeenCalled()
  })

  it('rejects more than 1000 S3 lifecycle rules before persistence', async () => {
    const { db, handler } = createHandler()
    const rule = {
      Status: 'Enabled',
      Filter: {},
      NoncurrentVersionExpiration: { NoncurrentDays: '1' },
    }

    await expect(
      handler.putBucketLifecycle('bucket', {
        LifecycleConfiguration: { Rule: Array.from({ length: 1001 }, () => rule) },
      })
    ).rejects.toMatchObject({ code: ErrorCode.MalformedXML })
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

  it('rejects every lifecycle operation when the feature flag is disabled', async () => {
    const configured = config.getConfig()
    vi.resetModules()
    vi.doMock('../../../config', () => ({
      ...config,
      getConfig: () => ({
        ...configured,
        storageLifecycleEnabled: false,
      }),
    }))
    const DisabledHandler = (await import('./s3-handler')).S3ProtocolHandler
    const db = {
      deleteLifecycleConfiguration: vi.fn(),
      findLifecycleBucket: vi.fn(),
      hasMigration: vi.fn(),
      putLifecycleConfiguration: vi.fn(),
    }
    const handler = new DisabledHandler(
      new Storage({} as never, db as never, {} as never),
      'tenant-id',
      'owner-id'
    )

    await expect(handler.getBucketLifecycle('bucket')).rejects.toMatchObject({
      code: ErrorCode.FeatureNotEnabled,
    })
    await expect(handler.putBucketLifecycle('bucket', null)).rejects.toMatchObject({
      code: ErrorCode.FeatureNotEnabled,
    })
    await expect(handler.deleteBucketLifecycle('bucket')).rejects.toMatchObject({
      code: ErrorCode.FeatureNotEnabled,
    })
    expect(db.findLifecycleBucket).not.toHaveBeenCalled()
    expect(db.putLifecycleConfiguration).not.toHaveBeenCalled()
    expect(db.deleteLifecycleConfiguration).not.toHaveBeenCalled()
  })
})
