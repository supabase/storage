import { MAX_OBJECTS_PER_REQUEST } from '@storage/limits'
import { vi } from 'vitest'
import { S3ProtocolHandler } from '../../../storage/protocols/s3/s3-handler'
import { Uploader } from '../../../storage/uploader'
import CompleteMultipartUpload from './commands/complete-multipart-upload'
import { Router, type S3Router } from './router'

afterEach(() => {
  vi.restoreAllMocks()
})

type S3HandlerStorage = ConstructorParameters<typeof S3ProtocolHandler>[0]

function createHandler() {
  return new S3ProtocolHandler({} as unknown as S3HandlerStorage, 'tenant-id')
}

describe('S3 router query matching', () => {
  it('parses key-only query params with an undefined value', () => {
    const router = new Router()

    expect(router.parseQueryMatch('uploads')).toEqual({
      key: 'uploads',
      value: undefined,
    })
  })

  it('matches key-only query params when the property exists', () => {
    const router = new Router()

    router.post(
      '/:Bucket/*?uploads',
      {
        schema: {},
        operation: 'test.operation',
      },
      async () => ({})
    )

    const route = router.routes().get('/:Bucket/*')?.[0]
    expect(route).toBeDefined()

    expect(router.matchRoute(route!, undefined, { uploads: undefined }, {})).toBe(true)
  })

  it('matches valued query params when the value matches', () => {
    const router = new Router()

    router.get(
      '/:Bucket/*?list-type=2',
      {
        schema: {},
        operation: 'test.operation',
      },
      async () => ({})
    )

    const route = router.routes().get('/:Bucket/*')?.[0]
    expect(route).toBeDefined()

    expect(router.matchRoute(route!, undefined, { 'list-type': '2' }, {})).toBe(true)
  })

  it('does not match valued query params when the value differs', () => {
    const router = new Router()

    router.get(
      '/:Bucket/*?list-type=2',
      {
        schema: {},
        operation: 'test.operation',
      },
      async () => ({})
    )

    const route = router.routes().get('/:Bucket/*')?.[0]
    expect(route).toBeDefined()

    expect(router.matchRoute(route!, undefined, { 'list-type': '1' }, {})).toBe(false)
  })

  it('matches wildcard routes even when the request has query params', () => {
    const router = new Router()

    router.get(
      '/:Bucket/*',
      {
        schema: {},
        operation: 'test.operation',
      },
      async () => ({})
    )

    const route = router.routes().get('/:Bucket/*')?.[0]
    expect(route).toBeDefined()

    expect(router.matchRoute(route!, undefined, { uploads: undefined }, {})).toBe(true)
  })

  it('does not enumerate request query keys for wildcard-only routes', () => {
    const router = new Router()

    router.get(
      '/:Bucket/*',
      {
        schema: {},
        operation: 'test.operation',
      },
      async () => ({})
    )

    const route = router.routes().get('/:Bucket/*')?.[0]
    expect(route).toBeDefined()

    const query = new Proxy(
      {},
      {
        ownKeys: () => {
          throw new Error('wildcard-only query match should not enumerate request query keys')
        },
      }
    )

    expect(router.matchRoute(route!, undefined, query, {})).toBe(true)
  })
})

describe('S3 router header matching', () => {
  it('matches routes that require a header by presence', () => {
    const router = new Router()

    router.put(
      '/:Bucket/*|x-amz-copy-source',
      {
        schema: {},
        operation: 'test.operation',
      },
      async () => ({})
    )

    const route = router.routes().get('/:Bucket/*')?.[0]
    expect(route).toBeDefined()

    expect(
      router.matchRoute(route!, undefined, {}, { 'x-amz-copy-source': '/source-bucket/source-key' })
    ).toBe(true)
  })

  it('matches routes that require a header value prefix', () => {
    const router = new Router()

    router.post(
      '/:Bucket|content-type=multipart/form-data',
      {
        schema: {},
        operation: 'test.operation',
      },
      async () => ({})
    )

    const route = router.routes().get('/:Bucket')?.[0]
    expect(route).toBeDefined()

    expect(
      router.matchRoute(
        route!,
        undefined,
        {},
        { 'content-type': 'multipart/form-data; boundary=abc123' }
      )
    ).toBe(true)
  })

  it('rejects routes when a required header is missing or has the wrong value', () => {
    const router = new Router()

    router.post(
      '/:Bucket|content-type=multipart/form-data',
      {
        schema: {},
        operation: 'test.operation',
      },
      async () => ({})
    )

    const route = router.routes().get('/:Bucket')?.[0]
    expect(route).toBeDefined()

    expect(router.matchRoute(route!, undefined, {}, {})).toBe(false)
    expect(router.matchRoute(route!, undefined, {}, { 'content-type': 'application/json' })).toBe(
      false
    )
  })
})

describe('S3 router type matching', () => {
  it('matches iceberg-typed routes only for iceberg requests', () => {
    const router = new Router()

    router.get(
      '/:Bucket/*',
      {
        schema: {},
        operation: 'test.operation',
        type: 'iceberg',
      },
      async () => ({})
    )

    const route = router.routes().get('/:Bucket/*')?.[0]
    expect(route).toBeDefined()

    expect(router.matchRoute(route!, 'iceberg', {}, {})).toBe(true)
    expect(router.matchRoute(route!, undefined, {}, {})).toBe(false)
  })

  it('matches untyped routes only for untyped requests', () => {
    const router = new Router()

    router.get(
      '/:Bucket/*',
      {
        schema: {},
        operation: 'test.operation',
      },
      async () => ({})
    )

    const route = router.routes().get('/:Bucket/*')?.[0]
    expect(route).toBeDefined()

    expect(router.matchRoute(route!, undefined, {}, {})).toBe(true)
    expect(router.matchRoute(route!, 'iceberg', {}, {})).toBe(false)
  })
})

describe('S3ProtocolHandler.parseMetadataHeaders', () => {
  it('returns only x-amz-meta headers without the prefix', () => {
    const handler = createHandler()

    expect(
      handler.parseMetadataHeaders({
        'content-type': 'application/json',
        'x-amz-meta-color': 'blue',
        'x-amz-meta-size': 'large',
      })
    ).toEqual({
      color: 'blue',
      size: 'large',
    })
  })

  it('returns undefined when there are no metadata headers', () => {
    const handler = createHandler()

    expect(
      handler.parseMetadataHeaders({
        authorization: 'token',
        'content-type': 'application/json',
      })
    ).toBeUndefined()
  })

  it('keeps only string metadata values', () => {
    const handler = createHandler()

    expect(
      handler.parseMetadataHeaders({
        'x-amz-meta-color': 'blue',
        'x-amz-meta-count': 1,
        'x-amz-meta-enabled': true,
        'x-amz-meta-tags': ['a', 'b'],
        'x-amz-meta-config': { mode: 'fast' },
      })
    ).toEqual({
      color: 'blue',
    })
  })

  it('returns undefined when metadata headers are present but none are strings', () => {
    const handler = createHandler()

    expect(
      handler.parseMetadataHeaders({
        'x-amz-meta-count': 1,
        'x-amz-meta-enabled': false,
        'x-amz-meta-tags': ['a', 'b'],
      })
    ).toBeUndefined()
  })
})

describe('CompleteMultipartUpload route mapping', () => {
  it('maps ChecksumCRC32C from the backend response on iceberg routes', async () => {
    const router = new Router()
    const completeMultipartUpload = vi.fn().mockResolvedValue({
      ChecksumCRC32: 'crc32-value',
      ChecksumCRC32C: 'crc32c-value',
      ChecksumSHA1: 'sha1-value',
      ChecksumSHA256: 'sha256-value',
      ETag: 'etag-value',
    })

    CompleteMultipartUpload(router as unknown as S3Router)

    const route = router
      .routes()
      .get('/:Bucket/*')
      ?.find((candidate) => candidate.method === 'post' && candidate.type === 'iceberg')

    expect(route).toBeDefined()

    const response = await route!.handler!(
      {
        Params: {
          Bucket: 'public-bucket',
          '*': 'folder/object.txt',
        },
        Querystring: {
          uploadId: 'upload-id',
        },
        Body: {
          CompleteMultipartUpload: {
            Part: [{ PartNumber: 1, ETag: 'part-etag' }],
          },
        },
      } as never,
      {
        req: {
          internalIcebergBucketName: 'iceberg-bucket',
          storage: {
            backend: {
              completeMultipartUpload,
            },
          },
        },
      } as never
    )

    expect(completeMultipartUpload).toHaveBeenCalledWith(
      'iceberg-bucket',
      'folder/object.txt',
      'upload-id',
      '',
      [{ PartNumber: 1, ETag: 'part-etag' }]
    )
    expect(response).toMatchObject({
      responseBody: {
        CompleteMultipartUploadResult: {
          ChecksumCRC32: 'crc32-value',
          ChecksumCRC32C: 'crc32c-value',
          ChecksumSHA1: 'sha1-value',
          ChecksumSHA256: 'sha256-value',
          ETag: 'etag-value',
        },
      },
    })
  })
})

describe('DeleteObject route mapping', () => {
  it('accepts DeleteObjects payloads at the object request cap in router validation', async () => {
    const { default: DeleteObject } = await import('./commands/delete-object')
    const router = new Router()

    DeleteObject(router as unknown as S3Router)

    const route = router
      .routes()
      .get('/:Bucket')
      ?.find(
        (candidate) =>
          candidate.method === 'post' &&
          candidate.querystringMatches.some((match) => match.key === 'delete')
      )

    expect(route).toBeDefined()

    const validate = route!.compiledSchema()
    const data = {
      Params: { Bucket: 'bucket' },
      Querystring: { delete: '' },
      Body: {
        Delete: {
          Object: [...Array(MAX_OBJECTS_PER_REQUEST).keys()].map((i) => ({
            Key: `object-${i}`,
          })),
        },
      },
    }

    expect(data.Body.Delete.Object).toHaveLength(MAX_OBJECTS_PER_REQUEST)
    expect(validate(data)).toBe(true)
    expect(validate.errors).toBeNull()
  })

  it('accepts DeleteObjects payloads over the object request cap by default', async () => {
    const { default: DeleteObject } = await import('./commands/delete-object')
    const router = new Router()

    DeleteObject(router as unknown as S3Router)

    const route = router
      .routes()
      .get('/:Bucket')
      ?.find(
        (candidate) =>
          candidate.method === 'post' &&
          candidate.querystringMatches.some((match) => match.key === 'delete')
      )

    expect(route).toBeDefined()

    const validate = route!.compiledSchema()
    const data = {
      Params: { Bucket: 'bucket' },
      Querystring: { delete: '' },
      Body: {
        Delete: {
          Object: [...Array(MAX_OBJECTS_PER_REQUEST + 1).keys()].map((i) => ({
            Key: `object-${i}`,
          })),
        },
      },
    }

    expect(validate(data)).toBe(true)
    expect(validate.errors).toBeNull()
  })

  it('keeps DeleteObjects router validation tenant-agnostic when hard limits are enabled', async () => {
    const previousHardLimitsEnabled = process.env.REQUEST_HARD_LIMITS_ENABLED
    process.env.REQUEST_HARD_LIMITS_ENABLED = 'true'
    vi.resetModules()

    try {
      const [{ Router: FreshRouter }, { default: DeleteObject }] = await Promise.all([
        import('./router'),
        import('./commands/delete-object'),
      ])
      const router = new FreshRouter()

      DeleteObject(router as unknown as S3Router)

      const route = router
        .routes()
        .get('/:Bucket')
        ?.find(
          (candidate) =>
            candidate.method === 'post' &&
            candidate.querystringMatches.some((match) => match.key === 'delete')
        )

      expect(route).toBeDefined()

      const validate = route!.compiledSchema()
      const data = {
        Params: { Bucket: 'bucket' },
        Querystring: { delete: '' },
        Body: {
          Delete: {
            Object: [...Array(MAX_OBJECTS_PER_REQUEST + 1).keys()].map((i) => ({
              Key: `object-${i}`,
            })),
          },
        },
      }

      expect(validate(data)).toBe(true)
      expect(validate.errors).toBeNull()
    } finally {
      if (previousHardLimitsEnabled === undefined) {
        delete process.env.REQUEST_HARD_LIMITS_ENABLED
      } else {
        process.env.REQUEST_HARD_LIMITS_ENABLED = previousHardLimitsEnabled
      }
      vi.resetModules()
    }
  })

  it('rejects DeleteObjects payloads over the default cap in the handler when hard limits are enabled', async () => {
    const previousHardLimitsEnabled = process.env.REQUEST_HARD_LIMITS_ENABLED
    const previousMultiTenant = process.env.MULTI_TENANT
    process.env.REQUEST_HARD_LIMITS_ENABLED = 'true'
    process.env.MULTI_TENANT = 'false'
    vi.resetModules()

    try {
      const [{ Router: FreshRouter }, { default: DeleteObject }] = await Promise.all([
        import('./router'),
        import('./commands/delete-object'),
      ])
      const router = new FreshRouter()

      DeleteObject(router as unknown as S3Router)

      const route = router
        .routes()
        .get('/:Bucket')
        ?.find(
          (candidate) =>
            candidate.method === 'post' &&
            candidate.querystringMatches.some((match) => match.key === 'delete')
        )

      expect(route).toBeDefined()

      await expect(
        route!.handler!(
          {
            Params: { Bucket: 'bucket' },
            Querystring: { delete: '' },
            Headers: {},
            Body: {
              Delete: {
                Object: [...Array(MAX_OBJECTS_PER_REQUEST + 1).keys()].map((i) => ({
                  Key: `object-${i}`,
                })),
              },
            },
          },
          {
            tenantId: 'tenant-id',
          } as never
        )
      ).rejects.toMatchObject({
        code: 'InvalidRequest',
        message: `Bulk object requests are limited to ${MAX_OBJECTS_PER_REQUEST} objects per request.`,
      })
    } finally {
      if (previousHardLimitsEnabled === undefined) {
        delete process.env.REQUEST_HARD_LIMITS_ENABLED
      } else {
        process.env.REQUEST_HARD_LIMITS_ENABLED = previousHardLimitsEnabled
      }
      if (previousMultiTenant === undefined) {
        delete process.env.MULTI_TENANT
      } else {
        process.env.MULTI_TENANT = previousMultiTenant
      }
      vi.resetModules()
    }
  })

  it('returns 204 from iceberg single-object deletes', async () => {
    const previousIcebergDeleteEnabled = process.env.ICEBERG_S3_DELETE_ENABLED
    process.env.ICEBERG_S3_DELETE_ENABLED = 'true'
    vi.resetModules()

    try {
      const [{ Router: FreshRouter }, { default: DeleteObject }] = await Promise.all([
        import('./router'),
        import('./commands/delete-object'),
      ])
      const router = new FreshRouter()
      const deleteObject = vi.fn().mockResolvedValue(undefined)

      DeleteObject(router as unknown as S3Router)

      const route = router
        .routes()
        .get('/:Bucket/*')
        ?.find((candidate) => candidate.method === 'delete' && candidate.type === 'iceberg')

      expect(route).toBeDefined()

      const response = await route!.handler!(
        {
          Params: {
            Bucket: 'public-bucket',
            '*': 'metadata/file.avro',
          },
          Querystring: {},
        } as never,
        {
          req: {
            internalIcebergBucketName: 'internal-iceberg-bucket',
            storage: {
              backend: {
                deleteObject,
              },
            },
          },
        } as never
      )

      expect(deleteObject).toHaveBeenCalledWith(
        'internal-iceberg-bucket',
        'metadata/file.avro',
        undefined
      )
      expect(response).toEqual({
        statusCode: 204,
      })
    } finally {
      if (previousIcebergDeleteEnabled === undefined) {
        delete process.env.ICEBERG_S3_DELETE_ENABLED
      } else {
        process.env.ICEBERG_S3_DELETE_ENABLED = previousIcebergDeleteEnabled
      }
      vi.resetModules()
    }
  })
})

describe('S3ProtocolHandler multipart completion regressions', () => {
  it('preserves ChecksumCRC32C when completing multipart uploads', async () => {
    const backend = {
      completeMultipartUpload: vi.fn().mockResolvedValue({
        version: 'version-1',
        ChecksumCRC32: 'crc32-value',
        ChecksumCRC32C: 'crc32c-value',
        ChecksumSHA1: 'sha1-value',
        ChecksumSHA256: 'sha256-value',
        ETag: 'etag-value',
      }),
      headObject: vi.fn().mockResolvedValue({
        cacheControl: '',
        contentLength: 1,
        size: 1,
        mimetype: 'text/plain',
        eTag: 'etag-value',
        lastModified: new Date('2026-04-07T00:00:00.000Z'),
      }),
    }
    const superUserDb = {
      findMultipartUpload: vi.fn().mockResolvedValue({
        version: 'version-1',
        user_metadata: null,
        metadata: null,
      }),
      deleteMultipartUpload: vi.fn().mockResolvedValue(undefined),
    }
    const storage = {
      backend,
      db: {
        asSuperUser: vi.fn(() => superUserDb),
      },
      location: {
        getKeyLocation: vi.fn().mockReturnValue('tenant-id/bucket/object.txt'),
      },
    }

    const completeUploadResult = {} as Awaited<ReturnType<Uploader['completeUpload']>>

    vi.spyOn(Uploader.prototype, 'canUpload').mockResolvedValue(undefined)
    vi.spyOn(Uploader.prototype, 'completeUpload').mockResolvedValue(completeUploadResult)

    const handler = new S3ProtocolHandler(storage as never, 'tenant-id', 'owner-id')
    const response = await handler.completeMultiPartUpload({
      Bucket: 'bucket',
      Key: 'object.txt',
      UploadId: 'upload-id',
      MultipartUpload: {
        Parts: [{ PartNumber: 1, ETag: 'part-etag' }],
      },
    })

    expect(response).toMatchObject({
      responseBody: {
        CompleteMultipartUploadResult: {
          ChecksumCRC32: 'crc32-value',
          ChecksumCRC32C: 'crc32c-value',
          ChecksumSHA1: 'sha1-value',
          ChecksumSHA256: 'sha256-value',
          ETag: 'etag-value',
        },
      },
    })
  })
})

describe('S3ProtocolHandler headObject validation', () => {
  it('reports Key as the missing parameter for headObject', async () => {
    const handler = new S3ProtocolHandler({} as never, 'tenant-id')
    const missingKeyCommand = { Bucket: 'bucket' } as Parameters<S3ProtocolHandler['headObject']>[0]

    await expect(handler.headObject(missingKeyCommand)).rejects.toMatchObject({
      message: 'Missing Required Parameter Key',
    })
  })

  it('reports Key as the missing parameter for dbHeadObject', async () => {
    const handler = new S3ProtocolHandler({} as never, 'tenant-id')
    const missingKeyCommand = {
      Bucket: 'bucket',
    } as Parameters<S3ProtocolHandler['dbHeadObject']>[0]

    await expect(handler.dbHeadObject(missingKeyCommand)).rejects.toMatchObject({
      message: 'Missing Required Parameter Key',
    })
  })
})
