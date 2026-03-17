import CompleteMultipartUpload from '../http/routes/s3/commands/complete-multipart-upload'
import { Router, type S3Router } from '../http/routes/s3/router'
import { S3ProtocolHandler } from '../storage/protocols/s3/s3-handler'
import { Uploader } from '../storage/uploader'

afterEach(() => {
  jest.restoreAllMocks()
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

    expect(
      router.matchRoute(route!, {
        query: { uploads: undefined },
        headers: {},
      })
    ).toBe(true)
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

    expect(
      router.matchRoute(route!, {
        query: { 'list-type': '2' },
        headers: {},
      })
    ).toBe(true)
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

    expect(
      router.matchRoute(route!, {
        query: { 'list-type': '1' },
        headers: {},
      })
    ).toBe(false)
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

    expect(
      router.matchRoute(route!, {
        query: { uploads: undefined },
        headers: {},
      })
    ).toBe(true)
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
    const completeMultipartUpload = jest.fn().mockResolvedValue({
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

describe('S3ProtocolHandler multipart completion regressions', () => {
  it('preserves ChecksumCRC32C when completing multipart uploads', async () => {
    const backend = {
      completeMultipartUpload: jest.fn().mockResolvedValue({
        version: 'version-1',
        ChecksumCRC32: 'crc32-value',
        ChecksumCRC32C: 'crc32c-value',
        ChecksumSHA1: 'sha1-value',
        ChecksumSHA256: 'sha256-value',
        ETag: 'etag-value',
      }),
      headObject: jest.fn().mockResolvedValue({
        cacheControl: '',
        contentLength: 1,
        size: 1,
        mimetype: 'text/plain',
        eTag: 'etag-value',
        lastModified: new Date('2026-04-07T00:00:00.000Z'),
      }),
    }
    const superUserDb = {
      findMultipartUpload: jest.fn().mockResolvedValue({
        version: 'version-1',
        user_metadata: null,
        metadata: null,
      }),
      deleteMultipartUpload: jest.fn().mockResolvedValue(undefined),
    }
    const storage = {
      backend,
      db: {
        asSuperUser: jest.fn(() => superUserDb),
      },
      location: {
        getKeyLocation: jest.fn().mockReturnValue('tenant-id/bucket/object.txt'),
      },
    }

    const completeUploadResult = {} as Awaited<ReturnType<Uploader['completeUpload']>>

    jest.spyOn(Uploader.prototype, 'canUpload').mockResolvedValue(undefined)
    jest.spyOn(Uploader.prototype, 'completeUpload').mockResolvedValue(completeUploadResult)

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
