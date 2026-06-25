import { MAX_HEADER_NAME_LENGTH } from '@internal/http/header'
import { S3ProtocolHandler } from '@storage/protocols/s3/s3-handler'

describe('S3ProtocolHandler.dbHeadObject', () => {
  it('emits empty user metadata values as valid S3 metadata headers', async () => {
    const findObject = vi.fn().mockResolvedValue({
      created_at: '2026-06-25T00:00:00.000Z',
      metadata: {
        eTag: '"etag"',
        mimetype: 'text/plain',
        size: '0',
      },
      updated_at: '2026-06-25T00:00:00.000Z',
      user_metadata: {
        color: 'blue',
        empty: '',
      },
    })
    const storage = {
      from: vi.fn(() => ({
        findObject,
      })),
    }
    const handler = new S3ProtocolHandler(storage as never, 'tenant-id')

    const response = await handler.dbHeadObject({
      Bucket: 'bucket',
      Key: 'object.txt',
    })

    expect(response.headers).toMatchObject({
      'x-amz-meta-color': 'blue',
      'x-amz-meta-empty': '',
    })
    expect(response.headers).not.toHaveProperty('x-amz-missing-meta')
  })

  it('counts metadata as missing when the emitted S3 metadata header name is too long', async () => {
    const prefix = 'x-amz-meta-'
    const key = 'a'.repeat(MAX_HEADER_NAME_LENGTH - prefix.length + 1)
    const findObject = vi.fn().mockResolvedValue({
      created_at: '2026-06-25T00:00:00.000Z',
      metadata: {
        eTag: '"etag"',
        mimetype: 'text/plain',
        size: '0',
      },
      updated_at: '2026-06-25T00:00:00.000Z',
      user_metadata: {
        [key]: 'value',
      },
    })
    const storage = {
      from: vi.fn(() => ({
        findObject,
      })),
    }
    const handler = new S3ProtocolHandler(storage as never, 'tenant-id')

    const response = await handler.dbHeadObject({
      Bucket: 'bucket',
      Key: 'object.txt',
    })

    expect(response.headers).toHaveProperty('x-amz-missing-meta', 1)
    expect(response.headers).not.toHaveProperty(prefix + key)
  })

  it('counts empty user metadata keys as missing', async () => {
    const findObject = vi.fn().mockResolvedValue({
      created_at: '2026-06-25T00:00:00.000Z',
      metadata: {
        eTag: '"etag"',
        mimetype: 'text/plain',
        size: '0',
      },
      updated_at: '2026-06-25T00:00:00.000Z',
      user_metadata: {
        '': 'value',
        color: 'blue',
      },
    })
    const storage = {
      from: vi.fn(() => ({
        findObject,
      })),
    }
    const handler = new S3ProtocolHandler(storage as never, 'tenant-id')

    const response = await handler.dbHeadObject({
      Bucket: 'bucket',
      Key: 'object.txt',
    })

    expect(response.headers).toMatchObject({
      'x-amz-meta-color': 'blue',
      'x-amz-missing-meta': 1,
    })
    expect(response.headers).not.toHaveProperty('x-amz-meta-')
  })
})

describe('S3ProtocolHandler.getObject', () => {
  it('preserves backend not-modified responses for cache validators', async () => {
    const backendGetObject = vi.fn().mockResolvedValue({
      body: undefined,
      httpStatusCode: 304,
      metadata: {
        cacheControl: 'no-cache',
        contentLength: 0,
        eTag: '"current-etag"',
        httpStatusCode: 304,
        lastModified: new Date(),
        mimetype: 'text/plain',
        size: 29,
      },
    })
    const findObject = vi.fn().mockResolvedValue({
      user_metadata: null,
      version: 'object-version',
    })
    const getRootLocation = vi.fn(() => 'root-bucket')
    const getKeyLocation = vi.fn(() => 'tenant-id/bucket/object.txt')
    const storage = {
      backend: {
        getObject: backendGetObject,
      },
      from: vi.fn(() => ({
        findObject,
      })),
      location: {
        getKeyLocation,
        getRootLocation,
      },
    }
    const handler = new S3ProtocolHandler(storage as never, 'tenant-id')

    const response = await handler.getObject({
      Bucket: 'bucket',
      IfNoneMatch: '"current-etag"',
      Key: 'object.txt',
    })

    expect(response.statusCode).toBe(304)
    expect(response.responseBody).toBeUndefined()
    expect(backendGetObject).toHaveBeenCalledWith(
      'root-bucket',
      'tenant-id/bucket/object.txt',
      'object-version',
      {
        ifModifiedSince: undefined,
        ifNoneMatch: '"current-etag"',
        range: undefined,
      },
      undefined
    )
  })
})
