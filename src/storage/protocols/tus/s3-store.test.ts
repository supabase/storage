import { HttpResponse } from '@smithy/protocol-http'
import { Upload } from '@tus/server'
import { S3Store } from './s3-store'

class TestS3Store extends S3Store {
  getClient() {
    return this.client
  }
}

function createStore(
  handle: (request: unknown, options?: unknown) => Promise<{ response: HttpResponse }>,
  maxAttempts = 1
) {
  return new TestS3Store({
    s3ClientConfig: {
      bucket: 'test-bucket',
      region: 'us-east-1',
      endpoint: 'http://127.0.0.1:9000',
      credentials: {
        accessKeyId: 'test',
        secretAccessKey: 'test',
      },
      maxAttempts,
      requestHandler: { handle },
    },
  })
}

describe('S3Store', () => {
  test('create matches upstream apart from ContentEncoding', async () => {
    const store = createStore(vi.fn())
    const create = vi.spyOn(store.getClient(), 'createMultipartUpload').mockResolvedValue({
      Key: 'upload-id',
      UploadId: 'multipart-id',
      $metadata: {},
    } as never)
    const put = vi
      .spyOn(store.getClient(), 'putObject')
      .mockResolvedValue({ $metadata: {} } as never)
    const metadata = { contentType: 'text/plain', cacheControl: 'no-transform' }

    const plain = await store.create(new Upload({ id: 'upload-id', size: 10, offset: 0, metadata }))
    const encoded = await store.create(
      new Upload({
        id: 'upload-id',
        size: 10,
        offset: 0,
        metadata: { ...metadata, contentEncoding: 'gzip' },
      })
    )

    expect(create.mock.calls[1][0]).toEqual({ ...create.mock.calls[0][0], ContentEncoding: 'gzip' })
    expect(put.mock.calls[1][0].Metadata).toEqual(put.mock.calls[0][0].Metadata)
    expect(encoded.storage).toEqual(plain.storage)
  })

  test('removes the no-op logger middleware from the internal TUS client', () => {
    const store = createStore(vi.fn())

    expect(
      store
        .getClient()
        .middlewareStack.identify()
        .some((middleware) => middleware.includes('loggerMiddleware'))
    ).toBe(false)
  })

  test('preserves SDK retries after removing the logger middleware', async () => {
    const handle = vi
      .fn()
      .mockResolvedValueOnce({
        response: new HttpResponse({ statusCode: 500, headers: {}, body: new Uint8Array() }),
      })
      .mockResolvedValueOnce({
        response: new HttpResponse({ statusCode: 200, headers: {}, body: new Uint8Array() }),
      })
    const store = createStore(handle, 2)

    const result = await store.getClient().headBucket({ Bucket: 'test-bucket' })

    expect(handle).toHaveBeenCalledTimes(2)
    expect(result.$metadata).toMatchObject({ httpStatusCode: 200, attempts: 2 })
  })

  test('preserves SDK errors after removing the logger middleware', async () => {
    const expectedError = new Error('request failed')
    const handle = vi.fn().mockRejectedValue(expectedError)
    const store = createStore(handle)

    await expect(store.getClient().headBucket({ Bucket: 'test-bucket' })).rejects.toBe(
      expectedError
    )
    expect(expectedError).toMatchObject({
      $metadata: { attempts: 1, totalRetryDelay: 0 },
    })
  })
})
