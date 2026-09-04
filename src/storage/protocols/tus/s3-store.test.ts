import { HttpResponse } from '@smithy/protocol-http'
import { S3Store as TusS3Store } from '@tus/s3-store'
import type { Upload } from '@tus/server'
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

function baseUpload(overrides: Partial<Upload> = {}): Upload {
  return {
    id: 'tenant/bucket/empty.txt/version-1',
    offset: 0,
    size: 0,
    sizeIsDeferred: false,
    metadata: { contentType: 'text/plain' },
    creation_date: undefined,
    storage: undefined,
    ...overrides,
  } as Upload
}

describe('S3Store', () => {
  afterEach(() => {
    vi.restoreAllMocks()
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

  test('finalizes an empty multipart upload when Upload-Length is 0', async () => {
    const store = createStore(vi.fn())
    const upload = baseUpload({ size: 0 })
    const metadata = {
      file: upload,
      'upload-id': 'mpu-123',
      'tus-version': '1.0.0',
    }

    vi.spyOn(TusS3Store.prototype, 'create').mockResolvedValue(upload)
    const getMetadata = vi.spyOn(store as any, 'getMetadata').mockResolvedValue(metadata)
    const finish = vi.spyOn(store as any, 'finishMultipartUpload').mockResolvedValue('s3://loc')
    const complete = vi.spyOn(store as any, 'completeMetadata').mockResolvedValue(undefined)
    const clear = vi.spyOn(store as any, 'clearCache').mockResolvedValue(undefined)

    await expect(store.create(upload)).resolves.toBe(upload)

    expect(getMetadata).toHaveBeenCalledWith(upload.id)
    expect(finish).toHaveBeenCalledWith(metadata, [])
    expect(complete).toHaveBeenCalledWith(upload)
    expect(clear).toHaveBeenCalledWith(upload.id)
  })

  test('does not finalize multipart upload for non-zero or deferred sizes', async () => {
    const store = createStore(vi.fn())
    const finish = vi.spyOn(store as any, 'finishMultipartUpload')

    for (const upload of [
      baseUpload({ size: 1 }),
      baseUpload({ size: 0, sizeIsDeferred: true }),
    ]) {
      vi.spyOn(TusS3Store.prototype, 'create').mockResolvedValueOnce(upload)
      await store.create(upload)
    }

    expect(finish).not.toHaveBeenCalled()
  })
})
