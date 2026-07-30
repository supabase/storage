import type { IncomingMessage } from 'node:http'
import { Readable } from 'node:stream'
import { NoSuchUpload } from '@aws-sdk/client-s3'
import { HttpResponse } from '@smithy/protocol-http'
import { type MetadataValue, S3Store as TusS3Store } from '@tus/s3-store'
import { Upload } from '@tus/server'
import { runWithTusRequest } from './request-context'
import { S3Store } from './s3-store'

class TestS3Store extends S3Store {
  getClient() {
    return this.client
  }

  attemptWriteMutation() {
    return this.deleteIncompletePart('upload-id')
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

function createRequest(): IncomingMessage {
  return {
    aborted: false,
    complete: true,
    headers: { 'content-length': '1' },
  } as unknown as IncomingMessage
}

describe('S3Store', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  function mockMetadata(store: TestS3Store) {
    vi.spyOn(
      store as unknown as { getMetadata(id: string): Promise<MetadataValue> },
      'getMetadata'
    ).mockResolvedValue({
      file: new Upload({ id: 'upload-id', offset: 0 }),
      'upload-id': 'mpu-1',
      'tus-version': '1.0.0',
    })
  }

  function noSuchUpload() {
    return new NoSuchUpload({
      $metadata: { httpStatusCode: 404 },
      message: 'The specified multipart upload does not exist',
    })
  }

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

  test('settles multipart abort before deleting data and metadata', async () => {
    const store = createStore(vi.fn())
    const client = store.getClient()
    const abortStarted = Promise.withResolvers<void>()
    const pendingAbort = Promise.withResolvers<void>()
    mockMetadata(store)
    const abort = vi.spyOn(client, 'abortMultipartUpload').mockImplementation(() => {
      abortStarted.resolve()
      return pendingAbort.promise as never
    })
    const deleteObjects = vi.spyOn(client, 'deleteObjects').mockResolvedValue({} as never)

    const removal = store.remove('upload-id')

    await abortStarted.promise
    expect(abort).toHaveBeenCalledOnce()
    expect(deleteObjects).not.toHaveBeenCalled()
    pendingAbort.resolve()
    await removal

    expect(deleteObjects).toHaveBeenNthCalledWith(1, {
      Bucket: 'test-bucket',
      Delete: { Objects: [{ Key: 'upload-id' }, { Key: 'upload-id.part' }] },
    })
    expect(deleteObjects).toHaveBeenNthCalledWith(2, {
      Bucket: 'test-bucket',
      Delete: { Objects: [{ Key: 'upload-id.info' }] },
    })
  })

  test('deletes completed-upload artifacts when abort returns NoSuchUpload', async () => {
    const store = createStore(vi.fn())
    const client = store.getClient()
    mockMetadata(store)
    vi.spyOn(client, 'abortMultipartUpload').mockRejectedValue(noSuchUpload())
    const deleteObjects = vi.spyOn(client, 'deleteObjects').mockResolvedValue({} as never)

    await expect(store.remove('upload-id')).resolves.toBeUndefined()

    expect(deleteObjects).toHaveBeenCalledTimes(2)
  })

  test('retries reported DeleteObjects errors before deleting metadata', async () => {
    const store = createStore(vi.fn())
    const client = store.getClient()
    mockMetadata(store)
    vi.spyOn(client, 'abortMultipartUpload').mockRejectedValue(noSuchUpload())
    const deleteObjects = vi
      .spyOn(client, 'deleteObjects')
      .mockResolvedValueOnce({
        $metadata: {},
        Errors: [{ Key: 'upload-id', Code: 'InternalError', Message: 'retry later' }],
      } as never)
      .mockResolvedValue({ $metadata: {} } as never)

    await expect(store.remove('upload-id')).resolves.toBeUndefined()

    expect(deleteObjects).toHaveBeenCalledTimes(3)
    expect(deleteObjects).toHaveBeenNthCalledWith(2, {
      Bucket: 'test-bucket',
      Delete: { Objects: [{ Key: 'upload-id' }] },
    })
    expect(deleteObjects).toHaveBeenNthCalledWith(3, {
      Bucket: 'test-bucket',
      Delete: { Objects: [{ Key: 'upload-id.info' }] },
    })
  })

  test('keeps metadata when data cleanup still fails after retry', async () => {
    const store = createStore(vi.fn())
    const client = store.getClient()
    mockMetadata(store)
    vi.spyOn(client, 'abortMultipartUpload').mockResolvedValue(undefined as never)
    const deleteObjects = vi.spyOn(client, 'deleteObjects').mockResolvedValue({
      $metadata: {},
      Errors: [{ Key: 'upload-id', Code: 'AccessDenied', Message: 'denied' }],
    } as never)

    await expect(store.remove('upload-id')).rejects.toMatchObject({
      message: 'Failed to delete TUS upload artifacts for upload-id',
    })

    expect(deleteObjects).toHaveBeenCalledTimes(2)
  })

  test('retries the full deletion phase when S3 omits the failed key', async () => {
    const store = createStore(vi.fn())
    const client = store.getClient()
    mockMetadata(store)
    vi.spyOn(client, 'abortMultipartUpload').mockRejectedValue(noSuchUpload())
    const deleteObjects = vi
      .spyOn(client, 'deleteObjects')
      .mockResolvedValueOnce({
        $metadata: {},
        Errors: [{ Code: 'InternalError', Message: 'retry later' }],
      } as never)
      .mockResolvedValue({ $metadata: {} } as never)

    await expect(store.remove('upload-id')).resolves.toBeUndefined()

    expect(deleteObjects).toHaveBeenNthCalledWith(2, {
      Bucket: 'test-bucket',
      Delete: { Objects: [{ Key: 'upload-id' }, { Key: 'upload-id.part' }] },
    })
  })

  test('aggregates abort, data, and cache cleanup failures', async () => {
    const store = createStore(vi.fn())
    const client = store.getClient()
    const abortError = new Error('abort failed')
    const dataError = new Error('data cleanup failed')
    const cacheError = new Error('cache cleanup failed')
    mockMetadata(store)
    vi.spyOn(client, 'abortMultipartUpload').mockRejectedValue(abortError)
    vi.spyOn(client, 'deleteObjects').mockRejectedValue(dataError)
    vi.spyOn(
      store as unknown as { clearCache(id: string): Promise<void> },
      'clearCache'
    ).mockRejectedValue(cacheError)

    await expect(store.remove('upload-id')).rejects.toMatchObject({
      message: 'Failed to remove TUS upload upload-id',
      errors: [abortError, dataError, cacheError],
    })
  })

  test('preserves the upload when S3 rejects before a write mutation', async () => {
    const store = createStore(vi.fn())
    const writeError = new Error('metadata lookup failed')
    vi.spyOn(TusS3Store.prototype, 'write').mockRejectedValue(writeError)
    const remove = vi.spyOn(store, 'remove').mockResolvedValue(undefined)

    const result = runWithTusRequest(createRequest(), () =>
      store.write(Readable.from('x'), 'upload-id', 0)
    )

    await expect(result).rejects.toBe(writeError)
    expect(remove).not.toHaveBeenCalled()
  })

  test('invalidates the upload when S3 rejects after a write mutation', async () => {
    const store = createStore(vi.fn())
    const client = store.getClient()
    const writeError = new Error('upload part failed')
    vi.spyOn(client, 'deleteObject').mockResolvedValue(undefined as never)
    vi.spyOn(TusS3Store.prototype, 'write').mockImplementation(async function (this: TusS3Store) {
      await (this as TestS3Store).attemptWriteMutation()
      throw writeError
    })
    const remove = vi.spyOn(store, 'remove').mockResolvedValue(undefined)

    const result = runWithTusRequest(createRequest(), () =>
      store.write(Readable.from('x'), 'upload-id', 0)
    )

    await expect(result).rejects.toBe(writeError)
    expect(remove).toHaveBeenCalledWith('upload-id')
  })
})
