import { CopyObjectCommand, GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { ERRORS, ErrorCode, isStorageError } from '@internal/errors'
import { PassThrough, Readable } from 'stream'
import { type Mock, vi } from 'vitest'
import { getConfig } from '../../../config'
import { withOptionalVersion } from '../adapter'
import { MAX_PUT_OBJECT_SIZE, S3Backend } from './adapter'

vi.mock('@aws-sdk/client-s3', async () => {
  const originalModule =
    await vi.importActual<typeof import('@aws-sdk/client-s3')>('@aws-sdk/client-s3')
  return {
    ...originalModule,
    S3Client: vi.fn(function () {
      return {
        send: vi.fn(),
      }
    }),
  }
})

vi.mock('@aws-sdk/lib-storage', async () => {
  const originalModule =
    await vi.importActual<typeof import('@aws-sdk/lib-storage')>('@aws-sdk/lib-storage')
  return {
    ...originalModule,
    Upload: vi.fn(function () {}),
  }
})

vi.mock('@aws-sdk/s3-request-presigner', () => ({
  getSignedUrl: vi.fn().mockResolvedValue('http://signed.example.com/test-bucket/test-key'),
}))

type UploadOptionsShape = {
  queueSize?: number
  params?: {
    Body?: AsyncIterable<unknown>
  }
}

type MockUploadDoneResult = {
  ETag: string
  $metadata: {
    httpStatusCode: number
  }
}

type MockUploadInstance = {
  options: UploadOptionsShape
  abort: Mock
  done: Mock<() => Promise<MockUploadDoneResult>>
  on: Mock
  off: Mock
  emit: (event: string, payload: unknown) => void
}

describe('S3Backend', () => {
  let mockSend: Mock
  let mockUploadDone: Mock<(instance: MockUploadInstance) => Promise<MockUploadDoneResult>>
  let uploadInstances: MockUploadInstance[]

  async function drainUploadBody(instance: MockUploadInstance) {
    const body = instance.options.params?.Body
    if (!body) {
      return
    }

    for await (const _chunk of body) {
      // Drain the body to simulate what Upload does while sending multipart data.
    }
  }

  beforeEach(() => {
    vi.clearAllMocks()
    mockSend = vi.fn()
    mockUploadDone = vi.fn(async (instance) => {
      await drainUploadBody(instance)
      return {
        ETag: '"multipart-etag"',
        $metadata: {
          httpStatusCode: 200,
        },
      }
    })
    uploadInstances = []

    ;(S3Client as unknown as Mock).mockImplementation(function () {
      return {
        send: mockSend,
      }
    })

    ;(Upload as unknown as Mock).mockImplementation(function (options: UploadOptionsShape) {
      const handlers = new Map<string, Set<(payload: unknown) => void>>()
      const instance = {} as MockUploadInstance

      instance.options = options
      instance.abort = vi.fn()
      instance.done = vi.fn(() => mockUploadDone(instance))
      instance.on = vi.fn((event: string, handler: (payload: unknown) => void) => {
        const eventHandlers = handlers.get(event) ?? new Set()
        eventHandlers.add(handler)
        handlers.set(event, eventHandlers)
        return instance
      })
      instance.off = vi.fn((event: string, handler: (payload: unknown) => void) => {
        handlers.get(event)?.delete(handler)
        return instance
      })
      instance.emit = (event: string, payload: unknown) => {
        handlers.get(event)?.forEach((handler) => handler(payload))
      }

      uploadInstances.push(instance)
      return instance
    })
  })

  function createBackend() {
    return new S3Backend({
      region: 'us-east-1',
      endpoint: 'http://localhost:9000',
    })
  }

  describe('getObject', () => {
    test('should return correct default MIME type when S3 returns no ContentType', async () => {
      mockSend.mockResolvedValue({
        Body: Readable.from(['test content']),
        CacheControl: 'max-age=3600',
        ETag: '"abc123"',
        LastModified: new Date('2024-01-01'),
        ContentLength: 12,
        $metadata: {
          httpStatusCode: 200,
        },
      })

      const backend = createBackend()

      const result = await backend.getObject('test-bucket', 'test-key', undefined)

      expect(result.metadata.mimetype).toBe('application/octet-stream')
      expect(result.metadata.cacheControl).toBe('max-age=3600')
      expect(result.metadata.eTag).toBe('"abc123"')
      expect(result.httpStatusCode).toBe(200)
    })

    test('should use ContentType from S3 when provided', async () => {
      mockSend.mockResolvedValue({
        Body: Readable.from(['test content']),
        ContentType: 'image/png',
        CacheControl: 'no-cache',
        ETag: '"def456"',
        LastModified: new Date('2024-01-01'),
        ContentLength: 12,
        $metadata: {
          httpStatusCode: 200,
        },
      })

      const backend = createBackend()

      const result = await backend.getObject('test-bucket', 'test-key', undefined)

      expect(result.metadata.mimetype).toBe('image/png')
    })
  })

  describe('privateAssetUrl', () => {
    test('uses the primary S3 client when no private asset endpoint is configured', async () => {
      const backend = createBackend()

      await expect(backend.privateAssetUrl('test-bucket', 'test-key', undefined)).resolves.toBe(
        'http://signed.example.com/test-bucket/test-key'
      )

      const s3ClientMock = S3Client as unknown as Mock
      const defaultClient = s3ClientMock.mock.results[0].value
      expect(s3ClientMock).toHaveBeenCalledTimes(1)
      expect(getSignedUrl).toHaveBeenCalledWith(defaultClient, expect.any(GetObjectCommand), {
        expiresIn: 600,
      })
    })

    test('uses the private asset endpoint when signing private asset URLs', async () => {
      const backend = new S3Backend({
        region: 'us-east-1',
        endpoint: 'http://127.0.0.1:9000',
        privateAssetEndpoint: 'http://minio:9000',
        forcePathStyle: true,
      })

      await backend.privateAssetUrl('test-bucket', 'test-key', 'version-id')

      const s3ClientMock = S3Client as unknown as Mock
      expect(s3ClientMock).toHaveBeenCalledTimes(2)
      expect(s3ClientMock.mock.calls[0][0]).toMatchObject({
        endpoint: 'http://127.0.0.1:9000',
        forcePathStyle: true,
        region: 'us-east-1',
      })
      expect(s3ClientMock.mock.calls[1][0]).toMatchObject({
        endpoint: 'http://minio:9000',
        forcePathStyle: true,
        region: 'us-east-1',
      })

      const privateAssetClient = s3ClientMock.mock.results[1].value
      const privateAssetCommand = (getSignedUrl as Mock).mock.calls[0][1] as GetObjectCommand
      expect(privateAssetCommand.input).toMatchObject({
        Bucket: 'test-bucket',
        Key: withOptionalVersion('test-key', 'version-id'),
      })
      expect(getSignedUrl).toHaveBeenCalledWith(privateAssetClient, privateAssetCommand, {
        expiresIn: 600,
      })
    })
  })

  describe('copyObject', () => {
    test('uses REPLACE metadata directive when metadata should be overwritten', async () => {
      mockSend.mockResolvedValue({
        CopyObjectResult: {
          ETag: '"copy-etag"',
          LastModified: new Date('2026-05-18T00:00:00Z'),
        },
        $metadata: {
          httpStatusCode: 200,
        },
      })

      const backend = createBackend()
      await backend.copyObject(
        'test-bucket',
        'source-key',
        'source-version',
        'destination-key',
        'destination-version',
        {
          cacheControl: 'max-age=999',
          mimetype: 'image/gif',
        },
        undefined,
        { copyMetadata: false }
      )

      expect(mockSend).toHaveBeenCalledTimes(1)
      expect(mockSend.mock.calls[0][0]).toBeInstanceOf(CopyObjectCommand)
      const input = mockSend.mock.calls[0][0].input
      expect(input).toMatchObject({
        Bucket: 'test-bucket',
        Key: withOptionalVersion('destination-key', 'destination-version'),
        CacheControl: 'max-age=999',
        ContentType: 'image/gif',
        MetadataDirective: 'REPLACE',
      })
      expect(input.Metadata).toBeUndefined()
    })

    test('uses COPY metadata directive when metadata should be preserved', async () => {
      mockSend.mockResolvedValue({
        CopyObjectResult: {
          ETag: '"copy-etag"',
          LastModified: new Date('2026-05-18T00:00:00Z'),
        },
        $metadata: {
          httpStatusCode: 200,
        },
      })

      const backend = createBackend()
      await backend.copyObject(
        'test-bucket',
        'source-key',
        'source-version',
        'destination-key',
        'destination-version',
        {
          cacheControl: 'max-age=999',
          mimetype: 'image/gif',
        },
        undefined,
        { copyMetadata: true }
      )

      expect(mockSend).toHaveBeenCalledTimes(1)
      expect(mockSend.mock.calls[0][0]).toBeInstanceOf(CopyObjectCommand)
      const input = mockSend.mock.calls[0][0].input
      expect(input).toMatchObject({
        Bucket: 'test-bucket',
        Key: withOptionalVersion('destination-key', 'destination-version'),
        MetadataDirective: 'COPY',
      })
      expect(input.CacheControl).toBeUndefined()
      expect(input.ContentType).toBeUndefined()
      expect(input.Metadata).toBeUndefined()
    })
  })

  describe('uploadObject', () => {
    test('uses PutObject for known-size uploads within the single-request limit', async () => {
      mockSend.mockResolvedValue({
        ETag: '"put-etag"',
        $metadata: {
          httpStatusCode: 200,
        },
      })

      const backend = createBackend()
      const result = await backend.uploadObject(
        'test-bucket',
        'test-key',
        undefined,
        Readable.from(['hello']),
        'text/plain',
        'max-age=60',
        undefined,
        5
      )

      expect(mockSend).toHaveBeenCalledTimes(1)
      expect(mockSend.mock.calls[0][0]).toBeInstanceOf(PutObjectCommand)
      expect(mockSend.mock.calls[0][0].input).toMatchObject({
        Bucket: 'test-bucket',
        Key: 'test-key',
        ContentType: 'text/plain',
        CacheControl: 'max-age=60',
        ContentLength: 5,
      })
      expect(Upload).not.toHaveBeenCalled()
      expect(result).toMatchObject({
        httpStatusCode: 200,
        cacheControl: 'max-age=60',
        eTag: '"put-etag"',
        mimetype: 'text/plain',
        contentLength: 5,
        size: 5,
      })
    })

    test('uses PutObject for zero-byte uploads when content length is known', async () => {
      mockSend.mockResolvedValue({
        ETag: '"empty-etag"',
        $metadata: {
          httpStatusCode: 200,
        },
      })

      const backend = createBackend()
      const result = await backend.uploadObject(
        'test-bucket',
        'empty-key',
        undefined,
        Readable.from([]),
        'application/octet-stream',
        'no-cache',
        undefined,
        0
      )

      expect(mockSend).toHaveBeenCalledTimes(1)
      expect(mockSend.mock.calls[0][0]).toBeInstanceOf(PutObjectCommand)
      expect(mockSend.mock.calls[0][0].input).toMatchObject({
        Bucket: 'test-bucket',
        Key: 'empty-key',
        ContentType: 'application/octet-stream',
        CacheControl: 'no-cache',
        ContentLength: 0,
      })
      expect(Upload).not.toHaveBeenCalled()
      expect(result).toMatchObject({
        httpStatusCode: 200,
        cacheControl: 'no-cache',
        eTag: '"empty-etag"',
        mimetype: 'application/octet-stream',
        contentLength: 0,
        size: 0,
      })
    })

    test('uses source stream bytes for over-limit multipart upload metadata', async () => {
      const overLimit = MAX_PUT_OBJECT_SIZE + 1

      // Emit a progress value that disagrees with both the declared length and the
      // request body; metadata should use the bytes read from the source stream.
      mockUploadDone.mockImplementationOnce(async (instance) => {
        instance.emit('httpUploadProgress', { loaded: 1 })
        await drainUploadBody(instance)
        return {
          ETag: '"multipart-etag"',
          $metadata: {
            httpStatusCode: 200,
          },
        }
      })

      const backend = createBackend()
      const result = await backend.uploadObject(
        'test-bucket',
        'test-key',
        undefined,
        Readable.from(['hello']),
        'text/plain',
        'max-age=60',
        undefined,
        overLimit
      )

      expect(Upload).toHaveBeenCalledTimes(1)
      expect(uploadInstances[0].options.queueSize).toBe(getConfig().storageS3UploadQueueSize)
      expect(mockSend).not.toHaveBeenCalled()
      expect(result).toMatchObject({
        httpStatusCode: 200,
        cacheControl: 'max-age=60',
        eTag: '"multipart-etag"',
        mimetype: 'text/plain',
        contentLength: 5,
        size: 5,
        lastModified: expect.any(Date),
      })
    })

    test('uses source stream bytes for over-limit multipart upload without progress', async () => {
      const overLimit = MAX_PUT_OBJECT_SIZE + 1

      const backend = createBackend()
      const result = await backend.uploadObject(
        'test-bucket',
        'test-key',
        undefined,
        Readable.from(['hello']),
        'text/plain',
        'max-age=60',
        undefined,
        overLimit
      )

      expect(Upload).toHaveBeenCalledTimes(1)
      expect(uploadInstances[0].options.queueSize).toBe(getConfig().storageS3UploadQueueSize)
      expect(mockSend).not.toHaveBeenCalled()
      expect(result).toMatchObject({
        httpStatusCode: 200,
        cacheControl: 'max-age=60',
        eTag: '"multipart-etag"',
        mimetype: 'text/plain',
        contentLength: 5,
        size: 5,
        lastModified: expect.any(Date),
      })
    })

    test('uses multipart upload when content length is unknown', async () => {
      mockUploadDone.mockImplementationOnce(async (instance) => {
        instance.emit('httpUploadProgress', { loaded: 42 })
        await drainUploadBody(instance)
        return {
          ETag: '"multipart-etag"',
          $metadata: {
            httpStatusCode: 200,
          },
        }
      })

      const backend = createBackend()
      const result = await backend.uploadObject(
        'test-bucket',
        'test-key',
        undefined,
        Readable.from(['hello']),
        'text/plain',
        'max-age=60'
      )

      expect(Upload).toHaveBeenCalledTimes(1)
      expect(uploadInstances[0].options.queueSize).toBe(getConfig().storageS3UploadQueueSize)
      expect(mockSend).not.toHaveBeenCalled()
      expect(result).toMatchObject({
        httpStatusCode: 200,
        cacheControl: 'max-age=60',
        eTag: '"multipart-etag"',
        mimetype: 'text/plain',
        contentLength: 5,
        size: 5,
        lastModified: expect.any(Date),
      })
    })

    test('removes multipart success listeners after upload completes', async () => {
      const abortController = new AbortController()
      const body = Readable.from(['hello'])

      const backend = createBackend()
      await backend.uploadObject(
        'test-bucket',
        'test-key',
        undefined,
        body,
        'text/plain',
        'max-age=60',
        abortController.signal
      )

      expect(body.listenerCount('error')).toBe(0)

      abortController.abort()
      expect(uploadInstances[0].abort).not.toHaveBeenCalled()
    })

    test('aborts multipart upload when the source stream errors after emitting bytes', async () => {
      const sourceError = ERRORS.InvalidRequest('Incomplete trailer section')
      const body = new Readable({
        read() {
          this.push(Buffer.from('hello'))
          this.destroy(sourceError)
        },
      })

      mockUploadDone.mockImplementationOnce((instance) => {
        void drainUploadBody(instance).catch(() => undefined)

        return new Promise((_resolve, reject) => {
          instance.abort.mockImplementation(() => reject(sourceError))
        })
      })

      const backend = createBackend()
      const upload = backend.uploadObject(
        'test-bucket',
        'test-key',
        undefined,
        body,
        'text/plain',
        'max-age=60'
      )
      const uploadError = upload.catch((error: unknown) => error)

      await vi.waitFor(() => {
        expect(uploadInstances[0].abort).toHaveBeenCalledTimes(1)
      })
      await expect(uploadError).resolves.toMatchObject({
        code: ErrorCode.InvalidRequest,
        message: 'Incomplete trailer section',
      })
    })

    test('rejects an already-errored multipart source stream without starting upload', async () => {
      const sourceError = ERRORS.InvalidRequest('Incomplete trailer section')
      const body = new PassThrough()
      body.on('error', () => undefined)
      body.write(Buffer.from('hello'))
      body.destroy(sourceError)

      const backend = createBackend()
      const upload = backend
        .uploadObject('test-bucket', 'test-key', undefined, body, 'text/plain', 'max-age=60')
        .catch((error: unknown) => error)

      await expect(upload).resolves.toMatchObject({
        code: ErrorCode.InvalidRequest,
        message: 'Incomplete trailer section',
      })
      expect(Upload).not.toHaveBeenCalled()
    })

    test('returns zero-byte metadata for unknown-size uploads without progress', async () => {
      const backend = createBackend()
      const result = await backend.uploadObject(
        'test-bucket',
        'empty-key',
        undefined,
        Readable.from([]),
        'application/octet-stream',
        'no-cache'
      )

      expect(Upload).toHaveBeenCalledTimes(1)
      expect(uploadInstances[0].options.queueSize).toBe(getConfig().storageS3UploadQueueSize)
      expect(mockSend).not.toHaveBeenCalled()
      expect(result).toMatchObject({
        httpStatusCode: 200,
        cacheControl: 'no-cache',
        eTag: '"multipart-etag"',
        mimetype: 'application/octet-stream',
        contentLength: 0,
        size: 0,
        lastModified: expect.any(Date),
      })
    })

    test('maps PutObject abort errors to AbortedTerminate', async () => {
      mockSend.mockRejectedValueOnce(Object.assign(new Error('aborted'), { name: 'AbortError' }))

      const backend = createBackend()

      try {
        await backend.uploadObject(
          'test-bucket',
          'test-key',
          undefined,
          Readable.from(['hello']),
          'text/plain',
          'max-age=60',
          undefined,
          5
        )
        throw new Error('Expected uploadObject to throw')
      } catch (error) {
        expect(isStorageError(ErrorCode.AbortedTerminate, error)).toBe(true)
        expect((error as Error).message).toBe('Upload was aborted')
      }
    })
  })
})
