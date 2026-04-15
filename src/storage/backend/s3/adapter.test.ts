'use strict'

import { HeadObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { Upload } from '@aws-sdk/lib-storage'
import { ErrorCode, isStorageError } from '@internal/errors'
import { Readable } from 'stream'
import { type Mock, vi } from 'vitest'
import { getConfig } from '../../../config'
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

type UploadOptionsShape = {
  queueSize?: number
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

  beforeEach(() => {
    vi.clearAllMocks()
    mockSend = vi.fn()
    mockUploadDone = vi.fn().mockResolvedValue({
      ETag: '"multipart-etag"',
      $metadata: {
        httpStatusCode: 200,
      },
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

    test('falls back to multipart upload when content length exceeds the single-request limit', async () => {
      const overLimit = MAX_PUT_OBJECT_SIZE + 1
      const lastModified = new Date('2024-01-01T00:00:00.000Z')

      mockUploadDone.mockImplementationOnce(async (instance) => {
        instance.emit('httpUploadProgress', { loaded: 1 })
        return {
          ETag: '"multipart-etag"',
          $metadata: {
            httpStatusCode: 200,
          },
        }
      })
      mockSend.mockResolvedValueOnce({
        CacheControl: 'max-age=60',
        ContentType: 'text/plain',
        ContentLength: overLimit,
        ETag: '"head-etag"',
        LastModified: lastModified,
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
        overLimit
      )

      expect(Upload).toHaveBeenCalledTimes(1)
      expect(uploadInstances[0].options.queueSize).toBe(getConfig().storageS3UploadQueueSize)
      expect(mockSend).toHaveBeenCalledTimes(1)
      expect(mockSend.mock.calls[0][0]).toBeInstanceOf(HeadObjectCommand)
      expect(result).toMatchObject({
        httpStatusCode: 200,
        cacheControl: 'max-age=60',
        eTag: '"head-etag"',
        mimetype: 'text/plain',
        contentLength: overLimit,
        size: overLimit,
        lastModified,
      })
    })

    test('uses multipart upload when content length is unknown', async () => {
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
        contentLength: 0,
        size: 0,
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
