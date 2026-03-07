'use strict'

import { S3Client } from '@aws-sdk/client-s3'
import { Readable } from 'stream'
import { S3Backend } from '../storage/backend/s3/adapter'

jest.mock('@aws-sdk/client-s3', () => {
  const originalModule = jest.requireActual('@aws-sdk/client-s3')
  return {
    ...originalModule,
    S3Client: jest.fn().mockImplementation(() => ({
      send: jest.fn(),
    })),
  }
})

describe('S3Backend', () => {
  let mockSend: jest.Mock

  beforeEach(() => {
    jest.clearAllMocks()
    mockSend = jest.fn()
      ; (S3Client as jest.Mock).mockImplementation(() => ({
        send: mockSend,
      }))
  })

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

      const backend = new S3Backend({
        region: 'us-east-1',
        endpoint: 'http://localhost:9000',
      })

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

      const backend = new S3Backend({
        region: 'us-east-1',
        endpoint: 'http://localhost:9000',
      })

      const result = await backend.getObject('test-bucket', 'test-key', undefined)

      expect(result.metadata.mimetype).toBe('image/png')
    })
  })

  describe('deleteObjects', () => {
    test('should use batch DeleteObjectsCommand when backend supports it', async () => {
      mockSend.mockResolvedValue({
        Deleted: [{ Key: 'file1.txt' }, { Key: 'file2.txt' }],
        $metadata: { httpStatusCode: 200 },
      })

      const backend = new S3Backend({ region: 'us-east-1', endpoint: 'http://localhost:9000' })
      await backend.deleteObjects('test-bucket', ['file1.txt', 'file2.txt'])

      expect(mockSend).toHaveBeenCalledTimes(1)
      expect(mockSend.mock.calls[0][0].constructor.name).toBe('DeleteObjectsCommand')
    })

    test('should fall back to individual DeleteObjectCommands when backend returns NotImplemented', async () => {
      const err = Object.assign(new Error('NotImplemented'), { Code: 'NotImplemented' })
      mockSend
        .mockRejectedValueOnce(err)
        .mockResolvedValue({ $metadata: { httpStatusCode: 204 } })

      const backend = new S3Backend({ region: 'us-east-1', endpoint: 'http://localhost:9000' })
      await backend.deleteObjects('test-bucket', ['file1.txt', 'file2.txt'])

      expect(mockSend).toHaveBeenCalledTimes(3)
      expect(mockSend.mock.calls[0][0].constructor.name).toBe('DeleteObjectsCommand')
      expect(mockSend.mock.calls[1][0].constructor.name).toBe('DeleteObjectCommand')
      expect(mockSend.mock.calls[2][0].constructor.name).toBe('DeleteObjectCommand')
    })

    test('should ignore NoSuchKey errors in the individual fallback', async () => {
      const notImplemented = Object.assign(new Error('NotImplemented'), { Code: 'NotImplemented' })
      const noSuchKey = Object.assign(new Error('NoSuchKey'), { Code: 'NoSuchKey' })
      mockSend
        .mockRejectedValueOnce(notImplemented)
        .mockResolvedValueOnce({ $metadata: { httpStatusCode: 204 } })
        .mockRejectedValueOnce(noSuchKey)

      const backend = new S3Backend({ region: 'us-east-1', endpoint: 'http://localhost:9000' })
      await expect(
        backend.deleteObjects('test-bucket', ['file1.txt', 'file2.txt'])
      ).resolves.toBeUndefined()
    })

    test('should throw when an individual fallback delete fails with a real error', async () => {
      const notImplemented = Object.assign(new Error('NotImplemented'), { Code: 'NotImplemented' })
      const accessDenied = Object.assign(new Error('AccessDenied'), { Code: 'AccessDenied' })
      mockSend
        .mockRejectedValueOnce(notImplemented)
        .mockResolvedValueOnce({ $metadata: { httpStatusCode: 204 } })
        .mockRejectedValueOnce(accessDenied)

      const backend = new S3Backend({ region: 'us-east-1', endpoint: 'http://localhost:9000' })
      await expect(
        backend.deleteObjects('test-bucket', ['file1.txt', 'file2.txt'])
      ).rejects.toThrow()
    })

    test('should rethrow errors that are not NotImplemented', async () => {
      const err = Object.assign(new Error('AccessDenied'), { Code: 'AccessDenied' })
      mockSend.mockRejectedValue(err)

      const backend = new S3Backend({ region: 'us-east-1', endpoint: 'http://localhost:9000' })
      await expect(backend.deleteObjects('test-bucket', ['file1.txt'])).rejects.toThrow()
      expect(mockSend).toHaveBeenCalledTimes(1)
    })
  })
})
