'use strict'

import { CopyObjectCommand, S3Client, UploadPartCopyCommand } from '@aws-sdk/client-s3'
import { Readable } from 'stream'
import { S3Backend } from '../storage/backend/s3/adapter'
import { encodeBucketAndObjectPathForTest } from './utils/path-encoding'

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
    ;(S3Client as jest.Mock).mockImplementation(() => ({
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

  describe('copyObject', () => {
    test('should preserve "/" and encode path tokens in CopySource', async () => {
      const lastModified = new Date('2024-01-01T00:00:00.000Z')
      mockSend.mockResolvedValue({
        CopyObjectResult: {
          ETag: '"copy-etag"',
          LastModified: lastModified,
        },
        $metadata: {
          httpStatusCode: 200,
        },
      })

      const backend = new S3Backend({
        region: 'us-east-1',
        endpoint: 'http://localhost:9000',
      })

      const sourceKey = 'source path/일이삼-🙂?#%.jpg'
      const destinationKey = 'dest/path/copied-🙂.jpg'

      await backend.copyObject('test-bucket', sourceKey, undefined, destinationKey, undefined)

      expect(mockSend).toHaveBeenCalledTimes(1)
      const command = mockSend.mock.calls[0][0] as CopyObjectCommand
      expect(command).toBeInstanceOf(CopyObjectCommand)
      expect(command.input.CopySource).toBe(
        encodeBucketAndObjectPathForTest('test-bucket', sourceKey)
      )
      expect(command.input.CopySource).toContain('test-bucket/source%20path/')
      expect(command.input.CopySource).not.toContain('test-bucket%2F')
    })
  })

  describe('uploadPartCopy', () => {
    test('should preserve "/" and encode path tokens in CopySource for unicode source keys', async () => {
      const lastModified = new Date('2024-01-01T00:00:00.000Z')
      mockSend.mockResolvedValue({
        CopyPartResult: {
          ETag: '"copy-etag"',
          LastModified: lastModified,
        },
      })

      const backend = new S3Backend({
        region: 'us-east-1',
        endpoint: 'http://localhost:9000',
      })

      const sourceKey = 'source path/folder/일이삼-🙂?#%.jpg'
      const destinationKey = 'dest/path/copied-🙂.jpg'

      const result = await backend.uploadPartCopy(
        'test-bucket',
        destinationKey,
        '',
        'upload-id',
        1,
        sourceKey,
        undefined,
        { fromByte: 0, toByte: 1024 }
      )

      expect(mockSend).toHaveBeenCalledTimes(1)
      const command = mockSend.mock.calls[0][0] as UploadPartCopyCommand
      expect(command).toBeInstanceOf(UploadPartCopyCommand)
      expect(command.input.CopySource).toBe(
        encodeBucketAndObjectPathForTest('test-bucket', sourceKey)
      )
      expect(command.input.CopySource).toContain('test-bucket/source%20path/folder/')
      expect(command.input.CopySource).not.toContain('test-bucket%2F')
      expect(command.input.CopySourceRange).toBe('bytes=0-1024')
      expect(result).toEqual({
        eTag: '"copy-etag"',
        lastModified,
      })
    })
  })
})
