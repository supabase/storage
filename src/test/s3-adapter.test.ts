'use strict'

import { S3Backend } from '../storage/backend/s3/adapter'
import { S3Client } from '@aws-sdk/client-s3'
import { Readable } from 'stream'

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
})
