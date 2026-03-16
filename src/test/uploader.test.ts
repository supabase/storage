import { FastifyRequest } from 'fastify'
import { Readable } from 'stream'
import { ErrorCode, isStorageError } from '../internal/errors'
import { fileUploadFromRequest } from '../storage/uploader'

describe('fileUploadFromRequest', () => {
  test('keeps multipart/form-data file size undefined even when the request content-length exceeds 5GB', async () => {
    const file = Readable.from(['payload']) as Readable & { truncated: boolean }
    file.truncated = false

    const requestFile = jest.fn().mockResolvedValue({
      file,
      fields: {
        cacheControl: { value: '3600' },
        contentType: { value: 'image/png' },
        metadata: { value: '{"source":"multipart"}' },
      },
      mimetype: 'application/octet-stream',
    })

    const upload = await fileUploadFromRequest(
      {
        headers: {
          'content-type': 'multipart/form-data; boundary=abc123',
          'content-length': String(5 * 1024 * 1024 * 1024 + 512),
        },
        file: requestFile,
        tenantId: 'stub-tenant',
      } as unknown as FastifyRequest,
      {
        objectName: 'test.txt',
        fileSizeLimit: 150,
      }
    )

    expect(requestFile).toHaveBeenCalledWith({ limits: { fileSize: 150 } })
    expect(upload.body).toBe(file)
    expect(upload.contentLength).toBeUndefined()
    expect(upload.mimeType).toBe('image/png')
    expect(upload.cacheControl).toBe('max-age=3600')
    expect(upload.userMetadata).toEqual({ source: 'multipart' })
    expect(upload.isTruncated()).toBe(false)

    file.truncated = true
    expect(upload.isTruncated()).toBe(true)
  })

  test('prefers x-amz-decoded-content-length for aws-chunked truncation checks', async () => {
    const upload = await fileUploadFromRequest(
      {
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '177',
          'x-amz-decoded-content-length': '123',
        },
        raw: Readable.from(['payload']),
        streamingSignatureV4: {} as FastifyRequest['streamingSignatureV4'],
        tenantId: 'stub-tenant',
      } as unknown as FastifyRequest,
      {
        objectName: 'test.txt',
        fileSizeLimit: 150,
      }
    )

    expect(upload.isTruncated()).toBe(false)
  })

  test('ignores x-amz-decoded-content-length outside aws-chunked S3 uploads and rejects oversized bodies', async () => {
    try {
      await fileUploadFromRequest(
        {
          headers: {
            'content-type': 'application/octet-stream',
            'content-length': '177',
            'x-amz-decoded-content-length': '123',
          },
          raw: Readable.from(['payload']),
          tenantId: 'stub-tenant',
        } as unknown as FastifyRequest,
        {
          objectName: 'test.txt',
          fileSizeLimit: 150,
        }
      )
      throw new Error('Expected fileUploadFromRequest to throw')
    } catch (error) {
      expect(isStorageError(ErrorCode.EntityTooLarge, error)).toBe(true)
    }
  })

  test('rejects known-size binary uploads that already exceed the size limit', async () => {
    try {
      await fileUploadFromRequest(
        {
          headers: {
            'content-type': 'application/octet-stream',
            'content-length': '177',
          },
          raw: Readable.from(['payload']),
          tenantId: 'stub-tenant',
        } as unknown as FastifyRequest,
        {
          objectName: 'test.txt',
          fileSizeLimit: 150,
        }
      )
      throw new Error('Expected fileUploadFromRequest to throw')
    } catch (error) {
      expect(isStorageError(ErrorCode.EntityTooLarge, error)).toBe(true)
    }
  })
})
