import { FastifyRequest } from 'fastify'
import { Readable } from 'stream'
import { fileUploadFromRequest } from '../storage/uploader'

describe('fileUploadFromRequest', () => {
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

  test('ignores x-amz-decoded-content-length outside aws-chunked S3 uploads', async () => {
    const upload = await fileUploadFromRequest(
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

    expect(upload.isTruncated()).toBe(true)
  })
})
