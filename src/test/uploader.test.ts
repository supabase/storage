import { once } from 'events'
import { FastifyRequest } from 'fastify'
import { PassThrough, Readable } from 'stream'
import { ErrorCode, isStorageError, StorageBackendError } from '../internal/errors'
import { ObjectAdminDelete } from '../storage/events'
import { TenantLocation } from '../storage/locator'
import { fileUploadFromRequest, Uploader } from '../storage/uploader'

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
    const raw = new PassThrough()

    try {
      await fileUploadFromRequest(
        {
          headers: {
            'content-type': 'application/octet-stream',
            'content-length': '177',
          },
          raw,
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
      expect(raw.listenerCount('aborted')).toBe(0)
      expect(raw.listenerCount('close')).toBe(0)
      expect(raw.listenerCount('end')).toBe(0)
      expect(raw.listenerCount('error')).toBe(0)
      expect(raw.readableFlowing).not.toBe(true)
    }
  })

  test('wraps binary request bodies so downstream stream failures do not destroy the raw request', async () => {
    const raw = new PassThrough()
    const upload = await fileUploadFromRequest(
      {
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '7',
        },
        raw,
        tenantId: 'stub-tenant',
      } as unknown as FastifyRequest,
      {
        objectName: 'test.txt',
        fileSizeLimit: 150,
      }
    )

    expect(upload.body).not.toBe(raw)

    const proxyError = once(upload.body, 'error')
    upload.body.destroy(new Error('downstream failed'))

    const [error] = await proxyError
    expect((error as Error).message).toBe('downstream failed')
    expect(raw.destroyed).toBe(false)
  })

  test('cleans up raw request listeners after a successful proxied upload stream completes', async () => {
    const raw = new PassThrough()
    const upload = await fileUploadFromRequest(
      {
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '7',
        },
        raw,
        tenantId: 'stub-tenant',
      } as unknown as FastifyRequest,
      {
        objectName: 'test.txt',
        fileSizeLimit: 150,
      }
    )

    const proxyClosed = once(upload.body, 'close')
    upload.body.resume()
    raw.end('payload')
    await proxyClosed

    expect(raw.listenerCount('aborted')).toBe(0)
    expect(raw.listenerCount('close')).toBe(0)
    expect(raw.listenerCount('end')).toBe(0)
    expect(raw.listenerCount('error')).toBe(0)
  })

  test('propagates raw request stream errors to the upload body proxy', async () => {
    const raw = new PassThrough()
    const upload = await fileUploadFromRequest(
      {
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '7',
        },
        raw,
        tenantId: 'stub-tenant',
      } as unknown as FastifyRequest,
      {
        objectName: 'test.txt',
        fileSizeLimit: 150,
      }
    )

    const proxyError = once(upload.body, 'error')
    const requestError = new Error('request stream failed')
    raw.destroy(requestError)

    const [error] = await proxyError
    expect(error).toBe(requestError)
    expect(upload.body.destroyed).toBe(true)
  })

  test('destroys the upload body proxy when the raw request closes without EOF', async () => {
    const raw = new PassThrough()
    const upload = await fileUploadFromRequest(
      {
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '7',
        },
        raw,
        tenantId: 'stub-tenant',
      } as unknown as FastifyRequest,
      {
        objectName: 'test.txt',
        fileSizeLimit: 150,
      }
    )

    const proxyError = once(upload.body, 'error')
    raw.destroy()

    const [error] = await proxyError
    expect((error as Error).message).toBe('Request stream closed before upload could complete')
    expect(upload.body.destroyed).toBe(true)
  })

  test('rejects binary uploads when the raw request stream is already closed', async () => {
    const raw = new PassThrough()
    raw.destroy()

    try {
      await fileUploadFromRequest(
        {
          headers: {
            'content-type': 'application/octet-stream',
            'content-length': '7',
          },
          raw,
          tenantId: 'stub-tenant',
        } as unknown as FastifyRequest,
        {
          objectName: 'test.txt',
          fileSizeLimit: 150,
        }
      )
      throw new Error('Expected fileUploadFromRequest to throw')
    } catch (error) {
      expect(isStorageError(ErrorCode.InvalidRequest, error)).toBe(true)
      expect((error as Error).message).toBe('Request stream closed before upload could begin')
    }
  })

  test('marks proxied upload failures to close the client connection after the response', async () => {
    const raw = new PassThrough()
    const file = await fileUploadFromRequest(
      {
        headers: {
          'content-type': 'application/octet-stream',
          'content-length': '7',
        },
        raw,
        tenantId: 'stub-tenant',
      } as unknown as FastifyRequest,
      {
        objectName: 'test.txt',
        fileSizeLimit: 150,
      }
    )

    const objectAdminDeleteSendSpy = jest
      .spyOn(ObjectAdminDelete, 'send')
      .mockResolvedValue(undefined)

    const uploader = new Uploader(
      {
        uploadObject: jest.fn(async (_bucket, _key, _version, body: Readable) => {
          body.destroy(new Error('stream pipeline failed'))
          throw StorageBackendError.fromError(new Error('socket hang up'))
        }),
      } as any,
      {
        tenantId: 'stub-tenant',
        reqId: 'req-1',
        tenant: () => ({ ref: 'stub-tenant' }),
        testPermission: jest.fn().mockResolvedValue(undefined),
      } as any,
      new TenantLocation('test-bucket')
    )

    try {
      await uploader.upload({
        bucketId: 'bucket',
        objectName: 'test.txt',
        file,
        uploadType: 'standard',
      })
      throw new Error('Expected upload to throw')
    } catch (error) {
      expect(error).toBeInstanceOf(StorageBackendError)
      expect((error as StorageBackendError).shouldCloseConnection()).toBe(true)
      expect((error as StorageBackendError).message).toBe('socket hang up')
    } finally {
      objectAdminDeleteSendSpy.mockRestore()
    }
  })
})
