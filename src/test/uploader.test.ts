import { once } from 'events'
import { FastifyRequest } from 'fastify'
import { PassThrough, Readable } from 'stream'
import { ErrorCode, isStorageError, StorageBackendError } from '../internal/errors'
import * as monitoringMetrics from '../internal/monitoring/metrics'
import { ObjectAdminDelete, ObjectCreatedPostEvent } from '../storage/events'
import { TenantLocation } from '../storage/locator'
import { fileUploadFromRequest, Uploader } from '../storage/uploader'

type UploaderBackend = ConstructorParameters<typeof Uploader>[0]
type UploaderDatabase = ConstructorParameters<typeof Uploader>[1]
type CompleteUploadResult = Awaited<ReturnType<Uploader['completeUpload']>>

function createUploader(
  backend: Partial<UploaderBackend> & Pick<UploaderBackend, 'uploadObject'>,
  db: Partial<UploaderDatabase> &
    Pick<UploaderDatabase, 'tenantId' | 'reqId' | 'tenant' | 'testPermission'>
) {
  return new Uploader(
    backend as UploaderBackend,
    db as UploaderDatabase,
    new TenantLocation('test-bucket')
  )
}

function createUploaderDb(overrides: Partial<UploaderDatabase> = {}) {
  const db = {
    tenantId: 'stub-tenant',
    reqId: 'req-1',
    sbReqId: 'sb-req-1',
    tenant: () => ({ ref: 'stub-tenant', host: 'stub-tenant.local' }),
    testPermission: vi.fn(async () => undefined),
    hasMigration: vi.fn().mockResolvedValue(false),
    ...overrides,
  } as Partial<UploaderDatabase> &
    Pick<UploaderDatabase, 'tenantId' | 'reqId' | 'tenant' | 'testPermission'>

  return db
}

describe('fileUploadFromRequest', () => {
  test('keeps multipart/form-data file size undefined even when the request content-length exceeds 5GB', async () => {
    const file = Readable.from(['payload']) as Readable & { truncated: boolean }
    file.truncated = false

    const requestFile = vi.fn().mockResolvedValue({
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
    expect(upload.declaredContentLength).toBe(5 * 1024 * 1024 * 1024 + 512)
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

    expect(upload.contentLength).toBeUndefined()
    expect(upload.declaredContentLength).toBe(123)
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
    expect(upload.contentLength).toBeUndefined()
    expect(upload.declaredContentLength).toBe(7)

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

    const objectAdminDeleteSendSpy = vi
      .spyOn(ObjectAdminDelete, 'send')
      .mockResolvedValue(undefined)

    const uploader = createUploader(
      {
        uploadObject: vi.fn(async (_bucket, _key, _version, body: Readable) => {
          body.destroy(new Error('stream pipeline failed'))
          throw StorageBackendError.fromError(new Error('socket hang up'))
        }),
      },
      {
        tenantId: 'stub-tenant',
        reqId: 'req-1',
        tenant: () => ({ ref: 'stub-tenant', host: 'stub-tenant.local' }),
        testPermission: vi.fn().mockResolvedValue(undefined),
        hasMigration: vi.fn().mockResolvedValue(false),
      }
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

  test('keeps declared request size for permission checks but omits backend size for request-backed uploads', async () => {
    const capturedWrites: Array<{ metadata?: { contentLength?: number } }> = []
    const backend = {
      uploadObject: vi.fn().mockResolvedValue({
        httpStatusCode: 200,
        cacheControl: 'no-cache',
        eTag: '"etag"',
        mimetype: 'text/plain',
        contentLength: 7,
        lastModified: new Date(),
        size: 7,
        contentRange: undefined,
      }),
    }
    const uploader = createUploader(backend, {
      tenantId: 'stub-tenant',
      reqId: 'req-1',
      tenant: () => ({ ref: 'stub-tenant', host: 'stub-tenant.local' }),
      hasMigration: vi.fn().mockResolvedValue(false),
      testPermission: vi.fn(async (fn) =>
        fn({
          createObject: vi.fn(async (payload: { metadata?: { contentLength?: number } }) => {
            capturedWrites.push(payload)
          }),
          upsertObject: vi.fn(async (payload: { metadata?: { contentLength?: number } }) => {
            capturedWrites.push(payload)
          }),
        })
      ),
    })
    const completeUploadSpy = vi.spyOn(uploader, 'completeUpload').mockResolvedValue({
      metadata: { eTag: '"etag"' },
      obj: { id: 'obj-id' },
    } as CompleteUploadResult)

    await uploader.upload({
      bucketId: 'bucket',
      objectName: 'test.txt',
      uploadType: 'standard',
      file: {
        body: Readable.from(['payload']),
        mimeType: 'text/plain',
        cacheControl: 'no-cache',
        declaredContentLength: 7,
        isTruncated: () => false,
      },
    })

    expect(capturedWrites[0]?.metadata?.contentLength).toBe(7)
    expect(backend.uploadObject).toHaveBeenCalledTimes(1)
    expect(backend.uploadObject.mock.calls[0][7]).toBeUndefined()

    completeUploadSpy.mockRestore()
  })
})

describe('Uploader metrics', () => {
  test('non-upsert permission checks use the real versioned write for a current delete marker', async () => {
    const createObject = vi.fn().mockResolvedValue(undefined)
    const upsertObject = vi.fn().mockResolvedValue(undefined)
    const db = createUploaderDb({
      hasMigration: vi.fn().mockResolvedValue(true),
      asSuperUser: vi.fn().mockReturnValue({
        findObject: vi.fn().mockResolvedValue({ is_delete_marker: true }),
      }),
      testPermission: vi.fn(async (fn) => fn({ createObject, upsertObject } as never)),
    })
    const uploader = createUploader({ uploadObject: vi.fn() }, db)

    await uploader.canUpload({
      bucketId: 'bucket',
      objectName: 'deleted.txt',
      owner: undefined,
      isUpsert: false,
      userMetadata: undefined,
      metadata: undefined,
    })

    expect(upsertObject).toHaveBeenCalledWith(
      expect.objectContaining({ bucket_id: 'bucket', name: 'deleted.txt' })
    )
    expect(createObject).not.toHaveBeenCalled()
  })

  test('prepareUpload records upload start attributes without tenant id labels', async () => {
    const recordSpy = vi.spyOn(monitoringMetrics, 'recordUploadStarted')
    const uploader = createUploader(
      {
        uploadObject: vi.fn(),
      },
      createUploaderDb()
    )

    try {
      await uploader.prepareUpload({
        bucketId: 'bucket',
        objectName: 'test.txt',
        owner: undefined,
        isUpsert: false,
        userMetadata: undefined,
        metadata: undefined,
        uploadType: 'standard',
      })

      expect(recordSpy).toHaveBeenCalledWith('standard')
    } finally {
      recordSpy.mockRestore()
    }
  })

  test('completeUpload records upload success attributes without tenant id labels', async () => {
    const recordSpy = vi.spyOn(monitoringMetrics, 'recordUploadSuccess')
    const sendWebhookSpy = vi
      .spyOn(ObjectCreatedPostEvent, 'sendWebhook')
      .mockResolvedValue(undefined)
    const transactionDb = {
      waitObjectLock: vi.fn().mockResolvedValue(undefined),
      findObject: vi.fn().mockResolvedValue(undefined),
      upsertObject: vi.fn().mockResolvedValue({ id: 'object-id' }),
    }
    const db = createUploaderDb({
      asSuperUser: vi.fn().mockReturnValue({
        connection: {
          setAbortSignal: vi.fn(),
        },
        withTransaction: vi.fn(async (fn) => fn(transactionDb)),
      }),
    })
    const uploader = createUploader(
      {
        uploadObject: vi.fn(),
      },
      db
    )

    try {
      await uploader.completeUpload({
        version: 'version-1',
        bucketId: 'bucket',
        objectName: 'test.txt',
        owner: undefined,
        objectMetadata: {
          eTag: '"etag"',
          mimetype: 'text/plain',
          cacheControl: 'max-age=3600',
          lastModified: new Date(),
          contentLength: 7,
          httpStatusCode: 200,
          size: 7,
        },
        uploadType: 'standard',
        isUpsert: false,
        userMetadata: undefined,
      })

      expect(recordSpy).toHaveBeenCalledWith('standard')
    } finally {
      recordSpy.mockRestore()
      sendWebhookSpy.mockRestore()
    }
  })

  test('completeUpload allows a non-upsert upload over a current delete marker', async () => {
    const sendWebhookSpy = vi
      .spyOn(ObjectCreatedPostEvent, 'sendWebhook')
      .mockResolvedValue(undefined)
    const transactionDb = {
      waitObjectLock: vi.fn().mockResolvedValue(undefined),
      findObject: vi.fn().mockResolvedValue({
        id: 'marker-id',
        version: 'marker-version',
        is_delete_marker: true,
        is_versioned: true,
      }),
      upsertObject: vi.fn().mockResolvedValue({ id: 'new-object-id', is_versioned: true }),
    }
    const uploader = createUploader(
      { uploadObject: vi.fn() },
      createUploaderDb({
        asSuperUser: vi.fn().mockReturnValue({
          connection: { setAbortSignal: vi.fn() },
          withTransaction: vi.fn(async (fn) => fn(transactionDb)),
        }),
      })
    )

    try {
      await expect(
        uploader.completeUpload({
          version: 'new-version',
          bucketId: 'bucket',
          objectName: 'deleted.txt',
          owner: undefined,
          objectMetadata: {
            eTag: 'etag',
            mimetype: 'text/plain',
            cacheControl: 'no-cache',
            lastModified: new Date(),
            contentLength: 1,
            httpStatusCode: 200,
            size: 1,
          },
          uploadType: 'standard',
          isUpsert: false,
          userMetadata: undefined,
        })
      ).resolves.toMatchObject({ obj: { id: 'new-object-id' } })
      expect(transactionDb.upsertObject).toHaveBeenCalledOnce()
    } finally {
      sendWebhookSpy.mockRestore()
    }
  })

  test.each([
    ['ENABLED', true, true, 0],
    ['DISABLED or SUSPENDED null-version replacement', false, false, 1],
    ['SUSPENDED with an enabled current version', true, false, 0],
  ] as const)('completeUpload backend cleanup follows %s write semantics', async (_mode, currentIsVersioned, newIsVersioned, expectedDeletes) => {
    const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send').mockResolvedValue(undefined)
    const sendWebhookSpy = vi
      .spyOn(ObjectCreatedPostEvent, 'sendWebhook')
      .mockResolvedValue(undefined)
    const transactionDb = {
      waitObjectLock: vi.fn().mockResolvedValue(undefined),
      findObject: vi
        .fn()
        .mockResolvedValueOnce({
          id: 'old-object-id',
          version: 'old-version',
          metadata: {},
          is_delete_marker: false,
          is_versioned: currentIsVersioned,
        })
        .mockResolvedValueOnce(
          currentIsVersioned
            ? undefined
            : {
                id: 'old-object-id',
                version: 'old-version',
                is_delete_marker: false,
                is_versioned: false,
              }
        ),
      upsertObject: vi.fn().mockResolvedValue({
        id: 'new-object-id',
        version: 'new-version',
        is_versioned: newIsVersioned,
      }),
    }
    const db = createUploaderDb({
      hasMigration: vi.fn().mockResolvedValue(true),
      asSuperUser: vi.fn().mockReturnValue({
        connection: { setAbortSignal: vi.fn() },
        withTransaction: vi.fn(async (fn) => fn(transactionDb)),
      }),
    })
    const uploader = createUploader({ uploadObject: vi.fn() }, db)

    try {
      await uploader.completeUpload({
        version: 'new-version',
        bucketId: 'bucket',
        objectName: 'test.txt',
        owner: undefined,
        objectMetadata: {
          eTag: 'etag',
          mimetype: 'text/plain',
          cacheControl: 'no-cache',
          lastModified: new Date(),
          contentLength: 1,
          httpStatusCode: 200,
          size: 1,
        },
        uploadType: 'standard',
        isUpsert: true,
        userMetadata: undefined,
      })

      expect(deleteSpy).toHaveBeenCalledTimes(expectedDeletes)
      if (expectedDeletes > 0) {
        expect(deleteSpy).toHaveBeenCalledWith(expect.objectContaining({ version: 'old-version' }))
      }
    } finally {
      deleteSpy.mockRestore()
      sendWebhookSpy.mockRestore()
    }
  })

  test('completeUpload deletes the archived null-version content replaced while suspended', async () => {
    const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send').mockResolvedValue(undefined)
    const sendWebhookSpy = vi
      .spyOn(ObjectCreatedPostEvent, 'sendWebhook')
      .mockResolvedValue(undefined)
    const currentVersion = {
      id: 'enabled-current-id',
      version: 'enabled-current-version',
      is_delete_marker: false,
      is_versioned: true,
    }
    const replaceableVersion = {
      id: 'null-version-id',
      version: 'old-null-version',
      is_delete_marker: false,
      is_versioned: false,
    }
    const transactionDb = {
      waitObjectLock: vi.fn().mockResolvedValue(undefined),
      findObject: vi
        .fn()
        .mockResolvedValueOnce(currentVersion)
        .mockResolvedValueOnce(replaceableVersion),
      upsertObject: vi.fn().mockResolvedValue({
        id: 'null-version-id',
        version: 'new-version',
        is_versioned: false,
      }),
    }
    const uploader = createUploader(
      { uploadObject: vi.fn() },
      createUploaderDb({
        hasMigration: vi.fn().mockResolvedValue(true),
        asSuperUser: vi.fn().mockReturnValue({
          connection: { setAbortSignal: vi.fn() },
          withTransaction: vi.fn(async (fn) => fn(transactionDb)),
        }),
      })
    )

    try {
      await uploader.completeUpload({
        version: 'new-version',
        bucketId: 'bucket',
        objectName: 'test.txt',
        owner: undefined,
        objectMetadata: {
          eTag: 'etag',
          mimetype: 'text/plain',
          cacheControl: 'no-cache',
          lastModified: new Date(),
          contentLength: 1,
          httpStatusCode: 200,
          size: 1,
        },
        uploadType: 'standard',
        isUpsert: true,
        userMetadata: undefined,
      })

      expect(transactionDb.findObject).toHaveBeenNthCalledWith(
        2,
        'bucket',
        'test.txt',
        'id, version, is_delete_marker, is_versioned',
        {
          forUpdate: true,
          dontErrorOnEmpty: true,
          includeNoncurrent: true,
          isVersioned: false,
        }
      )
      expect(deleteSpy).toHaveBeenCalledWith(
        expect.objectContaining({ version: 'old-null-version' })
      )
    } finally {
      deleteSpy.mockRestore()
      sendWebhookSpy.mockRestore()
    }
  })
})
