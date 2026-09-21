import { randomUUID } from 'node:crypto'
import { once } from 'events'
import { type FastifyInstance, FastifyRequest } from 'fastify'
import FormData from 'form-data'
import { PassThrough, Readable } from 'stream'
import buildApp from '../app'
import { getConfig } from '../config'
import { ErrorCode, isStorageError, StorageBackendError } from '../internal/errors'
import * as monitoringMetrics from '../internal/monitoring/metrics'
import { withOptionalVersion } from '../storage/backend'
import { ObjectAdminDelete, ObjectCreatedPostEvent, ObjectCreatedPutEvent } from '../storage/events'
import { TenantLocation } from '../storage/locator'
import { fileUploadFromRequest, Uploader } from '../storage/uploader'
import { useStorage } from './utils/storage'

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

function createCompleteUploadDb(
  superUserDb: Partial<UploaderDatabase>,
  overrides: Partial<UploaderDatabase> = {}
) {
  const permissionDb = {
    createObject: vi.fn().mockResolvedValue(undefined),
    upsertObject: vi.fn().mockResolvedValue(undefined),
  }
  const scopedSuperUserDb = {
    ...superUserDb,
    withTransaction: vi.fn(async (fn: (db: unknown) => unknown) => fn(scopedSuperUserDb)),
  }
  const scopedDb = {
    asSuperUser: vi.fn().mockReturnValue(scopedSuperUserDb),
    testPermission: vi.fn(async (fn) => fn(permissionDb as never)),
  }
  const db = createUploaderDb({
    connection: { setAbortSignal: vi.fn() } as never,
    withTransaction: vi.fn(async (fn) => fn(scopedDb as never)),
    ...overrides,
  })

  return { db, permissionDb, scopedDb }
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

describe('completeUpload replays', () => {
  const objectMetadata = {
    eTag: 'etag',
    mimetype: 'text/plain',
    cacheControl: 'no-cache',
    lastModified: new Date(),
    contentLength: 1,
    httpStatusCode: 200,
    size: 1,
  }
  const request = {
    version: 'version-1',
    bucketId: 'bucket',
    objectName: 'test.txt',
    owner: undefined,
    objectMetadata,
    uploadType: 'resumable' as const,
    isUpsert: false,
    userMetadata: undefined,
  }

  afterEach(() => vi.restoreAllMocks())

  test('treats a repeated completion of the committed version as already done', async () => {
    const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send').mockResolvedValue(undefined)
    const sendWebhookSpy = vi
      .spyOn(ObjectCreatedPostEvent, 'sendWebhook')
      .mockResolvedValue(undefined)
    const transactionDb = {
      waitObjectLock: vi.fn().mockResolvedValue(undefined),
      findObject: vi.fn().mockResolvedValue({
        id: 'object-id',
        version: 'version-1',
        is_delete_marker: false,
        is_versioned: true,
      }),
      upsertObject: vi.fn(),
    }
    const { db, scopedDb } = createCompleteUploadDb(transactionDb)
    const uploader = createUploader({ uploadObject: vi.fn() }, db)

    await expect(uploader.completeUpload(request)).resolves.toMatchObject({
      obj: { id: 'object-id', version: 'version-1' },
      isNew: false,
    })
    expect(transactionDb.upsertObject).not.toHaveBeenCalled()
    expect(scopedDb.testPermission).not.toHaveBeenCalled()
    expect(sendWebhookSpy).not.toHaveBeenCalled()
    expect(deleteSpy).not.toHaveBeenCalled()
  })

  test('keeps the content of an already committed version when the completion fails', async () => {
    const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send').mockResolvedValue(undefined)
    const committedLookup = vi.fn().mockResolvedValue({ id: 'object-id' })
    const { db } = createCompleteUploadDb(
      { waitObjectLock: vi.fn().mockRejectedValue(new Error('lock timeout')) },
      { asSuperUser: vi.fn().mockReturnValue({ findObject: committedLookup }) as never }
    )
    const uploader = createUploader({ uploadObject: vi.fn() }, db)

    await expect(uploader.completeUpload(request)).rejects.toThrow('lock timeout')
    expect(committedLookup).toHaveBeenCalledWith(
      'bucket',
      'test.txt',
      'id',
      { dontErrorOnEmpty: true },
      'version-1'
    )
    expect(deleteSpy).not.toHaveBeenCalled()
  })

  test('removes the content of a version that never committed when the completion fails', async () => {
    const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send').mockResolvedValue(undefined)
    const { db } = createCompleteUploadDb(
      { waitObjectLock: vi.fn().mockRejectedValue(new Error('lock timeout')) },
      {
        asSuperUser: vi
          .fn()
          .mockReturnValue({ findObject: vi.fn().mockResolvedValue(undefined) }) as never,
      }
    )
    const uploader = createUploader({ uploadObject: vi.fn() }, db)

    await expect(uploader.completeUpload(request)).rejects.toThrow('lock timeout')
    expect(deleteSpy).toHaveBeenCalledOnce()
    expect(deleteSpy).toHaveBeenCalledWith(
      expect.objectContaining({ bucketId: 'bucket', name: 'test.txt', version: 'version-1' })
    )
  })

  test('keeps the content when it cannot tell whether the version committed', async () => {
    const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send').mockResolvedValue(undefined)
    const { db } = createCompleteUploadDb(
      { waitObjectLock: vi.fn().mockRejectedValue(new Error('lock timeout')) },
      {
        asSuperUser: vi.fn().mockReturnValue({
          findObject: vi.fn().mockRejectedValue(new Error('connection lost')),
        }) as never,
      }
    )
    const uploader = createUploader({ uploadObject: vi.fn() }, db)

    await expect(uploader.completeUpload(request)).rejects.toThrow('lock timeout')
    expect(deleteSpy).not.toHaveBeenCalled()
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
      expect.objectContaining({ bucket_id: 'bucket', name: 'deleted.txt' }),
      { currentVersion: true }
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
    const { db } = createCompleteUploadDb(transactionDb)
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
    const { db, permissionDb, scopedDb } = createCompleteUploadDb(transactionDb)
    const uploader = createUploader({ uploadObject: vi.fn() }, db)

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
      expect(scopedDb.testPermission).toHaveBeenCalledOnce()
      expect(permissionDb.upsertObject).toHaveBeenCalledWith(
        expect.objectContaining({ bucket_id: 'bucket', name: 'deleted.txt' }),
        { currentVersion: true }
      )
      expect(permissionDb.createObject).not.toHaveBeenCalled()
    } finally {
      sendWebhookSpy.mockRestore()
    }
  })

  test.each([
    ['ENABLED (new versioned row, nothing replaced)', true, undefined, 0],
    [
      'DISABLED or SUSPENDED null-version replacement',
      false,
      { id: 'old-object-id', version: 'old-version', isDeleteMarker: false },
      1,
    ],
    ['SUSPENDED with an enabled current version (fresh null-version row)', false, undefined, 0],
    [
      'SUSPENDED replacing a null-version delete marker',
      false,
      { id: 'old-object-id', version: 'old-version', isDeleteMarker: true },
      0,
    ],
  ] as const)('completeUpload backend cleanup follows %s write semantics', async (_mode, newIsVersioned, replaced, expectedDeletes) => {
    const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send').mockResolvedValue(undefined)
    const sendWebhookSpy = vi
      .spyOn(ObjectCreatedPostEvent, 'sendWebhook')
      .mockResolvedValue(undefined)
    const transactionDb = {
      waitObjectLock: vi.fn().mockResolvedValue(undefined),
      findBucketById: vi.fn().mockResolvedValue({ versioning_status: 'ENABLED' }),
      findObject: vi.fn().mockResolvedValue({
        id: 'old-object-id',
        version: 'old-version',
        metadata: {},
        is_delete_marker: false,
        is_versioned: true,
      }),
      // The write reports the row it replaced in place, if any.
      upsertObject: vi.fn().mockResolvedValue({
        id: 'new-object-id',
        version: 'new-version',
        is_versioned: newIsVersioned,
        replaced,
      }),
    }
    const { db } = createCompleteUploadDb(transactionDb, {
      hasMigration: vi.fn().mockResolvedValue(true),
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

      // Status lock before row locks, and the status is passed to the write.
      expect(transactionDb.findBucketById).toHaveBeenCalledWith('bucket', 'versioning_status', {
        forShare: true,
      })
      expect(transactionDb.upsertObject).toHaveBeenCalledWith(expect.anything(), {
        versioningStatus: 'ENABLED',
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

  test.each([
    ['ENABLED (archives, does not free, the previous version)', undefined, undefined],
    [
      'DISABLED or SUSPENDED null-version replacement (frees the previous version)',
      { id: 'old-object-id', version: 'old-version', isDeleteMarker: false, metadata: {} },
      { version: 'old-version', metadata: {} },
    ],
    [
      'SUSPENDED with an enabled current version (fresh null-version row, nothing freed)',
      undefined,
      undefined,
    ],
    [
      'SUSPENDED replacing a null-version delete marker (no bytes to debit)',
      { id: 'old-object-id', version: 'old-version', isDeleteMarker: true },
      undefined,
    ],
  ] as const)('completeUpload reports oldObject on the webhook only when %s', async (_mode, replaced, expectedOldObject) => {
    const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send').mockResolvedValue(undefined)
    const putSpy = vi.spyOn(ObjectCreatedPutEvent, 'sendWebhook').mockResolvedValue(undefined)
    const postSpy = vi.spyOn(ObjectCreatedPostEvent, 'sendWebhook').mockResolvedValue(undefined)
    const transactionDb = {
      waitObjectLock: vi.fn().mockResolvedValue(undefined),
      findBucketById: vi.fn().mockResolvedValue({ versioning_status: 'ENABLED' }),
      findObject: vi.fn().mockResolvedValue({
        id: 'old-object-id',
        version: 'old-version',
        metadata: {},
        is_delete_marker: false,
        is_versioned: true,
      }),
      upsertObject: vi.fn().mockResolvedValue({
        id: 'new-object-id',
        version: 'new-version',
        is_versioned: true,
        replaced,
      }),
    }
    const { db } = createCompleteUploadDb(transactionDb, {
      hasMigration: vi.fn().mockResolvedValue(true),
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

      expect(postSpy).not.toHaveBeenCalled()
      expect(putSpy).toHaveBeenCalledOnce()
      if (expectedOldObject) {
        expect(putSpy).toHaveBeenCalledWith(
          expect.objectContaining({ oldObject: expect.objectContaining(expectedOldObject) })
        )
      } else {
        expect(putSpy).toHaveBeenCalledWith(expect.objectContaining({ oldObject: undefined }))
      }
    } finally {
      deleteSpy.mockRestore()
      putSpy.mockRestore()
      postSpy.mockRestore()
    }
  })

  test('completeUpload still reports oldObject, with a null size, when the replaced row is missing metadata', async () => {
    const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send').mockResolvedValue(undefined)
    const putSpy = vi.spyOn(ObjectCreatedPutEvent, 'sendWebhook').mockResolvedValue(undefined)
    const transactionDb = {
      waitObjectLock: vi.fn().mockResolvedValue(undefined),
      findBucketById: vi.fn().mockResolvedValue({ versioning_status: 'DISABLED' }),
      findObject: vi.fn().mockResolvedValue({
        id: 'old-object-id',
        version: 'old-version',
        metadata: null,
        is_delete_marker: false,
        is_versioned: false,
      }),
      upsertObject: vi.fn().mockResolvedValue({
        id: 'new-object-id',
        version: 'new-version',
        is_versioned: false,
        replaced: {
          id: 'old-object-id',
          version: 'old-version',
          isDeleteMarker: false,
          metadata: null,
        },
      }),
    }
    const { db } = createCompleteUploadDb(transactionDb, {
      hasMigration: vi.fn().mockResolvedValue(true),
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

      expect(putSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          oldObject: expect.objectContaining({ version: 'old-version', metadata: null }),
        })
      )
    } finally {
      deleteSpy.mockRestore()
      putSpy.mockRestore()
    }
  })

  test('completeUpload deletes the archived null-version content replaced while suspended', async () => {
    const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send').mockResolvedValue(undefined)
    const sendWebhookSpy = vi
      .spyOn(ObjectCreatedPostEvent, 'sendWebhook')
      .mockResolvedValue(undefined)
    const transactionDb = {
      waitObjectLock: vi.fn().mockResolvedValue(undefined),
      findBucketById: vi.fn().mockResolvedValue({ versioning_status: 'SUSPENDED' }),
      findObject: vi.fn().mockResolvedValue({
        id: 'enabled-current-id',
        version: 'enabled-current-version',
        is_delete_marker: false,
        is_versioned: true,
      }),
      // The archived null-version row was resurrected in place by the write.
      upsertObject: vi.fn().mockResolvedValue({
        id: 'null-version-id',
        version: 'new-version',
        is_versioned: false,
        replaced: { id: 'null-version-id', version: 'old-null-version', isDeleteMarker: false },
      }),
    }
    const { db } = createCompleteUploadDb(transactionDb, {
      hasMigration: vi.fn().mockResolvedValue(true),
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

      // A single locked read of the current row: the replaced row comes back
      // from the write itself.
      expect(transactionDb.findObject).toHaveBeenCalledTimes(1)
      expect(deleteSpy).toHaveBeenCalledWith(
        expect.objectContaining({ version: 'old-null-version' })
      )
    } finally {
      deleteSpy.mockRestore()
      sendWebhookSpy.mockRestore()
    }
  })
})

describe('Upload completion conflicts', () => {
  const store = useStorage()
  const { serviceKeyAsync, storageS3Bucket, storageBackendType, tenantId } = getConfig()
  const objectName = 'same.txt'
  const contents = ['first upload', 'second upload']
  let app: FastifyInstance
  let bucketId: string
  let versions: string[]
  let authorization: string

  beforeEach(async () => {
    bucketId = `upload-race-${randomUUID()}`
    versions = []
    authorization = `Bearer ${await serviceKeyAsync}`
    await store.database.createBucket({ id: bucketId, name: bucketId, public: false })
    vi.spyOn(ObjectCreatedPostEvent, 'sendWebhook').mockResolvedValue(undefined)
    vi.spyOn(ObjectCreatedPutEvent, 'sendWebhook').mockResolvedValue(undefined)
    vi.spyOn(ObjectAdminDelete.prototype, 'send').mockImplementation(async function (
      this: ObjectAdminDelete
    ) {
      await ObjectAdminDelete.handle({
        id: '__sync',
        name: ObjectAdminDelete.queueName,
        expireInSeconds: 0,
        data: this.payload,
      })
    })
    app = buildApp()
    await app.ready()
  })

  afterEach(async () => {
    try {
      await app.close()
      await store.database.deleteObjects(bucketId, [objectName], 'name')
      await store.database.deleteBucket(bucketId)
      const key = store.storage.location.getKeyLocation({ tenantId, bucketId, objectName })
      if (versions.length > 0) {
        await store.adapter.deleteObjects(
          storageS3Bucket,
          versions.map((version) => withOptionalVersion(key, version))
        )
      }
    } finally {
      vi.restoreAllMocks()
    }
  })

  describe.each(['binary', 'multipart'])('%s POST uploads', (format) => {
    test.each([
      { upsert: 'false', statuses: [200, 400] },
      { upsert: undefined, statuses: [200, 400] },
      { upsert: 'true', statuses: [200, 200] },
    ])('resolves concurrent uploads with x-upsert=$upsert', async ({ upsert, statuses }) => {
      const release = Promise.withResolvers<void>()
      const completeUpload = Uploader.prototype.completeUpload
      vi.spyOn(Uploader.prototype, 'completeUpload').mockImplementation(async function (
        this: Uploader,
        options
      ) {
        versions.push(options.version)
        await release.promise // permissions and byte uploads
        return completeUpload.call(this, options)
      })
      const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send')

      const upload = (content: string, request: number) => {
        const metadata = JSON.stringify({ request })
        const headers = {
          authorization,
          ...(upsert === undefined ? {} : { 'x-upsert': upsert }),
        }
        if (format === 'multipart') {
          const form = new FormData()
          form.append('metadata', metadata)
          form.append('file', Buffer.from(content), {
            filename: objectName,
            contentType: 'text/plain',
          })
          return app.inject({
            method: 'POST',
            url: `/object/${bucketId}/${objectName}`,
            headers: { ...headers, ...form.getHeaders() },
            payload: form,
          })
        }
        return app.inject({
          method: 'POST',
          url: `/object/${bucketId}/${objectName}`,
          headers: {
            ...headers,
            'content-type': 'text/plain',
            'x-metadata': Buffer.from(metadata).toString('base64'),
          },
          payload: content,
        })
      }
      const responsesPromise = Promise.all(contents.map(upload))

      try {
        await vi.waitFor(() => expect(versions).toHaveLength(2), { timeout: 5000 })
        release.resolve()
        const responses = await responsesPromise
        expect(responses.map((response) => response.statusCode).sort()).toEqual(statuses)

        const current = await store.database.findObject(
          bucketId,
          objectName,
          'id, version, user_metadata'
        )
        const winner = current.user_metadata?.request
        if (winner !== 0 && winner !== 1) throw new Error('Missing winning request metadata')
        expect(responses[winner].statusCode).toBe(200)

        if (upsert !== 'true') {
          const sequentialDuplicate = await upload(contents[1 - winner], 1 - winner)
          expect(sequentialDuplicate.statusCode).toBe(400)
          expect(sequentialDuplicate.json()).toEqual({
            statusCode: '409',
            code: ErrorCode.KeyAlreadyExists,
            error: 'Duplicate',
            message: 'The resource already exists',
          })
          expect(responses[1 - winner].json()).toEqual(sequentialDuplicate.json())
        }

        const discardedVersion = versions.find((version) => version !== current.version)
        expect(discardedVersion).toBeDefined()
        expect(deleteSpy).toHaveBeenCalledExactlyOnceWith(
          expect.objectContaining({ bucketId, name: objectName, version: discardedVersion })
        )
        const key = store.storage.location.getKeyLocation({ tenantId, bucketId, objectName })
        await expect(
          store.adapter.headObject(storageS3Bucket, key, discardedVersion)
        ).rejects.toMatchObject(
          storageBackendType === 'file' ? { code: 'ENOENT' } : { httpStatusCode: 404 }
        )

        const download = await app.inject({
          method: 'GET',
          url: `/object/${bucketId}/${objectName}`,
          headers: { authorization },
        })
        expect(download.statusCode).toBe(200)
        expect(download.body).toBe(contents[winner])
      } finally {
        release.resolve()
        await responsesPromise
      }
    })
  })

  test('allows a non-upsert completion retry of the current upload version', async () => {
    const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send')
    const uploaded = await store.uploader.upload({
      bucketId,
      objectName,
      isUpsert: false,
      uploadType: 'resumable',
      file: {
        body: Readable.from([contents[0]]),
        mimeType: 'text/plain',
        cacheControl: 'no-cache',
        isTruncated: () => false,
      },
    })
    const version = uploaded.obj.version
    expect(version).toBeTypeOf('string')
    if (!version) throw new Error('Missing uploaded version')
    versions.push(version)

    const retried = await store.uploader.completeUpload({
      bucketId,
      objectName,
      version,
      isUpsert: false,
      uploadType: 'resumable',
      objectMetadata: uploaded.metadata,
    })
    expect(retried.obj.id).toBe(uploaded.obj.id)
    expect(retried.obj.version).toBe(version)
    expect(deleteSpy).not.toHaveBeenCalled()

    const download = await app.inject({
      method: 'GET',
      url: `/object/${bucketId}/${objectName}`,
      headers: { authorization },
    })
    expect(download.statusCode).toBe(200)
    expect(download.body).toBe(contents[0])
  })
})
