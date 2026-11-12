import { ERRORS } from '@internal/errors'
import { logSchema } from '@internal/monitoring'
import { DBError } from '@storage/database/errors'
import { Uploader } from '@storage/uploader'
import { DataStore, Server, ERRORS as TUS_ERRORS, Upload } from '@tus/server'
import Fastify from 'fastify'
import { DatabaseError } from 'pg'
import { fetch } from 'undici'
import { authenticatedRoutes } from './index'
import {
  type MultiPartRequest,
  onIncomingRequest,
  onResponseError,
  onUploadFinish,
  onUploadHead,
} from './lifecycle'

const uploadId = 'tenant-123/bucket/object.txt/version-123'

function createRawTusRequest({
  headers = {},
  method = 'POST',
  sbReqId = 'sb-req-123',
}: {
  headers?: Record<string, string>
  method?: string
  sbReqId?: string
} = {}) {
  const reqLog = {
    error: vi.fn(),
    warn: vi.fn(),
  }
  const metadata = {
    cacheControl: 'no-cache',
    contentLength: 10,
    size: 10,
    mimetype: 'text/plain',
    eTag: 'etag',
  }
  const findObject = vi.fn().mockResolvedValue(undefined)

  const request = {
    headers,
    log: reqLog,
    method,
    upload: {
      isUpsert: false,
      owner: 'owner-123',
      storage: {
        backend: { headObject: vi.fn().mockResolvedValue(metadata) },
        db: { findObject },
        location: { getKeyLocation: vi.fn().mockReturnValue('tenant-123/bucket/object.txt') },
      },
      tenantId: 'tenant-123',
      reqId: 'req-123',
      sbReqId,
    },
    url: `/upload/resumable/${Buffer.from('bucket/object.txt/version-123').toString('base64url')}`,
  } as unknown as MultiPartRequest

  return {
    rawReq: {
      method,
      runtime: {
        name: 'node',
        node: {
          req: request,
        },
      },
    } as unknown as Parameters<typeof onIncomingRequest>[0],
    reqLog,
    metadata,
    findObject,
    request,
  }
}

describe('tus database errors', () => {
  it.each([
    {
      message: 'no more connections allowed (max_client_conn)',
      statusCode: 429,
      body: 'Too many connections issued to the database',
    },
    {
      message: 'received invalid response: 58',
      statusCode: 500,
      body: 'database error, code: 08P01',
    },
  ])('returns $statusCode for 08P01: $message', async ({ message, statusCode, body }) => {
    const pgError = new DatabaseError(message, message.length, 'error')
    pgError.code = '08P01'
    const error = DBError.fromDBError(pgError, { queryName: 'FindBucketById' })
    let rawRequest: MultiPartRequest | undefined
    const maxSize = vi.fn(async (request: Parameters<typeof onResponseError>[0]) => {
      rawRequest = request.runtime?.node?.req as MultiPartRequest
      throw error
    })
    const tusServer = new Server({
      path: '/upload/resumable/sign',
      datastore: new DataStore(),
      maxSize,
      onResponseError,
    })
    const app = Fastify()

    try {
      await app.register(authenticatedRoutes, { tusServer, signed: true })
      const address = await app.listen({ host: '127.0.0.1', port: 0 })
      const response = await fetch(`${address}/upload/resumable/sign`, {
        method: 'POST',
        headers: {
          'Tus-Resumable': '1.0.0',
          'Upload-Length': '36',
          'Content-Length': '0',
        },
      })
      const responseBody = await response.text()

      expect(maxSize).toHaveBeenCalledOnce()
      expect(response.status).toBe(statusCode)
      expect(response.headers.get('Tus-Resumable')).toBe('1.0.0')
      expect(responseBody).toBe(body)
      expect(rawRequest?.executionError).toBe(error)
    } finally {
      await app.close()
    }
  })
})

describe('tus lifecycle logging', () => {
  it.each([
    {
      name: 'lock timeout',
      error: ERRORS.LockTimeout(),
      response: { status_code: 503, body: 'acquiring lock timeout' },
    },
    { name: 'request abort', error: TUS_ERRORS.ABORTED, response: undefined },
    { name: 'file write failure', error: TUS_ERRORS.FILE_WRITE_ERROR, response: undefined },
  ])('preserves the original idle timeout after a $name', ({ error, response }) => {
    const { rawReq, request } = createRawTusRequest()
    const idleTimeout = ERRORS.TusError('TUS request body idle timeout - no bytes received', 408)
    request.executionError = idleTimeout

    expect(onResponseError(rawReq, error)).toEqual(response)
    expect(request.executionError).toBe(idleTimeout)
  })

  it('records a renderable response error when no earlier error exists', () => {
    const { rawReq, request } = createRawTusRequest()
    const error = ERRORS.LockTimeout()

    expect(onResponseError(rawReq, error)).toEqual({
      status_code: 503,
      body: 'acquiring lock timeout',
    })
    expect(request.executionError).toBe(error)
  })

  it('records a plain TUS response error when no earlier error exists', () => {
    const { rawReq, request } = createRawTusRequest()

    expect(onResponseError(rawReq, TUS_ERRORS.ABORTED)).toBeUndefined()
    expect(request.executionError).toMatchObject({
      code: 'TusError',
      httpStatusCode: 400,
      message: TUS_ERRORS.ABORTED.body,
      metadata: TUS_ERRORS.ABORTED,
    })
  })

  it('logs upload metadata parse failures with sbReqId through logSchema', async () => {
    const warningSpy = vi.spyOn(logSchema, 'warning').mockImplementation(() => undefined)
    const { rawReq, reqLog } = createRawTusRequest({
      headers: {
        'upload-metadata': 'contentType invalid',
      },
    })

    await expect(onIncomingRequest(rawReq, uploadId, {} as DataStore)).rejects.toThrow(Error)

    expect(warningSpy).toHaveBeenCalledWith(reqLog, 'Failed to parse upload metadata', {
      type: 'tus',
      tenantId: 'tenant-123',
      project: 'tenant-123',
      reqId: 'req-123',
      error: expect.any(Error),
      sbReqId: 'sb-req-123',
    })
    expect(reqLog.warn).not.toHaveBeenCalled()
  })

  it('logs user metadata parse failures with sbReqId through logSchema', async () => {
    const warningSpy = vi.spyOn(logSchema, 'warning').mockImplementation(() => undefined)
    const canUploadSpy = vi.spyOn(Uploader.prototype, 'canUpload').mockResolvedValue(undefined)
    const { rawReq, reqLog } = createRawTusRequest({
      headers: {
        'upload-metadata': 'contentType aW1hZ2UvcG5n,metadata e2ludmFsaWQtanNvbg==',
      },
    })

    await onIncomingRequest(rawReq, uploadId, {} as DataStore)

    expect(canUploadSpy).toHaveBeenCalledOnce()
    expect(warningSpy).toHaveBeenCalledWith(reqLog, 'Failed to parse user metadata', {
      type: 'tus',
      tenantId: 'tenant-123',
      project: 'tenant-123',
      reqId: 'req-123',
      error: expect.any(Error),
      sbReqId: 'sb-req-123',
    })
    expect(reqLog.warn).not.toHaveBeenCalled()
  })

  it('passes the validated content encoding to canUpload on create', async () => {
    const canUploadSpy = vi.spyOn(Uploader.prototype, 'canUpload').mockResolvedValue(undefined)
    const { rawReq } = createRawTusRequest({
      headers: {
        'upload-metadata': `contentEncoding ${Buffer.from('gzip, aws-chunked').toString('base64')}`,
      },
    })

    await onIncomingRequest(rawReq, uploadId, {} as DataStore)

    expect(canUploadSpy).toHaveBeenCalledWith(
      expect.objectContaining({ metadata: expect.objectContaining({ contentEncoding: 'gzip' }) })
    )
  })

  it('passes the stored content encoding to canUpload on patch', async () => {
    const canUploadSpy = vi.spyOn(Uploader.prototype, 'canUpload').mockResolvedValue(undefined)
    const { rawReq } = createRawTusRequest({ method: 'PATCH' })
    const datastore = {
      getUpload: vi.fn().mockResolvedValue({ metadata: { contentEncoding: 'br' }, size: 3 }),
    } as unknown as DataStore

    await onIncomingRequest(rawReq, uploadId, datastore)

    expect(canUploadSpy).toHaveBeenCalledWith(
      expect.objectContaining({ metadata: expect.objectContaining({ contentEncoding: 'br' }) })
    )
  })
})

describe('tus upload completion', () => {
  it.each([
    [
      'object-123',
      {
        'Tus-Complete': '1',
        'Supabase-Object-Id': 'object-123',
        'Supabase-Object-Version-Id': 'version-123',
      },
    ],
    [undefined, { 'Tus-Complete': '1', 'Supabase-Object-Version-Id': 'version-123' }],
  ] as const)('returns completion headers for object ID %s', async (id, expectedHeaders) => {
    const { rawReq, metadata } = createRawTusRequest()
    vi.spyOn(Uploader.prototype, 'completeUpload').mockResolvedValue({
      obj: { name: 'object.txt', id },
      isNew: true,
      metadata,
    })
    const result = await onUploadFinish(rawReq, new Upload({ id: uploadId, size: 10, offset: 10 }))
    expect(result.headers).toEqual(expectedHeaders)
  })

  it.each([0, 10])('returns object ID and version on completed HEAD for %i bytes', async (size) => {
    const { rawReq, findObject } = createRawTusRequest({ method: 'HEAD' })
    findObject.mockResolvedValue({ id: 'object-123', name: 'object.txt' })
    const response = new Response(null, {
      headers: { 'Upload-Length': String(size), 'Upload-Offset': String(size) },
    })

    await onUploadHead(rawReq, response)

    expect(findObject).toHaveBeenCalledWith(
      'bucket',
      'object.txt',
      'id',
      { dontErrorOnEmpty: true, excludeDeleteMarkers: true },
      'version-123'
    )
    expect(response.headers.get('Supabase-Object-Id')).toBe('object-123')
    expect(response.headers.get('Supabase-Object-Version-Id')).toBe('version-123')
  })

  it('omits completion headers when the version is missing or hidden by RLS', async () => {
    const { rawReq, findObject } = createRawTusRequest({ method: 'HEAD' })
    const response = new Response(null, {
      headers: { 'Upload-Length': '10', 'Upload-Offset': '10' },
    })

    await onUploadHead(rawReq, response)

    expect(findObject).toHaveBeenCalledOnce()
    expect(response.headers.has('Supabase-Object-Id')).toBe(false)
    expect(response.headers.has('Supabase-Object-Version-Id')).toBe(false)
  })

  it('omits completion headers when the caller lacks SELECT permission', async () => {
    const { rawReq, findObject } = createRawTusRequest({ method: 'HEAD' })
    findObject.mockRejectedValue(ERRORS.AccessDenied('permission denied for table objects'))
    const response = new Response(null, {
      headers: { 'Upload-Length': '10', 'Upload-Offset': '10' },
    })

    await expect(onUploadHead(rawReq, response)).resolves.toBeUndefined()
    expect(response.headers.has('Supabase-Object-Id')).toBe(false)
    expect(response.headers.has('Supabase-Object-Version-Id')).toBe(false)
  })

  it('propagates other lookup failures', async () => {
    const { rawReq, findObject } = createRawTusRequest({ method: 'HEAD' })
    const error = ERRORS.DatabaseTimeout(new Error('lookup timed out'))
    findObject.mockRejectedValue(error)
    const response = new Response(null, {
      headers: { 'Upload-Length': '10', 'Upload-Offset': '10' },
    })

    await expect(onUploadHead(rawReq, response)).rejects.toBe(error)
  })

  it.each<{ status: number; headers: Record<string, string> }>([
    { status: 200, headers: { 'Upload-Length': '10', 'Upload-Offset': '5' } },
    { status: 200, headers: { 'Upload-Defer-Length': '1', 'Upload-Offset': '0' } },
    { status: 404, headers: { 'Upload-Length': '10', 'Upload-Offset': '10' } },
  ])('does not look up an ID for HEAD $status with $headers', async ({ status, headers }) => {
    const { rawReq, findObject } = createRawTusRequest({ method: 'HEAD' })
    const response = new Response(null, { status, headers })

    await onUploadHead(rawReq, response)

    expect(findObject).not.toHaveBeenCalled()
    expect(response.headers.has('Supabase-Object-Id')).toBe(false)
    expect(response.headers.has('Supabase-Object-Version-Id')).toBe(false)
  })
})
