import { ERRORS } from '@internal/errors'
import { logSchema } from '@internal/monitoring'
import { Uploader } from '@storage/uploader'
import { type DataStore, Upload } from '@tus/server'
import { type MultiPartRequest, onIncomingRequest, onUploadFinish, onUploadHead } from './lifecycle'

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
  }
}

describe('tus lifecycle logging', () => {
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
    ['object-123', { 'Tus-Complete': '1', 'Supabase-Object-Id': 'object-123' }],
    [undefined, { 'Tus-Complete': '1' }],
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

  it.each([0, 10])('returns the exact committed version ID on HEAD for %i bytes', async (size) => {
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
  })

  it('omits the ID when the version is missing or hidden by RLS', async () => {
    const { rawReq, findObject } = createRawTusRequest({ method: 'HEAD' })
    const response = new Response(null, {
      headers: { 'Upload-Length': '10', 'Upload-Offset': '10' },
    })

    await onUploadHead(rawReq, response)

    expect(findObject).toHaveBeenCalledOnce()
    expect(response.headers.has('Supabase-Object-Id')).toBe(false)
  })

  it('omits the ID when the caller lacks SELECT permission', async () => {
    const { rawReq, findObject } = createRawTusRequest({ method: 'HEAD' })
    findObject.mockRejectedValue(ERRORS.AccessDenied('permission denied for table objects'))
    const response = new Response(null, {
      headers: { 'Upload-Length': '10', 'Upload-Offset': '10' },
    })

    await expect(onUploadHead(rawReq, response)).resolves.toBeUndefined()
    expect(response.headers.has('Supabase-Object-Id')).toBe(false)
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
  })
})
