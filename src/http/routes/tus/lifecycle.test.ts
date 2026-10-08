import { logSchema } from '@internal/monitoring'
import { Uploader } from '@storage/uploader'
import type { DataStore } from '@tus/server'
import { type MultiPartRequest, onIncomingRequest } from './lifecycle'

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

  const request = {
    headers,
    log: reqLog,
    method,
    upload: {
      isUpsert: false,
      owner: 'owner-123',
      storage: {
        backend: {},
        db: {},
        location: {},
      },
      tenantId: 'tenant-123',
      reqId: 'req-123',
      sbReqId,
    },
    url: '/upload/resumable',
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
