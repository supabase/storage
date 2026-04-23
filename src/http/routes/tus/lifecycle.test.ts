import { EventEmitter } from 'node:events'
import type { ServerResponse } from 'node:http'
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
  const response = new EventEmitter()
  const reqLog = {
    error: vi.fn(),
    warn: vi.fn(),
  }
  const dispose = vi.fn().mockResolvedValue(undefined)

  const request = {
    headers,
    log: reqLog,
    method,
    upload: {
      db: {
        dispose,
      },
      isUpsert: false,
      owner: 'owner-123',
      storage: {
        backend: {},
        db: {},
        location: {},
      },
      tenantId: 'tenant-123',
      sbReqId,
    },
    url: '/upload/resumable',
  } as unknown as MultiPartRequest

  return {
    dispose,
    rawReq: {
      method,
      node: {
        req: request,
        res: response as unknown as ServerResponse,
      },
    } as unknown as Parameters<typeof onIncomingRequest>[0],
    reqLog,
    response,
  }
}

describe('tus lifecycle logging', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('logs db dispose failures with sbReqId through logSchema', async () => {
    const error = new Error('dispose failed')
    const errorSpy = vi.spyOn(logSchema, 'error').mockImplementation(() => undefined)
    const { dispose, rawReq, reqLog, response } = createRawTusRequest({
      method: 'HEAD',
    })

    dispose.mockRejectedValueOnce(error)

    await onIncomingRequest(rawReq, uploadId, {} as DataStore)

    response.emit('finish')
    await new Promise((resolve) => setImmediate(resolve))

    expect(errorSpy).toHaveBeenCalledWith(reqLog, 'Error disposing db connection', {
      type: 'db-connection',
      error,
      sbReqId: 'sb-req-123',
    })
    expect(reqLog.error).not.toHaveBeenCalled()
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
      error: expect.any(Error),
      sbReqId: 'sb-req-123',
    })
    expect(reqLog.warn).not.toHaveBeenCalled()
  })
})
