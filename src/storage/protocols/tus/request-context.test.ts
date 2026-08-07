import type { IncomingMessage } from 'node:http'
import {
  runWithTusRequest,
  setTusRequestCancellationSignal,
  writeWithRequestCompletion,
} from './request-context'

type RequestOptions = {
  aborted?: boolean
  complete?: boolean
  contentLength?: string
}

function createRequest(options: RequestOptions = {}): IncomingMessage {
  const contentLength = 'contentLength' in options ? options.contentLength : '5'

  return {
    aborted: options.aborted ?? false,
    complete: options.complete ?? true,
    headers: contentLength === undefined ? {} : { 'content-length': contentLength },
    log: { warn: vi.fn() },
    upload: {
      tenantId: 'tenant-123',
      reqId: 'req-123',
      sbReqId: 'sb-req-123',
    },
  } as unknown as IncomingMessage
}

function createStore(remove = vi.fn(async () => undefined)) {
  return { remove }
}

describe('TUS request write completion', () => {
  it.each([
    {
      name: 'a complete fixed-length body',
      request: createRequest(),
    },
    {
      name: 'a fixed-length body whose bytes committed before a late abort',
      request: createRequest({ aborted: true, complete: false }),
    },
    {
      name: 'a complete chunked body',
      request: createRequest({ contentLength: undefined }),
    },
  ])('keeps the upload after $name', async ({ request }) => {
    const store = createStore()

    const offset = await runWithTusRequest(request, () =>
      writeWithRequestCompletion(store, 'file', 'upload-id', 7, async () => 12)
    )

    expect(offset).toBe(12)
    expect(store.remove).not.toHaveBeenCalled()
  })

  it.each(
    [
      {
        name: 'the fixed-length body is short',
        request: createRequest({ contentLength: '6' }),
        reason: 'length_mismatch',
      },
      {
        name: 'Content-Length is unsafe',
        request: createRequest({ contentLength: '9007199254740992' }),
        reason: 'length_mismatch',
        newOffset: 12,
      },
      {
        name: 'the datastore returns a lower offset',
        request: createRequest(),
        reason: 'offset_mismatch',
        newOffset: 6,
      },
      {
        name: 'a chunked request is aborted',
        request: createRequest({ aborted: true, complete: false, contentLength: undefined }),
        reason: 'aborted',
      },
      {
        name: 'a chunked HTTP message is incomplete',
        request: createRequest({ complete: false, contentLength: undefined }),
        reason: 'incomplete',
      },
    ].map((testCase) => ({ newOffset: 12, ...testCase }))
  )('invalidates when $name', async ({ request, reason, newOffset }) => {
    const store = createStore()

    await expect(
      runWithTusRequest(request, () =>
        writeWithRequestCompletion(store, 'file', 'upload-id', 7, async () => newOffset)
      )
    ).rejects.toMatchObject({
      status_code: 400,
      body: 'Upload request body was incomplete\n',
    })

    expect(store.remove).toHaveBeenCalledWith('upload-id')
    expect(
      (request as IncomingMessage & { log: { warn: ReturnType<typeof vi.fn> } }).log.warn
    ).toHaveBeenCalledWith(
      expect.objectContaining({
        metadata: JSON.stringify({ backend: 'file', reason, outcome: 'success' }),
      }),
      'TUS upload invalidated'
    )
  })

  it('invalidates a chunked write cancelled internally by tus', async () => {
    const request = createRequest({ contentLength: undefined })
    const store = createStore()
    const cancellation = new AbortController()

    const result = runWithTusRequest(request, () => {
      setTusRequestCancellationSignal(cancellation.signal)
      cancellation.abort()
      return writeWithRequestCompletion(store, 's3', 'upload-id', 7, async () => 12)
    })

    await expect(result).rejects.toMatchObject({ status_code: 400 })
    expect(store.remove).toHaveBeenCalledWith('upload-id')
  })

  it('invalidates an ambiguous datastore write error', async () => {
    const request = createRequest()
    const store = createStore()
    const writeError = new Error('backend write failed')

    const result = runWithTusRequest(request, () =>
      writeWithRequestCompletion(store, 's3', 'upload-id', 7, async () => {
        throw writeError
      })
    )

    await expect(result).rejects.toBe(writeError)
    expect(store.remove).toHaveBeenCalledWith('upload-id')
  })

  it('surfaces both write and cleanup failures', async () => {
    const writeError = new Error('backend write failed')
    const cleanupError = new Error('cleanup failed')
    const store = createStore(
      vi.fn(async () => {
        throw cleanupError
      })
    )

    const result = runWithTusRequest(createRequest(), () =>
      writeWithRequestCompletion(store, 's3', 'upload-id', 7, async () => {
        throw writeError
      })
    )

    await expect(result).rejects.toMatchObject({
      message: 'Failed to invalidate incomplete TUS upload upload-id',
      errors: [writeError, cleanupError],
    })
  })

  it('leaves direct datastore writes unchanged outside a request', async () => {
    const store = createStore()

    await expect(
      writeWithRequestCompletion(store, 'file', 'upload-id', 7, async () => 8)
    ).resolves.toBe(8)
    expect(store.remove).not.toHaveBeenCalled()
  })
})
