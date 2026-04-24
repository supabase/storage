import type { FastifyRequest } from 'fastify'
import { Readable } from 'stream'
import type { StorageBackendAdapter } from '../backend'

async function readStream(stream: unknown) {
  const chunks: Buffer[] = []

  for await (const chunk of stream as AsyncIterable<Buffer | string | Uint8Array>) {
    if (typeof chunk === 'string') {
      chunks.push(Buffer.from(chunk))
      continue
    }

    chunks.push(Buffer.from(chunk))
  }

  return Buffer.concat(chunks).toString('utf8')
}

async function loadRendererModule(overrides?: { imgProxyRequestTimeout?: number }) {
  vi.resetModules()

  const configModule = await import('../../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig({
    imgProxyHttpMaxSockets: 0,
    imgProxyRequestTimeout: 30,
    imgProxyURL: 'https://imgproxy.example.test/base',
    ...overrides,
  })

  return import('./image')
}

function createBackend(privateURL = 'https://origin.example/assets/cat.png?token=a b') {
  return {
    headObject: vi.fn().mockResolvedValue({
      cacheControl: 'max-age=3600',
      contentLength: 123,
      eTag: '"source-etag"',
      lastModified: new Date('2022-10-01T00:00:00.000Z'),
      mimetype: 'image/jpeg',
      size: 123,
    }),
    privateAssetUrl: vi.fn().mockResolvedValue(privateURL),
  } as unknown as StorageBackendAdapter
}

function createRequest(headers: FastifyRequest['headers'] = {}) {
  return { headers } as FastifyRequest
}

function createRenderOptions(signal = new AbortController().signal) {
  return {
    bucket: 'bucket',
    key: 'folder/cat.png',
    signal,
    version: 'version',
  }
}

describe('ImageRenderer fetch client', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
    vi.useRealTimers()
  })

  it('fetches the transformed image with native fetch and maps the streamed response metadata', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response('rendered-body', {
        headers: {
          'content-length': '13',
          'content-type': 'image/webp',
          'last-modified': 'Wed, 12 Oct 2022 11:17:02 GMT',
        },
      })
    )
    vi.stubGlobal('fetch', fetchMock)

    const { ImageRenderer } = await loadRendererModule()
    const renderer = new ImageRenderer(createBackend()).setTransformations({
      format: 'webp',
      width: 100,
    })

    const result = await renderer.getAsset(
      createRequest({ accept: 'image/avif,image/webp' }),
      createRenderOptions()
    )

    expect(fetchMock).toHaveBeenCalledTimes(1)

    const [url, init] = fetchMock.mock.calls[0]
    expect(url).toBe(
      'https://imgproxy.example.test/base/public/width:100/resizing_type:fill/format:webp/plain/https%3A%2F%2Forigin.example%2Fassets%2Fcat.png%3Ftoken%3Da%20b'
    )
    expect(init).toMatchObject({
      method: 'GET',
      signal: expect.any(AbortSignal),
    })
    expect(init?.headers).toBeInstanceOf(Headers)
    expect((init?.headers as Headers).get('accept')).toBe('image/avif,image/webp')

    await expect(readStream(result.body)).resolves.toBe('rendered-body')
    expect(result.metadata).toMatchObject({
      cacheControl: 'max-age=3600',
      contentLength: 13,
      eTag: '"source-etag"',
      httpStatusCode: 200,
      lastModified: new Date('2022-10-12T11:17:02.000Z'),
      mimetype: 'image/webp',
      size: 13,
    })
    expect(result.transformations).toEqual(['width:100', 'resizing_type:fill', 'format:webp'])
  })

  describe('retry delays', () => {
    beforeEach(() => {
      vi.useFakeTimers()
    })

    it.each([
      429, 500,
    ])('retries imgproxy status %i before returning the response', async (status) => {
      const fetchMock = vi
        .fn<typeof fetch>()
        .mockResolvedValueOnce(new Response('try again', { status }))
        .mockResolvedValueOnce(
          new Response('rendered', {
            headers: {
              'content-length': '8',
              'content-type': 'image/png',
            },
          })
        )
      vi.stubGlobal('fetch', fetchMock)

      const { ImageRenderer } = await loadRendererModule()
      const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png'))

      const resultPromise = renderer.getAsset(createRequest(), createRenderOptions())

      await vi.advanceTimersByTimeAsync(status === 500 ? 150 : 50)
      expect(
        fetchMock,
        'single retry delay should trigger the second imgproxy request'
      ).toHaveBeenCalledTimes(2)
      const result = await resultPromise

      expect(fetchMock.mock.calls[0][0]).toBe(
        'https://imgproxy.example.test/base/public/plain/local:///tmp/cat.png'
      )
      await expect(readStream(result.body)).resolves.toBe('rendered')
      expect(result.metadata.httpStatusCode).toBe(200)
    })

    it('retries mixed retryable imgproxy failures before returning the response', async () => {
      const fetchMock = vi
        .fn<typeof fetch>()
        .mockResolvedValueOnce(new Response('rate limited', { status: 429 }))
        .mockResolvedValueOnce(new Response('server error', { status: 500 }))
        .mockResolvedValueOnce(new Response('rate limited again', { status: 429 }))
        .mockResolvedValueOnce(
          new Response('rendered', {
            headers: {
              'content-length': '8',
              'content-type': 'image/png',
            },
          })
        )
      vi.stubGlobal('fetch', fetchMock)

      const { ImageRenderer } = await loadRendererModule()
      const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png'))

      const resultPromise = renderer.getAsset(createRequest(), createRenderOptions())

      await vi.advanceTimersByTimeAsync(500) // Mixed retry delays: 50 + 300 + 150 ms
      expect(
        fetchMock,
        'mixed retry delays should trigger three retries and one successful request'
      ).toHaveBeenCalledTimes(4)
      const result = await resultPromise

      await expect(readStream(result.body)).resolves.toBe('rendered')
      expect(result.metadata.httpStatusCode).toBe(200)
    })

    it('surfaces the final retryable imgproxy failure after exhausting retries', async () => {
      const fetchMock = vi
        .fn<typeof fetch>()
        .mockImplementation(() => Promise.resolve(new Response('still retrying', { status: 429 })))
      vi.stubGlobal('fetch', fetchMock)

      const { ImageRenderer } = await loadRendererModule()
      const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png'))

      const resultPromise = renderer
        .getAsset(createRequest(), createRenderOptions())
        .catch((e) => e)

      await vi.advanceTimersByTimeAsync(750) // 429 retry delays: 50 + 100 + 150 + 200 + 250 ms
      expect(
        fetchMock,
        'retry exhaustion should perform the initial request plus five retries'
      ).toHaveBeenCalledTimes(6)

      await expect(resultPromise).resolves.toMatchObject({
        httpStatusCode: 429,
        message: 'still retrying',
      })
    })
  })

  it('does not retry non-retryable imgproxy failures and preserves the response body', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response('invalid image request', {
        status: 400,
      })
    )
    vi.stubGlobal('fetch', fetchMock)

    const { ImageRenderer } = await loadRendererModule()
    const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png')).setTransformations({
      height: 50,
    })

    await expect(renderer.getAsset(createRequest(), createRenderOptions())).rejects.toMatchObject({
      httpStatusCode: 400,
      message: 'invalid image request',
      metadata: {
        transformations: ['height:50', 'resizing_type:fill'],
      },
    })
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('falls back to the request error message when imgproxy returns an empty error body', async () => {
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(new Response(null, { status: 400 }))
    vi.stubGlobal('fetch', fetchMock)

    const { ImageRenderer } = await loadRendererModule()
    const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png'))

    await expect(renderer.getAsset(createRequest(), createRenderOptions())).rejects.toMatchObject({
      httpStatusCode: 400,
      message: 'Request failed with status code 400',
    })
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('maps imgproxy error response stream failures to an internal error without hanging', async () => {
    const streamError = new Error('imgproxy stream failed')
    const errorBody = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode('partial error body'))
        controller.error(streamError)
      },
    })
    const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(errorBody, {
        status: 400,
      })
    )
    vi.stubGlobal('fetch', fetchMock)

    const { ImageRenderer } = await loadRendererModule()
    const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png'))

    await expect(renderer.getAsset(createRequest(), createRenderOptions())).rejects.toMatchObject({
      httpStatusCode: 500,
      message: 'imgproxy stream failed',
    })
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('decodes Uint8Array chunks from imgproxy error response bodies', async () => {
    const { ImageRenderer } = await loadRendererModule()

    class TestImageRenderer extends ImageRenderer {
      readRequestError(error: never) {
        return this.handleRequestError(error)
      }
    }

    const errorBody = new Readable({
      objectMode: true,
      read() {
        this.push(new TextEncoder().encode('invalid image request'))
        this.push(null)
      },
    })
    const renderer = new TestImageRenderer(createBackend('local:///tmp/cat.png'))

    await expect(
      renderer.readRequestError({
        response: {
          data: errorBody,
          status: 400,
        },
      } as never)
    ).resolves.toMatchObject({
      httpStatusCode: 400,
      message: 'invalid image request',
    })
  })

  it('bubbles caller-driven aborts without remapping them to storage errors', async () => {
    const controller = new AbortController()
    const abortError = new DOMException('caller stopped', 'AbortError')
    const fetchMock = vi.fn<typeof fetch>().mockImplementation((_url, init) => {
      const signal = init?.signal as AbortSignal
      controller.abort(abortError)
      return Promise.reject(signal.reason ?? abortError)
    })
    vi.stubGlobal('fetch', fetchMock)

    const { ImageRenderer } = await loadRendererModule()
    const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png'))

    await expect(
      renderer.getAsset(createRequest(), createRenderOptions(controller.signal))
    ).rejects.toBe(abortError)
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('prefers caller abort reason when abort races with a fetch failure', async () => {
    const controller = new AbortController()
    const abortError = new DOMException('caller stopped', 'AbortError')
    const fetchMock = vi.fn<typeof fetch>().mockImplementation(() => {
      controller.abort(abortError)
      return Promise.reject(new TypeError('network failure'))
    })
    vi.stubGlobal('fetch', fetchMock)

    const { ImageRenderer } = await loadRendererModule()
    const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png'))

    await expect(
      renderer.getAsset(createRequest(), createRenderOptions(controller.signal))
    ).rejects.toBe(abortError)
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('maps fetch timeout failures to the existing internal error shape', async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockRejectedValue(new DOMException('The operation timed out', 'TimeoutError'))
    vi.stubGlobal('fetch', fetchMock)

    const { ImageRenderer } = await loadRendererModule()
    const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png')).setTransformations({
      width: 50,
    })

    await expect(renderer.getAsset(createRequest(), createRenderOptions())).rejects.toMatchObject({
      httpStatusCode: 500,
      metadata: {
        transformations: ['width:50', 'resizing_type:fill'],
      },
      message: 'timeout of 30000ms exceeded',
    })
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  describe('headers-phase timeout', () => {
    it('schedules the request timeout via clearable globalThis.setTimeout and clears it on response', async () => {
      const abortTimeoutSpy = vi.spyOn(AbortSignal, 'timeout')
      const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout')
      const clearTimeoutSpy = vi.spyOn(globalThis, 'clearTimeout')

      const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
        new Response('body', {
          headers: { 'content-length': '4', 'content-type': 'image/png' },
        })
      )
      vi.stubGlobal('fetch', fetchMock)

      const TIMEOUT_MS = 17_000
      const { ImageRenderer } = await loadRendererModule({
        imgProxyRequestTimeout: TIMEOUT_MS / 1000,
      })
      const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png'))

      await renderer.getAsset(createRequest(), createRenderOptions())

      expect(
        abortTimeoutSpy,
        'AbortSignal.timeout leaks an unclearable internal timer that also keeps aborting mid-body stream'
      ).not.toHaveBeenCalled()

      const headerTimerIdx = setTimeoutSpy.mock.calls.findIndex(([, ms]) => ms === TIMEOUT_MS)
      expect(
        headerTimerIdx,
        'the header-phase timeout must be scheduled via globalThis.setTimeout so it is clearable'
      ).toBeGreaterThanOrEqual(0)

      const headerTimerId = setTimeoutSpy.mock.results[headerTimerIdx].value
      expect(
        clearTimeoutSpy,
        'the header-phase timer must be cleared once response headers arrive'
      ).toHaveBeenCalledWith(headerTimerId)
    })

    it('clears the header-phase timer on each retry, not just the final one', async () => {
      vi.useFakeTimers()
      const clearTimeoutSpy = vi.spyOn(globalThis, 'clearTimeout')

      const fetchMock = vi
        .fn<typeof fetch>()
        .mockResolvedValueOnce(new Response('try again', { status: 429 }))
        .mockResolvedValueOnce(
          new Response('rendered', {
            headers: { 'content-length': '8', 'content-type': 'image/png' },
          })
        )
      vi.stubGlobal('fetch', fetchMock)

      const TIMEOUT_MS = 17_000
      const { ImageRenderer } = await loadRendererModule({
        imgProxyRequestTimeout: TIMEOUT_MS / 1000,
      })
      const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png'))

      const resultPromise = renderer.getAsset(createRequest(), createRenderOptions())
      await vi.advanceTimersByTimeAsync(50)
      await resultPromise

      const clearsWithTimerId = clearTimeoutSpy.mock.calls.filter(
        ([id]) => id != null && (typeof id === 'object' || typeof id === 'number')
      )
      expect(clearsWithTimerId.length).toBeGreaterThanOrEqual(2)
    })

    it('propagates caller aborts to the fetch signal after headers arrive so body streams tear down', async () => {
      const controller = new AbortController()

      let capturedSignal: AbortSignal | undefined
      const fetchMock = vi.fn<typeof fetch>().mockImplementation((_url, init) => {
        capturedSignal = init?.signal as AbortSignal
        return Promise.resolve(
          new Response('body', {
            headers: { 'content-length': '4', 'content-type': 'image/png' },
          })
        )
      })
      vi.stubGlobal('fetch', fetchMock)

      const { ImageRenderer } = await loadRendererModule()
      const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png'))

      await renderer.getAsset(createRequest(), createRenderOptions(controller.signal))

      expect(capturedSignal!.aborted).toBe(false)

      const abortReason = new DOMException('caller stopped', 'AbortError')
      controller.abort(abortReason)

      expect(capturedSignal!.aborted).toBe(true)
      expect(capturedSignal!.reason).toBe(abortReason)
    })

    it('removes caller abort listener after the body stream finishes', async () => {
      const controller = new AbortController()

      let capturedSignal: AbortSignal | undefined
      const fetchMock = vi.fn<typeof fetch>().mockImplementation((_url, init) => {
        capturedSignal = init?.signal as AbortSignal
        return Promise.resolve(
          new Response('body', {
            headers: { 'content-length': '4', 'content-type': 'image/png' },
          })
        )
      })
      vi.stubGlobal('fetch', fetchMock)

      const { ImageRenderer } = await loadRendererModule()
      const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png'))

      const result = await renderer.getAsset(
        createRequest(),
        createRenderOptions(controller.signal)
      )
      await readStream(result.body)

      controller.abort(new DOMException('late caller abort', 'AbortError'))

      expect(capturedSignal!.aborted).toBe(false)
    })

    it('removes caller abort listener after non-retryable error body is consumed', async () => {
      const controller = new AbortController()

      let capturedSignal: AbortSignal | undefined
      const fetchMock = vi.fn<typeof fetch>().mockImplementation((_url, init) => {
        capturedSignal = init?.signal as AbortSignal
        return Promise.resolve(new Response('invalid image request', { status: 400 }))
      })
      vi.stubGlobal('fetch', fetchMock)

      const { ImageRenderer } = await loadRendererModule()
      const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png'))

      await expect(
        renderer.getAsset(createRequest(), createRenderOptions(controller.signal))
      ).rejects.toMatchObject({
        httpStatusCode: 400,
      })

      controller.abort(new DOMException('late caller abort', 'AbortError'))

      expect(capturedSignal!.aborted).toBe(false)
    })

    it('aborts the body stream when imgproxy stalls between chunks', async () => {
      const IDLE_TIMEOUT_MS = 50

      const stallingBody = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('first-chunk-'))
          // Intentionally never close or enqueue again.
        },
      })
      const fetchMock = vi.fn<typeof fetch>().mockResolvedValue(
        new Response(stallingBody, {
          headers: { 'content-type': 'image/png' },
        })
      )
      vi.stubGlobal('fetch', fetchMock)

      const { ImageRenderer } = await loadRendererModule({
        imgProxyRequestTimeout: IDLE_TIMEOUT_MS / 1000,
      })
      const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png'))

      const result = await renderer.getAsset(createRequest(), createRenderOptions())

      const readError = await readStream(result.body).catch((e: unknown) => e)
      expect(readError).toBeInstanceOf(Error)
      expect((readError as Error).message).toContain('idle timeout')
    })

    it('still enforces the timeout while the header phase is pending', async () => {
      const SHORT_TIMEOUT_MS = 50
      const fetchMock = vi.fn<typeof fetch>().mockImplementation((_url, init) => {
        const signal = init?.signal as AbortSignal
        return new Promise<Response>((_resolve, reject) => {
          signal.addEventListener('abort', () => reject(signal.reason), { once: true })
        })
      })
      vi.stubGlobal('fetch', fetchMock)

      const { ImageRenderer } = await loadRendererModule({
        imgProxyRequestTimeout: SHORT_TIMEOUT_MS / 1000,
      })
      const renderer = new ImageRenderer(createBackend('local:///tmp/cat.png'))

      await expect(renderer.getAsset(createRequest(), createRenderOptions())).rejects.toMatchObject(
        {
          httpStatusCode: 500,
          message: `timeout of ${SHORT_TIMEOUT_MS}ms exceeded`,
        }
      )
    })
  })
})
