import { EventEmitter } from 'node:events'
import * as http from 'node:http'
import * as https from 'node:https'
import type { AddressInfo } from 'node:net'
import { NodeHttpHandler } from '@smithy/node-http-handler'
import { HttpRequest } from '@smithy/protocol-http'
import type { Server } from '@tus/server'
import Fastify, { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import { getConfig } from '../../../config'
import { requestContext } from '../../plugins/request-context'
import {
  authenticatedRoutes,
  createTusLockS3Client,
  handleTusRequestWithIdleTimeout,
  publicRoutes,
} from './index'
import type { MultiPartRequest } from './lifecycle'

describe('TUS S3 clients', () => {
  test('removes the no-op logger middleware from the lock client', () => {
    const httpAgent = new http.Agent()
    const httpsAgent = new https.Agent()
    const client = createTusLockS3Client({ httpAgent, httpsAgent })

    try {
      expect(
        client.middlewareStack
          .identify()
          .some((middleware) => middleware.includes('loggerMiddleware'))
      ).toBe(false)
    } finally {
      client.destroy()
      httpAgent.destroy()
      httpsAgent.destroy()
    }
  })

  test('maps the TUS S3 timeout to Smithy socketTimeout', async () => {
    const server = http.createServer((_req, res) => {
      res.writeHead(200, { 'content-type': 'text/plain' })
      res.end('ok')
    })
    await new Promise<void>((resolve) => {
      server.listen(0, '127.0.0.1', resolve)
    })
    const port = (server.address() as AddressInfo).port

    const httpAgent = new http.Agent()
    const httpsAgent = new https.Agent()
    const client = createTusLockS3Client({ httpAgent, httpsAgent })

    try {
      const handler = client.config.requestHandler
      expect(handler).toBeInstanceOf(NodeHttpHandler)
      if (!(handler instanceof NodeHttpHandler)) {
        throw new Error('expected NodeHttpHandler')
      }

      await handler.handle(
        new HttpRequest({
          protocol: 'http:',
          hostname: '127.0.0.1',
          port,
          method: 'GET',
          path: '/',
          headers: { host: `127.0.0.1:${port}` },
        })
      )

      expect(handler.httpHandlerConfigs()).toMatchObject({
        connectionTimeout: 5000,
        socketTimeout: getConfig().tusS3ClientTimeout,
      })
      expect(handler.httpHandlerConfigs().requestTimeout).toBeUndefined()
    } finally {
      client.destroy()
      httpAgent.destroy()
      httpsAgent.destroy()
      await new Promise<void>((resolve, reject) => {
        server.close((error) => {
          if (error) {
            reject(error)
            return
          }
          resolve()
        })
      })
    }
  })
})

describe('TUS route request context', () => {
  let app: FastifyInstance
  let observedUpload: MultiPartRequest['upload'] | undefined
  let handle: ReturnType<typeof vi.fn>
  let disposalsBeforeErrorReply: number | undefined

  beforeEach(async () => {
    observedUpload = undefined
    disposalsBeforeErrorReply = undefined

    app = Fastify()
    app.decorateRequest('tenantId')
    app.decorateRequest('owner')
    app.decorateRequest('db')
    app.decorateRequest('storage')

    await app.register(requestContext)

    app.addHook('onRequest', async (request) => {
      request.tenantId = 'tenant-123'
      request.owner = 'owner-123'
      request.db = { dispose: vi.fn() } as never
      request.storage = {
        backend: {},
        db: {},
        location: {},
      } as never
    })
    app.addHook('onError', async (request, reply) => {
      expect(reply.raw.headersSent).toBe(false)
      disposalsBeforeErrorReply = vi.mocked(request.db.dispose).mock.calls.length
    })

    handle = vi.fn(async (rawReq, rawRes) => {
      observedUpload = (rawReq as MultiPartRequest).upload
      rawRes.statusCode = 204
      rawRes.end()
    })

    await app.register(publicRoutes, {
      tusServer: { handle } as unknown as Server,
      signed: false,
    })
    await app.register(authenticatedRoutes, {
      tusServer: { handle } as unknown as Server,
      signed: false,
    })
  })

  afterEach(async () => {
    await app.close()
  })

  it('threads sbReqId onto the public route raw upload context', async () => {
    const response = await app.inject({
      method: 'OPTIONS',
      url: '/public/object',
      headers: {
        'sb-request-id': 'sb-req-123',
        'x-upsert': 'true',
      },
    })

    expect(response.statusCode).toBe(204)
    expect(observedUpload).toMatchObject({
      owner: 'owner-123',
      tenantId: 'tenant-123',
      isUpsert: true,
      sbReqId: 'sb-req-123',
    })
  })

  it('disposes the db when the response closes', async () => {
    await app.inject({ method: 'OPTIONS', url: '/public/object' })
    expect(observedUpload?.db.dispose).toHaveBeenCalled()
  })

  it.each(['HEAD', 'DELETE'] as const)(
    'releases the authenticated %s lease before replying to a pre-header failure',
    async (method) => {
      handle.mockRejectedValueOnce(new Error('TUS failed before writing headers'))

      const response = await app.inject({ method, url: '/object' })

      expect(response.statusCode).toBe(500)
      expect(handle).toHaveBeenCalledOnce()
      expect(disposalsBeforeErrorReply).toBe(1)
    }
  )

  it('keeps the db until the handler settles after the client disconnects', async () => {
    const started = Promise.withResolvers<void>()
    const disconnectSeen = Promise.withResolvers<{
      disposed: number
      socketDestroyed: boolean
      headersSent: boolean
    }>()
    let releaseHandler: () => void = () => undefined
    const release = new Promise<void>((resolve) => {
      releaseHandler = resolve
    })

    handle.mockImplementationOnce(async (rawReq, rawRes) => {
      observedUpload = (rawReq as MultiPartRequest).upload
      rawReq.on('error', () => undefined)
      rawRes.on('error', () => undefined)
      const closed = new Promise<void>((resolve) => {
        rawRes.once('close', () => resolve())
      })
      started.resolve()
      await closed
      disconnectSeen.resolve({
        disposed: vi.mocked(observedUpload.db.dispose).mock.calls.length,
        socketDestroyed: rawReq.socket?.destroyed === true,
        headersSent: rawRes.headersSent,
      })
      await release
    })

    await app.listen({ host: '127.0.0.1', port: 0 })
    const { port } = app.server.address() as AddressInfo
    const client = http.request({
      host: '127.0.0.1',
      port,
      method: 'OPTIONS',
      path: '/public/object',
    })
    client.on('error', () => undefined)
    client.end()

    await started.promise
    client.destroy()
    await expect(disconnectSeen.promise).resolves.toEqual({
      disposed: 0,
      socketDestroyed: true,
      headersSent: false,
    })

    releaseHandler()
    await handle.mock.results[0]?.value
    await new Promise((resolve) => setImmediate(resolve))

    expect(observedUpload?.db.dispose).toHaveBeenCalledOnce()
  })
})

class FakeSocket {
  bytesRead = 0
}

class FakeIncomingMessage extends EventEmitter {
  socket: FakeSocket | undefined = new FakeSocket()
  complete = false
  readableEnded = false
  readableLength = 0
  destroyed = false
  headers: Record<string, string> = {}
  executionError?: Error
  destroy = vi.fn((_err?: Error) => {
    this.destroyed = true
  })
}

describe('handleTusRequestWithIdleTimeout', () => {
  const { tusBodyIdleTimeoutMs } = getConfig()

  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  function createReqRes(headers: Record<string, string> = {}) {
    const raw = new FakeIncomingMessage()
    raw.headers = headers
    const req = { raw } as unknown as FastifyRequest
    const res = { raw: {} } as unknown as FastifyReply
    return { req, res, raw }
  }

  // A `handle` that never resolves on its own, so the idle-check timers
  // get a chance to fire before the wrapper's own `finally` disarms them.
  function pendingHandle() {
    return vi.fn(() => new Promise<void>(() => {}))
  }

  test.each([false, true])(
    'releases the lease on pre-header errors, body timer=%s',
    async (hasBody) => {
      const { req, res } = createReqRes(hasBody ? { 'content-length': '10' } : {})
      const dispose = vi.fn()
      req.db = { dispose } as never
      const pending = Promise.withResolvers<void>()
      const expectedError = new Error('TUS failed before writing headers')
      const tusServer = { handle: vi.fn(() => pending.promise) } as unknown as Server
      const failure = handleTusRequestWithIdleTimeout(tusServer, req, res).catch((error) => error)
      try {
        expect(req.dbLeaseOwnedByHandler).toBe(true)
        expect(dispose).not.toHaveBeenCalled()
      } finally {
        pending.reject(expectedError)
      }
      expect(await failure).toBe(expectedError)
      expect(dispose).toHaveBeenCalledOnce()
      expect(vi.getTimerCount()).toBe(0)
    }
  )

  async function withTusBodyIdleTimeoutMs<T>(
    value: number,
    run: (indexModule: typeof import('./index')) => Promise<T>
  ): Promise<T> {
    vi.resetModules()
    const actualConfig = await vi.importActual<typeof import('../../../config')>('../../../config')
    vi.doMock('../../../config', () => ({
      ...actualConfig,
      getConfig: () => ({ ...actualConfig.getConfig(), tusBodyIdleTimeoutMs: value }),
    }))
    try {
      return await run(await import('./index'))
    } finally {
      vi.doUnmock('../../../config')
      vi.resetModules()
    }
  }

  test('skips arming the idle timer when tusBodyIdleTimeoutMs is configured to 0 (disabled)', async () => {
    await withTusBodyIdleTimeoutMs(
      0,
      async ({ handleTusRequestWithIdleTimeout: handleWithDisabledTimeout }) => {
        const setTimeoutSpy = vi.spyOn(global, 'setTimeout')
        const handle = vi.fn().mockResolvedValue(undefined)
        const { req, res, raw } = createReqRes({ 'content-length': '10' })
        const tusServer = { handle } as unknown as Server

        await handleWithDisabledTimeout(tusServer, req, res)

        expect(handle).toHaveBeenCalledWith(raw, res.raw)
        expect(setTimeoutSpy).not.toHaveBeenCalled()
      }
    )
  })

  test('skips arming the idle timer when the request declares no body', async () => {
    const setTimeoutSpy = vi.spyOn(global, 'setTimeout')
    const handle = vi.fn().mockResolvedValue(undefined)
    const { req, res, raw } = createReqRes()
    const tusServer = { handle } as unknown as Server

    await handleTusRequestWithIdleTimeout(tusServer, req, res)

    expect(handle).toHaveBeenCalledWith(raw, res.raw)
    expect(setTimeoutSpy).not.toHaveBeenCalled()
  })

  test('skips arming the idle timer when the body is already fully received', async () => {
    const setTimeoutSpy = vi.spyOn(global, 'setTimeout')
    const handle = vi.fn().mockResolvedValue(undefined)
    const { req, res, raw } = createReqRes({ 'content-length': '10' })
    raw.complete = true
    const tusServer = { handle } as unknown as Server

    await handleTusRequestWithIdleTimeout(tusServer, req, res)

    expect(handle).toHaveBeenCalledWith(raw, res.raw)
    expect(setTimeoutSpy).not.toHaveBeenCalled()
  })

  test('destroys the connection when no bytes arrive before the idle check fires', async () => {
    const handle = pendingHandle()
    const { req, res, raw } = createReqRes({ 'content-length': '10' })
    const tusServer = { handle } as unknown as Server

    void handleTusRequestWithIdleTimeout(tusServer, req, res)
    await vi.advanceTimersByTimeAsync(tusBodyIdleTimeoutMs)

    expect(raw.destroy).toHaveBeenCalledTimes(1)
    expect(raw.destroy.mock.calls[0][0]).toMatchObject({
      message: 'TUS request body idle timeout - no bytes received',
    })
    expect(raw.executionError).toBe(raw.destroy.mock.calls[0][0])
  })

  test('reschedules instead of timing out while bytesRead keeps increasing, then times out once it stalls', async () => {
    const handle = pendingHandle()
    const { req, res, raw } = createReqRes({ 'content-length': '10' })
    const tusServer = { handle } as unknown as Server

    void handleTusRequestWithIdleTimeout(tusServer, req, res)

    // Bytes arrive just before the first check - should reschedule, not time out.
    raw.socket!.bytesRead = 5
    await vi.advanceTimersByTimeAsync(tusBodyIdleTimeoutMs)
    expect(raw.destroy).not.toHaveBeenCalled()

    // No further bytes arrive during the second window - now it should time out.
    await vi.advanceTimersByTimeAsync(tusBodyIdleTimeoutMs)
    expect(raw.destroy).toHaveBeenCalledTimes(1)
  })

  test('reschedules while the destination write is draining a backpressure backlog, then times out once the backlog is empty and nothing else arrives', async () => {
    const handle = pendingHandle()
    const { req, res, raw } = createReqRes({ 'content-length': '10' })
    const tusServer = { handle } as unknown as Server

    void handleTusRequestWithIdleTimeout(tusServer, req, res)

    // Client burst fills the buffer faster than the destination write can
    // drain it: bytesRead and readableLength climb together.
    raw.socket!.bytesRead = 100_000
    raw.readableLength = 65_536
    await vi.advanceTimersByTimeAsync(tusBodyIdleTimeoutMs)
    expect(raw.destroy).not.toHaveBeenCalled()

    // bytesRead stops growing, but the destination write is still actively draining the backlog
    raw.readableLength = 32_768
    await vi.advanceTimersByTimeAsync(tusBodyIdleTimeoutMs)
    expect(raw.destroy).not.toHaveBeenCalled()

    // no change in bytesRead or readableLength, but readableLength still > 0
    await vi.advanceTimersByTimeAsync(tusBodyIdleTimeoutMs)
    expect(raw.destroy).not.toHaveBeenCalled()

    // The last bit of draining still shows up as bytesConsumed increasing, even though readableLength itself is now zero.
    raw.readableLength = 0
    await vi.advanceTimersByTimeAsync(tusBodyIdleTimeoutMs)
    expect(raw.destroy).not.toHaveBeenCalled()

    // A full window passes with the backlog empty and nothing new arriving from the client
    await vi.advanceTimersByTimeAsync(tusBodyIdleTimeoutMs)
    expect(raw.destroy).toHaveBeenCalledTimes(1)
  })

  test('skips arming the idle timer when the request is already destroyed', async () => {
    const setTimeoutSpy = vi.spyOn(global, 'setTimeout')
    const handle = vi.fn().mockResolvedValue(undefined)
    const { req, res, raw } = createReqRes({ 'content-length': '10' })
    raw.destroyed = true
    const tusServer = { handle } as unknown as Server

    await handleTusRequestWithIdleTimeout(tusServer, req, res)

    expect(handle).toHaveBeenCalledWith(raw, res.raw)
    expect(setTimeoutSpy).not.toHaveBeenCalled()
  })

  test('disarms without re-destroying if the connection is destroyed by something else before the idle check runs', async () => {
    const handle = pendingHandle()
    const { req, res, raw } = createReqRes({ 'content-length': '10' })
    const tusServer = { handle } as unknown as Server

    void handleTusRequestWithIdleTimeout(tusServer, req, res)
    raw.destroyed = true
    await vi.advanceTimersByTimeAsync(tusBodyIdleTimeoutMs)

    expect(raw.destroy).not.toHaveBeenCalled()
  })

  test('disarms the idle timer on an abrupt disconnect, which closes without ever emitting end', async () => {
    const handle = pendingHandle()
    const { req, res, raw } = createReqRes({ 'content-length': '10' })
    const tusServer = { handle } as unknown as Server

    void handleTusRequestWithIdleTimeout(tusServer, req, res)
    raw.emit('close')

    await vi.advanceTimersByTimeAsync(tusBodyIdleTimeoutMs * 2)
    expect(raw.destroy).not.toHaveBeenCalled()
  })

  test('disarms without destroying the connection if the idle check runs as the body finishes arriving', async () => {
    const handle = pendingHandle()
    const { req, res, raw } = createReqRes({ 'content-length': '10' })
    const tusServer = { handle } as unknown as Server

    void handleTusRequestWithIdleTimeout(tusServer, req, res)
    // The body finishes arriving in the same window the idle check runs,
    // racing the 'end' listener that would otherwise disarm it first.
    raw.complete = true
    await vi.advanceTimersByTimeAsync(tusBodyIdleTimeoutMs)

    expect(raw.destroy).not.toHaveBeenCalled()
    expect(raw.executionError).toBeUndefined()
  })
})
