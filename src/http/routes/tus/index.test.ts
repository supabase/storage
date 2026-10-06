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
import { createTusLockS3Client, handleTusRequestWithIdleTimeout, publicRoutes } from './index'
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

  test('maps STORAGE_S3_CLIENT_TIMEOUT to Smithy socketTimeout', async () => {
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
        socketTimeout: getConfig().storageS3ClientTimeout,
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

describe('public tus route request context', () => {
  let app: FastifyInstance
  let observedUpload: MultiPartRequest['upload'] | undefined

  beforeEach(async () => {
    observedUpload = undefined

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

    await app.register(publicRoutes, {
      tusServer: {
        handle: vi.fn(async (rawReq, rawRes) => {
          observedUpload = (rawReq as MultiPartRequest).upload
          rawRes.statusCode = 204
          rawRes.end()
        }),
      } as unknown as Server,
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
})

class FakeSocket extends EventEmitter {
  setTimeout = vi.fn((_ms: number, cb?: () => void) => {
    if (cb) this.on('timeout', cb)
    return this
  })
}

class FakeIncomingMessage extends EventEmitter {
  socket: FakeSocket | undefined = new FakeSocket()
  complete = false
  readableEnded = false
  headers: Record<string, string> = {}
  executionError?: Error
  destroy = vi.fn()
}

describe('handleTusRequestWithIdleTimeout', () => {
  function createReqRes(headers: Record<string, string> = {}) {
    const raw = new FakeIncomingMessage()
    raw.headers = headers
    const req = { raw } as unknown as FastifyRequest
    const res = { raw: {} } as unknown as FastifyReply
    return { req, res, raw }
  }

  test('skips arming the idle timer when the request declares no body', async () => {
    const handle = vi.fn().mockResolvedValue(undefined)
    const { req, res, raw } = createReqRes()
    const tusServer = { handle } as unknown as Server

    await handleTusRequestWithIdleTimeout(tusServer, req, res)

    expect(handle).toHaveBeenCalledWith(raw, res.raw)
    expect(raw.socket?.setTimeout).not.toHaveBeenCalled()
  })

  test('skips arming the idle timer when the body is already fully received', async () => {
    const handle = vi.fn().mockResolvedValue(undefined)
    const { req, res, raw } = createReqRes({ 'content-length': '10' })
    raw.complete = true
    const tusServer = { handle } as unknown as Server

    await handleTusRequestWithIdleTimeout(tusServer, req, res)

    expect(handle).toHaveBeenCalledWith(raw, res.raw)
    expect(raw.socket?.setTimeout).not.toHaveBeenCalled()
  })

  test('destroys the connection when the idle timer fires before the body completes', async () => {
    const handle = vi.fn().mockResolvedValue(undefined)
    const { req, res, raw } = createReqRes({ 'content-length': '10' })
    const tusServer = { handle } as unknown as Server

    const pending = handleTusRequestWithIdleTimeout(tusServer, req, res)
    // Fire the timer before `handle` resolves, simulating a client that
    // stopped sending bytes partway through the body.
    raw.socket?.emit('timeout')
    await pending

    expect(raw.destroy).toHaveBeenCalledTimes(1)
    expect(raw.destroy.mock.calls[0][0]).toMatchObject({
      message: 'TUS request body idle timeout - no bytes received',
    })
    expect(raw.executionError).toBe(raw.destroy.mock.calls[0][0])
  })

  test('disarms without destroying the connection if the idle timer fires as the body finishes arriving', async () => {
    const handle = vi.fn().mockResolvedValue(undefined)
    const { req, res, raw } = createReqRes({ 'content-length': '10' })
    const tusServer = { handle } as unknown as Server

    const pending = handleTusRequestWithIdleTimeout(tusServer, req, res)
    // The body finishes arriving in the same window the idle timer fires,
    // racing the 'end' listener that would otherwise disarm it first.
    raw.complete = true
    raw.socket?.emit('timeout')
    await pending

    expect(raw.destroy).not.toHaveBeenCalled()
    expect(raw.executionError).toBeUndefined()
  })
})
