import { execFileSync } from 'node:child_process'
import { createHash, randomBytes } from 'node:crypto'
import { subscribe, unsubscribe } from 'node:diagnostics_channel'
import { once } from 'node:events'
import { mkdtempSync, readFileSync, rmSync } from 'node:fs'
import * as http from 'node:http'
import * as https from 'node:https'
import { type AddressInfo, createServer as createTcpServer, Socket } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { type Duplex, PassThrough, Readable } from 'node:stream'
import * as stream from 'node:stream/promises'
import tls from 'node:tls'
import {
  CompleteMultipartUploadCommand,
  CopyObjectCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  S3Client,
  type S3ClientConfig,
  UploadPartCommand,
  UploadPartCopyCommand,
} from '@aws-sdk/client-s3'
import { normalizeRawError, StorageBackendError } from '@internal/errors'
import { ConfiguredRetryStrategy } from '@smithy/core/retry'
import { HttpRequest } from '@smithy/protocol-http'
import type { HttpHandlerOptions } from '@smithy/types'
import { UndiciHttpHandler } from '@smithy/undici-http-handler'
import type { Agent, buildConnector, Dispatcher } from 'undici'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'

const { busy, free, pending, errors, agentOptions, connectorOptions } = vi.hoisted(() => ({
  busy: { record: vi.fn() },
  free: { record: vi.fn() },
  pending: { record: vi.fn() },
  errors: { record: vi.fn() },
  agentOptions: [] as Agent.Options[],
  connectorOptions: [] as buildConnector.BuildOptions[],
}))

vi.mock('undici', async (importOriginal) => {
  const undici = await importOriginal<typeof import('undici')>()
  return {
    ...undici,
    buildConnector: (options: buildConnector.BuildOptions) => {
      connectorOptions.push(options)
      return undici.buildConnector(options)
    },
    Agent: class extends undici.Agent {
      constructor(options: Agent.Options) {
        super(options)
        agentOptions.push(options)
      }

      dispatch(opts: Dispatcher.DispatchOptions, handler: Dispatcher.DispatchHandler) {
        if (!(opts.headers as Record<string, string> | undefined)?.['x-test-connect-timeout']) {
          return super.dispatch(opts, handler)
        }
        const controller = { abort() {}, pause() {}, resume() {}, paused: false, aborted: false }
        handler.onRequestStart?.(controller as never, {})
        handler.onResponseError?.(
          controller as never,
          new undici.errors.ConnectTimeoutError(
            'Connect Timeout Error (attempted address: 10.0.0.1:443, timeout: 5000ms)'
          )
        )
        return true
      }
    },
  }
})

vi.mock('@internal/monitoring/metrics', () => ({
  httpPoolBusySockets: busy,
  httpPoolFreeSockets: free,
  httpPoolPendingRequests: pending,
  httpPoolErrors: errors,
}))

import { createAgent, type InstrumentedAgent } from './agent'

let server: http.Server
let origin: string
let handler: http.RequestListener = () => {}

async function listen(listener: http.RequestListener) {
  const s = http.createServer(listener)
  await new Promise<void>((r) => s.listen(0, '127.0.0.1', r))
  return { server: s, origin: `http://127.0.0.1:${(s.address() as AddressInfo).port}` }
}

async function stop(s: http.Server) {
  s.closeAllConnections()
  await new Promise((r) => s.close(r))
}

function createS3Client(
  agent: InstrumentedAgent,
  maxAttempts = 3,
  endpoint = origin,
  extra: Partial<S3ClientConfig> = {}
) {
  return new S3Client({
    ...extra,
    endpoint,
    region: 'us-east-1',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    forcePathStyle: true,
    maxAttempts,
    retryStrategy: new ConfiguredRetryStrategy(maxAttempts, () => 0),
    requestHandler: new UndiciHttpHandler({ dispatcher: agent.dispatcher }),
  })
}

describe.concurrent('createAgent failure classification', () => {
  const list = {
    operation: 'list',
    send: (client: S3Client) => client.send(new ListObjectsV2Command({ Bucket: 'bucket' })),
  }
  const complete = {
    operation: 'complete',
    send: (client: S3Client) =>
      client.send(
        new CompleteMultipartUploadCommand({ Bucket: 'bucket', Key: 'key', UploadId: 'upload' })
      ),
  }
  const deleteObjects = {
    operation: 'delete',
    send: (client: S3Client) =>
      client.send(
        new DeleteObjectsCommand({
          Bucket: 'bucket',
          Delete: {
            Objects: Array.from({ length: 1000 }, (_, i) => ({ Key: 'k'.repeat(100) + i })),
          },
        })
      ),
  }
  const failures: Record<
    string,
    { failure: string; name: string; code: string; causeCode?: string; attempts?: number }
  > = {
    socket: {
      failure: 'socket',
      name: 'Error',
      code: 'ECONNRESET',
      causeCode: 'UND_ERR_SOCKET',
    },
    length: {
      failure: 'length',
      name: 'Error',
      code: 'ECONNRESET',
      causeCode: 'UND_ERR_RES_CONTENT_LENGTH_MISMATCH',
    },
    body: {
      failure: 'body',
      name: 'Error',
      code: 'ECONNRESET',
      causeCode: 'UND_ERR_BODY_TIMEOUT',
      attempts: 2,
    },
    preSocket: {
      failure: 'pre-socket',
      name: 'TimeoutError',
      code: 'ECONNRESET',
      causeCode: 'UND_ERR_SOCKET',
    },
    preReset: {
      failure: 'pre-reset',
      name: 'TimeoutError',
      code: 'ECONNRESET',
    },
    reset: {
      failure: 'reset',
      name: 'Error',
      code: 'ECONNRESET',
    },
    headers: {
      failure: 'headers',
      name: 'TimeoutError',
      code: 'UND_ERR_HEADERS_TIMEOUT',
      attempts: 2,
    },
  }

  it.each([
    // Only the timeout under test is short, so a starved event loop cannot fire another one first,
    // and the header timeout leaves the server time to count the request.
    { ...list, ...failures.socket, requestTimeoutMs: 0 },
    { ...list, ...failures.socket, requestTimeoutMs: 5000 },
    { ...list, ...failures.length, requestTimeoutMs: 0 },
    { ...list, ...failures.length, requestTimeoutMs: 5000 },
    // CompleteMultipartUpload also buffers the body in throw200ExceptionsMiddleware.
    { ...complete, ...failures.body, requestTimeoutMs: 5000 },
    { ...deleteObjects, ...failures.socket, requestTimeoutMs: 5000 },
    { ...list, ...failures.preSocket, requestTimeoutMs: 5000 },
    { ...list, ...failures.preReset, requestTimeoutMs: 5000 },
    { ...list, ...failures.reset, requestTimeoutMs: 5000 },
    { ...list, ...failures.headers, requestTimeoutMs: 1000 },
  ])(
    'retries $operation XML after a $failure failure with timeout $requestTimeoutMs',
    async ({ send, failure, requestTimeoutMs, name, code, causeCode, attempts = 3 }) => {
      let requests = 0
      const s3 = await listen((req, res) => {
        req.resume()
        req.once('end', () => {
          requests++
          if (failure === 'headers') return
          if (failure === 'pre-socket') return req.socket.end()
          if (failure === 'pre-reset') return req.socket.resetAndDestroy()
          if (failure === 'length') {
            res.writeHead(200, { 'content-length': '1000', connection: 'close' })
            res.end(' ')
          } else {
            res.writeHead(200, { 'content-type': 'application/xml' })
            res.write(' ', () => {
              if (failure === 'socket') req.socket.end()
              if (failure === 'reset') req.socket.resetAndDestroy()
            })
          }
        })
      })
      const bodyTimeoutMs = failure === 'body' ? 100 : undefined
      const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs, bodyTimeoutMs })
      const client = createS3Client(agent, attempts, s3.origin)
      try {
        const error = await send(client).catch((error: unknown) => error)
        expect(error).toMatchObject({
          name,
          code,
          $metadata: { attempts },
        })
        if (causeCode) expect(error).toMatchObject({ cause: { code: causeCode } })
        const wrapped = StorageBackendError.fromError(error)
        expect(wrapped.render()).toMatchObject({
          statusCode: '500',
          code: 'S3Error',
          message: name,
        })
        expect(normalizeRawError(error, 'info')).toMatchObject({ name })
        expect(JSON.parse(normalizeRawError(wrapped, 'info').raw ?? '').originalError.name).toBe(
          name
        )
        expect(requests).toBe(attempts)
      } finally {
        await agent.close()
        client.destroy()
        await stop(s3.server)
      }
    }
  )

  it.each(['socket', 'length', 'body'] as const)(
    'preserves the GetObject %s error name without retrying after returning the stream',
    async (failure) => {
      let requests = 0
      const s3 = await listen((req, res) => {
        requests++
        res.writeHead(
          200,
          failure === 'length' ? { 'content-length': '2', connection: 'close' } : {}
        )
        if (failure === 'length') return res.end('a')
        res.write('a', () => {
          if (failure !== 'body') req.socket.end()
        })
      })
      // Only the timeout under test is short, so a starved event loop cannot fire another one first.
      const bodyTimeoutMs = failure === 'body' ? 100 : undefined
      const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 5000, bodyTimeoutMs })
      const client = createS3Client(agent, 3, s3.origin)
      try {
        const result = await client.send(new GetObjectCommand({ Bucket: 'bucket', Key: 'key' }))
        expect(result.$metadata.attempts).toBe(1)
        await expect(result.Body!.transformToString()).rejects.toMatchObject({
          name: 'Error',
          code: 'ECONNRESET',
          cause: { code: failures[failure].causeCode },
        })
        expect(requests).toBe(1)
      } finally {
        await agent.close()
        client.destroy()
        await stop(s3.server)
      }
    }
  )

  it('allows buffered bodies whose socket write outlasts the inactivity timeout', async () => {
    const intervals: NodeJS.Timeout[] = []
    const received: number[] = []
    const slowReadBytes = 6 * 1024 * 1024
    // Outlast the timer while leaving a tail larger than the socket buffers, then drain at once.
    const s3 = await listen((req, res) => {
      let count = 0
      received.push(0)
      const index = received.length - 1
      const iv = setInterval(() => {
        const chunk = req.read(64 * 1024)
        if (!chunk) return
        received[index] = count += chunk.length
        if (count >= slowReadBytes) {
          clearInterval(iv)
          req.on('data', (data: Buffer) => {
            received[index] = count += data.length
          })
        }
      }, 20)
      intervals.push(iv)
      req.on('end', () => {
        clearInterval(iv)
        res.end('ok')
      })
      req.on('close', () => clearInterval(iv))
    })
    const agent = createAgent('t', { maxSockets: 3, requestTimeoutMs: 1500 })
    const text = 'é'.repeat(16 * 1024 * 1024)
    const buffer = Buffer.from(text)
    const send = (body: Buffer | Readable | string) =>
      agent.dispatcher.request({
        origin: s3.origin,
        path: '/',
        method: 'PUT',
        headers: { 'content-length': String(buffer.length) },
        body,
        signal: AbortSignal.timeout(10000),
      })
    try {
      const responses = await Promise.all([send(buffer), send(Readable.from([buffer])), send(text)])
      expect(await Promise.all(responses.map((r) => r.body.text()))).toEqual(['ok', 'ok', 'ok'])
      expect(received).toEqual([buffer.length, buffer.length, buffer.length])
    } finally {
      intervals.forEach(clearInterval)
      await agent.close()
      await stop(s3.server)
    }
  }, 10000)
})

describe('createAgent', () => {
  it.each(['immediate', 'deferred'])(
    'settles an invalid origin on the %s dispatch path',
    async (path) => {
      handler = (_req, res) => res.end('ok')
      const agent = createAgent('t', { maxSockets: 1 })
      const client = new UndiciHttpHandler({ dispatcher: agent.dispatcher })
      const invalid = new HttpRequest({
        protocol: 'http:',
        hostname: 'bad host',
        method: 'GET',
        path: '/',
        headers: {},
      })
      let release: (() => void) | undefined
      let immediate: ReturnType<typeof vi.spyOn> | undefined
      try {
        if (path === 'deferred') {
          await getText(agent, '/')
          immediate = vi.spyOn(globalThis, 'setImmediate')
        }
        const invalidRequest = client.handle(invalid)
        if (path === 'deferred') expect(immediate).toHaveBeenCalledOnce()
        await expect(invalidRequest).rejects.toMatchObject({ code: 'ERR_INVALID_URL' })

        handler = (_req, res) => {
          release = () => res.end('ok')
        }
        const pending = agent.dispatcher.request({ origin, path: '/', method: 'GET' })
        await vi.waitFor(() => expect(release).toBeDefined())
        let closed = false
        const closing = agent.close().then(() => {
          closed = true
        })
        await new Promise((resolve) => setImmediate(resolve))
        expect(closed).toBe(false)
        release?.()
        await (await pending).body.text()
        await expect(closing).resolves.toBeUndefined()
      } finally {
        immediate?.mockRestore()
        release?.()
        await agent.dispatcher.destroy()
      }
    }
  )

  it('settles an invalid dispatch when the error handler throws', async () => {
    const agent = createAgent('t', { maxSockets: 1 })
    const failure = new Error('error handler failed')
    try {
      expect(() =>
        agent.dispatcher.dispatch(
          { origin: 'http://bad host', path: '/', method: 'GET' },
          {
            onResponseError() {
              throw failure
            },
          }
        )
      ).toThrow(failure)
      await expect(agent.close()).resolves.toBeUndefined()
    } finally {
      await agent.dispatcher.destroy()
    }
  })

  const put = (
    agent: InstrumentedAgent,
    body: Readable,
    contentLength?: number | string,
    signal = AbortSignal.timeout(3000)
  ) =>
    agent.dispatcher.request({
      origin,
      path: '/',
      method: 'PUT',
      headers: contentLength === undefined ? {} : { 'content-length': String(contentLength) },
      body,
      signal,
    })

  const getText = async (agent: InstrumentedAgent, path: string) => {
    const { body } = await agent.dispatcher.request({ origin, path, method: 'GET' })
    return body.text()
  }

  async function occupy(agent: InstrumentedAgent, onRequest?: (req: http.IncomingMessage) => void) {
    let release: (() => void) | undefined
    handler = (req, res) => {
      onRequest?.(req)
      release = () => res.end('ok')
    }
    const hold = agent.dispatcher.request({ origin, path: '/', method: 'GET', headersTimeout: 0 })
    void hold.catch(() => {})
    await vi.waitFor(() => expect(release).toBeDefined())
    return { hold, release: () => release?.() }
  }

  function expectErrors(count: number, type: string) {
    vi.advanceTimersByTime(5000)
    expect(errors.record).toHaveBeenCalledWith(count, expect.objectContaining({ type }))
  }

  const disposables: Array<() => unknown> = []
  function own<T extends InstrumentedAgent | Readable | S3Client>(resource: T): T {
    // Destroying the client closes the dispatcher, so it must go after agent.close().
    if (resource instanceof S3Client) disposables.unshift(() => resource.destroy())
    else disposables.push(() => ('dispatcher' in resource ? resource.close() : resource.destroy()))
    return resource
  }

  beforeEach(async () => {
    agentOptions.length = 0
    connectorOptions.length = 0
    ;({ server, origin } = await listen((req, res) => handler(req, res)))
  })

  afterEach(async () => {
    vi.useRealTimers()
    server.closeAllConnections()
    for (const dispose of disposables.splice(0).reverse()) await dispose()
    await stop(server)
  })

  it.each([1, 2])(
    'keeps serving after requests fail synchronously on an invalid header value with %s sockets',
    async (maxSockets) => {
      handler = (_req, res) => res.end('ok')
      const agent = own(createAgent('t', { maxSockets }))
      for (let i = 0; i < 3; i++) {
        await expect(
          agent.dispatcher.request({
            origin,
            path: '/',
            method: 'PUT',
            headers: { 'cache-control': 'max-age=Ā' },
            body: 'x',
          })
        ).rejects.toMatchObject({ code: 'UND_ERR_INVALID_ARG' })
      }
      const { statusCode, body } = await agent.dispatcher.request({
        origin,
        path: '/',
        method: 'GET',
      })
      await body.dump()
      expect(statusCode).toBe(200)
    }
  )

  it.each([0, 100])(
    'keeps upload source errors unchanged with timeout %s',
    async (requestTimeoutMs) => {
      let received = false
      handler = (req) => {
        req.on('data', () => {
          received = true
        })
      }
      const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs }))
      const input = own(new PassThrough())
      input.write('a')
      const request = put(agent, input, '2')
      const rejection = request.then(
        () => undefined,
        (error: unknown) => error
      )
      await vi.waitFor(() => expect(received).toBe(true))
      const sourceError = Object.assign(new Error('aborted'), { code: 'ECONNRESET' })
      input.destroy(sourceError)
      expect(await rejection).toBe(sourceError)
      expect(sourceError.name).toBe('Error')
    }
  )

  it('does not retry a 304 carrying Content-Length', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    let requests = 0
    handler = (_req, res) => {
      requests++
      res.writeHead(304, { 'content-length': '11' })
      res.end()
    }
    const agent = own(createAgent('t', { maxSockets: 1 }))
    agent.monitor()
    const client = own(createS3Client(agent))
    const outcome = await client
      .send(new GetObjectCommand({ Bucket: 'bucket', Key: 'key', IfNoneMatch: '"e1"' }))
      .catch((e: unknown) => e)
    expect(outcome).toMatchObject({ $metadata: { httpStatusCode: 304, attempts: 1 } })
    expect(requests).toBe(1)
    await new Promise((r) => setTimeout(r, 20))
    vi.advanceTimersByTime(5000)
    expect(errors.record).toHaveBeenCalledWith(0, {
      name: 't',
      protocol: 'http',
      type: 'socket_error',
    })
  })

  it('lets a copy outlast the inactivity timeout while its headers are pending', async () => {
    let requests = 0
    handler = (_req, res) => {
      requests++
      setTimeout(() => {
        res.writeHead(200, { 'content-type': 'application/xml' })
        res.end('<CopyObjectResult><ETag>"e"</ETag></CopyObjectResult>')
      }, 1500)
    }
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 100, copyTimeoutMs: 0 }))
    const client = own(createS3Client(agent))
    const result = await client.send(
      new CopyObjectCommand({ Bucket: 'bucket', Key: 'dst', CopySource: 'bucket/src' })
    )
    expect(result.$metadata.attempts).toBe(1)
    expect(requests).toBe(1)
  })

  const copyObject = (client: S3Client, options: HttpHandlerOptions) =>
    client.send(
      new CopyObjectCommand({ Bucket: 'bucket', Key: 'dst', CopySource: 'bucket/src' }),
      options
    )
  it.each([
    { operation: 'CopyObject', copyTimeoutMs: 100, send: copyObject },
    {
      operation: 'UploadPartCopy',
      copyTimeoutMs: 100,
      send: (client: S3Client, options: HttpHandlerOptions) =>
        client.send(
          new UploadPartCopyCommand({
            Bucket: 'bucket',
            Key: 'dst',
            CopySource: 'bucket/src',
            UploadId: 'upload',
            PartNumber: 1,
          }),
          options
        ),
    },
    {
      operation: 'CopyObject without a copy-specific timeout',
      copyTimeoutMs: undefined,
      send: copyObject,
    },
  ])('applies the configured timeout to $operation', async ({ send, copyTimeoutMs }) => {
    handler = (req, res) => {
      if (req.method === 'HEAD') res.writeHead(200).end()
      else req.resume()
    }
    // A copy timeout, when set, must be the one that fires.
    const requestTimeoutMs = copyTimeoutMs ? 5000 : 100
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs, copyTimeoutMs }))
    const client = own(createS3Client(agent, 1))
    await expect(send(client, { abortSignal: AbortSignal.timeout(5000) })).rejects.toMatchObject({
      name: 'TimeoutError',
      code: 'UND_ERR_HEADERS_TIMEOUT',
    })
    const next = await client.send(new HeadObjectCommand({ Bucket: 'bucket', Key: 'next' }), {
      abortSignal: AbortSignal.timeout(5000),
    })
    expect(next.$metadata.httpStatusCode).toBe(200)
  })

  it.each([0, 100])(
    'does not retry a malformed response header after Content-Length with timeout %s',
    async (requestTimeoutMs) => {
      let requests = 0
      handler = (req, res) => {
        req.resume()
        requests++
        res.socket?.write('HTTP/1.1 200 OK\r\nContent-Length: 2\r\nBad Header\r\n\r\nok')
      }
      const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs }))
      const client = own(createS3Client(agent, 3))
      const error = await client
        .send(new ListObjectsV2Command({ Bucket: 'bucket' }))
        .catch((e: unknown) => e)
      expect(error).toBeInstanceOf(Error)
      expect(requests).toBe(1)
    }
  )

  it.each([0, 100])(
    'lets the upload source end after a 2xx response with timeout %s',
    async (requestTimeoutMs) => {
      const payload = Buffer.from('hello world')
      let received = 0
      handler = (req, res) => {
        req.on('data', (chunk: Buffer) => {
          received += chunk.length
          if (received === payload.length) res.end('ok')
        })
      }
      const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs }))
      const input = own(new PassThrough())
      input.write(payload)
      setTimeout(() => input.end(), 50)
      const source = new PassThrough()
      const statusCode = await stream.pipeline(input, source, async (body) => {
        const { statusCode, body: res } = await agent.dispatcher.request({
          origin,
          path: '/',
          method: 'PUT',
          headers: { 'content-length': String(payload.length) },
          body,
          signal: AbortSignal.timeout(3000),
        })
        await res.text()
        return statusCode
      })
      expect(statusCode).toBe(200)
      expect(received).toBe(payload.length)
      await vi.waitFor(() => expect(source.readableEnded).toBe(true))
    }
  )

  it.each([0, 100])(
    'leaves the upload source paused for the caller after an early non-2xx response with timeout %s',
    async (requestTimeoutMs) => {
      handler = (req, res) => {
        req.once('data', () => res.writeHead(403).end('<Error/>'))
      }
      const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs }))
      const input = own(new PassThrough())
      input.write('a')
      const { statusCode, body } = await put(agent, input, '2')
      expect(statusCode).toBe(403)
      await body.text()
      await vi.waitFor(() => expect(input.isPaused()).toBe(true))
      expect(input.destroyed).toBe(false)
      input.write('b')
      await new Promise((resolve) => setImmediate(resolve))
      expect(input.readableLength).toBe(1)
    }
  )

  it.each([0, 100])(
    'returns an early S3 error to a pipeline-fed UploadPart under WHEN_REQUIRED with timeout %s',
    async (requestTimeoutMs) => {
      handler = (req, res) => {
        req.once('data', () =>
          res
            .writeHead(404, { 'content-type': 'application/xml' })
            .end(
              '<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchUpload</Code><Message>x</Message></Error>'
            )
        )
      }
      const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs }))
      const client = own(
        createS3Client(agent, 1, origin, { requestChecksumCalculation: 'WHEN_REQUIRED' })
      )
      const source = own(new PassThrough())
      source.write(Buffer.alloc(1024))
      await expect(
        stream.pipeline(
          source,
          new PassThrough(),
          (s: AsyncIterable<unknown>, { signal }: { signal?: AbortSignal } = {}) =>
            client.send(
              new UploadPartCommand({
                Bucket: 'b',
                Key: 'k',
                UploadId: 'u',
                PartNumber: 1,
                Body: s as Readable,
                ContentLength: 4096,
              }),
              { abortSignal: signal }
            )
        )
      ).rejects.toMatchObject({ name: 'NoSuchUpload' })
    }
  )

  it.each([0, 5000])(
    'normalizes repeated and padded response headers for the SDK with timeout %s',
    async (requestTimeoutMs) => {
      handler = (_req, res) => {
        res.setHeader('content-type', ['image/png  ', 'text/html'])
        res.setHeader('etag', '"e1"\t')
        res.setHeader('last-modified', [
          'Wed, 21 Oct 2015 07:28:00 GMT',
          'Thu, 22 Oct 2015 07:28:00 GMT',
        ])
        res.setHeader('x-amz-meta-a', ['one  ', 'two'])
        res.setHeader('x-amz-meta-b', 'caf\u00c3\u00a0')
        res.end()
      }
      const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs }))
      const client = own(createS3Client(agent, 1))
      const result = await client.send(new HeadObjectCommand({ Bucket: 'bucket', Key: 'key' }))
      expect(result.ContentType).toBe('image/png')
      expect(result.ETag).toBe('"e1"')
      expect(result.LastModified).toEqual(new Date('Wed, 21 Oct 2015 07:28:00 GMT'))
      expect(result.Metadata).toEqual({ a: 'one, two', b: 'caf\u00c3\u00a0' })
    }
  )

  it('does not expose the S3 address in connect timeout errors', async () => {
    const agent = own(createAgent('t', { maxSockets: 1 }))
    const error = await agent.dispatcher
      .request({
        origin,
        path: '/',
        method: 'GET',
        headers: { 'x-test-connect-timeout': '1' },
      })
      .catch((e: unknown) => e)
    expect(error).toMatchObject({
      code: 'UND_ERR_CONNECT_TIMEOUT',
      message: 'Connect Timeout Error',
    })
  })

  it('frames an undeclared small stream body as chunked without a timeout', async () => {
    let headers: http.IncomingHttpHeaders = {}
    handler = (req, res) => {
      headers = req.headers
      req.resume()
      req.on('end', () => res.end('ok'))
    }
    const agent = own(createAgent('t', { maxSockets: 1 }))
    const input = own(new PassThrough())
    input.end('hello')
    const { body } = await put(agent, input)
    expect(await body.text()).toBe('ok')
    expect(headers['transfer-encoding']).toBe('chunked')
    expect(headers['content-length']).toBeUndefined()
  })

  it('times out stalled upload input after Early Hints and releases the connection', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    const received: Buffer[] = []
    handler = (req, res) => {
      if (req.method === 'PUT') {
        res.writeEarlyHints({ link: '</object>; rel=preload' })
        req.on('data', (chunk: Buffer) => received.push(chunk))
      } else res.end('ok')
    }
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 }))
    agent.monitor()
    const disconnected = once(agent.dispatcher, 'disconnect')
    const input = own(new PassThrough())
    input.write('a')
    const client = new UndiciHttpHandler({ dispatcher: agent.dispatcher })
    const url = new URL(origin)

    await expect(
      client.handle(
        new HttpRequest({
          protocol: url.protocol,
          hostname: url.hostname,
          port: Number(url.port),
          path: '/',
          method: 'PUT',
          headers: { 'content-length': '2' },
          body: input,
        }),
        { abortSignal: AbortSignal.timeout(2000) }
      )
    ).rejects.toMatchObject({ name: 'TimeoutError', code: 'ETIMEDOUT' })
    expect(Buffer.concat(received).toString()).toBe('a')
    expect(input.destroyed).toBe(true)
    await disconnected
    expectErrors(1, 'timeout_socket_error')

    expect(await getText(agent, '/')).toBe('ok')
  })

  it.each([
    { requestTimeoutMs: 0, parts: 1 },
    { requestTimeoutMs: 200, parts: 8 },
  ])(
    'leaves a slow upload source with $parts parts alone with timeout $requestTimeoutMs',
    async ({ requestTimeoutMs, parts }) => {
      let received = ''
      handler = (req, res) => {
        req.on('data', (chunk: Buffer) => {
          received += chunk.toString()
        })
        req.on('end', () => res.end('ok'))
      }
      const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs }))
      const input = own(
        Readable.from(
          (async function* () {
            for (let i = 0; i < parts; i++) {
              await new Promise((resolve) => setTimeout(resolve, 50))
              yield Buffer.from('a')
            }
          })()
        )
      )
      const { body } = await put(agent, input, parts)
      expect(await body.text()).toBe('ok')
      expect(received).toBe('a'.repeat(parts))
    }
  )

  it('times out even when destroying the upload source waits for its async generator', async () => {
    handler = (req) => req.resume()
    let release: () => void = () => {}
    const stalled = new Promise<void>((resolve) => {
      release = resolve
    })
    const input = own(
      Readable.from(
        (async function* () {
          yield Buffer.from('a')
          await stalled
          yield Buffer.from('b')
        })()
      )
    )
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 }))
    disposables.push(release)
    await expect(put(agent, input, '2')).rejects.toMatchObject({
      name: 'TimeoutError',
      code: 'ETIMEDOUT',
    })
  })

  it.each([
    {
      finishes: true,
      title: 'does not run the input timer while a buffered chunk waits for socket drainage',
    },
    { finishes: false, title: 'times out stalled input after socket drainage' },
  ])('$title', async ({ finishes }) => {
    let resume: NodeJS.Timeout | undefined
    let burst: NodeJS.Timeout | undefined
    let received = 0
    handler = (req, res) => {
      req.pause()
      // Drain slowly for longer than the timeout without idling the socket for that long.
      let slow = true
      burst = setInterval(() => {
        req.resume()
        setImmediate(() => slow && req.pause())
      }, 20)
      resume = setTimeout(() => {
        slow = false
        clearInterval(burst)
        req.resume()
      }, 250)
      req.on('data', (chunk: Buffer) => {
        received += chunk.length
      })
      req.on('end', () => res.end('ok'))
    }
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 })
    const input = new PassThrough()
    const length = 32 * 1024 * 1024
    input.write(Buffer.alloc(length))
    const end = finishes ? setTimeout(() => input.end(), 200) : undefined
    try {
      const request = put(agent, input, finishes ? length : length + 1)
      if (finishes) {
        expect(await (await request).body.text()).toBe('ok')
        expect(received).toBe(length)
      } else {
        await expect(request).rejects.toMatchObject({ name: 'TimeoutError', code: 'ETIMEDOUT' })
        expect(input.destroyed).toBe(true)
        expect(received).toBeGreaterThan(0)
      }
    } finally {
      clearTimeout(end)
      clearTimeout(resume)
      clearInterval(burst)
      input.destroy()
      await agent.close()
    }
  })

  it('times out an upload the server stops reading', async () => {
    handler = (req) => {
      req.pause()
    }
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 }))
    const input = own(new PassThrough())
    const length = 32 * 1024 * 1024
    input.end(Buffer.alloc(length))
    await expect(put(agent, input, length)).rejects.toMatchObject({
      code: 'UND_ERR_HEADERS_TIMEOUT',
    })
  })

  it('times out an upload the server stops reading while its source trickles', async () => {
    handler = (req) => {
      req.pause()
    }
    let socket: Socket | undefined
    const watch = (message: unknown) => {
      socket = (message as { socket: Socket }).socket
    }
    subscribe('undici:client:sendHeaders', watch)
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 200 }))
    const input = own(new PassThrough({ highWaterMark: 2 ** 30 }))
    // Fill the kernel buffers without a backlog in the socket, then trickle below its high-water mark.
    let trickled = 0
    const feed = setInterval(() => {
      if (!socket || input.destroyed) return
      if (socket.writableLength === 0) input.write(Buffer.alloc(16 * 1024))
      else if (Date.now() - trickled >= 10) {
        trickled = Date.now()
        input.write(Buffer.alloc(256))
      }
    }, 1)
    disposables.push(() => {
      clearInterval(feed)
      unsubscribe('undici:client:sendHeaders', watch)
    })
    await expect(put(agent, input, 2 ** 30, AbortSignal.timeout(4000))).rejects.toMatchObject({
      code: 'UND_ERR_HEADERS_TIMEOUT',
    })
  })

  it.each(['abort', 'socket error', 'source close', 'source error'] as const)(
    'cleans up a streamed upload on %s',
    async (failure) => {
      vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
      let received = false
      handler = (req) => {
        req.on('data', () => {
          received = true
          if (failure === 'socket error') req.socket.destroy()
        })
      }
      const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 2000 })
      agent.monitor()
      const disconnected = once(agent.dispatcher, 'disconnect')
      const input = new PassThrough()
      input.write('a')
      const controller = new AbortController()
      const request = put(agent, input, undefined, controller.signal)
      const error = request.catch((err: unknown) => err)
      try {
        await vi.waitFor(() => expect(received).toBe(true))
        if (failure === 'abort') {
          controller.abort()
          expect(await error).toMatchObject({ name: 'AbortError' })
        } else if (failure === 'socket error') {
          expect(await error).toMatchObject({
            name: 'TimeoutError',
            code: 'ECONNRESET',
            cause: { code: 'UND_ERR_SOCKET' },
          })
          expect(input.errored).toBe(await error)
        } else if (failure === 'source close') {
          input.destroy()
          expect(await error).toMatchObject({ name: 'AbortError', code: 'UND_ERR_ABORTED' })
        } else {
          const sourceError = new Error('upload input failed')
          input.destroy(sourceError)
          expect(await error).toBe(sourceError)
        }
        expect(input.destroyed).toBe(true)
        await disconnected
        expectErrors(failure === 'socket error' ? 1 : 0, 'socket_error')
      } finally {
        controller.abort()
        input.destroy()
        await agent.close()
      }
    }
  )

  it.each(['source close', 'source error'] as const)(
    'stops a streamed upload on %s during socket drainage',
    async (failure) => {
      let received = false
      handler = (req) => {
        req.once('data', () => {
          req.pause()
          received = true
        })
      }
      const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 }))
      const input = own(new PassThrough())
      const length = 32 * 1024 * 1024
      input.write(Buffer.alloc(length))
      const request = put(agent, input, String(length)).catch((error: unknown) => error)
      await vi.waitFor(() => expect(received).toBe(true))
      if (failure === 'source close') {
        input.destroy()
        expect(await request).toMatchObject({ name: 'AbortError', code: 'UND_ERR_ABORTED' })
      } else {
        const sourceError = new Error('upload input failed')
        input.destroy(sourceError)
        expect(await request).toBe(sourceError)
      }
    }
  )

  it('disables undici response timers in favor of its own', () => {
    own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 }))
    expect(agentOptions).toEqual([
      expect.objectContaining({
        allowH2: false,
        headersTimeout: 0,
        bodyTimeout: 0,
        connect: expect.any(Function),
        keepAliveTimeout: 15_000,
        keepAliveMaxTimeout: 15_000,
        pipelining: 1,
      }),
    ])
    expect(connectorOptions).toEqual([
      {
        allowH2: false,
        timeout: 5000,
        keepAlive: true,
        keepAliveInitialDelay: 1000,
        secureContext: expect.anything(),
      },
    ])
  })

  it.each([
    { options: { requestTimeoutMs: 100 }, fireAt: 100 },
    { options: { requestTimeoutMs: 0, bodyTimeoutMs: 200 }, fireAt: 200 },
    { options: { requestTimeoutMs: 5000, bodyTimeoutMs: 200 }, fireAt: 200 },
    {
      options: { requestTimeoutMs: 0, copyTimeoutMs: 0, bodyTimeoutMs: 200 },
      headers: { 'x-amz-copy-source': 'bucket/key' },
      fireAt: 200,
    },
    { options: { requestTimeoutMs: 100, bodyTimeoutMs: 0 }, fireAt: undefined },
  ])(
    'times out a response body whose consumer stops reading with %o',
    async ({ options, headers, fireAt }) => {
      handler = (req, res) => {
        if (req.method !== 'GET') return res.end('ok')
        res.writeHead(200, { 'content-length': 256 * 64 * 1024 })
        const chunk = Buffer.alloc(64 * 1024)
        let written = 0
        const write = () => {
          while (written++ < 256) if (!res.write(chunk)) return res.once('drain', write)
          res.end()
        }
        write()
      }
      const agent = own(createAgent('t', { maxSockets: 1, ...options }))
      const { body } = await agent.dispatcher.request({ origin, path: '/', method: 'GET', headers })
      const failure = new Promise<NodeJS.ErrnoException>((resolve) => body.once('error', resolve))
      let failed = false
      void failure.then(() => {
        failed = true
      })
      await new Promise((resolve) => body.once('data', resolve))
      body.pause()

      if (fireAt === undefined) {
        await new Promise((resolve) => setTimeout(resolve, 400))
        expect(failed).toBe(false)
        body.destroy()
      } else {
        await vi.waitFor(() => expect(failed).toBe(true), { timeout: 2000 })
        expect(await failure).toMatchObject({
          code: 'ECONNRESET',
          cause: { code: 'UND_ERR_BODY_TIMEOUT' },
        })
      }

      const next = await agent.dispatcher.request({ origin, path: '/', method: 'PUT', body: 'x' })
      expect(await next.body.text()).toBe('ok')
    }
  )

  it('applies the 30 s default body timeout to the response socket', async () => {
    handler = (_req, res) => {
      res.writeHead(200, { 'content-length': 10 })
      res.write('hello')
    }
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 0 }))
    let socket: Socket | undefined
    const capture = (message: unknown) => {
      socket = (message as { socket: Socket }).socket
    }
    subscribe('undici:client:sendHeaders', capture)
    try {
      const { body } = await agent.dispatcher.request({ origin, path: '/', method: 'GET' })
      await new Promise((resolve) => body.once('data', resolve))
      expect(socket?.timeout).toBe(30_000)
      body.destroy()
    } finally {
      unsubscribe('undici:client:sendHeaders', capture)
    }
  })

  it('treats trailer framing bytes as body activity', async () => {
    const raw = createTcpServer((socket) => {
      socket.once('data', () => {
        socket.write(
          'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\nx-note: '
        )
        let sent = 0
        const timer = setInterval(() => {
          if (sent++ < 10) return socket.write('a')
          clearInterval(timer)
          socket.write('\r\n\r\n')
        }, 60)
        socket.once('close', () => clearInterval(timer))
      })
    })
    await new Promise<void>((resolve) => raw.listen(0, '127.0.0.1', resolve))
    const agent = own(createAgent('t', { maxSockets: 1, bodyTimeoutMs: 250 }))
    try {
      const { body } = await agent.dispatcher.request({
        origin: `http://127.0.0.1:${(raw.address() as AddressInfo).port}`,
        path: '/',
        method: 'GET',
      })
      expect(await body.text()).toBe('hello')
    } finally {
      raw.close()
    }
  })

  it('stops the body idle timer once the response ends', async () => {
    handler = (_req, res) => res.end('ok')
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 50 }))
    agent.monitor()
    expect(await getText(agent, '/')).toBe('ok')
    await new Promise((resolve) => setTimeout(resolve, 150))
    expectErrors(0, 'timeout_socket_error')
  })

  it.each([
    { framing: 'content-length', headers: { 'content-length': String(65 * 1024) } },
    { framing: 'chunked', headers: {} },
  ])(
    'delivers a fully received $framing body that its consumer reads after the body timeout',
    async ({ headers }) => {
      vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
      handler = (_req, res) => {
        res.writeHead(200, headers)
        res.write(Buffer.alloc(60 * 1024))
        // The last piece overfills the consumer's buffer, pausing undici just before the end.
        setTimeout(() => res.end(Buffer.alloc(5 * 1024)), 20)
      }
      const agent = own(createAgent('t', { maxSockets: 1, bodyTimeoutMs: 100 }))
      agent.monitor()
      const { body } = await agent.dispatcher.request({ origin, path: '/', method: 'GET' })
      await new Promise((resolve) => setTimeout(resolve, 300))
      expect((await body.arrayBuffer()).byteLength).toBe(65 * 1024)
      expectErrors(0, 'timeout_socket_error')
    }
  )

  it('does not retry an SDK request aborted by a timeout signal', async () => {
    let requests = 0
    handler = (req) => {
      requests++
      req.resume()
    }
    const agent = own(createAgent('t', { maxSockets: 1 }))
    const client = own(createS3Client(agent))
    await expect(
      client.send(new HeadObjectCommand({ Bucket: 'bucket', Key: 'key' }), {
        abortSignal: AbortSignal.timeout(100),
      })
    ).rejects.toMatchObject({
      name: 'AbortError',
    })
    expect(requests).toBe(1)
  })

  it('wraps any caller abort reason of an SDK upload in an AbortError', async () => {
    handler = (req) => {
      req.resume()
    }
    const agent = own(createAgent('t', { maxSockets: 1 }))
    const client = own(createS3Client(agent))
    const controller = new AbortController()
    const body = own(new PassThrough())
    body.write('a')
    const upload = client
      .send(
        new UploadPartCommand({
          Bucket: 'bucket',
          Key: 'key',
          UploadId: 'upload',
          PartNumber: 1,
          Body: body,
          ContentLength: 2,
        }),
        { abortSignal: controller.signal }
      )
      .catch((error: unknown) => error)
    await new Promise((resolve) => setTimeout(resolve, 50))
    controller.abort(new Error('caller left'))
    expect(await upload).toMatchObject({ name: 'AbortError', cause: { message: 'caller left' } })
  })

  it('times out a silent S3 request and releases the connection for a queued request', async () => {
    const requests: string[] = []
    handler = (req, res) => {
      requests.push(req.url!)
      if (req.url?.includes('/silent')) return
      res.writeHead(200).end()
    }
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 }))
    const client = own(createS3Client(agent, 1))
    const silent = client
      .send(new HeadObjectCommand({ Bucket: 'bucket', Key: 'silent' }), {
        abortSignal: AbortSignal.timeout(5000),
      })
      .catch((error: unknown) => error)

    await vi.waitFor(() => expect(requests).toHaveLength(1))
    const next = client.send(new HeadObjectCommand({ Bucket: 'bucket', Key: 'next' }), {
      abortSignal: AbortSignal.timeout(5000),
    })

    expect(await silent).toMatchObject({ name: 'TimeoutError', code: 'UND_ERR_HEADERS_TIMEOUT' })
    expect((await next).$metadata.httpStatusCode).toBe(200)
    expect(requests).toHaveLength(2)
  })

  it.each([0, 100])(
    'writes each streamed upload chunk with its framing in one socket write with timeout %s',
    async (requestTimeoutMs) => {
      handler = (req, res) => {
        req.resume()
        req.on('end', () => res.end())
      }
      const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs }))
      const client = own(createS3Client(agent))
      const chunks = 16
      let sent = 0
      const body = new Readable({
        read() {
          this.push(sent++ < chunks ? Buffer.alloc(64 * 1024) : null)
        },
      })
      const writes = vi.spyOn(
        Socket.prototype as unknown as { _writeGeneric: () => void },
        '_writeGeneric'
      )
      await client.send(
        new UploadPartCommand({
          Bucket: 'bucket',
          Key: 'key',
          UploadId: 'upload',
          PartNumber: 1,
          Body: body,
          ContentLength: chunks * 64 * 1024,
        })
      )
      // aws-chunked hands undici three pieces per chunk: size line, data and CRLF.
      expect(writes.mock.calls.length).toBeLessThan(chunks)
    }
  )

  it.each(['WHEN_SUPPORTED', 'WHEN_REQUIRED'] as const)(
    'delivers mixed upload chunks intact through the SDK with %s checksums',
    async (requestChecksumCalculation) => {
      const expected = Buffer.concat([
        Buffer.from('prefix'),
        Buffer.alloc(512 * 1024, 42),
        Buffer.from('middle'),
        Buffer.alloc(128 * 1024, 85),
        Buffer.from('suffix'),
      ])
      let received = Buffer.alloc(0)
      handler = (req, res) => {
        const chunks: Buffer[] = []
        req.on('data', (chunk: Buffer) => chunks.push(chunk))
        req.on('end', () => {
          const wire = Buffer.concat(chunks)
          if (req.headers['content-encoding'] === 'aws-chunked') {
            const decoded: Buffer[] = []
            let offset = 0
            for (;;) {
              const lineEnd = wire.indexOf('\r\n', offset)
              const size = Number.parseInt(wire.toString('ascii', offset, lineEnd), 16)
              if (!Number.isFinite(size) || size === 0) break
              const start = lineEnd + 2
              decoded.push(wire.subarray(start, start + size))
              offset = start + size + 2
            }
            received = Buffer.concat(decoded)
          } else received = wire
          res.end()
        })
      }
      const agent = own(createAgent('t', { maxSockets: 1 }))
      const client = own(createS3Client(agent, 1, origin, { requestChecksumCalculation }))
      await client.send(
        new UploadPartCommand({
          Bucket: 'bucket',
          Key: 'key',
          UploadId: 'upload',
          PartNumber: 1,
          Body: Readable.from([
            expected.subarray(0, 6),
            expected.subarray(6, 600000),
            expected.subarray(600000),
          ]),
          ContentLength: expected.length,
        })
      )
      expect(received).toEqual(expected)
    },
    20_000
  )

  it('delivers an early response that outlasts the input timeout and leaves the input paused', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    let progress: NodeJS.Timeout | undefined
    const response = '<Error/>'
    handler = (req, res) => {
      const write = (offset: number) => {
        if (offset === response.length - 1) return res.end(response[offset])
        res.write(response[offset])
        progress = setTimeout(() => write(offset + 1), 100)
      }
      req.once('data', () => {
        res.writeHead(403)
        write(0)
      })
    }
    // A starved event loop must not fire the body timeout between the bytes of the response.
    const agent = own(
      createAgent('t', { maxSockets: 1, requestTimeoutMs: 500, bodyTimeoutMs: 5000 })
    )
    agent.monitor()
    const input = own(new PassThrough())
    input.write('a')
    disposables.push(() => clearTimeout(progress))
    const { statusCode, body } = await put(agent, input, '2')
    expect(statusCode).toBe(403)
    expect(await body.text()).toBe(response)
    expect(input.destroyed).toBe(false)
    expect(input.isPaused()).toBe(true)
    expectErrors(0, 'timeout_socket_error')

    handler = (_req, res) => res.end('ok')
    expect(await getText(agent, '/')).toBe('ok')
  })

  it.each(['headers', 'body', 'socket'] as const)(
    'records established-connection %s errors',
    async (failure) => {
      vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
      handler = (req, res) => {
        if (req.url === '/ok') return res.end('ok')
        if (failure === 'body') {
          res.writeHead(200)
          res.write('a')
        } else if (failure === 'socket') res.destroy()
      }
      const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 50 }))
      agent.monitor()
      // A finished request must leave no header timer on the socket the failing one reuses.
      expect(await getText(agent, '/ok')).toBe('ok')
      const request = agent.dispatcher
        .request({ origin, path: '/', method: 'GET', signal: AbortSignal.timeout(5000) })
        .then(({ body }) => body.text())
      await expect(request).rejects.toMatchObject(
        failure === 'headers'
          ? { code: 'UND_ERR_HEADERS_TIMEOUT' }
          : {
              code: 'ECONNRESET',
              cause: { code: failure === 'socket' ? 'UND_ERR_SOCKET' : 'UND_ERR_BODY_TIMEOUT' },
            }
      )
      expectErrors(1, failure === 'socket' ? 'socket_error' : 'timeout_socket_error')
      expect(errors.record).toHaveBeenCalledWith(
        0,
        expect.objectContaining({ type: 'create_socket_error' })
      )
    }
  )

  it("monitor() is idempotent, unref'd, cleared by close() and restartable", async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    const agent = createAgent('t', { maxSockets: 1 })
    const timer = agent.monitor()
    expect(timer.hasRef()).toBe(false)
    expect(agent.monitor()).toBe(timer)
    expect(vi.getTimerCount()).toBe(1)
    await agent.close()
    expect(vi.getTimerCount()).toBe(0)
    expect(agent.monitor()).not.toBe(timer)
    expect(vi.getTimerCount()).toBe(1)
    await agent.close()
    expect(vi.getTimerCount()).toBe(0)
  })

  it.each(['GET', 'HEAD'] as const)(
    'reuses one connection for sequential %s requests',
    async (method) => {
      let connections = 0
      server.on('connection', () => connections++)
      handler = (_req, res) => {
        res.writeHead(200, { 'content-length': '0' })
        res.end()
      }
      const agent = own(createAgent('t', { maxSockets: 10 }))
      for (let i = 0; i < 3; i++) {
        const { body } = await agent.dispatcher.request({ origin, path: '/', method })
        await body.dump()
      }
      expect(connections).toBe(1)
    }
  )

  it('records busy, free, queued and connect-error metrics', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    const release: Array<() => void> = []
    handler = (_req, res) => {
      release.push(() => res.end('ok'))
    }
    const agent = createAgent('t', { maxSockets: 2 })
    agent.monitor()

    const req = () => agent.dispatcher.request({ origin, path: '/', method: 'GET' })
    const all = [req(), req(), req()]
    await vi.waitFor(() => expect(release).toHaveLength(2))

    vi.advanceTimersByTime(5000)
    const queuedBusy = busy.record.mock.lastCall
    const queuedPending = pending.record.mock.lastCall

    release[0]()
    release[1]()
    await vi.waitFor(() => expect(release).toHaveLength(3))
    release[2]()
    await Promise.all(all.map(async (r) => (await r).body.text()))
    expect(queuedBusy).toEqual([2, { name: 't', protocol: 'http' }])
    expect(queuedPending).toEqual([1, { name: 't', region: expect.any(String) }])
    vi.advanceTimersByTime(5000)
    expect(busy.record).toHaveBeenLastCalledWith(0, { name: 't', protocol: 'http' })
    expect(free.record.mock.lastCall?.[0]).toBeGreaterThan(0)

    await expect(
      agent.dispatcher.request({ origin: 'http://127.0.0.1:1', path: '/', method: 'GET' })
    ).rejects.toThrow()
    vi.advanceTimersByTime(5000)
    expect(errors.record).toHaveBeenCalledWith(1, {
      name: 't',
      protocol: 'http',
      type: 'create_socket_error',
    })

    await expect(
      agent.dispatcher.request({ origin: 'https://127.0.0.1:1', path: '/', method: 'GET' })
    ).rejects.toThrow()
    errors.record.mockClear()
    vi.advanceTimersByTime(5000)
    for (const protocol of ['http', 'https']) {
      expect(errors.record).toHaveBeenCalledWith(1, {
        name: 't',
        protocol,
        type: 'create_socket_error',
      })
    }
    expect(free.record).toHaveBeenCalledWith(0, { name: 't', protocol: 'https' })

    await agent.close()
    errors.record.mockClear()
    vi.advanceTimersByTime(5000)
    expect(errors.record).not.toHaveBeenCalled()
  })

  it.each([1, 2])(
    'cancels a waiting upload before any of %s sockets is freed',
    async (maxSockets) => {
      vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
      const release: Array<() => void> = []
      let uploads = 0
      handler = (req, res) => {
        if (req.method === 'PUT') uploads++
        release.push(() => res.end('ok'))
      }
      const agent = own(createAgent('t', { maxSockets }))
      agent.monitor()
      const holds = Array.from({ length: maxSockets }, () =>
        agent.dispatcher.request({ origin, path: '/', method: 'GET' })
      )
      for (const hold of holds) void hold.catch(() => {})
      await vi.waitFor(() => expect(release).toHaveLength(maxSockets))
      const input = own(new PassThrough())
      input.write('a')
      const controller = new AbortController()
      const reason = new Error('caller disconnected')
      let failure: unknown
      const queued = put(agent, input, 2, controller.signal).catch((error: unknown) => {
        failure = error
      })
      controller.abort(reason)
      await vi.waitFor(() => expect(failure).toMatchObject({ name: 'AbortError', cause: reason }), {
        timeout: 200,
      })
      expect(input.destroyed).toBe(true)
      expect(release).toHaveLength(maxSockets)
      release.forEach((end) => end())
      await Promise.all(holds.map(async (r) => (await r).body.dump()))
      await queued
      expect(uploads).toBe(0)
      // Like Node's agent, undici drops the cancelled request once a socket frees.
      await new Promise((resolve) => setImmediate(resolve))
      vi.advanceTimersByTime(5000)
      expect(pending.record).toHaveBeenLastCalledWith(0, expect.anything())
    }
  )

  it('cancels an upload before its initial TLS handshake finishes', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    const sockets: Socket[] = []
    const tcp = createTcpServer((socket) => {
      sockets.push(socket)
      socket.resume()
    })
    await new Promise<void>((resolve) => tcp.listen(0, '127.0.0.1', resolve))
    const agent = createAgent('t', { maxSockets: 1 })
    agent.monitor()
    const input = own(new PassThrough())
    input.write('a')
    const controller = new AbortController()
    const reason = new Error('caller disconnected during TLS handshake')
    let failure: unknown
    const request = agent.dispatcher
      .request({
        origin: `https://127.0.0.1:${(tcp.address() as AddressInfo).port}`,
        path: '/',
        method: 'PUT',
        headers: { 'content-length': '2' },
        body: input,
        signal: controller.signal,
      })
      .catch((error: unknown) => {
        failure = error
      })
    try {
      await vi.waitFor(() => expect(sockets).toHaveLength(1))
      const canceledConnection = once(agent.dispatcher, 'connectionError')
      controller.abort(reason)
      await vi.waitFor(() => expect(failure).toMatchObject({ name: 'AbortError', cause: reason }), {
        timeout: 200,
      })
      await canceledConnection
      expect(input.destroyed).toBe(true)
      await vi.waitFor(() => expect(sockets[0].destroyed).toBe(true), { timeout: 1000 })
      vi.advanceTimersByTime(5000)
      expect(errors.record).toHaveBeenCalledWith(0, {
        name: 't',
        protocol: 'https',
        type: 'create_socket_error',
      })
    } finally {
      for (const socket of sockets) socket.destroy()
      await agent.close()
      await agent.dispatcher.destroy()
      await request
      await new Promise<void>((resolve) => tcp.close(() => resolve()))
    }
  })

  it('keeps pool waiting separate when requests alternate origins', async () => {
    const second = await listen((_req, res) => res.end('second'))
    const agent = own(createAgent('t', { maxSockets: 1 }))
    const { hold, release } = await occupy(agent)
    try {
      const endpoint = new URL(second.origin)
      const readSecond = async () => {
        const response = await agent.dispatcher.request({
          origin: endpoint,
          path: '/',
          method: 'GET',
          signal: AbortSignal.timeout(1000),
        })
        expect(await response.body.text()).toBe('second')
      }
      await readSecond()
      const input = own(new PassThrough())
      input.write('a')
      const abort = new AbortController()
      const queued = put(agent, input, 2, abort.signal).catch((error: unknown) => error)
      await readSecond()
      abort.abort()
      expect(await queued).toMatchObject({ name: 'AbortError' })
      expect(input.destroyed).toBe(true)
    } finally {
      release()
      await (await hold).body.dump()
      await stop(second.server)
    }
  })

  it('does not cancel a connection attempt when its request expires', async () => {
    const sockets: Socket[] = []
    const tcp = createTcpServer((socket) => sockets.push(socket.resume()))
    await new Promise<void>((resolve) => tcp.listen(0, '127.0.0.1', resolve))
    const agent = own(createAgent('t', { maxSockets: 1 }))
    const codes: string[] = []
    agent.dispatcher.on('connectionError', (_origin, _targets, error) => codes.push(error.code))
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
    try {
      const request = agent.dispatcher.request({
        origin: `https://127.0.0.1:${(tcp.address() as AddressInfo).port}`,
        path: '/',
        method: 'GET',
      })
      const expired = expect(request).rejects.toMatchObject({ code: 'ETIMEDOUT' })
      await vi.waitFor(() => expect(sockets).toHaveLength(1))
      await vi.advanceTimersByTimeAsync(5000)
      await expired
      vi.useRealTimers()
      await new Promise((resolve) => setTimeout(resolve, 50))
      expect(codes).not.toContain('UND_ERR_ABORTED')
    } finally {
      for (const socket of sockets) socket.destroy()
      await new Promise<void>((resolve) => tcp.close(() => resolve()))
    }
  })

  it('cancels the connection attempt of an aborted request whose error handler throws', async () => {
    const sockets: Socket[] = []
    const tcp = createTcpServer((socket) => sockets.push(socket.resume()))
    await new Promise<void>((resolve) => tcp.listen(0, '127.0.0.1', resolve))
    const agent = own(createAgent('t', { maxSockets: 1 }))
    const [cancelled, failure] = [new Error('cancelled'), new Error('error handler failed')]
    let controller: Dispatcher.DispatchController | undefined
    let failures = 0
    try {
      agent.dispatcher.dispatch(
        {
          origin: `https://127.0.0.1:${(tcp.address() as AddressInfo).port}`,
          path: '/',
          method: 'GET',
        },
        {
          onRequestStart(requestController) {
            controller = requestController
          },
          onResponseError() {
            failures++
            throw failure
          },
        }
      )
      await vi.waitFor(() => expect(sockets).toHaveLength(1))
      expect(controller).toMatchObject({ aborted: false, reason: null })
      expect(() => controller?.abort(cancelled)).toThrow(failure)
      expect(controller).toMatchObject({ aborted: true, reason: cancelled })
      await vi.waitFor(() => expect(sockets[0].destroyed).toBe(true))
      // Undici fails the request again when its connection attempt ends.
      expect(failures).toBe(1)
    } finally {
      for (const socket of sockets) socket.destroy()
      await new Promise<void>((resolve) => tcp.close(() => resolve()))
    }
  })

  it('serves a request that undici starts inside dispatch on a connected, unused socket', async () => {
    handler = (_req, res) => res.end('ok')
    let connections = 0
    server.on('connection', () => connections++)
    const agent = own(createAgent('t', { maxSockets: 1 }))
    // Undici rejects this at its start, after connecting and before writing anything.
    await expect(
      agent.dispatcher.request({
        origin,
        path: '/',
        method: 'PUT',
        headers: { 'content-length': '5' },
        body: 'abc',
      })
    ).rejects.toMatchObject({ code: 'UND_ERR_REQ_CONTENT_LENGTH_MISMATCH' })
    expect(await getText(agent, '/')).toBe('ok')
    expect(connections).toBe(1)
  })

  it('gives a request that waited for its connection one controller that pauses its response', async () => {
    let finish: (() => void) | undefined
    handler = (_req, res) => {
      res.write('a')
      finish = () => res.end('b')
    }
    const agent = own(createAgent('t', { maxSockets: 1 }))
    const done = Promise.withResolvers<void>()
    const controllers: Dispatcher.DispatchController[] = []
    let body = ''
    agent.dispatcher.dispatch(
      { origin, path: '/', method: 'GET' },
      {
        onRequestStart: (controller) => void controllers.push(controller),
        onResponseData(_controller, chunk) {
          body += chunk
          if (body === 'a') controllers[0].pause()
        },
        onResponseEnd: () => done.resolve(),
        onResponseError: (_controller, error) => done.reject(error),
      }
    )
    await vi.waitFor(() => expect(body).toBe('a'))
    expect(controllers[0].paused).toBe(true)
    finish?.()
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(body).toBe('a')
    controllers[0].resume()
    await done.promise
    expect(body).toBe('ab')
    expect(controllers).toHaveLength(1)
  })

  it('reports from dispatch whether the pool can take another request', () => {
    handler = (_req, res) => res.end('ok')
    const agent = own(createAgent('t', { maxSockets: 2 }))
    const send = () => agent.dispatcher.dispatch({ origin, path: '/', method: 'GET' }, {})
    expect([send(), send()]).toEqual([true, false])
  })

  it('reports from dispatch that the pool is full when a request starts on its last socket', async () => {
    handler = (_req, res) => res.end('ok')
    const agent = own(createAgent('t', { maxSockets: 1 }))
    // Undici rejects this at its start, then starts the next request inside dispatch.
    await agent.dispatcher
      .request({
        origin,
        path: '/',
        method: 'PUT',
        headers: { 'content-length': '5' },
        body: 'abc',
      })
      .catch(() => {})
    expect(agent.dispatcher.dispatch({ origin, path: '/', method: 'GET' }, {})).toBe(false)
  })

  it('expires a waiting upload after five seconds without freeing the occupied socket', async () => {
    let uploads = 0
    const agent = own(createAgent('t', { maxSockets: 1 }))
    const { hold, release } = await occupy(agent, (req) => {
      if (req.method === 'PUT') uploads++
    })
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
    const input = own(new PassThrough())
    input.write('a')
    let failure: unknown
    const queued = put(agent, input, 2, new AbortController().signal).catch((error: unknown) => {
      failure = error
    })
    await vi.advanceTimersByTimeAsync(4999)
    expect(failure).toBeUndefined()
    await vi.advanceTimersByTimeAsync(1)
    expect(failure).toMatchObject({ name: 'TimeoutError', code: 'ETIMEDOUT' })
    expect(input.destroyed).toBe(true)
    vi.useRealTimers()
    release()
    await (await hold).body.dump()
    await queued
    expect(uploads).toBe(0)
  })

  it.each([
    { source: 'close', requestTimeoutMs: 100 },
    { source: 'error', requestTimeoutMs: 0 },
    { source: 'error', requestTimeoutMs: 100 },
  ])(
    'rejects a queued upload on source $source before a socket is freed with timeout $requestTimeoutMs',
    async ({ source, requestTimeoutMs }) => {
      const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs }))
      const { hold, release } = await occupy(agent)
      const input = own(new PassThrough())
      input.write('a')
      const reason = new Error('upload source failed while queued')
      let failure: unknown
      const queued = put(agent, input, 2).catch((error: unknown) => {
        failure = error
      })
      input.destroy(source === 'error' ? reason : undefined)
      await vi.waitFor(() => expect(failure).toBeDefined(), { timeout: 200 })
      if (source === 'error') expect(failure).toBe(reason)
      else expect(failure).toMatchObject({ name: 'AbortError', code: 'UND_ERR_ABORTED' })
      release()
      await (await hold).body.dump()
      await queued
    }
  )

  it.each([
    { reason: null, expected: { code: 'UND_ERR_DESTROYED' } },
    { reason: new Error('custom'), expected: { message: 'custom' } },
  ])('destroy promptly rejects requests waiting outside the pool with $expected', async (c) => {
    const release: Array<() => void> = []
    handler = (_req, res) => release.push(() => res.end('ok'))
    const agent = createAgent('t', { maxSockets: 1 })
    const request = () =>
      agent.dispatcher
        .request({ origin, path: '/', method: 'GET' })
        .then(({ body }) => body.text())
        .catch((error: unknown) => error)
    const active = request()
    await vi.waitFor(() => expect(release).toHaveLength(1))
    const queued = request()
    const start = Date.now()
    await agent.dispatcher.destroy(c.reason)
    expect(await active).toMatchObject(c.expected)
    expect(await queued).toMatchObject(c.expected)
    expect(Date.now() - start).toBeLessThan(1000)
    expect(await request()).toMatchObject({ code: 'UND_ERR_DESTROYED' })
  })

  it('delivers SDK bulk-delete XML intact', async () => {
    const received: Buffer[] = []
    let receivedBytes = 0
    let contentLength = 0
    handler = (req, res) => {
      contentLength = Number(req.headers['content-length'])
      req.on('data', (chunk: Buffer) => {
        received.push(chunk)
        receivedBytes += chunk.length
      })
      req.on('end', () => res.end('<DeleteResult/>'))
    }
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 200 }))
    const client = own(createS3Client(agent, 1))
    const keys = Array.from({ length: 1000 }, (_, i) => `tenant/bucket/${"'".repeat(990)}${i}`)
    const result = await client.send(
      new DeleteObjectsCommand({
        Bucket: 'bucket',
        Delete: { Objects: keys.map((Key) => ({ Key })) },
      }),
      { abortSignal: AbortSignal.timeout(5000) }
    )
    expect(result.$metadata.httpStatusCode).toBe(200)
    expect(receivedBytes).toBe(contentLength)
    const xml = Buffer.concat(received).toString()
    const receivedKeys = Array.from(xml.matchAll(/<Key>(.*?)<\/Key>/g), ([, key]) =>
      key.replaceAll('&apos;', "'")
    )
    expect(receivedKeys).toEqual(keys)
  })

  it('forwards upgraded connections to the caller', async () => {
    let upgradedSocket: Duplex | undefined
    server.on('upgrade', (_req, socket) => {
      upgradedSocket = socket
      socket.end(
        'HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\nupgraded'
      )
    })
    const agent = createAgent('t', { maxSockets: 1 })
    let timeout: NodeJS.Timeout | undefined
    try {
      const options: Dispatcher.UpgradeOptions & { origin: string } = {
        origin,
        path: '/',
        protocol: 'test',
        signal: AbortSignal.timeout(2000),
      }
      const deadline = new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(() => reject(new Error('Upgrade callback was not forwarded')), 1000)
      })
      const { socket, headers } = await Promise.race([agent.dispatcher.upgrade(options), deadline])
      expect(headers.upgrade).toBe('test')
      const chunks: Buffer[] = []
      for await (const chunk of socket) chunks.push(chunk)
      expect(Buffer.concat(chunks).toString()).toBe('upgraded')
    } finally {
      clearTimeout(timeout)
      upgradedSocket?.destroy()
      await agent.close()
    }
  })

  it.each(['closes', 'resets'] as const)(
    'reports an idle single connection as free and ignores the server when it %s it',
    async (idle) => {
      vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
      let socket: Socket | undefined
      handler = (req, res) => {
        socket = req.socket
        res.end('ok')
      }
      const agent = own(createAgent('t', { maxSockets: 1 }))
      agent.monitor()
      const disconnected = once(agent.dispatcher, 'disconnect')
      await getText(agent, '/')
      await new Promise((resolve) => setImmediate(resolve))
      vi.advanceTimersByTime(5000)
      expect(free.record).toHaveBeenLastCalledWith(1, expect.anything())
      expect(busy.record).toHaveBeenLastCalledWith(0, expect.anything())

      if (idle === 'closes') server.closeIdleConnections()
      else socket?.resetAndDestroy()
      await disconnected
      vi.advanceTimersByTime(5000)
      expect(free.record).toHaveBeenLastCalledWith(0, expect.anything())
      expect(errors.record).not.toHaveBeenCalledWith(
        1,
        expect.objectContaining({ type: 'socket_error' })
      )
    }
  )

  it('close() is memoized and waits for in-flight requests', async () => {
    let end: (() => void) | undefined
    handler = (_req, res) => {
      end = () => res.end('ok')
    }
    const agent = createAgent('t', { maxSockets: 1 })
    const inflight = agent.dispatcher.request({ origin, path: '/', method: 'GET' })
    await vi.waitFor(() => expect(end).toBeDefined())

    const closing = agent.close()
    expect(agent.close()).toBe(closing)
    let done = false
    void closing.then(() => {
      done = true
    })
    await new Promise((r) => setTimeout(r, 50))
    expect(done).toBe(false)

    end?.()
    await (await inflight).body.text()
    await closing

    handler = (_req, res) => res.destroy()
    const failed = getText(agent, '/').catch(() => {})
    const again = agent.close()
    expect(again).not.toBe(closing)
    await Promise.all([failed, again])
  })

  it('keeps a keep-alive socket when a request dies in the pool queue', async () => {
    const held = new Map<string, () => void>()
    handler = (req, res) => {
      if (req.url === '/fast') return res.end('ok')
      held.set(req.url ?? '', () => res.end('ok'))
    }
    let connections = 0
    server.on('connection', () => connections++)
    const agent = own(createAgent('t', { maxSockets: 2, requestTimeoutMs: 0 }))
    const get = (path: string, signal?: AbortSignal) =>
      agent.dispatcher.request({ origin, path, method: 'GET', signal })
    const a = new AbortController()
    const reqA = get('/a', a.signal)
    const reqB = get('/b')
    const reqC = get('/c')
    for (const request of [reqA, reqB, reqC]) void request.catch(() => {})
    await vi.waitFor(() => expect(held.has('/a') && held.has('/b')).toBe(true))
    const d = new AbortController()
    const reqD = get('/d', d.signal)
    const reqE = get('/e', d.signal)
    for (const request of [reqD, reqE]) void request.catch(() => {})
    a.abort()
    await vi.waitFor(() => expect(held.has('/c')).toBe(true))
    d.abort()
    await expect(reqD).rejects.toMatchObject({ name: 'AbortError' })
    await expect(reqE).rejects.toMatchObject({ name: 'AbortError' })
    held.get('/b')?.()
    held.get('/c')?.()
    await (await reqB).body.text()
    await (await reqC).body.text()
    const before = connections
    await Promise.all(
      [0, 1].map(async () => {
        await (await get('/fast')).body.text()
      })
    )
    expect(connections).toBe(before)
  })

  it.each([0, 100])(
    'rejects a source that fails while dispatch is deferred with timeout %s',
    async (requestTimeoutMs) => {
      const other = await listen((_req, res) => res.end('other'))
      try {
        const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs }))
        const { hold, release } = await occupy(agent)
        const response = await agent.dispatcher.request({
          origin: other.origin,
          path: '/',
          method: 'GET',
        })
        await response.body.text()
        const input = own(new PassThrough())
        const reason = new Error('upload source failed while deferred')
        let failure: unknown
        const queued = put(agent, input, 5).catch((error: unknown) => {
          failure = error
        })
        input.destroy(reason)
        await vi.waitFor(() => expect(failure).toBe(reason), { timeout: 200 })
        release()
        await (await hold).body.dump()
        await queued
      } finally {
        await stop(other.server)
      }
    }
  )

  it('close() lets a request dispatched in the same turn finish', async () => {
    handler = (_req, res) => res.end('ok')
    const agent = own(createAgent('t', { maxSockets: 2 }))
    await getText(agent, '/')
    const second = agent.dispatcher.request({ origin, path: '/', method: 'GET' })
    const closed = agent.close()
    const response = await second
    expect(response.statusCode).toBe(200)
    await expect(response.body.text()).resolves.toBe('ok')
    await closed
  })

  it('does not open a connection for a request that died in the pool queue', async () => {
    const held: Array<() => void> = []
    handler = (req, res) => {
      if (req.url === '/held') held.push(() => res.end('held'))
      else if (req.url === '/close') res.setHeader('connection', 'close').end('closed')
      else res.end('ok')
    }
    let connections = 0
    server.on('connection', () => connections++)
    const agent = own(createAgent('t', { maxSockets: 2, requestTimeoutMs: 0 }))
    const get = (path: string, signal?: AbortSignal) =>
      agent.dispatcher.request({ origin, path, method: 'GET', signal })
    const [a, z] = [new AbortController(), new AbortController()]
    const first = get('/held', a.signal).catch((error: unknown) => error)
    const second = get('/held')
    await vi.waitFor(() => expect(held).toHaveLength(2))
    const closing = get('/close')
    const zombie = get('/zombie', z.signal).catch((error: unknown) => error)
    z.abort()
    expect(await zombie).toMatchObject({ name: 'AbortError' })
    a.abort()
    expect(await first).toMatchObject({ name: 'AbortError' })
    await expect((await closing).body.text()).resolves.toBe('closed')
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(connections).toBe(3)
    held.forEach((release) => release())
    await (await second).body.text()
  })

  it('keeps a keep-alive socket when an upload is cancelled just before undici starts it', async () => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval'] })
    const paths: string[] = []
    handler = (req, res) => {
      paths.push(req.url ?? '')
      res.end('ok')
    }
    let connections = 0
    server.on('connection', () => connections++)
    const agent = own(createAgent('t', { maxSockets: 1 }))
    agent.monitor()
    expect(await getText(agent, '/warm')).toBe('ok')
    await new Promise((resolve) => setImmediate(resolve))
    const abort = new AbortController()
    const input = own(new PassThrough())
    input.write('a')
    const cancelled = put(agent, input, 2, abort.signal)
    abort.abort()
    await expect(cancelled).rejects.toMatchObject({ name: 'AbortError' })
    expect(input.destroyed).toBe(true)
    expect(await getText(agent, '/next')).toBe('ok')
    expect(paths).toEqual(['/warm', '/next'])
    expect(connections).toBe(1)
    // Undici reads the cancelled body at its turn, which must not start the upload input timeout.
    await vi.advanceTimersByTimeAsync(35_000)
    expect(errors.record).not.toHaveBeenCalledWith(
      1,
      expect.objectContaining({ type: 'timeout_socket_error' })
    )
  })

  it('never sends a request that was cancelled with a falsy reason while it waited', async () => {
    const paths: string[] = []
    const held = Promise.withResolvers<void>()
    handler = async (req, res) => {
      paths.push(req.url ?? '')
      if (req.url === '/held') await held.promise
      res.end('ok')
    }
    const agent = own(createAgent('t', { maxSockets: 1 }))
    const first = getText(agent, '/held')
    await vi.waitFor(() => expect(paths).toEqual(['/held']))
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
    const abort = new AbortController()
    const cancelled = agent.dispatcher.request({
      origin,
      path: '/cancelled',
      method: 'PUT',
      body: 'x',
      signal: abort.signal,
    })
    abort.abort(0)
    await expect(cancelled).rejects.toMatchObject({ name: 'AbortError', cause: 0 })
    // The acquisition deadline goes with the request.
    expect(vi.getTimerCount()).toBe(0)
    vi.useRealTimers()
    held.resolve()
    expect(await first).toBe('ok')
    expect(await getText(agent, '/next')).toBe('ok')
    expect(paths).toEqual(['/held', '/next'])
  })

  it.each(['aborts', 'has an invalid header'])(
    'keeps a connection attempt a request waits on when another request on its client %s',
    async (failure) => {
      handler = (_req, res) => res.end('ok')
      const agent = own(createAgent('t', { maxSockets: 1 }))
      const request = (init: Partial<Dispatcher.RequestOptions> = {}) =>
        agent.dispatcher.request({ origin, path: '/', method: 'GET', ...init }).then(
          ({ body }) => body.text(),
          (error: unknown) => error
        )
      // Undici rejects this at its start, leaving a connected, unused socket.
      await request({ method: 'PUT', headers: { 'content-length': '5' }, body: 'abc' })
      // This one starts inside dispatch, where its abort destroys the socket and leaves a stale
      // drain, so undici hands the client that reconnects for the next request another one.
      void request({ signal: AbortSignal.abort() })
      const waiting = request()
      const abort = new AbortController()
      let sibling = failure === 'aborts' ? request({ signal: abort.signal }) : undefined
      const onConnect = () =>
        process.nextTick(() => {
          abort.abort()
          sibling ??= request({ headers: { 'x-invalid': 'a\r\nb' } })
        })
      subscribe('undici:client:beforeConnect', onConnect)
      try {
        expect(await waiting).toBe('ok')
        expect(await sibling).toMatchObject(
          failure === 'aborts' ? { name: 'AbortError' } : { code: 'UND_ERR_INVALID_ARG' }
        )
      } finally {
        unsubscribe('undici:client:beforeConnect', onConnect)
      }
    }
  )

  it('cancels a shared connection attempt once every request waiting on it has aborted', async () => {
    const sockets: Socket[] = []
    const tcp = createTcpServer((socket) => sockets.push(socket.resume()))
    await new Promise<void>((resolve) => tcp.listen(0, '127.0.0.1', resolve))
    const agent = own(createAgent('t', { maxSockets: 1 }))
    const endpoint = `https://127.0.0.1:${(tcp.address() as AddressInfo).port}`
    const get = (signal: AbortSignal) =>
      agent.dispatcher
        .request({ origin: endpoint, path: '/', method: 'GET', signal })
        .catch((error: unknown) => error)
    const first = new AbortController()
    const second = new AbortController()
    let reentered: Promise<unknown> | undefined
    // Dispatching from here queues a second request on the connecting client.
    const reenter = () => {
      unsubscribe('undici:client:beforeConnect', reenter)
      reentered = get(second.signal)
    }
    subscribe('undici:client:beforeConnect', reenter)
    const initial = get(first.signal)
    try {
      await vi.waitFor(() => expect(sockets).toHaveLength(1))
      second.abort()
      expect(await reentered).toMatchObject({ name: 'AbortError' })
      await new Promise((resolve) => setTimeout(resolve, 30))
      expect(sockets[0].destroyed).toBe(false)
      first.abort()
      expect(await initial).toMatchObject({ name: 'AbortError' })
      await vi.waitFor(() => expect(sockets[0].destroyed).toBe(true))
    } finally {
      unsubscribe('undici:client:beforeConnect', reenter)
      for (const socket of sockets) socket.destroy()
      await new Promise<void>((resolve) => tcp.close(() => resolve()))
    }
  })

  it('cancels the connection attempt of an aborted request after an invalid header failed on its client', async () => {
    const sockets: Socket[] = []
    const tcp = createTcpServer((socket) => sockets.push(socket.resume()))
    await new Promise<void>((resolve) => tcp.listen(0, '127.0.0.1', resolve))
    const agent = own(createAgent('t', { maxSockets: 1 }))
    const get = (init: Partial<Dispatcher.RequestOptions>) =>
      agent.dispatcher
        .request({
          origin: `https://127.0.0.1:${(tcp.address() as AddressInfo).port}`,
          path: '/',
          method: 'GET',
          ...init,
        })
        .catch((error: unknown) => error)
    const abort = new AbortController()
    try {
      expect(await get({ headers: { 'x-invalid': 'a\r\nb' } })).toMatchObject({
        code: 'UND_ERR_INVALID_ARG',
      })
      const cancelled = get({ signal: abort.signal })
      await vi.waitFor(() => expect(sockets).toHaveLength(1))
      abort.abort()
      expect(await cancelled).toMatchObject({ name: 'AbortError' })
      await vi.waitFor(() => expect(sockets[0].destroyed).toBe(true))
    } finally {
      for (const socket of sockets) socket.destroy()
      await new Promise<void>((resolve) => tcp.close(() => resolve()))
    }
  })

  it('fails a waiting request aborted without a reason and never sends it', async () => {
    const paths: string[] = []
    const held = Promise.withResolvers<void>()
    handler = async (req, res) => {
      paths.push(req.url ?? '')
      if (req.url === '/held') await held.promise
      res.end('ok')
    }
    const agent = own(createAgent('t', { maxSockets: 1 }))
    const first = getText(agent, '/held')
    await vi.waitFor(() => expect(paths).toEqual(['/held']))
    let failure: unknown
    agent.dispatcher.dispatch(
      { origin, path: '/cancelled', method: 'PUT', body: 'x' },
      {
        onRequestStart: (controller) => controller.abort(undefined as unknown as Error),
        onResponseError: (_controller, error) => {
          failure = error
        },
      }
    )
    expect(failure).toMatchObject({ code: 'UND_ERR_ABORTED' })
    held.resolve()
    expect(await first).toBe('ok')
    expect(await getText(agent, '/next')).toBe('ok')
    expect(paths).toEqual(['/held', '/next'])
  })

  it('rejects a stream upload that cannot connect without a timeout', async () => {
    const agent = own(createAgent('t', { maxSockets: 1 }))
    const input = own(new PassThrough())
    input.on('error', () => {})
    input.end('ab')
    await expect(
      agent.dispatcher.request({
        origin: 'http://127.0.0.1:1',
        path: '/',
        method: 'PUT',
        headers: { 'content-length': '2' },
        body: input,
      })
    ).rejects.toMatchObject({ code: 'ECONNREFUSED' })
    // An unhandled 'error' on the proxy body would surface here as an uncaught exception.
    await new Promise((r) => setTimeout(r, 50))
  })

  it('dispatches a waiting request after the busy connections are refused', async () => {
    const agent = own(createAgent('t', { maxSockets: 2 }))
    const refused = () =>
      agent.dispatcher
        .request({ origin: 'http://127.0.0.1:1', path: '/', method: 'GET' })
        .catch((error: { code?: string }) => error.code)
    // Undici's pool drops a refused client without emitting 'drain'.
    expect(await Promise.all([refused(), refused(), refused()])).toEqual([
      'ECONNREFUSED',
      'ECONNREFUSED',
      'ECONNREFUSED',
    ])
  }, 10_000)

  it.each([
    { name: 'is complete and unread', size: 16 * 1024 },
    { name: 'is still downloading into a reader', size: 4 * 1024 * 1024 },
  ])(
    'raises no unhandled error when a response body that $name is destroyed',
    async ({ name, size }) => {
      handler = (_req, res) => res.end(Buffer.alloc(size, 1))
      const agent = own(createAgent('t', { maxSockets: 1 }))
      const uncaught = vi.fn()
      process.on('uncaughtException', uncaught)
      try {
        const { body } = await agent.dispatcher.request({ origin, path: '/', method: 'GET' })
        // Destroyed from a callback, the error is emitted before undici's own error path can catch it.
        if (name === 'is complete and unread') setTimeout(() => body.destroy(), 50)
        else body.once('data', () => body.destroy())
        await new Promise((resolve) => setTimeout(resolve, 100))
        expect(uncaught).not.toHaveBeenCalled()
      } finally {
        process.off('uncaughtException', uncaught)
      }
    }
  )

  it.each(['before the headers', 'during the body'] as const)(
    'fails a request whose socket reports an error %s without closing',
    async (phase) => {
      let received = false
      handler = (_req, res) => {
        received = true
        if (phase === 'during the body') res.write('partial')
      }
      const agent = own(createAgent('t', { maxSockets: 1 }))
      const failure = Object.assign(new Error('bad record mac'), {
        code: 'ERR_SSL_DECRYPTION_FAILED_OR_BAD_RECORD_MAC',
      })
      let socket: Socket | undefined
      const capture = (message: unknown) => {
        socket = (message as { socket: Socket }).socket
      }
      subscribe('undici:client:sendHeaders', capture)
      try {
        const request = agent.dispatcher.request({ origin, path: '/', method: 'GET' })
        if (phase === 'before the headers') {
          const settled = request.catch((error: unknown) => error)
          await vi.waitFor(() => expect(socket && received).toBeTruthy())
          socket?.emit('error', failure)
          expect(await settled).toBe(failure)
        } else {
          const { body } = await request
          const errored = new Promise((resolve) => body.once('error', resolve))
          socket?.emit('error', failure)
          expect(await errored).toMatchObject({ code: 'ECONNRESET', cause: failure })
        }
      } finally {
        unsubscribe('undici:client:sendHeaders', capture)
        expect(socket?.destroyed).toBe(true)
      }
    }
  )

  it.each([
    [
      'cut short on a closing connection',
      'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhel',
    ],
    [
      'with an invalid chunk size',
      'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\nZZ\r\n',
    ],
    [
      'with oversized trailers',
      `HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n0\r\nx-big: ${'a'.repeat(20_000)}\r\n\r\n`,
    ],
  ])('reports a chunked response %s as a connection reset', async (_name, response) => {
    const raw = createTcpServer((socket) => {
      socket.on('error', () => {})
      socket.once('data', () => socket.end(response))
    })
    await new Promise<void>((resolve) => raw.listen(0, '127.0.0.1', resolve))
    try {
      const agent = own(createAgent('t', { maxSockets: 1 }))
      const { body } = await agent.dispatcher.request({
        origin: `http://127.0.0.1:${(raw.address() as AddressInfo).port}`,
        path: '/',
        method: 'GET',
      })
      await expect(body.text()).rejects.toMatchObject({ code: 'ECONNRESET' })
    } finally {
      await new Promise((resolve) => raw.close(resolve))
    }
  })

  it.each(['buffer', 'string'] as const)(
    'writes a %s one byte over the write size in one write over plain HTTP',
    async (kind) => {
      let received = 0
      handler = (req, res) => {
        req.on('data', (chunk: Buffer) => {
          received += chunk.length
        })
        req.on('end', () => res.end('ok'))
      }
      const agent = own(createAgent('t', { maxSockets: 1 }))
      const size = 256 * 1024 + 1
      const writes = vi.spyOn(Socket.prototype, 'write')
      const { body } = await agent.dispatcher.request({
        origin,
        path: '/',
        method: 'PUT',
        body: kind === 'string' ? 'a'.repeat(size) : Buffer.alloc(size, 1),
      })
      await body.dump()
      expect(received).toBe(size)
      // A plain socket's timeout sees the progress of a pending write.
      expect(Math.max(...writes.mock.calls.map(([chunk]) => chunk.length))).toBe(size)
    }
  )

  it('slices a buffer over the write size for a TLS origin', async () => {
    const bodies: unknown[] = []
    const collect = (message: unknown) => {
      bodies.push((message as { request: { body: unknown } }).request.body)
    }
    const agent = own(createAgent('t', { maxSockets: 1 }))
    subscribe('undici:request:create', collect)
    try {
      await agent.dispatcher
        .request({
          origin: origin.replace('http:', 'https:'),
          path: '/',
          method: 'PUT',
          body: Buffer.alloc(256 * 1024 + 1, 1),
        })
        .catch(() => {})
      expect(bodies).toHaveLength(1)
      // A TLS socket only refreshes its timeout when a write completes.
      expect(bodies[0]).not.toBeInstanceOf(Uint8Array)
    } finally {
      unsubscribe('undici:request:create', collect)
    }
  })

  it.each(['new', 'connected, unused'])(
    'does not abort a request started on a %s socket when the acquisition deadline passes',
    async (socket) => {
      let end: (() => void) | undefined
      handler = (_req, res) => {
        res.writeHead(200).flushHeaders()
        end = () => res.end('done')
      }
      const agent = own(createAgent('t', { maxSockets: 1 }))
      if (socket !== 'new') {
        // Undici rejects this at its start, then starts the next request inside dispatch.
        await agent.dispatcher
          .request({
            origin,
            path: '/',
            method: 'PUT',
            headers: { 'content-length': '5' },
            body: 'abc',
          })
          .catch(() => {})
      }
      vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
      const { body } = await agent.dispatcher.request({
        origin,
        path: '/',
        method: 'GET',
        headersTimeout: 0,
        bodyTimeout: 0,
      })
      await vi.advanceTimersByTimeAsync(6000)
      vi.useRealTimers()
      end?.()
      expect(await body.text()).toBe('done')
    }
  )

  it('close() keeps serving requests issued while it waits', async () => {
    const release: Array<() => void> = []
    handler = (_req, res) => {
      release.push(() => res.end('ok'))
    }
    const agent = own(createAgent('t', { maxSockets: 2 }))
    const text = (path: string) => getText(agent, path)
    const first = text('/a')
    await vi.waitFor(() => expect(release).toHaveLength(1))
    let done = false
    const closing = agent.close().then(() => {
      done = true
    })
    const second = text('/b')
    await vi.waitFor(() => expect(release).toHaveLength(2))
    release[1]()
    expect(await second).toBe('ok')
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect(done).toBe(false)
    release[0]()
    expect(await first).toBe('ok')
    await closing
    expect(done).toBe(true)
  })

  it('keeps the header timeout alive while a plain GET receives header fragments', async () => {
    // Undici's fast timers fire up to ~500 ms late, so outlast that to tell them apart.
    const sockets = new Set<Socket>()
    const raw = createTcpServer((socket) => {
      sockets.add(socket)
      socket.on('error', () => {})
      socket.once('data', () => {
        socket.write('HTTP/1.1 200 OK\r\n')
        let i = 0
        const iv = setInterval(() => {
          if (i < 25) socket.write(`x-part-${i++}: 1\r\n`)
          else {
            clearInterval(iv)
            socket.write('content-length: 2\r\n\r\nok')
          }
        }, 40)
        socket.on('close', () => clearInterval(iv))
      })
    })
    await new Promise<void>((r) => raw.listen(0, '127.0.0.1', r))
    try {
      const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 }))
      const { statusCode, body } = await agent.dispatcher.request({
        origin: `http://127.0.0.1:${(raw.address() as AddressInfo).port}`,
        path: '/',
        method: 'GET',
        signal: AbortSignal.timeout(3000),
      })
      expect(statusCode).toBe(200)
      expect(await body.text()).toBe('ok')
    } finally {
      for (const socket of sockets) socket.destroy()
      await new Promise((r) => raw.close(r))
    }
  })

  it('keeps every byte of an upload whose source outpaces a slow server', async () => {
    const chunks = Array.from({ length: 8 }, () => randomBytes(700 * 1024))
    const hash = createHash('sha256')
    handler = (req, res) => {
      req.pause()
      setTimeout(() => req.resume(), 300)
      req.on('data', (chunk: Buffer) => hash.update(chunk))
      req.on('end', () => res.end())
    }
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 5000 }))
    const size = 8 * 700 * 1024
    await (await put(agent, Readable.from(chunks), size)).body.dump()
    expect(hash.digest('hex')).toBe(
      createHash('sha256').update(Buffer.concat(chunks)).digest('hex')
    )
  })

  it('leaves no header timeout on a keep-alive socket after sequential requests', async () => {
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 1000 }))
    let socket: Socket | undefined
    const capture = (message: unknown) => {
      socket = (message as { socket: Socket }).socket
    }
    subscribe('undici:client:sendHeaders', capture)
    try {
      for (let i = 0; i < 5; i++) {
        await (await agent.dispatcher.request({ origin, path: '/', method: 'GET' })).body.dump()
      }
      expect(socket?.listenerCount('timeout')).toBe(0)
      expect(socket?.timeout || 0).toBe(0)
    } finally {
      unsubscribe('undici:client:sendHeaders', capture)
    }
  })

  it('times out the response headers after a slow upload source finishes', async () => {
    handler = (req) => {
      req.resume()
    }
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 0, copyTimeoutMs: 100 }))
    const input = own(new PassThrough())
    input.write('ab')
    setTimeout(() => input.end(), 250)
    const start = Date.now()
    await expect(
      agent.dispatcher.request({
        origin,
        path: '/',
        method: 'PUT',
        headers: { 'content-length': '2', 'x-amz-copy-source': 'bucket/src' },
        body: input,
        signal: AbortSignal.timeout(3000),
      })
    ).rejects.toMatchObject({ code: 'UND_ERR_HEADERS_TIMEOUT' })
    expect(Date.now() - start).toBeLessThan(1000)
  })

  // agentkeepalive's 30 s working-socket timeout bounded every phase on the old transport.
  it.each([
    { phase: 'response headers', code: 'UND_ERR_HEADERS_TIMEOUT' },
    { phase: 'upload input', code: 'ETIMEDOUT' },
  ])('idles out silent $phase with a zero request timeout', async ({ phase, code }) => {
    handler = (req) => {
      req.resume()
    }
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 0, bodyTimeoutMs: 200 }))
    const input = own(new PassThrough())
    if (phase === 'upload input') input.write('a')
    else input.end('a')
    const start = Date.now()
    await expect(
      agent.dispatcher.request({
        origin,
        path: '/',
        method: 'PUT',
        headers: { 'content-length': phase === 'upload input' ? '2' : '1' },
        body: input,
        signal: AbortSignal.timeout(3000),
      })
    ).rejects.toMatchObject({ code })
    expect(Date.now() - start).toBeLessThan(1000)
  })

  it('leaves stream uploads to the caller when every transport timeout is zero', async () => {
    let received = ''
    handler = (req, res) => {
      if (req.url === '/stall') return void req.resume()
      req.setEncoding('utf8').on('data', (chunk) => {
        received += chunk
      })
      req.on('end', () => res.end())
    }
    const agent = own(createAgent('t', { maxSockets: 1, requestTimeoutMs: 0, bodyTimeoutMs: 0 }))
    const put = (path: string, body: Readable, signal?: AbortSignal) =>
      agent.dispatcher.request({ origin, path, method: 'PUT', body, signal })
    const stalled = own(new PassThrough())
    stalled.write('a')
    await expect(put('/stall', stalled, AbortSignal.timeout(300))).rejects.toMatchObject({
      name: 'AbortError',
    })
    await put('/ok', Readable.from(['he', 'llo']))
    expect(received).toBe('hello')
  })
})

const opensslAvailable = (() => {
  try {
    execFileSync('openssl', ['version'], { stdio: 'ignore' })
    return true
  } catch {
    return false
  }
})()

describe.runIf(opensslAvailable)('createAgent over TLS', () => {
  let certDir: string
  let key: Buffer
  let cert: Buffer
  let originalCAs: string[]

  beforeAll(() => {
    certDir = mkdtempSync(join(tmpdir(), 'agent-tls-'))
    execFileSync(
      'openssl',
      [
        'req',
        '-x509',
        '-newkey',
        'ec',
        '-pkeyopt',
        'ec_paramgen_curve:prime256v1',
        '-nodes',
        '-keyout',
        join(certDir, 'key.pem'),
        '-out',
        join(certDir, 'cert.pem'),
        '-days',
        '2',
        '-subj',
        '/CN=127.0.0.1',
        '-addext',
        'subjectAltName=IP:127.0.0.1',
      ],
      { stdio: 'ignore' }
    )
    key = readFileSync(join(certDir, 'key.pem'))
    cert = readFileSync(join(certDir, 'cert.pem'))
    originalCAs = tls.getCACertificates('default')
    tls.setDefaultCACertificates([...originalCAs, cert.toString()])
  })

  afterAll(() => {
    tls.setDefaultCACertificates(originalCAs)
    rmSync(certDir, { recursive: true, force: true })
  })

  it.each([1, 2])(
    'serves waiting requests once the caller of a hung handshake aborts with %s sockets',
    async (maxSockets) => {
      const held = Promise.withResolvers<() => void>()
      const tlsServer = https.createServer({ key, cert }, (req, res) => {
        if (req.url === '/held') held.resolve(() => res.end('held'))
        else res.end('ok')
      })
      const hung: Socket[] = []
      let accepted = 0
      const tcp = createTcpServer((socket) => {
        if (++accepted === maxSockets) hung.push(socket.resume())
        else tlsServer.emit('connection', socket)
      })
      await new Promise<void>((r) => tcp.listen(0, '127.0.0.1', r))
      const agent = createAgent('t', { maxSockets })
      const get = async (path: string, signal?: AbortSignal) => {
        const { body } = await agent.dispatcher.request({
          origin: `https://127.0.0.1:${(tcp.address() as AddressInfo).port}`,
          path,
          method: 'GET',
          signal,
        })
        return body.text()
      }
      try {
        const occupied = maxSockets > 1 ? get('/held') : undefined
        if (occupied) await held.promise
        const abort = new AbortController()
        const cancelled = get('/cancelled', abort.signal).catch((error: unknown) => error)
        await vi.waitFor(() => expect(hung).toHaveLength(1))
        const waiting = [get('/next'), get('/last')]
        abort.abort()
        expect(await cancelled).toMatchObject({ name: 'AbortError' })
        expect(await Promise.all(waiting)).toEqual(['ok', 'ok'])
        expect(accepted).toBe(maxSockets + 1)
        if (occupied) {
          ;(await held.promise)()
          expect(await occupied).toBe('held')
        }
      } finally {
        for (const socket of hung) socket.destroy()
        await agent.dispatcher.destroy()
        await new Promise((r) => tcp.close(r))
      }
    }
  )

  it('cancels a hung reconnect whose caller aborts after its client served a request', async () => {
    const tlsServer = https.createServer({ key, cert }, (_req, res) => {
      res.setHeader('connection', 'close')
      res.end('ok')
    })
    const hung: Socket[] = []
    let accepted = 0
    const tcp = createTcpServer((socket) => {
      if (++accepted === 2) hung.push(socket.resume())
      else tlsServer.emit('connection', socket)
    })
    await new Promise<void>((r) => tcp.listen(0, '127.0.0.1', r))
    const agent = createAgent('t', { maxSockets: 1 })
    const get = async (path: string, signal?: AbortSignal) => {
      const { body } = await agent.dispatcher.request({
        origin: `https://127.0.0.1:${(tcp.address() as AddressInfo).port}`,
        path,
        method: 'GET',
        signal,
      })
      return body.text()
    }
    try {
      expect(await get('/first')).toBe('ok')
      const abort = new AbortController()
      const cancelled = get('/cancelled', abort.signal).catch((error: unknown) => error)
      await vi.waitFor(() => expect(hung).toHaveLength(1))
      abort.abort()
      expect(await cancelled).toMatchObject({ name: 'AbortError' })
      await vi.waitFor(() => expect(hung[0].destroyed).toBe(true))
    } finally {
      for (const socket of hung) socket.destroy()
      await agent.dispatcher.destroy()
      await new Promise((r) => tcp.close(r))
    }
  })

  it('keeps a slow consumer alive while the body keeps arriving', async () => {
    const chunk = 64 * 1024
    const timers: NodeJS.Timeout[] = []
    const tlsServer = https.createServer({ key, cert }, (_req, res) => {
      res.writeHead(200, { 'content-length': 12 * chunk })
      for (let i = 0; i < 12; i++) {
        timers.push(
          setTimeout(() => {
            if (i < 11) res.write(Buffer.alloc(chunk))
            else res.end(Buffer.alloc(chunk))
          }, i * 60)
        )
      }
    })
    await new Promise<void>((r) => tlsServer.listen(0, '127.0.0.1', r))
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 5000, bodyTimeoutMs: 200 })
    try {
      const { body } = await agent.dispatcher.request({
        origin: `https://127.0.0.1:${(tlsServer.address() as AddressInfo).port}`,
        path: '/',
        method: 'GET',
      })
      let total = 0
      for await (const data of body) {
        total += data.length
        await new Promise((r) => setTimeout(r, 80))
      }
      expect(total).toBe(12 * chunk)
    } finally {
      timers.forEach(clearTimeout)
      tlsServer.closeAllConnections()
      await agent.close()
      await new Promise((r) => tlsServer.close(r))
    }
  })
})
