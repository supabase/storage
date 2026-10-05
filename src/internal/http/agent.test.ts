import * as http from 'node:http'
import type { AddressInfo, Socket } from 'node:net'
import { type Duplex, PassThrough, Readable } from 'node:stream'
import {
  CompleteMultipartUploadCommand,
  DeleteObjectsCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from '@aws-sdk/client-s3'
import { normalizeRawError, StorageBackendError } from '@internal/errors'
import { ConfiguredRetryStrategy } from '@smithy/core/retry'
import { HttpRequest } from '@smithy/protocol-http'
import { UndiciHttpHandler } from '@smithy/undici-http-handler'
import type { Agent, Dispatcher } from 'undici'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

const { busy, free, pending, errors, agentOptions } = vi.hoisted(() => ({
  busy: { record: vi.fn() },
  free: { record: vi.fn() },
  pending: { record: vi.fn() },
  errors: { record: vi.fn() },
  agentOptions: [] as Agent.Options[],
}))

vi.mock('undici', async (importOriginal) => {
  const undici = await importOriginal<typeof import('undici')>()
  return {
    ...undici,
    Agent: class extends undici.Agent {
      constructor(options: Agent.Options) {
        super(options)
        agentOptions.push(options)
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

beforeEach(async () => {
  agentOptions.length = 0
  server = http.createServer((req, res) => handler(req, res))
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r))
  origin = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
})

afterEach(async () => {
  vi.useRealTimers()
  server.closeAllConnections()
  await new Promise((r) => server.close(r))
})

function createS3Client(agent: InstrumentedAgent, maxAttempts = 3) {
  return new S3Client({
    endpoint: origin,
    region: 'us-east-1',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    forcePathStyle: true,
    maxAttempts,
    retryStrategy: new ConfiguredRetryStrategy(maxAttempts, () => 0),
    requestHandler: new UndiciHttpHandler({ dispatcher: agent.dispatcher }),
  })
}

describe('createAgent', () => {
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
  const failures = {
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
      causeCode: undefined,
    },
    reset: {
      failure: 'reset',
      name: 'Error',
      code: 'ECONNRESET',
      causeCode: undefined,
    },
    headers: {
      failure: 'headers',
      name: 'TimeoutError',
      code: 'UND_ERR_HEADERS_TIMEOUT',
      causeCode: undefined,
    },
  }

  it.each([
    { ...list, ...failures.socket, requestTimeoutMs: 0 },
    { ...list, ...failures.socket, requestTimeoutMs: 100 },
    { ...list, ...failures.length, requestTimeoutMs: 0 },
    { ...list, ...failures.length, requestTimeoutMs: 100 },
    { ...list, ...failures.body, requestTimeoutMs: 100 },
    // CompleteMultipartUpload also buffers the body in throw200ExceptionsMiddleware.
    { ...complete, ...failures.body, requestTimeoutMs: 100 },
    { ...deleteObjects, ...failures.socket, requestTimeoutMs: 100 },
    { ...list, ...failures.preSocket, requestTimeoutMs: 100 },
    { ...list, ...failures.preReset, requestTimeoutMs: 100 },
    { ...list, ...failures.reset, requestTimeoutMs: 100 },
    { ...list, ...failures.headers, requestTimeoutMs: 100 },
  ])('retries $operation XML after a $failure failure with timeout $requestTimeoutMs', async ({
    send,
    failure,
    requestTimeoutMs,
    name,
    code,
    causeCode,
  }) => {
    let requests = 0
    handler = (req, res) => {
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
    }
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs })
    const client = createS3Client(agent)
    try {
      const error = await send(client).catch((error: unknown) => error)
      expect(error).toMatchObject({
        name,
        code,
        $metadata: { attempts: 3 },
      })
      if (causeCode) expect(error).toMatchObject({ cause: { code: causeCode } })
      const wrapped = StorageBackendError.fromError(error)
      expect(wrapped.render()).toMatchObject({ statusCode: '500', code: 'S3Error', message: name })
      expect(normalizeRawError(error, 'info')).toMatchObject({ name })
      expect(JSON.parse(normalizeRawError(wrapped, 'info').raw ?? '').originalError.name).toBe(name)
      expect(requests).toBe(3)
    } finally {
      server.closeAllConnections()
      await agent.close()
      client.destroy()
    }
  })

  it.each([
    'socket',
    'length',
    'body',
  ] as const)('preserves the GetObject %s error name without retrying after returning the stream', async (failure) => {
    let requests = 0
    handler = (req, res) => {
      requests++
      res.writeHead(200, failure === 'length' ? { 'content-length': '2', connection: 'close' } : {})
      if (failure === 'length') return res.end('a')
      res.write('a', () => {
        if (failure !== 'body') req.socket.end()
      })
    }
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 })
    const client = createS3Client(agent)
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
      server.closeAllConnections()
      await agent.close()
      client.destroy()
    }
  })

  it('keeps upload source errors unchanged', async () => {
    let received = false
    handler = (req) => {
      req.on('data', () => {
        received = true
      })
    }
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 })
    const input = new PassThrough()
    input.write('a')
    try {
      const request = agent.dispatcher.request({
        origin,
        path: '/',
        method: 'PUT',
        headers: { 'content-length': '2' },
        body: input,
        signal: AbortSignal.timeout(2000),
      })
      const rejection = request.then(
        () => undefined,
        (error: unknown) => error
      )
      await vi.waitFor(() => expect(received).toBe(true))
      const sourceError = Object.assign(new Error('aborted'), { code: 'ECONNRESET' })
      input.destroy(sourceError)
      expect(await rejection).toBe(sourceError)
      expect(sourceError.name).toBe('Error')
    } finally {
      input.destroy()
      await agent.close()
    }
  })

  it('leaves slow upload sources alone without a timeout', async () => {
    let received = ''
    handler = (req, res) => {
      req.on('data', (chunk: Buffer) => {
        received += chunk.toString()
      })
      req.on('end', () => res.end('ok'))
    }
    const agent = createAgent('t', { maxSockets: 1 })
    const input = Readable.from(
      (async function* () {
        await new Promise((resolve) => setTimeout(resolve, 50))
        yield Buffer.from('a')
      })()
    )
    try {
      const { body } = await agent.dispatcher.request({
        origin,
        path: '/',
        method: 'PUT',
        headers: { 'content-length': '1' },
        body: input,
      })
      expect(await body.text()).toBe('ok')
      expect(received).toBe('a')
    } finally {
      input.destroy()
      await agent.close()
    }
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
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 })
    agent.monitor()
    const disconnected = new Promise<void>((resolve) => {
      agent.dispatcher.once('disconnect', () => resolve())
    })
    const input = new PassThrough()
    input.write('a')
    const client = new UndiciHttpHandler({ dispatcher: agent.dispatcher })
    const url = new URL(origin)

    try {
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
      vi.advanceTimersByTime(5000)
      expect(errors.record).toHaveBeenCalledWith(
        1,
        expect.objectContaining({ type: 'timeout_socket_error' })
      )

      const { body } = await agent.dispatcher.request({ origin, path: '/', method: 'GET' })
      expect(await body.text()).toBe('ok')
    } finally {
      input.destroy()
      await agent.close()
    }
  })

  it('allows uploads that make progress for longer than the inactivity timeout', async () => {
    let received = ''
    handler = (req, res) => {
      req.on('data', (chunk: Buffer) => {
        received += chunk.toString()
      })
      req.on('end', () => res.end('ok'))
    }
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 200 })
    const input = Readable.from(
      (async function* () {
        for (let i = 0; i < 8; i++) {
          await new Promise((resolve) => setTimeout(resolve, 50))
          yield Buffer.from('a')
        }
      })()
    )
    try {
      const { body } = await agent.dispatcher.request({
        origin,
        path: '/',
        method: 'PUT',
        headers: { 'content-length': '8' },
        body: input,
      })
      expect(await body.text()).toBe('ok')
      expect(received).toBe('aaaaaaaa')
    } finally {
      input.destroy()
      await agent.close()
    }
  })

  it('times out even when destroying the upload source waits for its async generator', async () => {
    handler = (req) => req.resume()
    let release: () => void = () => {}
    const stalled = new Promise<void>((resolve) => {
      release = resolve
    })
    const input = Readable.from(
      (async function* () {
        yield Buffer.from('a')
        await stalled
        yield Buffer.from('b')
      })()
    )
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 })
    try {
      await expect(
        agent.dispatcher.request({
          origin,
          path: '/',
          method: 'PUT',
          headers: { 'content-length': '2' },
          body: input,
          signal: AbortSignal.timeout(2000),
        })
      ).rejects.toMatchObject({ name: 'TimeoutError', code: 'ETIMEDOUT' })
    } finally {
      release()
      input.destroy()
      await agent.close()
    }
  })

  it('does not run the input timer while a buffered chunk waits for socket drainage', async () => {
    let resume: NodeJS.Timeout | undefined
    let received = 0
    handler = (req, res) => {
      req.pause()
      resume = setTimeout(() => req.resume(), 250)
      req.on('data', (chunk: Buffer) => {
        received += chunk.length
      })
      req.on('end', () => res.end('ok'))
    }
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 })
    const input = new PassThrough()
    const length = 32 * 1024 * 1024
    input.write(Buffer.alloc(length))
    const end = setTimeout(() => input.end(), 200)
    try {
      const { body } = await agent.dispatcher.request({
        origin,
        path: '/',
        method: 'PUT',
        headers: { 'content-length': String(length) },
        body: input,
        signal: AbortSignal.timeout(3000),
      })
      expect(await body.text()).toBe('ok')
      expect(received).toBe(length)
    } finally {
      clearTimeout(end)
      clearTimeout(resume)
      input.destroy()
      await agent.close()
    }
  })

  it('rearms the input timeout after it fires during socket drainage', async () => {
    let resume: NodeJS.Timeout | undefined
    let received = 0
    handler = (req) => {
      req.pause()
      resume = setTimeout(() => req.resume(), 250)
      req.on('data', (chunk: Buffer) => {
        received += chunk.length
      })
    }
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 })
    const input = new PassThrough()
    const length = 32 * 1024 * 1024
    input.write(Buffer.alloc(length))
    try {
      await expect(
        agent.dispatcher.request({
          origin,
          path: '/',
          method: 'PUT',
          headers: { 'content-length': String(length + 1) },
          body: input,
          signal: AbortSignal.timeout(3000),
        })
      ).rejects.toMatchObject({ name: 'TimeoutError', code: 'ETIMEDOUT' })
      expect(received).toBe(length)
      expect(input.destroyed).toBe(true)
    } finally {
      clearTimeout(resume)
      input.destroy()
      await agent.close()
    }
  })

  it.each([
    'abort',
    'socket error',
    'source close',
    'source error',
  ] as const)('cleans up a streamed upload on %s', async (failure) => {
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
    const disconnected = new Promise<void>((resolve) => {
      agent.dispatcher.once('disconnect', () => resolve())
    })
    const input = new PassThrough()
    input.write('a')
    const controller = new AbortController()
    const request = agent.dispatcher.request({
      origin,
      path: '/',
      method: 'PUT',
      body: input,
      signal: controller.signal,
    })
    // Attach the rejection handler before triggering the failure.
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
      vi.advanceTimersByTime(5000)
      expect(errors.record).toHaveBeenCalledWith(
        failure === 'socket error' ? 1 : 0,
        expect.objectContaining({ type: 'socket_error' })
      )
    } finally {
      controller.abort()
      input.destroy()
      await agent.close()
    }
  })

  it.each([
    'source close',
    'source error',
  ] as const)('stops a streamed upload on %s during socket drainage', async (failure) => {
    let received = false
    handler = (req) => {
      req.once('data', () => {
        req.pause()
        received = true
      })
    }
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 })
    const input = new PassThrough()
    const length = 32 * 1024 * 1024
    input.write(Buffer.alloc(length))
    const request = agent.dispatcher
      .request({
        origin,
        path: '/',
        method: 'PUT',
        headers: { 'content-length': String(length) },
        body: input,
        signal: AbortSignal.timeout(3000),
      })
      .catch((error: unknown) => error)
    try {
      await vi.waitFor(() => expect(received).toBe(true))
      if (failure === 'source close') {
        input.destroy()
        expect(await request).toMatchObject({ name: 'AbortError', code: 'UND_ERR_ABORTED' })
      } else {
        const sourceError = new Error('upload input failed')
        input.destroy(sourceError)
        expect(await request).toBe(sourceError)
      }
    } finally {
      input.destroy()
      await agent.close()
    }
  })

  it.each([
    'source close',
    'source error',
  ] as const)('propagates %s while the upload waits for a connection', async (failure) => {
    let release: (() => void) | undefined
    handler = (_req, res) => {
      release = () => res.end('ok')
    }
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 })
    const first = agent.dispatcher.request({ origin, path: '/', method: 'GET', headersTimeout: 0 })
    const input = new PassThrough()
    try {
      await vi.waitFor(() => expect(release).toBeDefined())
      const queued = agent.dispatcher
        .request({
          origin,
          path: '/',
          method: 'PUT',
          body: input,
          signal: AbortSignal.timeout(2000),
        })
        .catch((error: unknown) => error)
      const closed = new Promise<void>((resolve) => input.once('close', resolve))
      const sourceError = new Error('queued upload input failed')
      if (failure === 'source close') input.destroy()
      else input.destroy(sourceError)
      await closed
      release?.()
      expect(await (await first).body.text()).toBe('ok')
      if (failure === 'source close') {
        expect(await queued).toMatchObject({ name: 'AbortError', code: 'UND_ERR_ABORTED' })
      } else expect(await queued).toBe(sourceError)
    } finally {
      release?.()
      input.destroy()
      await agent.close()
    }
  })

  it.each([
    { requestTimeoutMs: undefined, headersTimeout: 0, bodyTimeout: 30_000 },
    { requestTimeoutMs: 0, headersTimeout: 0, bodyTimeout: 30_000 },
    { requestTimeoutMs: 100, headersTimeout: 100, bodyTimeout: 100 },
    { requestTimeoutMs: 60_000, headersTimeout: 60_000, bodyTimeout: 60_000 },
  ])('configures response timeouts for $requestTimeoutMs', async (options) => {
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: options.requestTimeoutMs })
    try {
      expect(agentOptions).toEqual([
        expect.objectContaining({
          headersTimeout: options.headersTimeout,
          bodyTimeout: options.bodyTimeout,
        }),
      ])
    } finally {
      await agent.close()
    }
  })

  it('delivers an early response that outlasts the input timeout and releases the input', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    let progress: NodeJS.Timeout | undefined
    const response = '<Error/>'
    handler = (req, res) => {
      const write = (offset: number) => {
        if (offset === response.length - 1) return res.end(response[offset])
        res.write(response[offset])
        progress = setTimeout(() => write(offset + 1), 40)
      }
      req.once('data', () => {
        res.writeHead(403)
        write(0)
      })
    }
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 100 })
    agent.monitor()
    const input = new PassThrough()
    input.write('a')
    try {
      const { statusCode, body } = await agent.dispatcher.request({
        origin,
        path: '/',
        method: 'PUT',
        headers: { 'content-length': '2' },
        body: input,
        signal: AbortSignal.timeout(3000),
      })
      expect(statusCode).toBe(403)
      expect(await body.text()).toBe(response)
      expect(input.destroyed).toBe(true)
      vi.advanceTimersByTime(5000)
      expect(errors.record).toHaveBeenCalledWith(
        0,
        expect.objectContaining({ type: 'timeout_socket_error' })
      )

      handler = (_req, res) => res.end('ok')
      const next = await agent.dispatcher.request({ origin, path: '/', method: 'GET' })
      expect(await next.body.text()).toBe('ok')
    } finally {
      clearTimeout(progress)
      input.destroy()
      await agent.close()
    }
  })

  it.each([
    'headers',
    'body',
    'socket',
  ] as const)('records established-connection %s errors', async (failure) => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    handler = (_req, res) => {
      if (failure === 'body') {
        res.writeHead(200)
        res.write('a')
      } else if (failure === 'socket') res.destroy()
    }
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 50 })
    agent.monitor()
    try {
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
      vi.advanceTimersByTime(5000)
      expect(errors.record).toHaveBeenCalledWith(
        1,
        expect.objectContaining({
          type: failure === 'socket' ? 'socket_error' : 'timeout_socket_error',
        })
      )
      expect(errors.record).toHaveBeenCalledWith(
        0,
        expect.objectContaining({ type: 'connect_error' })
      )
    } finally {
      await agent.close()
    }
  })

  it('monitor() does not keep the process alive', async () => {
    const agent = createAgent('t', { maxSockets: 1 })
    const timer = agent.monitor()
    expect(timer.hasRef()).toBe(false)
    await agent.close()
  })

  it('monitor() is idempotent', async () => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    const agent = createAgent('t', { maxSockets: 1 })
    const timer = agent.monitor()
    expect(agent.monitor()).toBe(timer)
    expect(vi.getTimerCount()).toBe(1)
    await agent.close()
    expect(vi.getTimerCount()).toBe(0)
  })

  it('reuses the connection across HEAD requests', async () => {
    let connections = 0
    server.on('connection', () => connections++)
    handler = (_req, res) => {
      res.writeHead(200, { 'content-length': '0' })
      res.end()
    }
    const agent = createAgent('t', { maxSockets: 1 })
    for (let i = 0; i < 2; i++) {
      const { body } = await agent.dispatcher.request({ origin, path: '/', method: 'HEAD' })
      await body.dump()
    }
    expect(connections).toBe(1)
    await agent.close()
  })

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
      type: 'connect_error',
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
        type: 'connect_error',
      })
    }
    expect(free.record).toHaveBeenCalledWith(0, { name: 't', protocol: 'https' })

    await agent.close()
    errors.record.mockClear()
    vi.advanceTimersByTime(5000)
    expect(errors.record).not.toHaveBeenCalled()
  })

  it('allows buffered bodies whose socket write outlasts the inactivity timeout', async () => {
    const intervals: NodeJS.Timeout[] = []
    const received: number[] = []
    const slowReadBytes = 8 * 1024 * 1024
    // Outlast the timer while leaving a tail larger than the socket buffers, then drain at once.
    handler = (req, res) => {
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
    }
    const agent = createAgent('t', { maxSockets: 3, requestTimeoutMs: 1500 })
    const text = 'é'.repeat(16 * 1024 * 1024)
    const buffer = Buffer.from(text)
    const send = (body: Buffer | Readable | string) =>
      agent.dispatcher.request({
        origin,
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
      server.closeAllConnections()
      await agent.close()
    }
  }, 10000)

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
    const agent = createAgent('t', { maxSockets: 1, requestTimeoutMs: 200 })
    const client = createS3Client(agent, 1)
    const keys = Array.from({ length: 1000 }, (_, i) => `tenant/bucket/${"'".repeat(990)}${i}`)
    try {
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
    } finally {
      server.closeAllConnections()
      await agent.close()
      client.destroy()
    }
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

  it.each([
    'closes',
    'resets',
  ] as const)('reports an idle single connection as free and ignores the server when it %s it', async (idle) => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
    let socket: Socket | undefined
    handler = (req, res) => {
      socket = req.socket
      res.end('ok')
    }
    const agent = createAgent('t', { maxSockets: 1 })
    agent.monitor()
    const disconnected = new Promise<void>((resolve) => {
      agent.dispatcher.once('disconnect', () => resolve())
    })
    try {
      const { body } = await agent.dispatcher.request({ origin, path: '/', method: 'GET' })
      await body.text()
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
    } finally {
      await agent.close()
    }
  })

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
  })
})
