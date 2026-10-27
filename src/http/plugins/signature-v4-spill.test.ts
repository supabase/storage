import fs, { type ReadStream } from 'node:fs'
import * as fsp from 'node:fs/promises'
import { request as httpRequest, type ServerResponse } from 'node:http'
import type { AddressInfo } from 'node:net'
import os from 'node:os'
import path from 'node:path'
import { Readable } from 'node:stream'
import { finished } from 'node:stream/promises'
import { StorageBackendError } from '@internal/errors'
import { Hash } from '@smithy/hash-node'
import { HttpRequest } from '@smithy/protocol-http'
import { SignatureV4 as AwsSignatureV4 } from '@smithy/signature-v4'
import Fastify, { type FastifyReply } from 'fastify'
import { setErrorHandler } from '../error-handler'

const { getJwtSecretMock, verifyJwtMock } = vi.hoisted(() => ({
  getJwtSecretMock: vi.fn(),
  verifyJwtMock: vi.fn(),
}))

vi.mock('../../config', () => ({
  getConfig: () => ({
    anonKeyAsync: Promise.resolve('anon-key'),
    serviceKeyAsync: Promise.resolve('service-key'),
    isMultitenant: false,
    jwtCachingEnabled: false,
    s3ProtocolAccessKeyId: 'access-key',
    s3ProtocolAccessKeySecret: 'secret-key',
    storageS3Region: 'us-east-1',
    s3ProtocolPrefix: '',
  }),
}))

vi.mock('@internal/auth', () => ({
  isJwtToken: () => false,
  verifyJWT: verifyJwtMock,
  verifyJWTWithCache: verifyJwtMock,
  signJWT: vi.fn(),
}))

vi.mock('@internal/database', () => ({
  getJwtSecret: getJwtSecretMock,
  getTenantConfig: vi.fn(),
  s3CredentialsManager: { getS3CredentialsByAccessKey: vi.fn() },
}))

const signer = new AwsSignatureV4({
  credentials: { accessKeyId: 'access-key', secretAccessKey: 'secret-key' },
  region: 'us-east-1',
  service: 's3vectors',
  sha256: Hash.bind(null, 'sha256'),
  // Exercise hashing and replay instead of trusting a supplied payload hash.
  applyChecksum: false,
})
const largeBody = JSON.stringify({ value: 'x'.repeat(5 * 1024 * 1024) })

async function signBody(body = largeBody) {
  return signer.sign(
    new HttpRequest({
      protocol: 'http:',
      hostname: 'storage.test',
      method: 'POST',
      path: '/probe',
      headers: {
        host: 'storage.test',
        'content-type': 'application/json',
        'content-length': String(Buffer.byteLength(body)),
      },
      body,
    })
  )
}

describe('SignatureV4 spill ownership', () => {
  let tmpRoot: string
  let activeApp: ReturnType<typeof Fastify> | undefined
  let readers: ReadStream[]
  let replay: Readable | undefined
  let spillDirsBeforeParsing: string[]
  let beforeParsing: (() => Promise<void>) | undefined

  beforeEach(async () => {
    tmpRoot = await fsp.mkdtemp(path.join(os.tmpdir(), 'sigv4-spill-test-'))
    vi.spyOn(os, 'tmpdir').mockReturnValue(tmpRoot)
    readers = []
    activeApp = undefined
    replay = undefined
    beforeParsing = undefined
    spillDirsBeforeParsing = []
    const createReadStream = fs.createReadStream
    vi.spyOn(fs, 'createReadStream').mockImplementation((...args) => {
      const reader = createReadStream(...args)
      readers.push(reader)
      return reader
    })
    getJwtSecretMock.mockResolvedValue({ secret: 'jwt-secret', jwks: null })
    verifyJwtMock.mockResolvedValue({ role: 'service_role', sub: 'service-user' })
  })

  async function buildApp(bodyLimit = 1024 * 1024, useProductionErrorHandler = false) {
    const { signatureV4 } = await import('./signature-v4')
    const app = Fastify({ bodyLimit })
    activeApp = app
    app.decorateRequest('tenantId', 'tenant-id')
    if (useProductionErrorHandler) {
      setErrorHandler(app, { respectStatusCode: true })
    } else {
      app.setErrorHandler((error, _request, reply) => {
        const status = error as { httpStatusCode?: number; statusCode?: number; code?: string }
        void reply
          .status(status.httpStatusCode ?? status.statusCode ?? 500)
          .send({ code: status.code })
      })
    }
    app.register(signatureV4, { allowBodyHash: true })
    app.post(
      '/probe',
      {
        preParsing: async (_request, _reply, payload) => {
          replay = payload
          spillDirsBeforeParsing = await fsp.readdir(tmpRoot)
          await beforeParsing?.()
          return payload
        },
      },
      async (_request, reply) => reply.status(204).send()
    )
    return app
  }

  async function expectSpillRemoved() {
    await vi.waitFor(async () => {
      expect(readers.every((reader) => reader.closed)).toBe(true)
      expect(await fsp.readdir(tmpRoot)).toEqual([])
    })
  }

  async function sendBodyOverHttp(
    app: ReturnType<typeof Fastify>,
    body = largeBody,
    prefixBytes?: number
  ) {
    await app.listen({ host: '127.0.0.1', port: 0 })
    const signed = await signBody(body)
    const client = httpRequest({
      host: '127.0.0.1',
      port: (app.server.address() as AddressInfo).port,
      method: 'POST',
      path: '/probe',
      headers: signed.headers,
    })
    client.on('error', () => {})
    if (prefixBytes === undefined) client.end(body)
    else client.write(body.slice(0, prefixBytes))
    return client
  }

  afterEach(async () => {
    await activeApp?.close()
    if (replay) {
      const closed = finished(replay, { cleanup: true }).catch(() => {})
      replay.destroy()
      await closed
    }
    for (const reader of readers) reader.destroy()
    await fsp.rm(tmpRoot, { force: true, recursive: true })
  })

  it('closes the replay and removes its spill when Fastify rejects the body before reading it', async () => {
    const app = await buildApp()
    const signed = await signBody()
    const response = await app.inject({
      method: 'POST',
      url: '/probe',
      headers: signed.headers,
      payload: largeBody,
    })

    expect(response.statusCode).toBe(413)
    expect(response.json()).toEqual({ code: 'FST_ERR_CTP_BODY_TOO_LARGE' })
    expect(spillDirsBeforeParsing).toHaveLength(1)
    expect(readers).toHaveLength(0)
    await vi.waitFor(async () => {
      expect(replay?.destroyed).toBe(true)
    })
    await expectSpillRemoved()
  })

  it('keeps the spill until a successful request has consumed the complete replay', async () => {
    const app = await buildApp(20 * 1024 * 1024)
    const signed = await signBody()
    let parsedBody: unknown
    app.addHook('preHandler', async (request) => {
      parsedBody = request.body
    })

    const response = await app.inject({
      method: 'POST',
      url: '/probe',
      headers: signed.headers,
      payload: largeBody,
    })

    expect(response.statusCode).toBe(204)
    expect(parsedBody).toEqual(JSON.parse(largeBody))
    expect(spillDirsBeforeParsing).toHaveLength(1)
    expect(readers).toHaveLength(1)
    expect((replay as Readable & { receivedEncodedLength?: number }).receivedEncodedLength).toBe(
      Buffer.byteLength(largeBody)
    )
    await expectSpillRemoved()
  })

  it.each(['ENOENT', 'EIO'] as const)(
    'returns a generic 500 and cleans up when reading the replay fails with %s',
    async (code) => {
      const app = await buildApp(20 * 1024 * 1024, true)
      let executionError: Error | undefined
      app.addHook('onError', async (_request, _reply, error) => {
        executionError = error
      })
      const createReadStream = vi.mocked(fs.createReadStream).getMockImplementation()!
      vi.mocked(fs.createReadStream).mockImplementationOnce((filePath) => {
        if (code === 'ENOENT') fs.unlinkSync(filePath)
        return createReadStream(
          filePath,
          code === 'EIO'
            ? {
                fs: {
                  ...fs,
                  read: (...args: unknown[]) => {
                    const callback = args.at(-1) as (error: Error) => void
                    process.nextTick(
                      callback,
                      Object.assign(new Error(`EIO: i/o error, read '${filePath}'`), {
                        code,
                        path: String(filePath),
                        syscall: 'read',
                      })
                    )
                  },
                },
              }
            : undefined
        )
      })
      const signed = await signBody()

      const response = await app.inject({
        method: 'POST',
        url: '/probe',
        headers: signed.headers,
        payload: largeBody,
      })

      expect(response.statusCode).toBe(500)
      expect(response.json()).toEqual({
        statusCode: '500',
        error: 'InternalError',
        code: 'InternalError',
        message: 'Internal server error',
      })
      expect(response.body).not.toContain(tmpRoot)
      expect(executionError).toBeInstanceOf(StorageBackendError)
      expect(readers[0].errored).toMatchObject({
        code,
        path: expect.stringContaining('hashspill-'),
      })
      expect((executionError as StorageBackendError).getOriginalError()).toBe(readers[0].errored)
      await expectSpillRemoved()
    }
  )

  it('removes the spill before the error response when the signature does not match', async () => {
    const app = await buildApp()
    let spillDirsAtError: string[] | undefined
    app.addHook('onError', async () => {
      spillDirsAtError = await fsp.readdir(tmpRoot)
    })
    const signed = await signBody()

    const response = await app.inject({
      method: 'POST',
      url: '/probe',
      headers: signed.headers,
      payload: `${largeBody.slice(0, -2)}y}`,
    })

    expect(response.statusCode).toBe(403)
    expect(response.json()).toEqual({ code: 'SignatureDoesNotMatch' })
    expect(spillDirsAtError).toEqual([])
  })

  it('closes an unread replay and removes its spill when the client disconnects', async () => {
    const app = await buildApp(20 * 1024 * 1024)
    let entered = false
    const proceed = Promise.withResolvers<void>()
    beforeParsing = async () => {
      entered = true
      await proceed.promise
    }
    const client = await sendBodyOverHttp(app)

    try {
      await vi.waitFor(() => expect(entered).toBe(true))
      expect(spillDirsBeforeParsing).toHaveLength(1)
      expect(readers).toHaveLength(0)
      client.destroy()

      await expectSpillRemoved()
      expect(replay?.destroyed).toBe(true)
    } finally {
      client.destroy()
      proceed.resolve()
    }
  })

  it('removes the spill on disconnect while authentication is still pending', async () => {
    const app = await buildApp(20 * 1024 * 1024)
    const errors: unknown[] = []
    app.addHook('onError', async (_request, _reply, error) => {
      errors.push(error)
    })
    let reply: FastifyReply | undefined
    app.addHook('onRequest', async (_request, current) => {
      reply = current
    })
    let entered = false
    const proceed = Promise.withResolvers<void>()
    getJwtSecretMock.mockImplementationOnce(async () => {
      expect(await fsp.readdir(tmpRoot)).toHaveLength(1)
      entered = true
      await proceed.promise
      return { secret: 'jwt-secret', jwks: null }
    })
    const client = await sendBodyOverHttp(app)

    try {
      await vi.waitFor(() => expect(entered).toBe(true))
      client.destroy()

      await expectSpillRemoved()
      expect(readers).toHaveLength(0)

      proceed.resolve()
      await vi.waitFor(() => expect(reply?.sent).toBe(true))
      expect(errors).toEqual([])
      expect(replay).toBeUndefined()
    } finally {
      client.destroy()
      proceed.resolve()
    }
  })

  it('removes the spill when the response closes during hashing and hashing completes', async () => {
    const app = await buildApp(20 * 1024 * 1024)
    const body = JSON.stringify({ value: 'x'.repeat(8 * 1024 * 1024) })
    const signed = await signBody(body)
    const createWriteStream = vi.spyOn(fs, 'createWriteStream')
    let raw: ServerResponse | undefined
    app.addHook('onRequest', async (_request, reply) => {
      raw = reply.raw
    })
    let entered = false
    const proceed = Promise.withResolvers<void>()
    getJwtSecretMock.mockImplementationOnce(async () => {
      entered = true
      await proceed.promise
      return { secret: 'jwt-secret', jwks: null }
    })
    // Readable.from pre-pulls a sync generator; async keeps the close at the 6MB mark.
    const source = Readable.from(
      (async function* () {
        for (let offset = 0; offset < body.length; offset += 1024 * 1024) {
          if (offset === 6 * 1024 * 1024) {
            await vi.waitFor(() => expect(createWriteStream).toHaveBeenCalled())
            raw?.destroy()
          }
          yield Buffer.from(body.slice(offset, offset + 1024 * 1024))
        }
      })(),
      { highWaterMark: 1 }
    )
    app
      .inject({ method: 'POST', url: '/probe', headers: signed.headers, payload: source })
      .catch(() => {})

    try {
      await vi.waitFor(() => expect(entered).toBe(true))
      await expectSpillRemoved()
      expect(readers).toHaveLength(0)
    } finally {
      proceed.resolve()
    }
  })

  it('removes a partial spill when hashing exceeds the request size limit', async () => {
    const app = await buildApp()
    const body = JSON.stringify({ value: 'x'.repeat(21 * 1024 * 1024) })
    const signed = await signBody(body)
    const createWriteStream = vi.spyOn(fs, 'createWriteStream')
    const source = Readable.from(
      (function* () {
        for (let offset = 0; offset < body.length; offset += 1024 * 1024) {
          yield Buffer.from(body.slice(offset, offset + 1024 * 1024))
        }
      })()
    )

    await expect(
      app.inject({ method: 'POST', url: '/probe', headers: signed.headers, payload: source })
    ).rejects.toMatchObject({ code: 'EntityTooLarge', httpStatusCode: 413 })
    expect(createWriteStream).toHaveBeenCalled()
    expect(readers).toHaveLength(0)
    await expectSpillRemoved()
  })

  it('closes the spill writer and removes its files when the client disconnects during hashing', async () => {
    const app = await buildApp(20 * 1024 * 1024)
    const body = JSON.stringify({ value: 'x'.repeat(10 * 1024 * 1024) })
    const createWriteStream = vi.spyOn(fs, 'createWriteStream')
    const client = await sendBodyOverHttp(app, body, 6 * 1024 * 1024)

    try {
      await vi.waitFor(() => expect(createWriteStream).toHaveBeenCalledTimes(1))
      client.destroy()

      await expectSpillRemoved()
      await vi.waitFor(() => expect(createWriteStream.mock.results[0].value.closed).toBe(true))
      expect(readers).toHaveLength(0)
      expect(verifyJwtMock).not.toHaveBeenCalled()
    } finally {
      client.destroy()
    }
  })
})
