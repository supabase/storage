import { createHash, randomUUID } from 'node:crypto'
import { type ClientRequest, createServer, type IncomingMessage, request } from 'node:http'
import { setTimeout as sleep } from 'node:timers/promises'
import { getPostgresConnection, getServiceKeyUser } from '@internal/database'
import { createAgent } from '@internal/http'
import { UndiciHttpHandler } from '@smithy/undici-http-handler'
import type { MetadataValue } from '@tus/s3-store'
import { DataStore, MemoryKvStore, Server, Upload } from '@tus/server'
import Fastify, { type FastifyInstance } from 'fastify'
import { getConfig, mergeConfig } from '../config'
import { StoragePgDB } from '../storage/database'
import { LockNotifier, PgLocker } from '../storage/protocols/tus/postgres-locker'
import { S3Store } from '../storage/protocols/tus/s3-store'
import { UploadId } from '../storage/protocols/tus/upload-id'

describe('TUS timeout boundaries', () => {
  const idleTimeoutMs = 150
  let handleTusRequest: typeof import('../http/routes/tus').handleTusRequestWithIdleTimeout
  let app: FastifyInstance
  let patch: ClientRequest | undefined

  beforeAll(async () => {
    getConfig({ reload: true })
    mergeConfig({ tusBodyIdleTimeoutMs: idleTimeoutMs })
    handleTusRequest = (await import('../http/routes/tus')).handleTusRequestWithIdleTimeout
  })

  beforeEach(() => {
    app = Fastify()
    app.addContentTypeParser('application/offset+octet-stream', (_req, _body, done) => done(null))
  })

  afterEach(async () => {
    patch?.destroy()
    app.server.closeAllConnections()
    await app.close()
  })

  async function startPatch(tus: Server, bodyLength: number) {
    const incoming = Promise.withResolvers<IncomingMessage>()
    app.patch('/upload/file', async (req, res) => {
      incoming.resolve(req.raw)
      await handleTusRequest(tus, req, res)
    })
    const baseUrl = await app.listen({ port: 0, host: '127.0.0.1' })
    const response = Promise.withResolvers<number | Error>()
    patch = request(`${baseUrl}/upload/file`, {
      method: 'PATCH',
      headers: {
        'Tus-Resumable': '1.0.0',
        'Upload-Offset': '0',
        'Content-Type': 'application/offset+octet-stream',
        'Content-Length': String(bodyLength),
        Connection: 'close',
      },
    })
    patch.on('response', (res) => {
      res.resume()
      res.on('end', () => response.resolve(res.statusCode ?? 0))
    })
    patch.on('error', response.resolve)
    return { incoming: incoming.promise, response: response.promise, patch }
  }

  it('completes an upload after several idle intervals of backend backpressure', async () => {
    const body = Buffer.alloc(16 * 1024 * 1024, 'backpressure')
    const resume = Promise.withResolvers<void>()
    const consumed = createHash('sha256')
    let bytesWritten = 0
    class PausedStore extends DataStore {
      async getUpload(id: string) {
        return new Upload({ id, size: body.length, offset: 0 })
      }

      async write(stream: NodeJS.ReadableStream) {
        await resume.promise
        for await (const chunk of stream) {
          bytesWritten += chunk.length
          consumed.update(chunk)
        }
        return bytesWritten
      }
    }
    const tus = new Server({ path: '/upload', datastore: new PausedStore() })
    const upload = await startPatch(tus, body.length)
    try {
      vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
      upload.patch.end(body)
      const raw = await upload.incoming
      await vi.waitFor(() => expect(raw.readableLength).toBeGreaterThan(0))
      // Let the intermediate TUS streams fill before sampling the paused socket.
      await sleep(idleTimeoutMs)
      const bytesRead = raw.socket.bytesRead
      const buffered = raw.readableLength

      await vi.advanceTimersByTimeAsync(idleTimeoutMs * 4)

      expect(raw.destroyed).toBe(false)
      expect(raw.complete).toBe(false)
      expect(raw.socket.bytesRead).toBe(bytesRead)
      expect(raw.readableLength).toBe(buffered)
      resume.resolve()
      expect(await upload.response).toBe(204)
      expect(bytesWritten).toBe(body.length)
      expect(consumed.digest('hex')).toBe(createHash('sha256').update(body).digest('hex'))
    } finally {
      resume.resolve()
      vi.useRealTimers()
    }
  })

  it('releases the PostgreSQL lock after a pending S3 part times out', async () => {
    const partSize = 5 * 1024 * 1024
    const partReceived = vi.fn()
    const s3 = createServer((req, res) => {
      const url = new URL(req.url ?? '/', 'http://localhost')
      if (req.method === 'PUT' && url.searchParams.has('partNumber')) {
        req.resume()
        req.on('end', partReceived)
        // Accept the entire part, then leave the response silent until the SDK times out.
      } else if (req.method === 'GET' && url.searchParams.has('uploadId')) {
        res.end('<ListPartsResult><IsTruncated>false</IsTruncated></ListPartsResult>')
      } else {
        res.writeHead(404)
        res.end('<Error><Code>NoSuchKey</Code></Error>')
      }
    })
    let connection: Awaited<ReturnType<typeof getPostgresConnection>> | undefined
    const agent = createAgent('tus_timeout_test', { maxSockets: 1, requestTimeoutMs: 1000 })
    const handler = new UndiciHttpHandler({ dispatcher: agent.dispatcher })
    const cache = new MemoryKvStore<MetadataValue>()
    let write: Promise<number> | undefined
    try {
      await new Promise<void>((resolve) => s3.listen(0, '127.0.0.1', resolve))
      const address = s3.address()
      if (!address || typeof address === 'string') throw new Error('missing S3 listener')
      const store = new S3Store({
        partSize,
        cache,
        maxConcurrentPartUploads: 1,
        useTags: false,
        s3ClientConfig: {
          bucket: 'timeout-test',
          region: 'us-east-1',
          endpoint: `http://127.0.0.1:${address.port}`,
          forcePathStyle: true,
          credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
          maxAttempts: 1,
          requestHandler: handler,
        },
      })
      const originalWrite = store.write.bind(store)
      vi.spyOn(store, 'write').mockImplementation((...args) => {
        write = originalWrite(...args)
        return write
      })
      const config = getConfig()
      const superUser = await getServiceKeyUser(config.tenantId)
      connection = await getPostgresConnection({
        superUser,
        user: superUser,
        tenantId: config.tenantId,
        host: 'localhost',
        disableHostCheck: true,
      })
      const db = new StoragePgDB(connection, { tenantId: config.tenantId, host: 'localhost' })
      const resource = new UploadId({
        tenant: config.tenantId,
        bucket: randomUUID(),
        objectName: 'pending-part',
        version: randomUUID(),
      })
      const id = resource.toString()
      await cache.set(id, {
        'upload-id': 'pending-upload',
        'tus-version': '1.0.0',
        file: new Upload({ id, size: partSize * 4, offset: 0 }),
      })
      const notifier = new LockNotifier({
        start: async () => {},
        close: async () => {},
        publish: async () => {},
        subscribe: async () => {},
        unsubscribe: async () => {},
        on() {
          return this
        },
      })
      const tus = new Server({
        path: '/upload',
        datastore: store,
        getFileIdFromRequest: () => id,
        locker: new PgLocker(db, notifier),
      })
      const upload = await startPatch(tus, partSize * 4)
      upload.patch.write(Buffer.alloc(partSize))
      await vi.waitFor(() => expect(partReceived).toHaveBeenCalledOnce(), { timeout: 3000 })
      const acquire = () =>
        db.withTransaction((tx) =>
          tx.mustLockObject(resource.bucket, resource.objectName, resource.version)
        )

      // Sending exactly one part only flushes it after the idle timeout ends the input.
      const raw = await upload.incoming
      expect(raw.destroyed).toBe(true)
      expect(raw.executionError).toMatchObject({
        message: 'TUS request body idle timeout - no bytes received',
      })
      await expect(acquire()).rejects.toMatchObject({ code: 'ResourceLocked' })
      await vi.waitFor(() => expect(write).rejects.toMatchObject({ name: 'TimeoutError' }), {
        timeout: 5000,
      })
      await vi.waitFor(async () => expect(await acquire()).toBe(true))
    } finally {
      patch?.destroy()
      s3.closeAllConnections()
      await write?.catch(() => {})
      await agent.close()
      await new Promise<void>((resolve) => s3.close(() => resolve()))
      connection?.dispose()
    }
  }, 20_000)
})
