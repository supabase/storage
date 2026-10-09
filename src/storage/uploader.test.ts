import { randomUUID } from 'node:crypto'
import { mkdtemp, rm } from 'node:fs/promises'
import { type ClientRequest, request as httpRequest } from 'node:http'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Readable } from 'node:stream'
import { text } from 'node:stream/consumers'
import { gzipSync } from 'node:zlib'
import {
  SIGNED_URL_SCOPE_DOWNLOAD,
  SIGNED_URL_SCOPE_UPLOAD,
  signJWT,
  verifyJWT,
} from '@internal/auth'
import { ErrorCode } from '@internal/errors'
import { Upload } from '@tus/server'
import fastify from 'fastify'
import FormData from 'form-data'
import { onTestFinished } from 'vitest'
import { getConfig, mergeConfig } from '../config'
import { setErrorHandler } from '../http/error-handler'
import createObject from '../http/routes/object/createObject'
import getSignedObject from '../http/routes/object/getSignedObject'
import uploadSignedObject from '../http/routes/object/uploadSignedObject'
import { onCreate, onUploadFinish } from '../http/routes/tus/lifecycle'
import { authSchema, errorSchema } from '../http/schemas'
import { FileBackend } from './backend/file'
import type { Database } from './database'
import { ObjectCreatedCopyEvent, ObjectCreatedPostEvent } from './events'
import { TenantLocation } from './locator'
import { S3ProtocolHandler } from './protocols/s3/s3-handler'
import { FileStore } from './protocols/tus/file-store'
import { UploadId } from './protocols/tus/upload-id'
import { AssetRenderer } from './renderer/asset'
import { Storage } from './storage'

type SavedObject = Parameters<Database['upsertObject']>[0] & { id: string }

async function createFixture(allowedMimeTypes: string[] | null) {
  const config = getConfig()
  const directory = await mkdtemp(join(tmpdir(), 'storage-mime-test-'))
  mergeConfig({ storageFilePath: directory })
  const backend = new FileBackend()
  const location = new TenantLocation(config.storageS3Bucket)
  let saved: SavedObject | undefined
  const db = {
    tenantId: 'mime-tenant',
    reqId: 'mime-request',
    tenant: () => ({ ref: 'mime-tenant', host: 'localhost' }),
    asCaller: (): Database => database,
    asSuperUser: (): Database => database,
    findBucketById: async () => ({
      id: 'mime-bucket',
      file_size_limit: 1024,
      allowed_mime_types: allowedMimeTypes,
    }),
    testPermission: vi.fn().mockResolvedValue(undefined),
    connection: { setAbortSignal: vi.fn() },
    withTransaction: async (fn: (transaction: Database) => Promise<unknown>) => fn(database),
    waitObjectLock: vi.fn().mockResolvedValue(true),
    findObject: vi.fn().mockResolvedValue(undefined),
    hasMigration: vi.fn().mockResolvedValue(false),
    upsertObject: async (data: Parameters<Database['upsertObject']>[0]) => {
      saved = { id: randomUUID(), ...data }
      return saved
    },
    createMultipartUpload: vi.fn().mockResolvedValue(undefined),
  }
  const database = db as unknown as Database
  vi.spyOn(ObjectCreatedPostEvent, 'sendWebhook').mockResolvedValue(undefined)
  const storage = new Storage(backend, database, location)
  const app = fastify()
  onTestFinished(async () => {
    try {
      await app.close()
    } finally {
      mergeConfig(config)
      await rm(directory, { recursive: true, force: true })
    }
  })
  app.addSchema(authSchema)
  app.addSchema(errorSchema)
  setErrorHandler(app)
  app.addContentTypeParser('*', (_request, _payload, done) => done(null))
  app.addHook('onRequest', async (request) => {
    request.storage = storage
    request.tenantId = 'mime-tenant'
    request.signals = {
      body: new AbortController(),
      disconnect: new AbortController(),
    } as typeof request.signals
  })
  await app.register(createObject, { prefix: '/object' })
  await app.register(uploadSignedObject, { prefix: '/object' })
  await app.register(getSignedObject, { prefix: '/object' })
  app.get('/download', (request, reply) => {
    if (!saved) {
      throw new Error('No uploaded object')
    }
    return new AssetRenderer(backend).render(request, reply, {
      bucket: config.storageS3Bucket,
      key: location.getKeyLocation({
        tenantId: 'mime-tenant',
        bucketId: 'mime-bucket',
        objectName: saved.name,
      }),
      version: saved.version,
    })
  })

  return {
    app,
    backend,
    directory,
    db,
    storage,
    get saved() {
      return saved
    },
    upload(
      kind: 'binary' | 'multipart',
      contentType: string,
      bytes: Buffer,
      contentEncoding?: string
    ) {
      const form = new FormData()
      form.append('contentType', contentType)
      if (contentEncoding) form.append('contentEncoding', contentEncoding)
      form.append('file', bytes, { filename: 'sample.txt', contentType: 'text/plain' })
      return app.inject({
        method: 'POST',
        url: '/object/mime-bucket/sample.txt',
        headers: {
          authorization: 'Bearer test',
          ...(kind === 'binary' ? { 'content-type': contentType } : form.getHeaders()),
          ...(kind === 'binary' && contentEncoding ? { 'content-encoding': contentEncoding } : {}),
        },
        payload: kind === 'binary' ? bytes : form,
      })
    },
  }
}

describe.each(['binary', 'multipart'] as const)('%s MIME upload handling', (kind) => {
  it('preserves original parameters and non-UTF-8 bytes through upload and download', async () => {
    const fixture = await createFixture(['TEXT/PLAIN;charset=UTF-8'])
    const contentType = 'Text/Plain; charset=iso-8859-1; note="a,b;c"'
    const bytes = Buffer.from([0x63, 0x61, 0x66, 0xe9])
    const uploaded = await fixture.upload(kind, contentType, bytes)
    expect(uploaded.statusCode, uploaded.body).toBe(200)
    expect(fixture.saved?.metadata?.mimetype).toBe(contentType)
    const downloaded = await fixture.app.inject({ method: 'GET', url: '/download' })
    expect(downloaded.statusCode, downloaded.body).toBe(200)
    expect(downloaded.headers['content-type']).toBe(contentType)
    expect(downloaded.rawPayload).toEqual(bytes)
  })

  it('keeps HTML downloads as plain text after case-insensitive allow-list matching', async () => {
    const fixture = await createFixture(['text/html'])
    const contentType = 'Text/HTML; charset=UTF-8'
    const uploaded = await fixture.upload(kind, contentType, Buffer.from('<p>hello</p>'))
    expect(uploaded.statusCode, uploaded.body).toBe(200)
    expect(fixture.saved?.metadata?.mimetype).toBe(contentType)
    const downloaded = await fixture.app.inject({ method: 'GET', url: '/download' })
    expect(downloaded.statusCode, downloaded.body).toBe(200)
    expect(downloaded.headers['content-type']).toBe('text/plain')
    expect(downloaded.body).toBe('<p>hello</p>')
  })

  it.each([
    'image/png;foo=x, application/pdf',
    'image/png/extra',
  ])('rejects %j before uploading bytes', async (contentType) => {
    const fixture = await createFixture(['image/png'])
    const upload = vi.spyOn(fixture.backend, 'uploadObject')
    const response = await fixture.upload(kind, contentType, Buffer.from('example'))
    expect(response.statusCode, response.body).toBe(400)
    expect(response.json().code).toBe(ErrorCode.InvalidMimeType)
    expect(upload).not.toHaveBeenCalled()
    expect(fixture.saved).toBeUndefined()
  })
})

describe.each(['binary', 'multipart'] as const)('%s Content-Encoding upload', (kind) => {
  it('rejects invalid encoding before writing an object', async () => {
    const fixture = await createFixture(null)
    const upload = vi.spyOn(fixture.backend, 'uploadObject')
    const response = await fixture.upload(kind, 'text/plain', Buffer.from('payload'), 'gzip;')
    expect(response.statusCode).toBe(400)
    expect(response.json().code).toBe(ErrorCode.InvalidParameter)
    expect(upload).not.toHaveBeenCalled()
  })

  it('keeps compressed bytes and encoding across metadata, GET, and ranged GET', async () => {
    const fixture = await createFixture(null)
    const bytes = gzipSync(Buffer.from('large logical payload'.repeat(20)))
    const rls = vi.fn()
    fixture.db.testPermission.mockImplementation((fn) => fn({ createObject: rls }))

    const uploaded = await fixture.upload(kind, 'application/octet-stream', bytes, ', gzip,,')
    expect(uploaded.statusCode, uploaded.body).toBe(200)
    expect(fixture.saved?.metadata?.contentEncoding).toBe('gzip')
    expect(rls.mock.calls[0][0].metadata.contentEncoding).toBe('gzip')

    const downloaded = await fixture.app.inject({ method: 'GET', url: '/download' })
    expect(downloaded.statusCode, downloaded.body).toBe(200)
    expect(downloaded.headers['content-encoding']).toBe('gzip')
    expect(downloaded.headers['content-length']).toBe(String(bytes.length))
    expect(downloaded.rawPayload).toEqual(bytes)

    const ranged = await fixture.app.inject({
      method: 'GET',
      url: '/download',
      headers: { range: 'bytes=0-9' },
    })
    expect(ranged.statusCode, ranged.body).toBe(206)
    expect(ranged.headers['content-encoding']).toBe('gzip')
    expect(ranged.headers['content-range']).toBe(`bytes 0-9/${bytes.length}`)
    expect(ranged.rawPayload).toEqual(bytes.subarray(0, 10))
  })
})

describe('multipart Content-Encoding fields', () => {
  it.each([
    { label: 'duplicate values', values: ['gzip', 'gzip'] },
    { label: 'a file field', values: [Buffer.from('gzip')] },
  ])('rejects $label before writing an object', async ({ values }) => {
    const fixture = await createFixture(null)
    const upload = vi.spyOn(fixture.backend, 'uploadObject')
    const form = new FormData()
    for (const value of values) {
      form.append(
        'contentEncoding',
        value,
        Buffer.isBuffer(value) ? { filename: 'encoding.txt' } : undefined
      )
    }
    form.append('file', gzipSync('payload'), {
      filename: 'sample.gz',
      contentType: 'application/octet-stream',
    })

    const response = await fixture.app.inject({
      method: 'POST',
      url: '/object/mime-bucket/sample.txt',
      headers: { authorization: 'Bearer test', ...form.getHeaders() },
      payload: form,
    })

    expect(response.statusCode, response.body).toBe(400)
    expect(response.json().code).toBe(ErrorCode.InvalidParameter)
    expect(upload).not.toHaveBeenCalled()
  })
})

describe('signed Content-Encoding download', () => {
  it('preserves encoded bytes, no-transform, and expiry', async () => {
    const fixture = await createFixture(null)
    const bytes = gzipSync('signed compressed payload')
    const path = 'mime-bucket/sample.txt'
    const secret = getConfig().jwtSecret
    const uploadToken = await signJWT({ url: path, scope: SIGNED_URL_SCOPE_UPLOAD }, secret, 100)
    const uploaded = await fixture.app.inject({
      method: 'PUT',
      url: `/object/upload/sign/${path}?token=${uploadToken}`,
      headers: {
        'content-type': 'application/octet-stream',
        'content-encoding': 'gzip',
        'cache-control': 'max-age=86400, no-transform',
      },
      payload: bytes,
    })
    expect(uploaded.statusCode, uploaded.body).toBe(200)

    fixture.db.findObject.mockResolvedValue(fixture.saved)
    const token = await signJWT({ url: path, scope: SIGNED_URL_SCOPE_DOWNLOAD }, secret, 100)
    const response = await fixture.app.inject(`/object/sign/${path}?token=${token}`)
    const { exp } = await verifyJWT(token, secret)
    expect(response.statusCode, response.body).toBe(200)
    expect(response.headers['content-encoding']).toBe('gzip')
    expect(response.headers['cache-control']).toBe('no-transform')
    expect(response.headers['expires']).toBe(new Date(exp! * 1000).toUTCString())
    expect(response.rawPayload).toEqual(bytes)
  })
})

describe('S3 multipart MIME handling', () => {
  it('rejects invalid Content-Encoding before initiating an upload', async () => {
    const fixture = await createFixture(null)
    const initiate = vi.spyOn(fixture.backend, 'createMultiPartUpload')
    await expect(
      new S3ProtocolHandler(fixture.storage, 'mime-tenant').createMultiPartUpload({
        Bucket: 'mime-bucket',
        Key: 'sample.txt',
        ContentEncoding: 'gzip;',
      })
    ).rejects.toMatchObject({ code: ErrorCode.InvalidParameter })
    expect(initiate).not.toHaveBeenCalled()
    expect(fixture.db.createMultipartUpload).not.toHaveBeenCalled()
  })

  it('preserves MIME parameters and normalized encoding in backend, permission, and upload metadata', async () => {
    const fixture = await createFixture(['text/*'])
    const contentType = 'Text/Plain; charset=iso-8859-1; note="a,b;c"'
    const permission = vi.fn()
    fixture.db.testPermission.mockImplementation((fn) => fn({ upsertObject: permission }))
    const initiate = vi
      .spyOn(fixture.backend, 'createMultiPartUpload')
      .mockResolvedValue('upload-id')
    const handler = new S3ProtocolHandler(fixture.storage, 'mime-tenant')
    await handler.createMultiPartUpload({
      Bucket: 'mime-bucket',
      Key: 'sample.txt',
      ContentType: contentType,
      ContentEncoding: 'aws-chunked, gzip',
    })
    expect(initiate.mock.calls[0][3]).toBe(contentType)
    expect(initiate.mock.calls[0][6]).toBe('gzip')
    const metadata = {
      mimetype: contentType,
      contentEncoding: 'gzip',
    }
    expect(permission).toHaveBeenCalledWith(expect.objectContaining({ metadata }), {
      currentVersion: true,
    })
    expect(fixture.db.createMultipartUpload.mock.calls[0][7]).toEqual(metadata)
  })

  it('rejects a combined value before initiating the upload', async () => {
    const fixture = await createFixture(['image/png'])
    const initiate = vi.spyOn(fixture.backend, 'createMultiPartUpload')
    const handler = new S3ProtocolHandler(fixture.storage, 'mime-tenant')
    await expect(
      handler.createMultiPartUpload({
        Bucket: 'mime-bucket',
        Key: 'sample.png',
        ContentType: 'image/png;foo=x, application/pdf',
      })
    ).rejects.toMatchObject({ code: ErrorCode.InvalidMimeType })
    expect(initiate).not.toHaveBeenCalled()
  })
})

describe('S3 Content-Encoding upload', () => {
  it('persists encoding through PutObject', async () => {
    const fixture = await createFixture(null)
    const bytes = gzipSync(Buffer.from('object payload'))
    const handler = new S3ProtocolHandler(fixture.storage, 'mime-tenant')
    await handler.putObject(
      {
        Bucket: 'mime-bucket',
        Key: 'sample.txt',
        Body: Readable.from(bytes),
        ContentType: 'application/octet-stream',
        CacheControl: 'no-cache',
        ContentEncoding: 'aws-chunked, gzip',
        ContentLength: bytes.length,
      },
      { isTruncated: () => false }
    )

    expect(fixture.saved?.metadata?.contentEncoding).toBe('gzip')
  })
})

describe('copy Content-Encoding', () => {
  const rest = (contentEncoding?: string) => (storage: Storage) =>
    storage.from('mime-bucket').copyObject({
      sourceKey: 'sample.txt',
      destinationBucket: 'mime-bucket',
      destinationKey: 'copy.txt',
      upsert: true,
      copyMetadata: false,
      preserveUnspecifiedFileMetadata: true,
      metadata: { contentEncoding },
      uploadType: 'standard',
    })
  const s3 = (ContentEncoding?: string) => (storage: Storage) =>
    new S3ProtocolHandler(storage, 'mime-tenant').copyObject({
      Bucket: 'mime-bucket',
      Key: 'copy.txt',
      CopySource: 'mime-bucket/sample.txt',
      MetadataDirective: 'REPLACE',
      ContentEncoding,
    })

  it.each([
    ['S3 REPLACE without the header clears it', s3(), undefined],
    ['S3 REPLACE sets the header', s3('br'), 'br'],
    ['REST empty value clears the source encoding', rest(''), undefined],
    ['REST omitted value preserves the source encoding', rest(), 'gzip'],
    ['REST value is validated', rest(' br '), 'br'],
  ])('%s', async (_name, copy, expected) => {
    const fixture = await createFixture(null)
    const bytes = gzipSync('copy payload')
    const uploaded = await fixture.upload('binary', 'application/octet-stream', bytes, 'gzip')
    expect(uploaded.statusCode).toBe(200)
    fixture.db.findObject.mockResolvedValueOnce(fixture.saved)
    vi.spyOn(ObjectCreatedCopyEvent, 'sendWebhook').mockResolvedValue(undefined)

    await copy(fixture.storage)

    expect(fixture.saved?.metadata?.contentEncoding).toBe(expected)
    const downloaded = await fixture.app.inject('/download')
    expect(downloaded.statusCode).toBe(200)
    expect(downloaded.headers['content-encoding']).toBe(expected)
    expect(downloaded.rawPayload).toEqual(bytes)
  })
})

describe('TUS MIME handling', () => {
  it('removes metadata.contentEncoding when no stored coding remains', async () => {
    const fixture = await createFixture(null)
    const { metadata } = await onCreate(
      { runtime: { node: { req: { upload: { storage: fixture.storage } } } } } as never,
      new Upload({
        id: new UploadId({
          tenant: 'mime-tenant',
          bucket: 'mime-bucket',
          objectName: 'sample.txt',
          version: randomUUID(),
        }).toString(),
        offset: 0,
        metadata: { contentEncoding: 'identity, aws-chunked' },
      })
    )
    expect(metadata).not.toHaveProperty('contentEncoding')
  })

  it('persists encoding through completion', async () => {
    const fixture = await createFixture(null)
    const id = new UploadId({
      tenant: 'mime-tenant',
      bucket: 'mime-bucket',
      objectName: 'sample.txt',
      version: randomUUID(),
    }).toString()
    const request = {
      runtime: {
        node: { req: { upload: { storage: fixture.storage, tenantId: 'mime-tenant' } } },
      },
    } as never

    const bytes = gzipSync('resumable payload')
    const upload = new Upload({
      id,
      size: bytes.length,
      offset: 0,
      metadata: { contentEncoding: 'gzip' },
    })
    const { metadata } = await onCreate(request, upload)
    upload.metadata = metadata
    const store = new FileStore({
      directory: join(fixture.directory, getConfig().storageS3Bucket),
    })
    await store.create(upload)
    await store.write(Readable.from(bytes), id, 0)
    await onUploadFinish(request, await store.getUpload(id))

    expect(fixture.saved?.metadata?.contentEncoding).toBe('gzip')
    const downloaded = await fixture.app.inject('/download')
    expect(downloaded.statusCode).toBe(200)
    expect(downloaded.headers['content-encoding']).toBe('gzip')
    expect(downloaded.rawPayload).toEqual(bytes)
  })

  it.each([
    {
      contentType: 'Text/Plain; charset=iso-8859-1; note="a,b;c"',
      allowedMimeTypes: ['text/*'],
      valid: true,
    },
    {
      contentType: 'text/plain;foo=x, application/pdf',
      allowedMimeTypes: ['text/*'],
      valid: false,
    },
    { contentType: 'text/plain', allowedMimeTypes: [], valid: true },
    { contentType: 'text/plain', allowedMimeTypes: null, valid: true },
    { contentType: 'text/plain', allowedMimeTypes: ['image/png'], valid: false },
  ])('validates $contentType against $allowedMimeTypes', async ({
    contentType,
    allowedMimeTypes,
    valid,
  }) => {
    const fixture = await createFixture(allowedMimeTypes)
    const result = onCreate(
      { runtime: { node: { req: { upload: { storage: fixture.storage } } } } } as never,
      {
        id: new UploadId({
          tenant: 'mime-tenant',
          bucket: 'mime-bucket',
          objectName: 'sample.txt',
          version: randomUUID(),
        }).toString(),
        metadata: { contentType },
      } as never
    )
    if (valid) {
      await expect(result).resolves.toMatchObject({ metadata: { contentType } })
    } else {
      await expect(result).rejects.toMatchObject({ code: ErrorCode.InvalidMimeType })
    }
  })
})

describe('multipart fields after the file', () => {
  it('keeps streaming file bytes while ignoring a delayed encoding field', async () => {
    const fixture = await createFixture(null)
    let request: ClientRequest | undefined
    try {
      const bytes = gzipSync('delayed multipart payload'.repeat(20))
      const form = new FormData()
      form.append('contentEncoding', 'gzip')
      form.append('file', bytes, { filename: 'a.gz', contentType: 'application/octet-stream' })
      form.append('contentEncoding', 'br')
      const payload = form.getBuffer()
      const splitAt = payload.indexOf(bytes) + Math.floor(bytes.length / 2)
      const uploadObject = fixture.backend.uploadObject.bind(fixture.backend)
      vi.spyOn(fixture.backend, 'uploadObject').mockImplementationOnce((...args) => {
        // Deliver the remaining file bytes and trailing field only after the iterator exits.
        request?.end(payload.subarray(splitAt))
        return uploadObject(...args)
      })

      const address = await fixture.app.listen({ port: 0, host: '127.0.0.1' })
      const response = await new Promise<{ statusCode?: number; body: string }>(
        (resolve, reject) => {
          request = httpRequest(
            `${address}/object/mime-bucket/sample.txt`,
            {
              method: 'POST',
              headers: {
                authorization: 'Bearer test',
                ...form.getHeaders(),
                'content-length': payload.length,
              },
            },
            (res) => {
              text(res).then((body) => resolve({ statusCode: res.statusCode, body }), reject)
            }
          )
          request.on('error', reject)
          request.setTimeout(2000, () => request?.destroy(new Error('Upload stalled')))
          request.write(payload.subarray(0, splitAt))
        }
      )

      expect(response.statusCode, response.body).toBe(200)
      expect(fixture.saved?.metadata?.contentEncoding).toBe('gzip')
      const downloaded = await fixture.app.inject('/download')
      expect(downloaded.statusCode).toBe(200)
      expect(downloaded.headers['content-encoding']).toBe('gzip')
      expect(downloaded.rawPayload).toEqual(bytes)
    } finally {
      request?.destroy()
    }
  })

  it('keeps the first cacheControl', async () => {
    const fixture = await createFixture(null)
    const form = new FormData()
    form.append('cacheControl', '7200')
    form.append('file', Buffer.from('payload'), { filename: 'a.txt', contentType: 'text/plain' })
    form.append('cacheControl', '3600')
    const response = await fixture.app.inject({
      method: 'POST',
      url: '/object/mime-bucket/sample.txt',
      headers: { authorization: 'Bearer test', ...form.getHeaders() },
      payload: form.getBuffer(),
    })
    expect(response.statusCode, response.body).toBe(200)
    expect(fixture.saved?.metadata?.cacheControl).toBe('max-age=7200')
  })

  it.each([
    undefined,
    'gzip',
  ])('ignores a trailing contentEncoding (earlier value: %s)', async (contentEncoding) => {
    const fixture = await createFixture(null)
    const form = new FormData()
    if (contentEncoding) form.append('contentEncoding', contentEncoding)
    form.append('file', gzipSync('payload'), { filename: 'a.gz', contentType: 'text/plain' })
    form.append('contentEncoding', 'br')
    const response = await fixture.app.inject({
      method: 'POST',
      url: '/object/mime-bucket/sample.txt',
      headers: { authorization: 'Bearer test', ...form.getHeaders() },
      payload: form.getBuffer(),
    })
    expect(response.statusCode, response.body).toBe(200)
    expect(fixture.saved?.metadata?.contentEncoding).toBe(contentEncoding)
  })
})
