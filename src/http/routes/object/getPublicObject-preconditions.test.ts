import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { FileBackend } from '@storage/backend/file'
import { AssetRenderer } from '@storage/renderer/asset'
import fastify, { FastifyInstance } from 'fastify'
import { getConfig } from '../../../config'
import { setErrorHandler } from '../../error-handler'
import { errorSchema } from '../../schemas/error'
import getPublicObject from './getPublicObject'

vi.mock('fs-xattr', () => ({ getAttributeSync: () => undefined }))

describe('Storage public download preconditions', () => {
  let app: FastifyInstance
  let directory: string

  beforeEach(async () => {
    directory = await mkdtemp(join(tmpdir(), 'storage-download-preconditions-'))
    const backend = new FileBackend()
    backend.filePath = directory
    backend.etagAlgorithm = 'md5'
    const key = 'tenant/bucket/object.txt'
    const { storageS3Bucket } = getConfig()
    await mkdir(join(directory, storageS3Bucket, 'tenant/bucket'), { recursive: true })
    await writeFile(join(directory, storageS3Bucket, key), 'current object bytes')

    const renderer = new AssetRenderer(backend)
    const storage = {
      asSuperUser: () => storage,
      findBucket: async () => ({ id: 'bucket', public: true }),
      from: () => ({ findObject: async () => ({ id: 'object', version: undefined }) }),
      location: { getKeyLocation: () => key },
      renderer: () => renderer,
    }
    app = fastify()
    app.addSchema(errorSchema)
    setErrorHandler(app)
    app.decorateRequest('storage')
    app.decorateRequest('tenantId', 'tenant')
    app.decorateRequest('signals')
    app.addHook('onRequest', async (request) => {
      request.storage = storage as never
      request.signals = { disconnect: new AbortController() } as never
    })
    await app.register(getPublicObject, { prefix: '/object' })
  })

  afterEach(async () => {
    await app?.close()
    if (directory) await rm(directory, { recursive: true, force: true })
  })

  it('rejects a ranged download whose If-Match does not match the object', async () => {
    const response = await app.inject({
      url: '/object/public/bucket/object.txt',
      headers: { range: 'bytes=0-6', 'if-match': '"stale-etag"' },
    })

    expect(response.statusCode).toBe(412)
    expect(response.json()).toMatchObject({ code: 'PreconditionFailed' })
  })

  it('rejects a download whose If-Unmodified-Since predates the object', async () => {
    const response = await app.inject({
      url: '/object/public/bucket/object.txt',
      headers: { 'if-unmodified-since': 'Sat, 01 Jan 2000 00:00:00 GMT' },
    })

    expect(response.statusCode).toBe(412)
    expect(response.json()).toMatchObject({ code: 'PreconditionFailed' })
  })

  it('serves an ordinary download without preconditions', async () => {
    const response = await app.inject('/object/public/bucket/object.txt')

    expect(response.statusCode).toBe(200)
    expect(response.body).toBe('current object bytes')
  })

  it('serves the requested range when If-Match matches, ignoring an older date', async () => {
    const original = await app.inject('/object/public/bucket/object.txt')
    const response = await app.inject({
      url: '/object/public/bucket/object.txt',
      headers: {
        range: 'bytes=0-6',
        'if-match': original.headers.etag as string,
        'if-unmodified-since': 'Sat, 01 Jan 2000 00:00:00 GMT',
      },
    })

    expect(response.statusCode).toBe(206)
    expect(response.body).toBe('current')
  })

  it('evaluates a failed If-Match before a matching If-None-Match', async () => {
    const original = await app.inject('/object/public/bucket/object.txt')
    const response = await app.inject({
      url: '/object/public/bucket/object.txt',
      headers: {
        'if-match': '"stale-etag"',
        'if-none-match': original.headers.etag as string,
      },
    })

    expect(response.statusCode).toBe(412)
    expect(response.json()).toMatchObject({ code: 'PreconditionFailed' })
  })

  it('serves the object when If-Unmodified-Since matches Last-Modified', async () => {
    const original = await app.inject('/object/public/bucket/object.txt')
    const response = await app.inject({
      url: '/object/public/bucket/object.txt',
      headers: { 'if-unmodified-since': original.headers['last-modified'] as string },
    })

    expect(response.statusCode).toBe(200)
    expect(response.body).toBe('current object bytes')
  })

  it('preserves If-None-Match revalidation', async () => {
    const original = await app.inject('/object/public/bucket/object.txt')
    const response = await app.inject({
      url: '/object/public/bucket/object.txt',
      headers: { 'if-none-match': original.headers.etag as string },
    })

    expect(response.statusCode).toBe(304)
    expect(response.body).toBe('')
  })
})
