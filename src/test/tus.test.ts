import { randomBytes } from 'node:crypto'
import { once } from 'node:events'
import { mkdtemp } from 'node:fs/promises'
import * as http from 'node:http'
import { request as httpRequest, type IncomingMessage } from 'node:http'
import { connect } from 'node:net'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { text } from 'node:stream/consumers'
import { setTimeout as sleep } from 'node:timers/promises'

import {
  CreateBucketCommand,
  HeadObjectCommand,
  ListMultipartUploadsCommand,
  S3Client,
} from '@aws-sdk/client-s3'
import { signJWT } from '@internal/auth'
import { getPostgresConnection, getServiceKeyUser } from '@internal/database'
import { pathExists, removePath } from '@internal/fs'
import { logger } from '@internal/monitoring'
import { randomUUID } from 'crypto'
import { FastifyInstance } from 'fastify'
import fs from 'fs'
import * as tus from 'tus-js-client'
import { DetailedError } from 'tus-js-client'
import { getConfig } from '../config'
import type { StorageBackendAdapter } from '../storage/backend'
import type { StoragePgDB as StoragePgDBType } from '../storage/database/pg'
import type { TenantLocation as TenantLocationType } from '../storage/locator'
import type { Storage as StorageType } from '../storage/storage'
import { checkBucketExists } from './utils/storage'

const uploadFinishGate = {
  current: null as null | {
    reached: () => void
    release: Promise<void>
    finished: () => void
    closed?: Promise<void>
  },
}

const gatedLifecycle = async (
  importOriginal: () => Promise<typeof import('../http/routes/tus/lifecycle')>
) => {
  const mod = await importOriginal()
  return {
    ...mod,
    onUploadFinish: async (...args: Parameters<typeof mod.onUploadFinish>) => {
      const gate = uploadFinishGate.current
      if (!gate) return mod.onUploadFinish(...args)
      uploadFinishGate.current = null
      const res = args[0].runtime?.node?.res
      gate.closed = res?.closed
        ? Promise.resolve()
        : new Promise<void>((resolve) => res?.once('close', () => resolve()))
      gate.reached()
      await gate.release
      try {
        return await mod.onUploadFinish(...args)
      } finally {
        gate.finished()
      }
    },
  }
}

const assetPath = path.resolve(__dirname, 'assets', 'sadcat.jpg')
const assetSize = fs.statSync(assetPath).size
const openAssetStream = () => fs.createReadStream(assetPath)

function resumeTusUpload(uploadUrl: string, headers: Record<string, string>) {
  return new Promise<tus.HttpResponse>((resolve, reject) => {
    new tus.Upload(openAssetStream(), {
      uploadUrl,
      headers,
      onShouldRetry: () => false,
      onError: reject,
      onSuccess: ({ lastResponse }) => resolve(lastResponse),
    }).start()
  })
}

type TusTestConfig = {
  serviceKeyAsync: Promise<string>
  storageS3Bucket: string
  storageFilePath?: string
  storageBackendType: 'file' | 's3'
  tenantId: string
  tusPath: string
}

type TusTestContext = {
  Storage: typeof StorageType
  StoragePgDB: typeof StoragePgDBType
  TenantLocation: typeof TenantLocationType
  backend: StorageBackendAdapter
  baseUrl: string
  config: TusTestConfig
  fileBackendPath?: string
  server: FastifyInstance
  withOptionalVersion: (key: string, version?: string) => string
}

function expectedAssetEtag(backendType: TusTestConfig['storageBackendType']) {
  return backendType === 's3'
    ? '"53e1323c929d57b09b95fbe6d531865c-1"'
    : '"740f5c4bb4f6f2f73c1a301fa455c747"'
}

function encodeTusMetadata(metadata: Record<string, string>): string {
  return Object.entries(metadata)
    .map(([key, value]) => `${key} ${Buffer.from(value).toString('base64')}`)
    .join(',')
}

async function postRawTusUpload(
  baseUrl: string,
  target: string,
  metadata: Record<string, string>,
  signature?: string
) {
  const { hostname, port } = new URL(baseUrl)
  const request = httpRequest({
    hostname,
    port,
    method: 'POST',
    path: target,
    headers: {
      'tus-resumable': '1.0.0',
      'upload-length': '1',
      'content-type': 'application/offset+octet-stream',
      'upload-metadata': encodeTusMetadata(metadata),
      ...(signature ? { 'x-signature': signature } : {}),
    },
  }).end('x')
  const [response]: IncomingMessage[] = await once(request, 'response')
  return {
    status: response.statusCode,
    body: await text(response),
    location: response.headers.location,
  }
}

function decodeTusUploadId(location: string): string {
  const encodedUploadId = location.split('/').pop()

  if (!encodedUploadId) {
    throw new Error('TUS upload location is missing an encoded upload id')
  }

  return Buffer.from(encodedUploadId, 'base64url').toString('utf8')
}

function getTusDatastoreUploadId(
  config: Pick<TusTestConfig, 'tenantId'>,
  location: string
): string {
  return `${config.tenantId}/${decodeTusUploadId(location)}`
}

function getTusUploadPath(context: TusTestContext, location: string): string {
  if (!context.fileBackendPath) {
    throw new Error('getTusUploadPath is only valid for the file backend')
  }

  const relativeUploadId = decodeTusUploadId(location)
  return path.join(
    context.fileBackendPath,
    context.config.storageS3Bucket,
    context.config.tenantId,
    relativeUploadId
  )
}

function getStoredObjectPath(
  context: TusTestContext,
  bucketId: string,
  objectName: string,
  version: string
): string {
  if (!context.fileBackendPath) {
    throw new Error('getStoredObjectPath is only valid for the file backend')
  }

  return path.join(
    context.fileBackendPath,
    context.config.storageS3Bucket,
    context.config.tenantId,
    context.withOptionalVersion(`${bucketId}/${objectName}`, version)
  )
}

async function createTusUpload(
  context: Pick<TusTestContext, 'baseUrl' | 'config'>,
  authorization: string,
  metadata: Record<string, string>,
  uploadLength = 5
) {
  return fetch(`${context.baseUrl}${context.config.tusPath}`, {
    method: 'POST',
    headers: {
      authorization,
      'tus-resumable': '1.0.0',
      'upload-length': String(uploadLength),
      'upload-metadata': encodeTusMetadata(metadata),
      'x-upsert': 'true',
    },
  })
}

async function deleteTusUpload(location: string, authorization: string) {
  return fetch(location, {
    method: 'DELETE',
    headers: {
      authorization,
      'tus-resumable': '1.0.0',
      'x-upsert': 'true',
    },
  })
}

function expectTusErrorResponse(error: unknown) {
  expect(error).toBeInstanceOf(DetailedError)

  const response = (error as DetailedError).originalResponse
  expect(response).not.toBeNull()
  if (!response) {
    throw error
  }

  return response
}

async function createTusTestContext(
  backendType: 'file' | 's3',
  options: { fileBackendPath?: string; tusBodyIdleTimeoutMs?: number } = {}
): Promise<TusTestContext> {
  vi.resetModules()
  vi.doMock('../http/routes/tus/lifecycle', gatedLifecycle)

  const configModule = await import('../config')
  configModule.setEnvPaths(['.env.test', '.env'])
  configModule.getConfig({ reload: true })

  const overrides: Partial<{
    storageBackendType: 'file' | 's3'
    storageFilePath: string
    tusBodyIdleTimeoutMs: number
  }> = { storageBackendType: backendType }
  if (backendType === 'file') {
    overrides.storageFilePath = options.fileBackendPath
  }
  if (options.tusBodyIdleTimeoutMs !== undefined) {
    overrides.tusBodyIdleTimeoutMs = options.tusBodyIdleTimeoutMs
  }
  configModule.mergeConfig(overrides)

  const [appModule, backendModule, storageModule, databaseModule, locatorModule] =
    await Promise.all([
      import('../app'),
      import('../storage/backend'),
      import('../storage/storage'),
      import('../storage/database'),
      import('../storage/locator'),
    ])

  const server = appModule.default({ loggerInstance: logger })
  const listener = await server.listen({ host: '127.0.0.1', port: 0 })
  const config = configModule.getConfig() as TusTestConfig
  const backend = backendModule.createStorageBackend(config.storageBackendType)

  if (backendType === 's3' && backend.client instanceof S3Client) {
    const bucketExists = await checkBucketExists(backend.client, config.storageS3Bucket)
    if (!bucketExists) {
      await backend.client.send(new CreateBucketCommand({ Bucket: config.storageS3Bucket }))
    }
  }

  return {
    Storage: storageModule.Storage,
    StoragePgDB: databaseModule.StoragePgDB,
    TenantLocation: locatorModule.TenantLocation,
    backend,
    baseUrl: listener,
    config,
    fileBackendPath: options.fileBackendPath,
    server,
    withOptionalVersion: backendModule.withOptionalVersion,
  }
}

describe.each([
  { name: 'S3 backend', backendType: 's3' as const },
  { name: 'File backend', backendType: 'file' as const },
])('TUS resumable — $name', ({ backendType }) => {
  let context: TusTestContext
  let fileBackendPath: string | undefined
  let db: StoragePgDBType
  let storage: StorageType
  let connection: Awaited<ReturnType<typeof getPostgresConnection>>
  let bucketName: string

  beforeAll(async () => {
    if (backendType === 'file') {
      fileBackendPath = await mkdtemp(path.join(tmpdir(), 'storage-tus-'))
    }
    context = await createTusTestContext(backendType, { fileBackendPath })
  })

  afterAll(async () => {
    await context?.server?.close()
    vi.resetModules()
    if (fileBackendPath) {
      await removePath(fileBackendPath)
    }
  })

  beforeEach(async () => {
    const superUser = await getServiceKeyUser(context.config.tenantId)
    connection = await getPostgresConnection({
      superUser,
      user: superUser,
      tenantId: context.config.tenantId,
      host: 'localhost',
      disableHostCheck: true,
    })

    db = new context.StoragePgDB(connection, {
      tenantId: context.config.tenantId,
      host: 'localhost',
    })

    bucketName = randomUUID()
    storage = new context.Storage(
      context.backend,
      db,
      new context.TenantLocation(context.config.storageS3Bucket)
    )
  })

  afterEach(async () => {
    vi.useRealTimers()
    connection?.dispose()
  })

  it('advertises TUS protocol headers on OPTIONS preflight', async () => {
    const response = await fetch(`${context.baseUrl}${context.config.tusPath}`, {
      method: 'OPTIONS',
    })

    expect(response.status).toBe(204)
    expect(response.headers.get('tus-extension')).toEqual(expect.stringContaining('creation'))
    expect(response.headers.get('tus-max-size')).toMatch(/^\d+$/)
    expect(response.headers.get('tus-version')).toBe('1.0.0')
  })

  it('keeps completed HEAD resumable with INSERT-only SQL grants', async () => {
    const role = `tus_insert_${randomUUID().replaceAll('-', '')}`
    const policy = `${role}_insert`
    const objectName = 'insert-only.txt'
    let location: string | null = null

    await connection.query(`CREATE ROLE "${role}" NOLOGIN`)
    try {
      await storage.createBucket({ id: bucketName, name: bucketName })
      await connection.query(`GRANT USAGE ON SCHEMA storage TO "${role}"`)
      await connection.query(`GRANT INSERT ON storage.objects TO "${role}"`)
      await connection.query(`
        CREATE POLICY "${policy}" ON storage.objects FOR INSERT TO "${role}"
        WITH CHECK (bucket_id = '${bucketName}')
      `)
      const jwt = await signJWT({ sub: randomUUID(), role }, getConfig().jwtSecret, '1h')
      const headers = { authorization: `Bearer ${jwt}`, 'tus-resumable': '1.0.0' }
      const post = await fetch(`${context.baseUrl}${context.config.tusPath}`, {
        method: 'POST',
        headers: {
          ...headers,
          'upload-length': '1',
          'upload-metadata': encodeTusMetadata({ bucketName, objectName }),
        },
      })
      expect(post.status).toBe(201)
      location = post.headers.get('location')
      if (!location) throw new Error('Missing TUS upload location')

      const partial = await fetch(location, { method: 'HEAD', headers })
      expect(partial.status).toBe(200)
      expect(partial.headers.has('Supabase-Object-Version-Id')).toBe(false)
      const patch = await fetch(location, {
        method: 'PATCH',
        body: 'x',
        headers: {
          ...headers,
          'upload-offset': '0',
          'content-type': 'application/offset+octet-stream',
        },
      })
      expect(patch.status).toBe(204)
      const row = await db.findObject(bucketName, objectName, 'id,version')
      expect(patch.headers.get('Supabase-Object-Id')).toBe(row.id)
      expect(patch.headers.get('Supabase-Object-Version-Id')).toBe(row.version)

      const resumed = await resumeTusUpload(location, headers)
      expect(resumed.getStatus()).toBe(200)
      expect(resumed.getHeader('Upload-Offset')).toBe('1')
      expect(resumed.getHeader('Supabase-Object-Id')).toBeUndefined()
      expect(resumed.getHeader('Supabase-Object-Version-Id')).toBeUndefined()
    } finally {
      await connection.query(`DROP POLICY IF EXISTS "${policy}" ON storage.objects`)
      await connection.query(`DROP OWNED BY "${role}"`)
      await connection.query(`DROP ROLE "${role}"`)
      await storage
        .from(bucketName)
        .deleteObject(objectName)
        .catch(() => undefined)
      if (backendType === 's3' && location) {
        await context.backend.deleteObject(
          context.config.storageS3Bucket,
          `${getTusDatastoreUploadId(context.config, location)}.info`,
          undefined
        )
      }
      await storage.deleteBucket(bucketName).catch(() => undefined)
    }
  })

  it('can upload an asset with the TUS protocol', async () => {
    const objectName = randomUUID() + '-cat.jpeg'

    const bucket = await storage.createBucket({
      id: bucketName,
      name: bucketName,
      public: true,
    })

    const authorization = `Bearer ${await context.config.serviceKeyAsync}`

    let uploadUrl = ''
    const result = await new Promise<tus.HttpResponse>((resolve, reject) => {
      const upload = new tus.Upload(openAssetStream(), {
        endpoint: `${context.baseUrl}${context.config.tusPath}`,
        onShouldRetry: () => false,
        uploadDataDuringCreation: false,
        headers: {
          authorization,
          'x-upsert': 'true',
        },
        metadata: {
          bucketName,
          objectName,
          contentType: 'image/jpeg',
          cacheControl: '3600',
          metadata: JSON.stringify({
            test1: 'test1',
            test2: 'test2',
          }),
        },
        onError(error) {
          console.log('Failed because: ' + error)
          reject(error)
        },
        onSuccess: ({ lastResponse }) => {
          uploadUrl = upload.url ?? ''
          resolve(lastResponse)
        },
      })

      upload.start()
    })

    expect(result.getStatus()).toBe(204)

    const dbAsset = await storage.from(bucket.id).findObject(objectName, '*')
    expect(result.getHeader('Supabase-Object-Id')).toBe(dbAsset.id)
    expect(result.getHeader('Supabase-Object-Version-Id')).toBe(dbAsset.version)
    expect(result.getHeader('X-Supabase-Object-Id')).toBeUndefined()
    expect(result.getHeader('Access-Control-Expose-Headers')).toContain('Supabase-Object-Id')
    expect(result.getHeader('Access-Control-Expose-Headers')).toContain(
      'Supabase-Object-Version-Id'
    )

    const downloaded = await fetch(
      `${context.baseUrl}/object/${bucket.id}/${objectName}?versionId=${result.getHeader('Supabase-Object-Version-Id')}`,
      { headers: { authorization } }
    )
    expect(downloaded.status).toBe(200)
    expect(Buffer.from(await downloaded.arrayBuffer())).toEqual(fs.readFileSync(assetPath))

    const resumed = await resumeTusUpload(uploadUrl, { authorization })
    expect(resumed.getStatus()).toBe(200)
    expect(resumed.getHeader('Supabase-Object-Id')).toBe(dbAsset.id)
    expect(resumed.getHeader('Supabase-Object-Version-Id')).toBe(dbAsset.version)
    expect(resumed.getHeader('Access-Control-Expose-Headers')).toContain('Supabase-Object-Id')
    expect(resumed.getHeader('Access-Control-Expose-Headers')).toContain(
      'Supabase-Object-Version-Id'
    )

    const readerJwt = await signJWT(
      { sub: randomUUID(), role: 'authenticated' },
      getConfig().jwtSecret,
      '1h'
    )
    const hidden = await fetch(uploadUrl, {
      method: 'HEAD',
      headers: { authorization: `Bearer ${readerJwt}`, 'tus-resumable': '1.0.0' },
    })
    expect(hidden.status).toBe(200)
    expect(hidden.headers.has('Supabase-Object-Id')).toBe(false)
    expect(hidden.headers.has('Supabase-Object-Version-Id')).toBe(false)

    expect(dbAsset).toEqual({
      archived_at: null,
      bucket_id: bucket.id,
      created_at: expect.any(Date),
      id: expect.any(String),
      is_delete_marker: false,
      is_versioned: false,
      last_accessed_at: expect.any(Date),
      metadata: {
        cacheControl: 'max-age=3600',
        contentLength: assetSize,
        eTag: expectedAssetEtag(backendType),
        httpStatusCode: 200,
        lastModified: expect.any(String),
        mimetype: 'image/jpeg',
        size: assetSize,
      },
      user_metadata: {
        test1: 'test1',
        test2: 'test2',
      },
      name: objectName,
      owner: null,
      owner_id: null,
      path_tokens: [objectName],
      updated_at: expect.any(Date),
      version: expect.any(String),
    })

    if (backendType === 'file') {
      if (!dbAsset.version) {
        throw new Error('expected uploaded object version')
      }

      const storedObjectPath = getStoredObjectPath(context, bucket.id, objectName, dbAsset.version)
      expect(await pathExists(storedObjectPath)).toBe(true)
    }
  })

  it('can upload an asset with data during TUS creation', async () => {
    const objectName = randomUUID() + '-creation-cat.jpeg'
    const seenResponses: Array<{ method: string; status: number; uploadOffset?: string }> = []

    const bucket = await storage.createBucket({
      id: bucketName,
      name: bucketName,
      public: true,
    })

    const authorization = `Bearer ${await context.config.serviceKeyAsync}`

    const result = await new Promise<tus.HttpResponse>((resolve, reject) => {
      const upload = new tus.Upload(openAssetStream(), {
        endpoint: `${context.baseUrl}${context.config.tusPath}`,
        onShouldRetry: () => false,
        uploadDataDuringCreation: true,
        headers: {
          authorization,
          'x-upsert': 'true',
        },
        metadata: {
          bucketName,
          objectName,
          contentType: 'image/jpeg',
          cacheControl: '3600',
          metadata: JSON.stringify({
            creation: 'with-data',
          }),
        },
        onAfterResponse(req, res) {
          seenResponses.push({
            method: req.getMethod(),
            status: res.getStatus(),
            uploadOffset: res.getHeader('Upload-Offset'),
          })
        },
        onError(error) {
          console.log('Failed because: ' + error)
          reject(error)
        },
        onSuccess: ({ lastResponse }) => {
          resolve(lastResponse)
        },
      })

      upload.start()
    })

    expect(result.getStatus()).toBe(201)
    expect(seenResponses).toEqual([
      {
        method: 'POST',
        status: 201,
        uploadOffset: String(assetSize),
      },
    ])

    const dbAsset = await storage.from(bucket.id).findObject(objectName, '*')
    expect(result.getHeader('Supabase-Object-Version-Id')).toBe(dbAsset.version)
    expect(dbAsset).toEqual({
      archived_at: null,
      bucket_id: bucket.id,
      created_at: expect.any(Date),
      id: expect.any(String),
      is_delete_marker: false,
      is_versioned: false,
      last_accessed_at: expect.any(Date),
      metadata: {
        cacheControl: 'max-age=3600',
        contentLength: assetSize,
        eTag: expectedAssetEtag(backendType),
        httpStatusCode: 200,
        lastModified: expect.any(String),
        mimetype: 'image/jpeg',
        size: assetSize,
      },
      user_metadata: {
        creation: 'with-data',
      },
      name: objectName,
      owner: null,
      owner_id: null,
      path_tokens: [objectName],
      updated_at: expect.any(Date),
      version: expect.any(String),
    })

    if (backendType === 'file') {
      if (!dbAsset.version) {
        throw new Error('expected uploaded object version')
      }

      const storedObjectPath = getStoredObjectPath(context, bucket.id, objectName, dbAsset.version)
      expect(await pathExists(storedObjectPath)).toBe(true)
    }
  })

  it('can resume an interrupted upload with the TUS protocol', async () => {
    const chunkSize = 8 * 1024
    const objectName = `${randomUUID()}-resume-cat.jpeg`

    const bucket = await storage.createBucket({
      id: bucketName,
      name: bucketName,
      public: true,
    })

    const authorization = `Bearer ${await context.config.serviceKeyAsync}`
    let interruptedUploadUrl: string | null = null
    let interruptedBytesAccepted = 0

    await new Promise<void>((resolve, reject) => {
      let aborted = false

      const upload = new tus.Upload(openAssetStream(), {
        chunkSize,
        endpoint: `${context.baseUrl}${context.config.tusPath}`,
        onShouldRetry: () => false,
        uploadDataDuringCreation: false,
        headers: {
          authorization,
          'x-upsert': 'true',
        },
        metadata: {
          bucketName,
          objectName,
          contentType: 'image/jpeg',
          cacheControl: '3600',
          metadata: JSON.stringify({
            resume: 'true',
          }),
        },
        onUploadUrlAvailable: () => {
          interruptedUploadUrl = upload.url
        },
        onChunkComplete: (_chunkLength, bytesAccepted) => {
          interruptedUploadUrl = upload.url
          interruptedBytesAccepted = bytesAccepted

          if (aborted || bytesAccepted < chunkSize) {
            return
          }

          aborted = true
          upload.abort().then(resolve, reject)
        },
        onError(error) {
          reject(error)
        },
        onSuccess: () => {
          reject(new Error('upload should have been interrupted before completion'))
        },
      })

      upload.start()
    })

    expect(interruptedUploadUrl).toBeTruthy()
    expect(interruptedBytesAccepted).toBe(chunkSize)
    expect(interruptedBytesAccepted).toBeLessThan(assetSize)

    if (backendType === 's3') {
      const client = context.backend.client
      if (!(client instanceof S3Client)) {
        throw new Error('Expected S3 client for s3 backend')
      }

      const uploadId = getTusDatastoreUploadId(context.config, interruptedUploadUrl!)
      const metadataKey = `${uploadId}.info`

      const metadataObject = await client.send(
        new HeadObjectCommand({
          Bucket: context.config.storageS3Bucket,
          Key: metadataKey,
        })
      )

      expect(metadataObject.Metadata).toMatchObject({
        'tus-version': expect.any(String),
        'upload-id': expect.any(String),
      })

      const uploads = await client.send(
        new ListMultipartUploadsCommand({
          Bucket: context.config.storageS3Bucket,
          Prefix: uploadId,
        })
      )

      expect(uploads.Uploads?.find((upload) => upload.Key === uploadId)?.Key).toBe(uploadId)
    } else {
      const tusUploadPath = getTusUploadPath(context, interruptedUploadUrl!)
      expect(await pathExists(tusUploadPath)).toBe(true)
      expect(await pathExists(`${tusUploadPath}.json`)).toBe(true)
    }

    await new Promise<void>((resolve, reject) => {
      const upload = new tus.Upload(openAssetStream(), {
        chunkSize,
        uploadUrl: interruptedUploadUrl,
        onShouldRetry: () => false,
        headers: {
          authorization,
          'x-upsert': 'true',
        },
        onError(error) {
          reject(error)
        },
        onSuccess: () => {
          resolve()
        },
      })

      upload.start()
    })

    const dbAsset = await storage.from(bucket.id).findObject(objectName, '*')
    expect(dbAsset).toEqual({
      archived_at: null,
      bucket_id: bucket.id,
      created_at: expect.any(Date),
      id: expect.any(String),
      is_delete_marker: false,
      is_versioned: false,
      last_accessed_at: expect.any(Date),
      metadata: {
        cacheControl: 'max-age=3600',
        contentLength: assetSize,
        eTag: expectedAssetEtag(backendType),
        httpStatusCode: 200,
        lastModified: expect.any(String),
        mimetype: 'image/jpeg',
        size: assetSize,
      },
      user_metadata: {
        resume: 'true',
      },
      name: objectName,
      owner: null,
      owner_id: null,
      path_tokens: [objectName],
      updated_at: expect.any(Date),
      version: expect.any(String),
    })

    if (backendType === 'file') {
      if (!dbAsset.version) {
        throw new Error('expected uploaded object version')
      }

      const storedObjectPath = getStoredObjectPath(context, bucket.id, objectName, dbAsset.version)
      expect(await pathExists(storedObjectPath)).toBe(true)
    }
  })

  it('can delete an incomplete upload via TUS', async () => {
    const objectName = `${randomUUID()}-incomplete.txt`

    await storage.createBucket({
      id: bucketName,
      name: bucketName,
      public: true,
    })

    const authorization = `Bearer ${await context.config.serviceKeyAsync}`
    const createResponse = await createTusUpload(context, authorization, {
      bucketName,
      objectName,
      contentType: 'text/plain',
      cacheControl: '3600',
    })

    expect(createResponse.status).toBe(201)

    const location = createResponse.headers.get('location')
    expect(location).toBeTruthy()

    if (backendType === 's3') {
      const client = context.backend.client
      if (!(client instanceof S3Client)) {
        throw new Error('Expected S3 client for s3 backend')
      }

      const uploadId = getTusDatastoreUploadId(context.config, location!)
      const metadataKey = `${uploadId}.info`

      const metadataObject = await client.send(
        new HeadObjectCommand({
          Bucket: context.config.storageS3Bucket,
          Key: metadataKey,
        })
      )

      expect(metadataObject.Metadata).toMatchObject({
        'tus-version': expect.any(String),
        'upload-id': expect.any(String),
      })

      const uploadsBeforeDelete = await client.send(
        new ListMultipartUploadsCommand({
          Bucket: context.config.storageS3Bucket,
          Prefix: uploadId,
        })
      )

      expect(uploadsBeforeDelete.Uploads?.find((upload) => upload.Key === uploadId)?.Key).toBe(
        uploadId
      )

      const deleteResponse = await deleteTusUpload(location!, authorization)

      expect(deleteResponse.status).toBe(204)

      await expect(
        client.send(
          new HeadObjectCommand({
            Bucket: context.config.storageS3Bucket,
            Key: metadataKey,
          })
        )
      ).rejects.toMatchObject({
        $metadata: {
          httpStatusCode: 404,
        },
      })

      const uploadsAfterDelete = await client.send(
        new ListMultipartUploadsCommand({
          Bucket: context.config.storageS3Bucket,
          Prefix: uploadId,
        })
      )

      expect(uploadsAfterDelete.Uploads?.find((upload) => upload.Key === uploadId)).toBeUndefined()
    } else {
      const tusUploadPath = getTusUploadPath(context, location!)
      expect(await pathExists(tusUploadPath)).toBe(true)
      expect(await pathExists(`${tusUploadPath}.json`)).toBe(true)

      const deleteResponse = await deleteTusUpload(location!, authorization)

      expect(deleteResponse.status).toBe(204)
      expect(await pathExists(tusUploadPath)).toBe(false)
      expect(await pathExists(`${tusUploadPath}.json`)).toBe(false)
    }
  })

  describe('TUS Validation', () => {
    it('cannot upload to a non-existing bucket', async () => {
      const objectName = randomUUID() + '-cat.jpeg'

      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
        fileSizeLimit: '10kb',
      })

      try {
        const authorization = `Bearer ${await context.config.serviceKeyAsync}`
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(openAssetStream(), {
            endpoint: `${context.baseUrl}${context.config.tusPath}`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
            headers: {
              authorization,
              'x-upsert': 'true',
            },
            metadata: {
              bucketName: 'doesn-exist',
              objectName,
              contentType: 'image/jpeg',
              cacheControl: '3600',
            },
            onError(error) {
              console.log('Failed because: ' + error)
              reject(error)
            },
            onSuccess: () => {
              resolve(true)
            },
          })

          upload.start()
        })

        throw Error('it should error with bucket not found')
      } catch (e) {
        const response = expectTusErrorResponse(e)
        expect(response.getBody()).toEqual('Bucket not found')
        expect(response.getStatus()).toEqual(404)
      }
    })

    it('cannot upload an asset that exceeds the maximum bucket size', async () => {
      const objectName = randomUUID() + '-cat.jpeg'

      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
        fileSizeLimit: '10kb',
      })

      try {
        const authorization = `Bearer ${await context.config.serviceKeyAsync}`
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(openAssetStream(), {
            endpoint: `${context.baseUrl}${context.config.tusPath}`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
            headers: {
              authorization,
              'x-upsert': 'true',
            },
            metadata: {
              bucketName,
              objectName,
              contentType: 'image/jpeg',
              cacheControl: '3600',
            },
            onError(error) {
              console.log('Failed because: ' + error)
              reject(error)
            },
            onSuccess: () => {
              resolve(true)
            },
          })

          upload.start()
        })

        throw Error('it should error with max-size exceeded')
      } catch (e) {
        const response = expectTusErrorResponse(e)
        expect(response.getBody()).toEqual('Maximum size exceeded\n')
        expect(response.getStatus()).toEqual(413)
      }
    })

    it('cannot upload an asset that exceeds the maximum bucket size when the upload length is deferred', async () => {
      const objectName = randomUUID() + '-cat.jpeg'

      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
        fileSizeLimit: '10kb',
      })

      try {
        const authorization = `Bearer ${await context.config.serviceKeyAsync}`
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(openAssetStream(), {
            endpoint: `${context.baseUrl}${context.config.tusPath}`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
            uploadLengthDeferred: true,
            chunkSize: 64 * 1024,
            headers: {
              authorization,
              'x-upsert': 'true',
            },
            metadata: {
              bucketName,
              objectName,
              contentType: 'image/jpeg',
              cacheControl: '3600',
            },
            onError(error) {
              console.log('Failed because: ' + error)
              reject(error)
            },
            onSuccess: () => {
              resolve(true)
            },
          })

          upload.start()
        })

        throw Error('it should error with max-size exceeded')
      } catch (e) {
        const response = expectTusErrorResponse(e)
        expect(response.getBody()).toEqual('Maximum size exceeded\n')
        expect(response.getStatus()).toEqual(413)
      }
    })
  })

  describe('Signed Upload URL', () => {
    it('will allow uploading using signed upload url without authorization token', async () => {
      const bucket = await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const objectName = randomUUID() + '-cat.jpeg'

      const signedUpload = await storage
        .from(bucketName)
        .signUploadObjectUrl(objectName, `${bucketName}/${objectName}`, 3600)

      let uploadUrl = ''
      const result = await new Promise<tus.HttpResponse>((resolve, reject) => {
        const upload = new tus.Upload(openAssetStream(), {
          endpoint: `${context.baseUrl}${context.config.tusPath}/sign`,
          onShouldRetry: () => false,
          uploadDataDuringCreation: false,
          headers: {
            'x-signature': signedUpload.token,
          },
          metadata: {
            bucketName,
            objectName,
            contentType: 'image/jpeg',
            cacheControl: '3600',
            metadata: JSON.stringify({
              test1: 'test1',
              test3: 'test3',
            }),
          },
          onError(error) {
            console.log('Failed because: ' + error)
            reject(error)
          },
          onSuccess: ({ lastResponse }) => {
            uploadUrl = upload.url ?? ''
            resolve(lastResponse)
          },
        })

        upload.start()
      })

      expect(result.getStatus()).toBe(204)

      const dbAsset = await storage.from(bucket.id).findObject(objectName, '*')
      expect(result.getHeader('Supabase-Object-Id')).toBe(dbAsset.id)
      expect(result.getHeader('Supabase-Object-Version-Id')).toBe(dbAsset.version)
      const resumed = await resumeTusUpload(uploadUrl, { 'x-signature': signedUpload.token })
      expect(resumed.getStatus()).toBe(200)
      expect(resumed.getHeader('Supabase-Object-Id')).toBe(dbAsset.id)
      expect(resumed.getHeader('Supabase-Object-Version-Id')).toBe(dbAsset.version)

      const unsigned = await fetch(uploadUrl, {
        method: 'HEAD',
        headers: { 'tus-resumable': '1.0.0' },
      })
      expect(unsigned.status).toBe(400)
      expect(unsigned.headers.has('Supabase-Object-Id')).toBe(false)
      expect(unsigned.headers.has('Supabase-Object-Version-Id')).toBe(false)

      expect(dbAsset).toEqual({
        archived_at: null,
        bucket_id: bucket.id,
        created_at: expect.any(Date),
        id: expect.any(String),
        is_delete_marker: false,
        is_versioned: false,
        last_accessed_at: expect.any(Date),
        metadata: {
          cacheControl: 'max-age=3600',
          contentLength: assetSize,
          eTag: expectedAssetEtag(backendType),
          httpStatusCode: 200,
          lastModified: expect.any(String),
          mimetype: 'image/jpeg',
          size: assetSize,
        },
        user_metadata: {
          test1: 'test1',
          test3: 'test3',
        },
        name: objectName,
        owner: null,
        owner_id: null,
        path_tokens: [objectName],
        updated_at: expect.any(Date),
        version: expect.any(String),
      })
    })

    it('will allow uploading using signed upload url without authorization token, honouring the owner id', async () => {
      const bucket = await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const objectName = randomUUID() + '-cat.jpeg'

      const signedUpload = await storage
        .from(bucketName)
        .signUploadObjectUrl(objectName, `${bucketName}/${objectName}`, 3600, 'some-owner-id')

      const result = await new Promise((resolve, reject) => {
        const upload = new tus.Upload(openAssetStream(), {
          endpoint: `${context.baseUrl}${context.config.tusPath}/sign`,
          onShouldRetry: () => false,
          uploadDataDuringCreation: false,
          headers: {
            'x-signature': signedUpload.token,
          },
          metadata: {
            bucketName,
            objectName,
            contentType: 'image/jpeg',
            cacheControl: '3600',
          },
          onError(error) {
            console.log('Failed because: ' + error)
            reject(error)
          },
          onSuccess: () => {
            resolve(true)
          },
        })

        upload.start()
      })

      expect(result).toEqual(true)

      const dbAsset = await storage.from(bucket.id).findObject(objectName, '*')
      expect(dbAsset).toEqual({
        archived_at: null,
        bucket_id: bucket.id,
        created_at: expect.any(Date),
        id: expect.any(String),
        is_delete_marker: false,
        is_versioned: false,
        last_accessed_at: expect.any(Date),
        metadata: {
          cacheControl: 'max-age=3600',
          contentLength: assetSize,
          eTag: expectedAssetEtag(backendType),
          httpStatusCode: 200,
          lastModified: expect.any(String),
          mimetype: 'image/jpeg',
          size: assetSize,
        },
        user_metadata: null,
        name: objectName,
        owner: null,
        owner_id: 'some-owner-id',
        path_tokens: [objectName],
        updated_at: expect.any(Date),
        version: expect.any(String),
      })
    })

    it('will not allow uploading using signed upload url with an expired token', async () => {
      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const objectName = randomUUID() + '-cat.jpeg'

      const signedAt = new Date()
      vi.setSystemTime(signedAt)

      const signedUpload = await storage
        .from(bucketName)
        .signUploadObjectUrl(objectName, `${bucketName}/${objectName}`, 1)

      vi.setSystemTime(new Date(signedAt.getTime() + 2000))

      try {
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(openAssetStream(), {
            endpoint: `${context.baseUrl}${context.config.tusPath}/sign`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
            headers: {
              'x-signature': signedUpload.token,
            },
            metadata: {
              bucketName,
              objectName,
              contentType: 'image/jpeg',
              cacheControl: '3600',
            },
            onError(error) {
              console.log('Failed because: ' + error)
              reject(error)
            },
            onSuccess: () => {
              resolve(true)
            },
          })

          upload.start()
        })

        throw new Error('it should error with expired token')
      } catch (e) {
        expect((e as Error).message).not.toEqual('it should error with expired token')

        const response = expectTusErrorResponse(e)
        expect(response.getBody()).toEqual('"exp" claim timestamp check failed')
        expect(response.getStatus()).toEqual(400)
      }
    })

    it('will not allow uploading using signed upload url with an invalid token', async () => {
      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const objectName = randomUUID() + '-cat.jpeg'

      try {
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(openAssetStream(), {
            endpoint: `${context.baseUrl}${context.config.tusPath}/sign`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
            headers: {
              'x-signature': 'invalid-token',
            },
            metadata: {
              bucketName,
              objectName,
              contentType: 'image/jpeg',
              cacheControl: '3600',
            },
            onError(error) {
              console.log('Failed because: ' + error)
              reject(error)
            },
            onSuccess: () => {
              resolve(true)
            },
          })

          upload.start()
        })

        throw new Error('it should error with invalid token')
      } catch (e) {
        expect((e as Error).message).not.toEqual('it should error with invalid token')

        const response = expectTusErrorResponse(e)
        expect(response.getBody()).toEqual('Invalid Compact JWS')
        expect(response.getStatus()).toEqual(400)
      }
    })

    it('will not allow uploading using signed upload url without a token', async () => {
      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const objectName = randomUUID() + '-cat.jpeg'

      try {
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(openAssetStream(), {
            endpoint: `${context.baseUrl}${context.config.tusPath}/sign`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
            metadata: {
              bucketName,
              objectName,
              contentType: 'image/jpeg',
              cacheControl: '3600',
            },
            onError(error) {
              console.log('Failed because: ' + error)
              reject(error)
            },
            onSuccess: () => {
              resolve(true)
            },
          })

          upload.start()
        })

        throw new Error('it should error with missing token')
      } catch (e) {
        expect((e as Error).message).not.toEqual('it should error with missing token')

        const response = expectTusErrorResponse(e)
        expect(response.getBody()).toEqual('Missing x-signature header')
        expect(response.getStatus()).toEqual(400)
      }
    })

    it('requires a signature for every request target routed to the signed upload scope', async () => {
      await storage.createBucket({ id: bucketName, name: bucketName, public: false })

      const { tusPath } = context.config
      const objectName = 'private.txt'
      const metadata = { bucketName, objectName, contentType: 'text/plain' }
      const targets = [
        `${tusPath}/sign/`,
        `${tusPath}/%73ign/`,
        `${tusPath}/sig%6E/`,
        '/upload/%72esumable/%73ign/',
        `${context.baseUrl}${tusPath}/sign/`,
      ]
      const unsigned = await Promise.all([
        ...targets.map((target) => postRawTusUpload(context.baseUrl, target, metadata)),
        postRawTusUpload(context.baseUrl, targets[0], { ...metadata, bucketName: randomUUID() }),
      ])
      expect(unsigned.map(({ status, body }) => ({ status, body }))).toEqual(
        unsigned.map(() => ({ status: 400, body: 'Missing x-signature header' }))
      )
      expect(
        await db.findObject(bucketName, objectName, 'id', { dontErrorOnEmpty: true })
      ).toBeUndefined()

      const { token } = await storage
        .from(bucketName)
        .signUploadObjectUrl(objectName, `${bucketName}/${objectName}`, 3600, undefined, {
          upsert: true,
        })
      for (const target of [targets[1], targets[4]]) {
        const signed = await postRawTusUpload(context.baseUrl, target, metadata, token)
        expect(signed.status).toBe(201)
        expect(signed.location).toContain(`${tusPath}/sign/`)
      }
      expect(await db.findObject(bucketName, objectName, 'id')).toBeDefined()
    })
  })

  it.each([
    'content-length',
    'chunked',
    'unterminated chunked',
  ])('completes the upload after final data and disconnect with %s framing', async (framing) => {
    const objectName = `${randomUUID()}.bin`
    const body = randomBytes(1024 * 1024)
    const authorization = `Bearer ${await context.config.serviceKeyAsync}`
    await storage.createBucket({ id: bucketName, name: bucketName, public: false })

    const created = await fetch(`${context.baseUrl}${context.config.tusPath}`, {
      method: 'POST',
      headers: {
        authorization,
        'Tus-Resumable': '1.0.0',
        'Upload-Length': String(body.length),
        'Upload-Metadata': encodeTusMetadata({
          bucketName,
          objectName,
          contentType: 'application/octet-stream',
        }),
        'x-upsert': 'true',
      },
    })
    expect(created.status).toBe(201)
    const target = new URL(created.headers.get('location') as string, context.baseUrl)

    const reached = Promise.withResolvers<void>()
    const release = Promise.withResolvers<void>()
    const finished = Promise.withResolvers<void>()
    const gate = {
      reached: reached.resolve,
      release: release.promise,
      finished: finished.resolve,
      closed: undefined as Promise<void> | undefined,
    }
    uploadFinishGate.current = gate
    const socket = connect({ host: target.hostname, port: Number(target.port) })
    const incoming = Promise.withResolvers<IncomingMessage>()
    const observeRequest = (request: IncomingMessage) => {
      if (request.method === 'PATCH' && request.url === target.pathname + target.search) {
        incoming.resolve(request)
      }
    }
    context.server.server.on('request', observeRequest)

    try {
      socket.on('error', () => {})
      socket.setNoDelay(true)
      const headers =
        `PATCH ${target.pathname}${target.search} HTTP/1.1\r\n` +
        `Host: ${target.host}\r\n` +
        `Authorization: ${authorization}\r\n` +
        'Tus-Resumable: 1.0.0\r\n' +
        'Upload-Offset: 0\r\n' +
        'Content-Type: application/offset+octet-stream\r\n' +
        (framing === 'content-length'
          ? `Content-Length: ${body.length}\r\n\r\n`
          : 'Transfer-Encoding: chunked\r\n\r\n')
      const payload =
        framing === 'content-length'
          ? body
          : Buffer.concat([
              Buffer.from(`${body.length.toString(16)}\r\n`),
              body,
              Buffer.from(framing === 'chunked' ? '\r\n0\r\n\r\n' : '\r\n'),
            ])
      socket.write(headers)
      socket.write(payload)

      if (framing === 'unterminated chunked') {
        const request = await incoming.promise
        await vi.waitFor(() =>
          expect(request.socket.bytesRead).toBeGreaterThanOrEqual(
            Buffer.byteLength(headers) + payload.length
          )
        )
        await vi.waitFor(() => expect(request.readableLength).toBe(0))
        socket.end()
      }

      await reached.promise
      if (framing !== 'unterminated chunked') socket.end()
      await gate.closed
      release.resolve()
      await Promise.race([
        finished.promise,
        sleep(5000).then(() => {
          throw new Error('TUS finalization did not finish')
        }),
      ])
    } finally {
      uploadFinishGate.current = null
      context.server.server.removeListener('request', observeRequest)
      release.resolve()
      socket.destroy()
    }

    const head = await fetch(target, {
      method: 'HEAD',
      headers: { authorization, 'Tus-Resumable': '1.0.0' },
    })
    expect(head.headers.get('upload-offset')).toBe(String(body.length))

    const stored = await db.findObject(bucketName, objectName, 'id,version', {
      dontErrorOnEmpty: true,
    })
    expect(stored).toBeDefined()

    const object = await context.backend.getObject(
      context.config.storageS3Bucket,
      `${context.config.tenantId}/${bucketName}/${objectName}`,
      stored?.version
    )
    const bytes = Buffer.from(await new Response(object.body as ReadableStream).arrayBuffer())
    expect(bytes.length).toBe(body.length)
    expect(bytes.equals(body)).toBe(true)
  })
})

describe('File-backed TUS — path traversal', () => {
  let context: TusTestContext
  let connection: Awaited<ReturnType<typeof getPostgresConnection>>
  let fileBackendPath: string
  let storage: StorageType

  beforeAll(async () => {
    fileBackendPath = await mkdtemp(path.join(tmpdir(), 'storage-tus-traversal-'))
    context = await createTusTestContext('file', { fileBackendPath })
  })

  afterAll(async () => {
    await context?.server?.close()
    vi.resetModules()
    if (fileBackendPath) {
      await removePath(fileBackendPath)
    }
  })

  beforeEach(async () => {
    const superUser = await getServiceKeyUser(context.config.tenantId)
    connection = await getPostgresConnection({
      tenantId: context.config.tenantId,
      user: superUser,
      superUser,
      host: 'localhost',
      disableHostCheck: true,
    })

    const db = new context.StoragePgDB(connection, {
      tenantId: context.config.tenantId,
      host: 'localhost',
    })

    storage = new context.Storage(
      context.backend,
      db,
      new context.TenantLocation(context.config.storageS3Bucket)
    )
  })

  afterEach(async () => {
    connection.dispose()
  })

  it('rejects traversal object names and does not write outside the file-backed TUS root', async () => {
    const bucketName = randomUUID()
    const escapePrefix = `storage-tus-escape-${randomUUID()}`
    const bucketRoot = path.join(
      context.fileBackendPath!,
      context.config.storageS3Bucket,
      context.config.tenantId,
      bucketName
    )
    const escapedPath = path.join(tmpdir(), escapePrefix)
    const objectName = path
      .relative(bucketRoot, path.join(escapedPath, 'escape.txt'))
      .split(path.sep)
      .join('/')

    await storage.createBucket({
      id: bucketName,
      name: bucketName,
      public: true,
    })

    const authorization = `Bearer ${await context.config.serviceKeyAsync}`
    const createResponse = await createTusUpload(context, authorization, {
      bucketName,
      objectName,
      contentType: 'text/plain',
      cacheControl: '3600',
    })

    expect(createResponse.status).toBe(400)
    expect(await createResponse.text()).toContain('Invalid key')
    expect(createResponse.headers.get('location')).toBeNull()
    expect(await pathExists(bucketRoot)).toBe(false)
    expect(await pathExists(escapedPath)).toBe(false)
  })
})

describe('File-backed TUS — TUS_USE_FILE_VERSION_SEPARATOR', () => {
  let context: TusTestContext
  let connection: Awaited<ReturnType<typeof getPostgresConnection>>
  let fileBackendPath: string
  let storage: StorageType

  beforeAll(async () => {
    process.env.TUS_USE_FILE_VERSION_SEPARATOR = 'true'
    fileBackendPath = await mkdtemp(path.join(tmpdir(), 'storage-tus-file-separator-'))
    context = await createTusTestContext('file', { fileBackendPath })
  })

  afterAll(async () => {
    await context?.server?.close()
    delete process.env.TUS_USE_FILE_VERSION_SEPARATOR
    vi.resetModules()
    if (fileBackendPath) {
      await removePath(fileBackendPath)
    }
  })

  beforeEach(async () => {
    const superUser = await getServiceKeyUser(context.config.tenantId)
    connection = await getPostgresConnection({
      tenantId: context.config.tenantId,
      user: superUser,
      superUser,
      host: 'localhost',
      disableHostCheck: true,
    })

    const db = new context.StoragePgDB(connection, {
      tenantId: context.config.tenantId,
      host: 'localhost',
    })

    storage = new context.Storage(
      context.backend,
      db,
      new context.TenantLocation(context.config.storageS3Bucket)
    )
  })

  afterEach(async () => {
    connection.dispose()
  })

  it('uploads an object inside a folder', async () => {
    const bucketName = randomUUID()
    const objectName = `folder/sub/${randomUUID()}-cat.jpeg`

    await storage.createBucket({
      id: bucketName,
      name: bucketName,
      public: true,
    })

    const authorization = `Bearer ${await context.config.serviceKeyAsync}`

    await new Promise((resolve, reject) => {
      const upload = new tus.Upload(openAssetStream(), {
        endpoint: `${context.baseUrl}${context.config.tusPath}`,
        onShouldRetry: () => false,
        uploadDataDuringCreation: false,
        headers: {
          authorization,
          'x-upsert': 'true',
        },
        metadata: {
          bucketName,
          objectName,
          contentType: 'image/jpeg',
        },
        onError: reject,
        onSuccess: () => resolve(true),
      })

      upload.start()
    })

    const dbAsset = await storage.from(bucketName).findObject(objectName, '*')
    expect(dbAsset.name).toBe(objectName)
    expect(dbAsset.metadata?.size).toBe(assetSize)

    if (!dbAsset.version) {
      throw new Error('expected uploaded object version')
    }

    const storedObjectPath = getStoredObjectPath(context, bucketName, objectName, dbAsset.version)
    expect(storedObjectPath.endsWith(`-$v-${dbAsset.version}`)).toBe(true)
    expect(await pathExists(storedObjectPath)).toBe(true)
  })
})

describe('TUS body idle timeout', () => {
  const idleTimeoutMs = 200

  let context: TusTestContext
  let db: StoragePgDBType
  let storage: StorageType
  let connection: Awaited<ReturnType<typeof getPostgresConnection>>
  let bucketName: string
  let fileBackendPath: string

  beforeAll(async () => {
    fileBackendPath = await mkdtemp(path.join(tmpdir(), 'storage-tus-idle-'))
    context = await createTusTestContext('file', {
      fileBackendPath,
      tusBodyIdleTimeoutMs: idleTimeoutMs,
    })
  })

  afterAll(async () => {
    await context?.server?.close()
    vi.resetModules()
    await removePath(fileBackendPath)
  })

  beforeEach(async () => {
    const superUser = await getServiceKeyUser(context.config.tenantId)
    connection = await getPostgresConnection({
      superUser,
      user: superUser,
      tenantId: context.config.tenantId,
      host: 'localhost',
      disableHostCheck: true,
    })

    db = new context.StoragePgDB(connection, {
      tenantId: context.config.tenantId,
      host: 'localhost',
    })

    bucketName = randomUUID()
    storage = new context.Storage(
      context.backend,
      db,
      new context.TenantLocation(context.config.storageS3Bucket)
    )
    await storage.createBucket({ id: bucketName, name: bucketName, public: true })
  })

  afterEach(async () => {
    connection?.dispose()
  })

  async function createRawUpload(totalSize: number) {
    const authorization = `Bearer ${await context.config.serviceKeyAsync}`
    const objectName = `${randomUUID()}-idle-timeout.bin`

    const response = await createTusUpload(
      context,
      authorization,
      { bucketName, objectName, contentType: 'application/octet-stream' },
      totalSize
    )
    const location = response.headers.get('location')
    if (response.status !== 201 || !location) {
      throw new Error(`creation failed with status ${response.status}`)
    }

    return { uploadUrl: new URL(location, context.baseUrl), authorization }
  }

  function openRawPatch(
    uploadUrl: URL,
    authorization: string,
    offset: number,
    declaredLength: number
  ) {
    const req = http.request(uploadUrl, {
      method: 'PATCH',
      headers: {
        authorization,
        'Tus-Resumable': '1.0.0',
        'Upload-Offset': String(offset),
        'Content-Type': 'application/offset+octet-stream',
        'Content-Length': String(declaredLength),
      },
    })
    req.on('error', () => {})
    return req
  }

  async function headOffset(uploadUrl: URL, authorization: string): Promise<number> {
    return new Promise((resolve, reject) => {
      const req = http.request(
        uploadUrl,
        { method: 'HEAD', headers: { authorization, 'Tus-Resumable': '1.0.0' } },
        (res) => {
          res.resume()
          res.on('end', () => resolve(Number(res.headers['upload-offset'])))
        }
      )
      req.on('error', reject)
      req.end()
    })
  }

  it(
    'destroys a connection that goes silent mid-chunk, preserving already-received bytes',
    async () => {
      const totalSize = 2 * 1024 * 1024
      const sentSize = 64 * 1024
      const { uploadUrl, authorization } = await createRawUpload(totalSize)

      const patch = openRawPatch(uploadUrl, authorization, 0, totalSize)
      const destroyed = new Promise<void>((resolve) => {
        patch.on('error', () => resolve())
      })
      patch.write(Buffer.alloc(sentSize))
      // Deliberately never call .end() or .destroy()
      // simulate a connection that goes silent mid-chunk with no closing signal

      await destroyed
      patch.destroy()

      const offset = await headOffset(uploadUrl, authorization)
      expect(offset).toBe(sentSize)
    },
    idleTimeoutMs * 15
  )

  it(
    'does not time out a slow-but-steady trickle whose gaps stay under the idle threshold',
    async () => {
      const chunkSize = 16 * 1024
      const chunkCount = 6
      const totalSize = chunkSize * chunkCount
      const { uploadUrl, authorization } = await createRawUpload(totalSize)

      const patch = openRawPatch(uploadUrl, authorization, 0, totalSize)
      const response = new Promise<number>((resolve, reject) => {
        patch.on('response', (res) => resolve(res.statusCode ?? 0))
        patch.on('error', reject)
      })

      for (let i = 0; i < chunkCount; i++) {
        await sleep(idleTimeoutMs / 2)
        patch.write(Buffer.alloc(chunkSize))
      }
      patch.end()

      expect(await response).toBe(204)
    },
    idleTimeoutMs * 20
  )
})
