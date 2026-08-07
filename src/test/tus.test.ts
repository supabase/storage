import { once } from 'node:events'
import { mkdtemp } from 'node:fs/promises'
import { connect } from 'node:net'
import { tmpdir } from 'node:os'
import path from 'node:path'
import { setTimeout as delay } from 'node:timers/promises'
import {
  CreateBucketCommand,
  HeadObjectCommand,
  ListMultipartUploadsCommand,
  ListObjectsV2Command,
  NoSuchUpload,
  PutObjectCommand,
  type S3,
  S3Client,
} from '@aws-sdk/client-s3'
import { getPostgresConnection, getServiceKeyUser } from '@internal/database'
import { pathExists, removePath } from '@internal/fs'
import { logger } from '@internal/monitoring'
import { randomUUID } from 'crypto'
import { FastifyInstance } from 'fastify'
import fs from 'fs'
import * as tus from 'tus-js-client'
import { DetailedError } from 'tus-js-client'
import type { StorageBackendAdapter } from '../storage/backend'
import type { StoragePgDB as StoragePgDBType } from '../storage/database/pg'
import type { TenantLocation as TenantLocationType } from '../storage/locator'
import type { FileStore as StorageTusFileStore } from '../storage/protocols/tus/file-store'
import type { S3Store as StorageTusS3Store } from '../storage/protocols/tus/s3-store'
import type { Storage as StorageType } from '../storage/storage'
import { checkBucketExists } from './common'

const assetPath = path.resolve(__dirname, 'assets', 'sadcat.jpg')
const assetSize = fs.statSync(assetPath).size
const openAssetStream = () => fs.createReadStream(assetPath)

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
  TusFileStore: typeof StorageTusFileStore
  TusS3Store: typeof StorageTusS3Store
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

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined
  const timeoutPromise = new Promise<never>((_resolve, reject) => {
    timeout = setTimeout(() => reject(new Error(message)), timeoutMs)
  })

  try {
    return await Promise.race([promise, timeoutPromise])
  } finally {
    if (timeout !== undefined) {
      clearTimeout(timeout)
    }
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

function patchTusUpload(
  location: string,
  authorization: string,
  offset: number,
  body: Uint8Array<ArrayBuffer>
) {
  return fetch(location, {
    method: 'PATCH',
    headers: {
      authorization,
      'tus-resumable': '1.0.0',
      'upload-offset': String(offset),
      'content-type': 'application/offset+octet-stream',
    },
    body,
  })
}

type RawTusRequest = {
  socket: ReturnType<typeof connect>
  closed: Promise<void>
}

async function openRawTusRequest(
  endpoint: string,
  method: 'PATCH' | 'POST',
  headers: string[],
  body: Buffer
): Promise<RawTusRequest> {
  const url = new URL(endpoint)
  const socket = connect({ host: url.hostname, port: Number(url.port) })
  const closed = new Promise<void>((resolve) => socket.once('close', () => resolve()))

  await once(socket, 'connect')
  socket.on('error', () => {
    // The request is intentionally truncated.
  })

  const requestHeaders = [
    `${method} ${url.pathname}${url.search} HTTP/1.1`,
    `Host: ${url.host}`,
    ...headers,
    'Connection: close',
    '',
    '',
  ].join('\r\n')

  await new Promise<void>((resolve, reject) => {
    socket.write(Buffer.concat([Buffer.from(requestHeaders), body]), (error) =>
      error ? reject(error) : resolve()
    )
  })

  return { socket, closed }
}

function openTusPatch(
  location: string,
  authorization: string,
  declaredLength: number,
  body: Buffer
): Promise<RawTusRequest> {
  return openRawTusRequest(
    location,
    'PATCH',
    [
      `Authorization: ${authorization}`,
      'Tus-Resumable: 1.0.0',
      'Upload-Offset: 0',
      'Content-Type: application/offset+octet-stream',
      `Content-Length: ${declaredLength}`,
    ],
    body
  )
}

async function abortTusCreation<T>(
  endpoint: string,
  authorization: string,
  metadata: Record<string, string>,
  uploadLength: number,
  declaredLength: number,
  body: Buffer,
  waitForCreationStarted: () => Promise<T>
): Promise<T> {
  const { socket, closed } = await openRawTusRequest(
    endpoint,
    'POST',
    [
      `Authorization: ${authorization}`,
      'Tus-Resumable: 1.0.0',
      `Upload-Length: ${uploadLength}`,
      `Upload-Metadata: ${encodeTusMetadata(metadata)}`,
      'Content-Type: application/offset+octet-stream',
      `Content-Length: ${declaredLength}`,
      'X-Upsert: true',
    ],
    body
  )

  try {
    return await waitForCreationStarted()
  } finally {
    socket.destroy()
    await closed
  }
}

async function waitForTusUploadRemoval(location: string, authorization: string) {
  // It can wait up to 5 seconds for the lock.
  const deadline = Date.now() + 10_000
  let lastStatus: number | undefined

  do {
    const response = await fetch(location, {
      method: 'HEAD',
      headers: { authorization, 'tus-resumable': '1.0.0' },
    })
    lastStatus = response.status

    if (response.status === 404 || response.status === 410) {
      return response
    }

    await delay(25)
  } while (Date.now() < deadline)

  throw new Error(`TUS upload remained accessible after invalidation (status ${lastStatus})`)
}

type S3TusPrefixArtifacts = { objects: string[]; uploads: string[] }

async function listS3TusPrefixArtifacts(
  client: S3Client,
  bucket: string,
  prefix: string
): Promise<S3TusPrefixArtifacts> {
  const [objects, uploads] = await Promise.all([
    client.send(new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix })),
    client.send(new ListMultipartUploadsCommand({ Bucket: bucket, Prefix: prefix })),
  ])

  return {
    objects: objects.Contents?.flatMap((object) => (object.Key ? [object.Key] : [])) ?? [],
    uploads: uploads.Uploads?.flatMap((upload) => (upload.Key ? [upload.Key] : [])) ?? [],
  }
}

async function waitForS3TusPrefixCreation(
  client: S3Client,
  bucket: string,
  prefix: string
): Promise<S3TusPrefixArtifacts> {
  for (let attempt = 0; attempt < 400; attempt++) {
    const artifacts = await listS3TusPrefixArtifacts(client, bucket, prefix)
    if (artifacts.objects.length > 0 || artifacts.uploads.length > 0) {
      return artifacts
    }

    await delay(25)
  }

  throw new Error(`TUS creation did not start for S3 prefix ${prefix}`)
}

async function waitForS3TusPrefixRemoval(
  client: S3Client,
  bucket: string,
  prefix: string
): Promise<S3TusPrefixArtifacts> {
  let remaining = { objects: [] as string[], uploads: [] as string[] }
  const deadline = Date.now() + 5000

  do {
    remaining = await listS3TusPrefixArtifacts(client, bucket, prefix)
    if (remaining.objects.length === 0 && remaining.uploads.length === 0) {
      return remaining
    }

    await delay(25)
  } while (Date.now() < deadline)

  return remaining
}

function mockAwsCompletedMultipartAbort(store: StorageTusS3Store) {
  const client = (store as unknown as { client: S3 }).client

  return vi.spyOn(client, 'abortMultipartUpload').mockRejectedValue(
    new NoSuchUpload({
      $metadata: { httpStatusCode: 404 },
      message: 'The specified multipart upload does not exist',
    })
  )
}

type TusWriteStore = {
  write(...args: Parameters<StorageTusFileStore['write']>): Promise<number>
}

function observeNextTusWrite(storeClass: { prototype: unknown }) {
  const prototype = storeClass.prototype as TusWriteStore
  const originalWrite = prototype.write
  let signalWriteStarted: (() => void) | undefined
  const writeStarted = new Promise<void>((resolve) => {
    signalWriteStarted = resolve
  })
  const write = vi.spyOn(prototype, 'write').mockImplementation(function (
    this: TusWriteStore,
    ...args
  ) {
    signalWriteStarted?.()
    return originalWrite.apply(this, args)
  })

  return { write, writeStarted }
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
  options: { fileBackendPath?: string } = {}
): Promise<TusTestContext> {
  vi.resetModules()

  const configModule = await import('../config')
  configModule.setEnvPaths(['.env.test', '.env'])
  configModule.getConfig({ reload: true })

  const overrides: Partial<{
    storageBackendType: 'file' | 's3'
    storageFilePath: string
  }> = { storageBackendType: backendType }
  if (backendType === 'file') {
    overrides.storageFilePath = options.fileBackendPath
  }
  configModule.mergeConfig(overrides)

  const [
    appModule,
    backendModule,
    storageModule,
    databaseModule,
    locatorModule,
    tusFileStoreModule,
    tusS3StoreModule,
  ] = await Promise.all([
    import('../app'),
    import('../storage/backend'),
    import('../storage/storage'),
    import('../storage/database'),
    import('../storage/locator'),
    import('../storage/protocols/tus/file-store'),
    import('../storage/protocols/tus/s3-store'),
  ])

  const server = appModule.default({ loggerInstance: logger })
  const listener = await server.listen()
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
    TusFileStore: tusFileStoreModule.FileStore,
    TusS3Store: tusS3StoreModule.S3Store,
    backend,
    baseUrl: listener.replace('[::1]', '127.0.0.1'),
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

  it('can upload an asset with the TUS protocol', async () => {
    const objectName = randomUUID() + '-cat.jpeg'

    const bucket = await storage.createBucket({
      id: bucketName,
      name: bucketName,
      public: true,
    })

    const authorization = `Bearer ${await context.config.serviceKeyAsync}`

    const result = await new Promise((resolve, reject) => {
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
        onSuccess: () => {
          resolve(true)
        },
      })

      upload.start()
    })

    expect(result).toEqual(true)

    const dbAsset = await storage.from(bucket.id).findObject(objectName, '*')
    expect(dbAsset).toEqual({
      bucket_id: bucket.id,
      created_at: expect.any(Date),
      id: expect.any(String),
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

    const result = await new Promise((resolve, reject) => {
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
        onSuccess: () => {
          resolve(true)
        },
      })

      upload.start()
    })

    expect(result).toEqual(true)
    expect(seenResponses).toEqual([
      {
        method: 'POST',
        status: 201,
        uploadOffset: String(assetSize),
      },
    ])

    const dbAsset = await storage.from(bucket.id).findObject(objectName, '*')
    expect(dbAsset).toEqual({
      bucket_id: bucket.id,
      created_at: expect.any(Date),
      id: expect.any(String),
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
      bucket_id: bucket.id,
      created_at: expect.any(Date),
      id: expect.any(String),
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

  it('invalidates an upload after the incident-sized PATCH is truncated', async () => {
    const uploadLength = 12_445_929
    const declaredPatchLength = 6 * 1024 * 1024
    const objectName = `${randomUUID()}-aborted.txt`

    await storage.createBucket({
      id: bucketName,
      name: bucketName,
      public: true,
    })

    const authorization = `Bearer ${await context.config.serviceKeyAsync}`
    const createResponse = await createTusUpload(
      context,
      authorization,
      {
        bucketName,
        objectName,
        contentType: 'text/plain',
        cacheControl: '3600',
      },
      uploadLength
    )
    const location = createResponse.headers.get('location')

    expect(createResponse.status).toBe(201)
    expect(location).toBeTruthy()

    const storeClass = backendType === 's3' ? context.TusS3Store : context.TusFileStore
    const { write, writeStarted } = observeNextTusWrite(storeClass)
    let patchRequest: RawTusRequest | undefined

    try {
      patchRequest = await openTusPatch(
        location!,
        authorization,
        declaredPatchLength,
        Buffer.alloc(6_193_399, 0x61)
      )
      await withTimeout(writeStarted, 5000, 'truncated TUS PATCH did not enter the datastore write')
      patchRequest.socket.destroy()
      await patchRequest.closed
    } finally {
      write.mockRestore()
      patchRequest?.socket.destroy()
      await patchRequest?.closed
    }

    const headResponse = await waitForTusUploadRemoval(location!, authorization)
    expect([404, 410]).toContain(headResponse.status)
    expect(headResponse.headers.get('upload-offset')).toBeNull()
  })

  it.runIf(backendType === 's3')(
    'preserves a complete fixed-length PATCH when lock contention cancels its context',
    async () => {
      const uploadLength = 2 * 1024 * 1024
      const patchLength = 1024 * 1024
      const objectName = `${randomUUID()}-contended-complete.txt`

      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const authorization = `Bearer ${await context.config.serviceKeyAsync}`
      const createResponse = await createTusUpload(
        context,
        authorization,
        {
          bucketName,
          objectName,
          contentType: 'text/plain',
          cacheControl: '3600',
        },
        uploadLength
      )

      expect(createResponse.status).toBe(201)
      const location = createResponse.headers.get('location')
      expect(location).toBeTruthy()

      const originalWrite = context.TusS3Store.prototype.write
      let isFirstWrite = true
      let signalFirstWriteCompleted: (() => void) | undefined
      const firstWriteCompleted = new Promise<void>((resolve) => {
        signalFirstWriteCompleted = resolve
      })
      let releaseFirstWrite: (() => void) | undefined
      const firstWriteRelease = new Promise<void>((resolve) => {
        releaseFirstWrite = resolve
      })
      const write = vi
        .spyOn(context.TusS3Store.prototype, 'write')
        .mockImplementation(async function (this: StorageTusS3Store, readable, id, offset) {
          const newOffset = await originalWrite.call(this, readable, id, offset)
          if (isFirstWrite) {
            isFirstWrite = false
            signalFirstWriteCompleted?.()
            await firstWriteRelease
          }
          return newOffset
        })

      let firstResponsePromise: Promise<Response> | undefined
      try {
        firstResponsePromise = patchTusUpload(
          location!,
          authorization,
          0,
          Buffer.alloc(patchLength, 0x61)
        )

        await withTimeout(
          firstWriteCompleted,
          5000,
          'first TUS PATCH did not complete its datastore write'
        )

        const competingResponse = await patchTusUpload(
          location!,
          authorization,
          0,
          Buffer.from('b')
        )

        expect(competingResponse.status).toBe(503)

        releaseFirstWrite?.()
        const firstResponse = await firstResponsePromise
        expect(firstResponse.status).toBe(204)
        expect(firstResponse.headers.get('upload-offset')).toBe(String(patchLength))
      } finally {
        releaseFirstWrite?.()
        await firstResponsePromise?.catch(() => undefined)
        write.mockRestore()
      }

      const resumeResponse = await patchTusUpload(
        location!,
        authorization,
        patchLength,
        Buffer.from('b')
      )

      expect(resumeResponse.status).toBe(204)
      expect(resumeResponse.headers.get('upload-offset')).toBe(String(patchLength + 1))
    },
    20_000
  )

  it.runIf(backendType === 's3')(
    'invalidates an upload when a part write fails after attempting an S3 mutation',
    async () => {
      const uploadLength = 2 * 1024 * 1024
      const patchLength = 1024 * 1024
      const objectName = `${randomUUID()}-failed-part.txt`

      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const authorization = `Bearer ${await context.config.serviceKeyAsync}`
      const createResponse = await createTusUpload(
        context,
        authorization,
        {
          bucketName,
          objectName,
          contentType: 'text/plain',
          cacheControl: '3600',
        },
        uploadLength
      )

      expect(createResponse.status).toBe(201)
      const location = createResponse.headers.get('location')
      expect(location).toBeTruthy()

      const injectedFailure = new Error('injected part upload failure')
      const prototype = Object.getPrototypeOf(context.TusS3Store.prototype) as {
        uploadPart(...args: unknown[]): Promise<string>
        uploadIncompletePart(...args: unknown[]): Promise<string>
      }
      const uploadPart = vi.spyOn(prototype, 'uploadPart').mockRejectedValue(injectedFailure)
      const uploadIncompletePart = vi
        .spyOn(prototype, 'uploadIncompletePart')
        .mockRejectedValue(injectedFailure)

      try {
        const patchResponse = await patchTusUpload(
          location!,
          authorization,
          0,
          Buffer.alloc(patchLength, 0x61)
        )

        expect(patchResponse.status).toBe(500)
      } finally {
        uploadPart.mockRestore()
        uploadIncompletePart.mockRestore()
      }

      const headResponse = await waitForTusUploadRemoval(location!, authorization)
      expect([404, 410]).toContain(headResponse.status)
    }
  )

  it.runIf(backendType === 's3')(
    'invalidates a completed multipart upload through the AWS NoSuchUpload path',
    async () => {
      const uploadLength = 512 * 1024
      const objectName = `${randomUUID()}-completed-invalidation.txt`

      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const authorization = `Bearer ${await context.config.serviceKeyAsync}`
      const createResponse = await createTusUpload(
        context,
        authorization,
        {
          bucketName,
          objectName,
          contentType: 'text/plain',
          cacheControl: '3600',
        },
        uploadLength
      )

      expect(createResponse.status).toBe(201)
      const location = createResponse.headers.get('location')
      expect(location).toBeTruthy()

      const client = context.backend.client
      if (!(client instanceof S3Client)) {
        throw new Error('Expected S3 client for s3 backend')
      }

      const uploadId = getTusDatastoreUploadId(context.config, location!)
      const injectedFailure = new Error('injected post-completion cache failure')
      const upstreamPrototype = context.TusS3Store.prototype as unknown as {
        clearCache(id: string): Promise<void>
      }
      const originalClearCache = upstreamPrototype.clearCache
      const originalRemove = context.TusS3Store.prototype.remove
      let completedObjectObserved = false
      let abortMultipartUpload: ReturnType<typeof mockAwsCompletedMultipartAbort> | undefined

      const clearCache = vi
        .spyOn(upstreamPrototype, 'clearCache')
        .mockImplementationOnce(async (id) => {
          await client.send(
            new HeadObjectCommand({
              Bucket: context.config.storageS3Bucket,
              Key: id,
            })
          )
          completedObjectObserved = true
          await client.send(
            new PutObjectCommand({
              Bucket: context.config.storageS3Bucket,
              Key: `${id}.part`,
              Body: Buffer.from('staged incomplete tail'),
            })
          )
          throw injectedFailure
        })
        .mockImplementation(function (this: typeof upstreamPrototype, id) {
          return originalClearCache.call(this, id)
        })

      const remove = vi
        .spyOn(context.TusS3Store.prototype, 'remove')
        .mockImplementation(async function (this: StorageTusS3Store, id) {
          abortMultipartUpload = mockAwsCompletedMultipartAbort(this)
          return originalRemove.call(this, id)
        })

      try {
        const patchResponse = await patchTusUpload(
          location!,
          authorization,
          0,
          Buffer.alloc(uploadLength, 0x61)
        )

        expect(patchResponse.status).toBe(500)
        expect(completedObjectObserved).toBe(true)
        expect(remove).toHaveBeenCalledOnce()
        expect(remove).toHaveBeenCalledWith(uploadId)
        expect(abortMultipartUpload).toHaveBeenCalledOnce()
        expect(abortMultipartUpload).toHaveBeenCalledWith({
          Bucket: context.config.storageS3Bucket,
          Key: uploadId,
          UploadId: expect.any(String),
        })
      } finally {
        remove.mockRestore()
        abortMultipartUpload?.mockRestore()
        clearCache.mockRestore()
      }

      const headResponse = await waitForTusUploadRemoval(location!, authorization)
      expect([404, 410]).toContain(headResponse.status)

      const remaining = await waitForS3TusPrefixRemoval(
        client,
        context.config.storageS3Bucket,
        uploadId
      )
      expect(remaining).toEqual({ objects: [], uploads: [] })
    }
  )

  it.runIf(backendType === 's3')('invalidates a truncated creation-with-upload POST', async () => {
    const objectName = `${randomUUID()}-truncated-creation.txt`
    const uploadLength = 2 * 1024 * 1024

    await storage.createBucket({
      id: bucketName,
      name: bucketName,
      public: true,
    })

    const authorization = `Bearer ${await context.config.serviceKeyAsync}`
    const client = context.backend.client
    if (!(client instanceof S3Client)) {
      throw new Error('Expected S3 client for s3 backend')
    }

    const uploadPrefix = `${context.config.tenantId}/${bucketName}/${objectName}/`
    const { write, writeStarted } = observeNextTusWrite(context.TusS3Store)
    let startedArtifacts: S3TusPrefixArtifacts = { objects: [], uploads: [] }

    try {
      startedArtifacts = await abortTusCreation(
        `${context.baseUrl}${context.config.tusPath}`,
        authorization,
        {
          bucketName,
          objectName,
          contentType: 'text/plain',
          cacheControl: '3600',
        },
        uploadLength,
        1024 * 1024,
        Buffer.alloc(512 * 1024, 0x61),
        async () => {
          const artifacts = await waitForS3TusPrefixCreation(
            client,
            context.config.storageS3Bucket,
            uploadPrefix
          )
          await withTimeout(
            writeStarted,
            5000,
            'truncated creation POST did not enter the datastore write'
          )
          return artifacts
        }
      )
    } finally {
      write.mockRestore()
    }
    expect(startedArtifacts.objects.length + startedArtifacts.uploads.length).toBeGreaterThan(0)

    const remaining = await waitForS3TusPrefixRemoval(
      client,
      context.config.storageS3Bucket,
      uploadPrefix
    )
    expect(remaining).toEqual({ objects: [], uploads: [] })
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
            metadata: JSON.stringify({
              test1: 'test1',
              test3: 'test3',
            }),
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
        bucket_id: bucket.id,
        created_at: expect.any(Date),
        id: expect.any(String),
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
        bucket_id: bucket.id,
        created_at: expect.any(Date),
        id: expect.any(String),
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
