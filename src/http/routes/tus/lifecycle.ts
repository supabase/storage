import http from 'http'
import { BaseLogger } from 'pino'
import { Upload } from '@tus/server'
import { randomUUID } from 'crypto'
import { isRenderableError, Storage, StorageBackendError } from '../../../storage'
import { getConfig } from '../../../config'
import { Uploader } from '../../../storage/uploader'
import { TenantConnection } from '../../../database/connection'
import { UploadId } from '../../../storage/tus'

const { storageS3Bucket, tusPath } = getConfig()
const reExtractFileID = /([^/]+)\/?$/

export type MultiPartRequest = http.IncomingMessage & {
  log: BaseLogger
  upload: {
    storage: Storage
    db: TenantConnection
    owner?: string
    tenantId: string
    isNew: boolean
  }
}

/**
 * Runs on every TUS incoming request
 */
export async function onIncomingRequest(
  rawReq: http.IncomingMessage,
  res: http.ServerResponse,
  id: string
) {
  if (rawReq.method === 'OPTIONS') {
    return
  }

  const req = rawReq as MultiPartRequest
  const uploadID = UploadId.fromString(id)

  res.on('finish', () => {
    req.upload.db.dispose().catch((e) => {
      req.log.error({ error: e }, 'Error disposing db connection')
    })
  })

  const isUpsert = req.headers['x-upsert'] === 'true'

  const uploader = new Uploader(req.upload.storage.backend, req.upload.storage.db)
  await uploader.canUpload({
    owner: req.upload.owner,
    bucketId: uploadID.bucket,
    objectName: uploadID.objectName,
    isUpsert,
  })
}

/**
 * Generate URL for TUS upload, it encodes the uploadID to base64url
 */
export function generateUrl(
  _: http.IncomingMessage,
  { proto, host, path, id }: { proto: string; host: string; path: string; id: string }
) {
  proto = process.env.NODE_ENV === 'production' ? 'https' : proto

  // remove the tenant-id from the url, since we'll be using the tenant-id from the request
  id = id.split('/').slice(1).join('/')
  id = Buffer.from(id, 'utf-8').toString('base64url')
  return `${proto}://${host}${path}/${id}`
}

/**
 * Extract the uploadId from the request and decodes it from base64url
 */
export function getFileIdFromRequest(rawRwq: http.IncomingMessage) {
  const req = rawRwq as MultiPartRequest
  const match = reExtractFileID.exec(req.url as string)

  if (!match || tusPath.includes(match[1])) {
    return
  }

  const idMatch = Buffer.from(match[1], 'base64url').toString('utf-8')
  return req.upload.tenantId + '/' + idMatch
}

/**
 * Generate the uploadId for the TUS upload
 * the URL structure is as follows:
 *
 * /tenant-id/bucket-name/object-name/version
 */
export function namingFunction(
  rawReq: http.IncomingMessage,
  metadata?: Record<string, string | null>
) {
  const req = rawReq as MultiPartRequest

  if (!req.url) {
    throw new Error('no url set')
  }

  if (!metadata) {
    throw new StorageBackendError('metadata_header_invalid', 400, 'metadata header invalid')
  }

  try {
    const version = randomUUID()

    return new UploadId({
      tenant: req.upload.tenantId,
      bucket: metadata.bucketName || '',
      objectName: metadata.objectName || '',
      version,
    }).toString()
  } catch (e) {
    throw e
  }
}

/**
 * Runs before the upload URL is created
 */
export async function onCreate(
  rawReq: http.IncomingMessage,
  res: http.ServerResponse,
  upload: Upload
): Promise<http.ServerResponse> {
  const uploadID = UploadId.fromString(upload.id)

  const req = rawReq as MultiPartRequest
  const storage = req.upload.storage

  const bucket = await storage
    .asSuperUser()
    .findBucket(uploadID.bucket, 'id, file_size_limit, allowed_mime_types')

  const uploader = new Uploader(storage.backend, storage.db)

  if (upload.metadata && /^-?\d+$/.test(upload.metadata.cacheControl || '')) {
    upload.metadata.cacheControl = `max-age=${upload.metadata.cacheControl}`
  } else if (upload.metadata) {
    upload.metadata.cacheControl = 'no-cache'
  }

  if (upload.metadata?.contentType && bucket.allowed_mime_types) {
    uploader.validateMimeType(upload.metadata.contentType, bucket.allowed_mime_types)
  }

  return res
}

/**
 * Runs when the upload to the underline store is completed
 */
export async function onUploadFinish(
  rawReq: http.IncomingMessage,
  res: http.ServerResponse,
  upload: Upload
) {
  const req = rawReq as MultiPartRequest
  const resourceId = UploadId.fromString(upload.id)
  const isUpsert = req.headers['x-upsert'] === 'true'

  try {
    const s3Key = `${req.upload.tenantId}/${resourceId.bucket}/${resourceId.objectName}`
    const metadata = await req.upload.storage.backend.headObject(
      storageS3Bucket,
      s3Key,
      resourceId.version
    )

    const uploader = new Uploader(req.upload.storage.backend, req.upload.storage.db)

    await uploader.completeUpload({
      version: resourceId.version,
      bucketId: resourceId.bucket,
      objectName: resourceId.objectName,
      objectMetadata: metadata,
      isUpsert: isUpsert,
      isMultipart: true,
      owner: req.upload.owner,
    })

    return res
  } catch (e) {
    if (isRenderableError(e)) {
      ;(e as any).status_code = parseInt(e.render().statusCode, 10)
      ;(e as any).body = e.render().message
    }
    throw e
  }
}

type TusError = { status_code: number; body: string }

/**
 * Runs when there is an error on the TUS upload
 */
export function onResponseError(
  req: http.IncomingMessage,
  res: http.ServerResponse,
  e: TusError | Error
) {
  if (e instanceof Error) {
    ;(res as any).executionError = e
  }

  if (isRenderableError(e)) {
    return {
      status_code: parseInt(e.render().statusCode, 10),
      body: e.render().message,
    }
  }
}
