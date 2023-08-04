import http from 'http'
import { isRenderableError, Storage } from '../../../storage'
import { Metadata, Upload } from '@tus/server'
import { randomUUID } from 'crypto'
import { UploadId } from './upload-id'
import { Uploader } from '../../../storage/uploader'
import { TenantConnection } from '../../../database/connection'
import { BucketWithCredentials } from '../../../storage/schemas'

export type MultiPartRequest = http.IncomingMessage & {
  upload: {
    storage: Storage
    db: TenantConnection
    owner?: string
    tenantId: string
    isNew: boolean
    bucket: BucketWithCredentials
  }
}

export function namingFunction(rawReq: http.IncomingMessage) {
  const req = rawReq as MultiPartRequest

  if (!req.url) {
    throw new Error('no url set')
  }

  const metadataHeader = req.headers['upload-metadata']

  if (typeof metadataHeader !== 'string') {
    throw new Error('no metadata')
  }

  try {
    const params = Metadata.parse(metadataHeader)

    const version = randomUUID()

    const uploadId = new UploadId({
      tenant: req.upload.tenantId,
      bucket: params.bucketName || '',
      objectName: params.objectName || '',
      version,
    }).toString()

    if (req.upload.bucket?.credential_id) {
      return uploadId.split('/').slice(2).join('/')
    }

    return uploadId
  } catch (e) {
    throw e
  }
}

export async function onCreate(
  rawReq: http.IncomingMessage,
  res: http.ServerResponse,
  upload: Upload
): Promise<http.ServerResponse> {
  try {
    const req = rawReq as MultiPartRequest

    const id = req.upload.bucket.credential_id
      ? `${req.upload.tenantId}/${req.upload.bucket.id}/${upload.id}`
      : upload.id

    const uploadID = UploadId.fromString(id)

    const isUpsert = req.headers['x-upsert'] === 'true'
    const storage = req.upload.storage

    const bucket = await req.upload.bucket

    const uploader = new Uploader(storage.backend, storage.db)

    await uploader.prepareUpload({
      id: uploadID.version,
      owner: req.upload.owner,
      bucketId: uploadID.bucket,
      objectName: uploadID.objectName,
      isUpsert,
      isMultipart: true,
    })

    if (upload.metadata && /^-?\d+$/.test(upload.metadata.cacheControl || '')) {
      upload.metadata.cacheControl = `max-age=${upload.metadata.cacheControl}`
    } else if (upload.metadata) {
      upload.metadata.cacheControl = 'no-cache'
    }

    if (upload.metadata?.contentType && bucket.allowed_mime_types) {
      uploader.validateMimeType(upload.metadata.contentType, bucket.allowed_mime_types)
    }

    return res
  } catch (e) {
    if (isRenderableError(e)) {
      ;(e as any).status_code = parseInt(e.render().statusCode, 10)
      ;(e as any).body = e.render().message
    }
    throw e
  }
}

export async function onUploadFinish(
  rawReq: http.IncomingMessage,
  res: http.ServerResponse,
  upload: Upload
) {
  const req = rawReq as MultiPartRequest
  const id = req.upload.bucket.credential_id
    ? `${req.upload.tenantId}/${req.upload.bucket.id}/${upload.id}`
    : upload.id

  const resourceId = UploadId.fromString(id)
  const isUpsert = req.headers['x-upsert'] === 'true'

  try {
    const s3Key = req.upload.storage
      .from(req.upload.bucket)
      .computeObjectPath(resourceId.objectName)

    const metadata = await req.upload.storage.backend.headObject(s3Key, resourceId.version)

    const uploader = new Uploader(req.upload.storage.backend, req.upload.storage.db)

    await uploader.completeUpload({
      id: resourceId.version,
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
