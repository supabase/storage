import { SIGNED_URL_SCOPE_UPLOAD } from '@internal/auth'
import type { TenantConnection } from '@internal/database'
import { ERRORS, ErrorCode, isRenderableError, StorageBackendError } from '@internal/errors'
import { logSchema, RequestLogContext } from '@internal/monitoring'
import { UploadId } from '@storage/protocols/tus'
import { Storage } from '@storage/storage'
import { Uploader } from '@storage/uploader'
import { validateContentEncoding } from '@storage/validators/content-encoding'
import { detectMimeFromStoredObject, mimeFamiliesMatch } from '@storage/validators/file-signature'
import { validateMimeType } from '@storage/validators/mime-type'
import { DataStore, Metadata, Upload } from '@tus/server'
import { randomUUID } from 'crypto'
import type { FastifyBaseLogger } from 'fastify'
import http from 'http'
import type { ServerRequest as Request } from 'srvx'

import { getConfig } from '../../../config'

const { storageS3Bucket, tusPath, requestAllowXForwardedPrefix, storagePublicUrl } = getConfig()
const parsedPublicUrl = storagePublicUrl ? new URL(storagePublicUrl) : undefined
const reExtractFileID = /([^/]+)\/?$/

export const SIGNED_URL_SUFFIX = '/sign'

/**
 * Validates that the request object exists and returns it
 * @throws {Error} If the request object is missing
 */
function getNodeRequest(rawReq: Request): MultiPartRequest {
  const req = rawReq.runtime?.node?.req as MultiPartRequest

  if (!req) {
    throw ERRORS.InternalError(undefined, 'Request object is missing')
  }

  return req
}

export type MultiPartRequest = http.IncomingMessage & {
  executionError?: Error
  log: FastifyBaseLogger
  upload: RequestLogContext & {
    tenantId: string
    storage: Storage
    db: TenantConnection
    owner?: string
    isUpsert: boolean
    isSigned: boolean
    signatureVerified?: boolean
    resources?: string[]
  }
}

type TusError = { status_code: number; body: string }

function getTusError(error: { render(): { statusCode: string; message: string } }): TusError {
  const renderedError = error.render()
  return {
    status_code: parseInt(renderedError.statusCode, 10),
    body: renderedError.message,
  }
}

export async function verifySignedUploadRequest(req: MultiPartRequest, uploadID: UploadId) {
  if (!req.upload.isSigned) {
    return false
  }

  if (req.upload.signatureVerified) {
    return true
  }

  const signature = req.headers['x-signature']
  if (!signature || typeof signature !== 'string') {
    throw ERRORS.InvalidSignature('Missing x-signature header')
  }

  const payload = await req.upload.storage
    .from(uploadID.bucket)
    .verifyObjectSignature(signature, uploadID.objectName, SIGNED_URL_SCOPE_UPLOAD)

  req.upload.owner = payload.owner
  req.upload.isUpsert = payload.upsert
  req.upload.signatureVerified = true
  return true
}

/**
 * Runs on every TUS incoming request
 */
export async function onIncomingRequest(rawReq: Request, id: string, datastore: DataStore) {
  const req = getNodeRequest(rawReq)
  const uploadID = UploadId.fromString(id)

  req.upload.resources = [`${uploadID.bucket}/${uploadID.objectName}`]

  if (await verifySignedUploadRequest(req, uploadID)) {
    return
  }

  // Options and HEAD request don't need to be authorized
  if (rawReq.method === 'OPTIONS' || req.method === 'HEAD') {
    return
  }

  // All other requests need to be authorized if they have permission to upload
  const isUpsert = req.upload.isUpsert
  const uploader = new Uploader(
    req.upload.storage.backend,
    req.upload.storage.db,
    req.upload.storage.location
  )

  let contentType: string | undefined
  let contentEncoding: string | undefined
  let contentLength: number | undefined
  let rawMetadata: string | null | undefined

  if (req.method === 'POST') {
    const uploadMetadataHeader = req.headers['upload-metadata']
    if (uploadMetadataHeader && typeof uploadMetadataHeader === 'string') {
      try {
        const parsedMetadata = Metadata.parse(uploadMetadataHeader)
        contentType = parsedMetadata?.contentType ?? undefined
        contentEncoding = parsedMetadata?.contentEncoding ?? undefined
        rawMetadata = parsedMetadata?.metadata
      } catch (e) {
        logSchema.warning(req.log, 'Failed to parse upload metadata', {
          type: 'tus',
          tenantId: req.upload.tenantId,
          project: req.upload.tenantId,
          reqId: req.upload.reqId,
          sbReqId: req.upload.sbReqId,
          error: e,
        })
        throw ERRORS.InvalidParameter('upload-metadata', {
          error: e as Error,
          message: 'Invalid Upload-Metadata header',
        })
      }
    }
    const uploadLength = req.headers['upload-length']
    contentLength = uploadLength ? Number(uploadLength) : undefined
    contentEncoding = validateContentEncoding(contentEncoding)
  } else {
    const upload = await datastore.getUpload(id)
    contentType = upload.metadata?.contentType ?? undefined
    contentEncoding = upload.metadata?.contentEncoding ?? undefined
    contentLength = upload.size ?? undefined
    rawMetadata = upload.metadata?.metadata
  }

  let customMd: Record<string, string> | undefined
  if (rawMetadata) {
    try {
      customMd = JSON.parse(rawMetadata)
    } catch (e) {
      logSchema.warning(req.log, 'Failed to parse user metadata', {
        type: 'tus',
        tenantId: req.upload.tenantId,
        project: req.upload.tenantId,
        reqId: req.upload.reqId,
        sbReqId: req.upload.sbReqId,
        error: ERRORS.InvalidRequest('Failed to parse metadata', e as Error),
      })
    }
  }

  await uploader.canUpload({
    owner: req.upload.owner,
    bucketId: uploadID.bucket,
    objectName: uploadID.objectName,
    isUpsert,
    userMetadata: customMd,
    metadata: {
      mimetype: contentType,
      contentLength,
      contentEncoding,
    },
  })
}

/**
 * Generate URL for TUS upload, it encodes the uploadID to base64url
 */
export function generateUrl(
  rawReq: Request,
  { proto, host, path, id }: { proto: string; host: string; path: string; id: string }
) {
  const req = getNodeRequest(rawReq)

  if (!req.url) {
    throw ERRORS.InvalidParameter('url')
  }

  if (parsedPublicUrl) {
    proto = parsedPublicUrl.protocol.replace(':', '')
    host = parsedPublicUrl.host
  }

  // Force https in production. This overrides both forwarded headers and
  // STORAGE_PUBLIC_URL - production deployments must use HTTPS.
  proto = process.env.NODE_ENV === 'production' ? 'https' : proto

  let basePath = path

  const forwardedPath = req.headers['x-forwarded-prefix']
  if (requestAllowXForwardedPrefix && typeof forwardedPath === 'string') {
    // Remove trailing slash from forwardedPath to avoid double slashes
    basePath = forwardedPath.replace(/\/+$/, '') + path
  }

  const fullPath = req.upload.isSigned ? `${basePath}${SIGNED_URL_SUFFIX}` : basePath

  if (!parsedPublicUrl && req.headers['x-forwarded-host']) {
    const port = req.headers['x-forwarded-port']

    if (typeof port === 'string' && port && !['443', '80'].includes(port)) {
      if (!host.includes(':')) {
        host += `:${req.headers['x-forwarded-port']}`
      } else {
        host = host.replace(/:\d+$/, `:${req.headers['x-forwarded-port']}`)
      }
    }
  }

  // remove the tenant-id from the url, since we'll be using the tenant-id from the request
  id = id.split('/').slice(1).join('/')
  id = Buffer.from(id, 'utf-8').toString('base64url')
  return `${proto}://${host}${fullPath}/${id}`
}

/**
 * Extract the uploadId from the request and decodes it from base64url
 */
export function getFileIdFromRequest(rawReq: Request) {
  const req = getNodeRequest(rawReq)

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
export function namingFunction(rawReq: Request, metadata?: Record<string, string | null>) {
  const req = getNodeRequest(rawReq)

  if (!req.url) {
    throw new Error('no url set')
  }

  if (!metadata) {
    throw ERRORS.MetadataRequired()
  }

  return new UploadId({
    tenant: req.upload.tenantId,
    bucket: metadata.bucketName || '',
    objectName: metadata.objectName || '',
    version: randomUUID(),
  }).toString()
}

/**
 * Runs before the upload URL is created
 */
export async function onCreate(
  rawReq: Request,
  upload: Upload
): Promise<{ metadata?: Upload['metadata'] }> {
  const uploadID = UploadId.fromString(upload.id)

  const req = getNodeRequest(rawReq)

  const storage = req.upload.storage

  const bucket = await storage
    .asSuperUser()
    .findBucket(uploadID.bucket, 'id, file_size_limit, allowed_mime_types')

  const metadata = {
    ...(upload.metadata ? upload.metadata : {}),
  }

  if (/^-?\d+$/.test(metadata.cacheControl || '')) {
    metadata.cacheControl = `max-age=${metadata.cacheControl}`
  } else {
    metadata.cacheControl = 'no-cache'
  }

  if (metadata?.contentType && bucket.allowed_mime_types?.length) {
    validateMimeType(metadata.contentType, bucket.allowed_mime_types)
  }

  const contentEncoding = validateContentEncoding(metadata.contentEncoding ?? undefined)
  if (contentEncoding) {
    metadata.contentEncoding = contentEncoding
  } else {
    delete metadata.contentEncoding
  }

  return { metadata }
}

/**
 * Runs when the upload to the underline store is completed
 */
export async function onUploadFinish(rawReq: Request, upload: Upload) {
  const req = getNodeRequest(rawReq)

  const resourceId = UploadId.fromString(upload.id)

  try {
    const s3Key = req.upload.storage.location.getKeyLocation({
      tenantId: req.upload.tenantId,
      bucketId: resourceId.bucket,
      objectName: resourceId.objectName,
    })
    const metadata = await req.upload.storage.backend.headObject(
      storageS3Bucket,
      s3Key,
      resourceId.version
    )

    // Magic-byte MIME verification for the TUS leg (issue #639).
    //
    // onCreate above only validates the client-declared contentType against
    // bucket.allowed_mime_types. Without this post-upload check, a caller
    // can declare `image/jpeg` on a JPEG-restricted bucket and stream GIF
    // bytes through TUS: the declared string passes onCreate, and
    // onUploadFinish commits the mismatched object. That matches the gap
    // the standard upload leg closes in `uploader.ts`, and TUS must mirror
    // it so the two legs cannot diverge.
    //
    // We peek the first SIGNATURE_PEEK_BYTES of the already-stored object
    // via a ranged read (no scan of the full upload), detect its MIME, and
    // reject when the detected family does not match the declared one. On
    // mismatch we also delete the stored bytes so the bucket does not retain
    // the policy-violating object.
    const declaredContentType = upload.metadata?.contentType
    if (declaredContentType) {
      const bucket = await req.upload.storage
        .asSuperUser()
        .findBucket(resourceId.bucket, 'id, allowed_mime_types')

      if (bucket.allowed_mime_types?.length) {
        const detected = await detectMimeFromStoredObject((range) =>
          req.upload.storage.backend.getObject(storageS3Bucket, s3Key, resourceId.version, {
            range,
          })
        )

        if (detected && !mimeFamiliesMatch(detected, declaredContentType)) {
          await req.upload.storage.backend
            .deleteObject(storageS3Bucket, s3Key, resourceId.version)
            .catch(() => {
              // Best-effort cleanup: we still reject the upload below. Logging
              // the backend delete failure to the request log keeps operators
              // informed without masking the real rejection reason.
              req.log?.warn(
                { s3Key, version: resourceId.version },
                'tus signature-mismatch cleanup: backend delete failed'
              )
            })
          throw ERRORS.InvalidMimeType(`${detected} (declared as ${declaredContentType})`)
        }
      }
    }

    const uploader = new Uploader(
      req.upload.storage.backend,
      req.upload.storage.db,
      req.upload.storage.location
    )
    let customMd: undefined | Record<string, string> = undefined
    if (upload.metadata?.metadata) {
      try {
        customMd = JSON.parse(upload.metadata.metadata)
      } catch {
        // no-op
      }
    }

    const { obj } = await uploader.completeUpload({
      version: resourceId.version,
      bucketId: resourceId.bucket,
      objectName: resourceId.objectName,
      objectMetadata: metadata,
      isUpsert: req.upload.isUpsert,
      uploadType: 'resumable',
      owner: req.upload.owner,
      userMetadata: customMd,
    })

    const headers: Record<string, string> = {
      'Tus-Complete': '1',
      'Supabase-Object-Version-Id': resourceId.version,
    }
    if (obj.id) {
      headers['Supabase-Object-Id'] = obj.id
    }

    return { headers }
  } catch (e) {
    if (isRenderableError(e)) {
      throw Object.assign(e, getTusError(e))
    }
    throw e
  }
}

export async function onUploadHead(
  rawReq: Request,
  response: { ok: boolean; headers: Pick<Headers, 'get' | 'set'> }
) {
  const length = response.headers.get('Upload-Length')
  if (!response.ok || length === null || response.headers.get('Upload-Offset') !== length) {
    return
  }

  const req = getNodeRequest(rawReq)
  const id = getFileIdFromRequest(rawReq)
  if (!id) {
    return
  }

  const resourceId = UploadId.fromString(id)
  const obj = await req.upload.storage.db
    .findObject(
      resourceId.bucket,
      resourceId.objectName,
      'id',
      { dontErrorOnEmpty: true, excludeDeleteMarkers: true },
      resourceId.version
    )
    .catch((error: unknown) => {
      if (error instanceof StorageBackendError && error.code === ErrorCode.AccessDenied) {
        return undefined
      }
      throw error
    })
  if (obj?.id) {
    response.headers.set('Supabase-Object-Id', obj.id)
    response.headers.set('Supabase-Object-Version-Id', resourceId.version)
  }
}

/**
 * Runs when there is an error on the TUS upload
 */
export function onResponseError(rawReq: Request, e: TusError | Error) {
  const req = getNodeRequest(rawReq)

  // Keep the original request error when cancellation causes a secondary failure.
  if (e instanceof Error) {
    req.executionError ??= e
  } else {
    req.executionError ??= ERRORS.TusError(e.body, e.status_code).withMetadata(e)
  }

  if (isRenderableError(e)) {
    return getTusError(e)
  }
}
