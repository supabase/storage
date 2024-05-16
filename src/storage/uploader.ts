import { FastifyRequest } from 'fastify'
import { getFileSizeLimit } from './limits'
import { ObjectMetadata, StorageBackendAdapter } from './backend'
import { getConfig } from '../config'
import { ERRORS } from './errors'
import { Database } from './database'
import { ObjectAdminDelete, ObjectCreatedPostEvent, ObjectCreatedPutEvent } from '../queue'
import { randomUUID } from 'crypto'
import { FileUploadedSuccess, FileUploadStarted } from '../monitoring/metrics'

interface UploaderOptions extends UploadObjectOptions {
  fileSizeLimit?: number | null
  allowedMimeTypes?: string[] | null
}

const { storageS3Bucket, uploadFileSizeLimitStandard } = getConfig()

export interface UploadObjectOptions {
  bucketId: string
  objectName: string
  owner?: string
  isUpsert?: boolean
  uploadType?: 'standard' | 's3' | 'resumable'
}

/**
 * Uploader
 * Handles the upload of a multi-part request or binary body
 */
export class Uploader {
  constructor(private readonly backend: StorageBackendAdapter, private readonly db: Database) {}

  async canUpload(
    options: Pick<UploadObjectOptions, 'bucketId' | 'objectName' | 'isUpsert' | 'owner'>
  ) {
    const shouldCreateObject = !options.isUpsert

    if (shouldCreateObject) {
      await this.db.testPermission((db) => {
        return db.createObject({
          bucket_id: options.bucketId,
          name: options.objectName,
          version: '1',
          owner: options.owner,
        })
      })
    } else {
      await this.db.testPermission((db) => {
        return db.upsertObject({
          bucket_id: options.bucketId,
          name: options.objectName,
          version: '1',
          owner: options.owner,
        })
      })
    }
  }

  /**
   * Returns the upload version for the incoming file.
   * We check RLS policies before proceeding
   * @param options
   */
  async prepareUpload(options: UploadObjectOptions) {
    await this.canUpload(options)
    FileUploadStarted.inc({
      is_multipart: Boolean(options.uploadType).toString(),
    })

    return randomUUID()
  }

  /**
   * Extracts file information from the request and upload the buffer
   * to the remote storage
   * @param request
   * @param options
   */
  async upload(request: FastifyRequest, options: UploaderOptions) {
    const version = await this.prepareUpload(options)

    try {
      const file = await this.incomingFileInfo(request, options)

      if (options.allowedMimeTypes) {
        this.validateMimeType(file.mimeType, options.allowedMimeTypes)
      }

      const path = `${options.bucketId}/${options.objectName}`
      const s3Key = `${this.db.tenantId}/${path}`

      const objectMetadata = await this.backend.uploadObject(
        storageS3Bucket,
        s3Key,
        version,
        file.body,
        file.mimeType,
        file.cacheControl
      )

      if (file.isTruncated()) {
        throw ERRORS.EntityTooLarge()
      }

      return this.completeUpload({
        ...options,
        version,
        objectMetadata: objectMetadata,
      })
    } catch (e) {
      await ObjectAdminDelete.send({
        name: options.objectName,
        bucketId: options.bucketId,
        tenant: this.db.tenant(),
        version: version,
        reqId: this.db.reqId,
      })
      throw e
    }
  }

  async completeUpload({
    version,
    bucketId,
    objectName,
    owner,
    objectMetadata,
    uploadType,
    isUpsert,
  }: UploadObjectOptions & {
    objectMetadata: ObjectMetadata
    version: string
    emitEvent?: boolean
    uploadType?: 'standard' | 's3' | 'resumable'
  }) {
    try {
      return await this.db.withTransaction(async (db) => {
        await db.waitObjectLock(bucketId, objectName)

        const currentObj = await db
          .asSuperUser()
          .findObject(bucketId, objectName, 'id, version, metadata', {
            forUpdate: true,
            dontErrorOnEmpty: true,
          })

        const isNew = !Boolean(currentObj)

        // update object
        const newObject = await db.asSuperUser().upsertObject({
          bucket_id: bucketId,
          name: objectName,
          metadata: objectMetadata,
          version,
          owner,
        })

        const events: Promise<unknown>[] = []

        // schedule the deletion of the previous file
        if (currentObj && currentObj.version !== version) {
          events.push(
            ObjectAdminDelete.send({
              name: objectName,
              bucketId: bucketId,
              tenant: this.db.tenant(),
              version: currentObj.version,
              reqId: this.db.reqId,
            })
          )
        }

        const event = isUpsert && !isNew ? ObjectCreatedPutEvent : ObjectCreatedPostEvent

        events.push(
          event.sendWebhook({
            tenant: this.db.tenant(),
            name: objectName,
            bucketId: bucketId,
            metadata: objectMetadata,
            reqId: this.db.reqId,
            uploadType,
          })
        )

        await Promise.all(events)

        FileUploadedSuccess.inc({
          is_multipart: uploadType === 'resumable' ? 1 : 0,
          is_resumable: uploadType === 'resumable' ? 1 : 0,
          is_standard: uploadType === 'standard' ? 1 : 0,
          is_s3: uploadType === 's3' ? 1 : 0,
        })

        return { obj: newObject, isNew, metadata: objectMetadata }
      })
    } catch (e) {
      await ObjectAdminDelete.send({
        name: objectName,
        bucketId: bucketId,
        tenant: this.db.tenant(),
        version,
        reqId: this.db.reqId,
      })
      throw e
    }
  }

  validateMimeType(mimeType: string, allowedMimeTypes: string[]) {
    const requestedMime = mimeType.split('/')

    if (requestedMime.length < 2) {
      throw ERRORS.InvalidMimeType(mimeType)
    }

    const [type, ext] = requestedMime

    for (const allowedMimeType of allowedMimeTypes) {
      const allowedMime = allowedMimeType.split('/')

      if (requestedMime.length < 2) {
        continue
      }

      const [allowedType, allowedExtension] = allowedMime

      if (allowedType === type && allowedExtension === '*') {
        return true
      }

      if (allowedType === type && allowedExtension === ext) {
        return true
      }
    }

    throw ERRORS.InvalidMimeType(mimeType)
  }

  protected async incomingFileInfo(
    request: FastifyRequest,
    options?: Pick<UploaderOptions, 'fileSizeLimit'>
  ) {
    const contentType = request.headers['content-type']
    const fileSizeLimit = await getStandardMaxFileSizeLimit(
      this.db.tenantId,
      options?.fileSizeLimit
    )

    let body: NodeJS.ReadableStream
    let mimeType: string
    let isTruncated: () => boolean

    let cacheControl: string
    if (contentType?.startsWith('multipart/form-data')) {
      try {
        const formData = await request.file({ limits: { fileSize: fileSizeLimit } })

        if (!formData) {
          throw ERRORS.NoContentProvided()
        }

        // https://github.com/fastify/fastify-multipart/issues/162
        /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
        const cacheTime = formData.fields.cacheControl?.value

        body = formData.file
        /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
        mimeType = formData.fields.contentType?.value || formData.mimetype
        cacheControl = cacheTime ? `max-age=${cacheTime}` : 'no-cache'
        isTruncated = () => formData.file.truncated
      } catch (e) {
        throw ERRORS.NoContentProvided(e as Error)
      }
    } else {
      // just assume it's a binary file
      body = request.raw
      mimeType = request.headers['content-type'] || 'application/octet-stream'
      cacheControl = request.headers['cache-control'] ?? 'no-cache'
      isTruncated = () => {
        // @todo more secure to get this from the stream or from s3 in the next step
        return Number(request.headers['content-length']) > fileSizeLimit
      }
    }

    return {
      body,
      mimeType,
      cacheControl,
      isTruncated,
    }
  }
}

export async function getStandardMaxFileSizeLimit(
  tenantId: string,
  bucketSizeLimit?: number | null
) {
  let globalFileSizeLimit = await getFileSizeLimit(tenantId)

  if (typeof bucketSizeLimit === 'number') {
    globalFileSizeLimit = Math.min(bucketSizeLimit, globalFileSizeLimit)
  }

  if (uploadFileSizeLimitStandard && uploadFileSizeLimitStandard > 0) {
    globalFileSizeLimit = Math.min(uploadFileSizeLimitStandard, globalFileSizeLimit)
  }

  return globalFileSizeLimit
}
