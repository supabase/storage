import { FastifyRequest } from 'fastify'
import { getFileSizeLimit } from './limits'
import { StorageBackendAdapter } from './backend'
import { getConfig } from '../config'
import { StorageBackendError } from './errors'

interface UploaderOptions {
  key: string
}

const { globalS3Bucket } = getConfig()

/**
 * Uploader
 * Handles the upload of a multi-part request or binary body
 */
export class Uploader {
  constructor(private readonly backend: StorageBackendAdapter) {}

  /**
   * Extracts file information from the request and upload it them
   * to the remote storage if valid
   * @param request
   * @param options
   */
  async upload(request: FastifyRequest, options: UploaderOptions) {
    const file = await this.incomingFileInfo(request)

    const objectMetadata = await this.backend.uploadObject(
      globalS3Bucket,
      options.key,
      file.body,
      file.mimeType,
      file.cacheControl
    )

    if (file.isTruncated()) {
      throw new StorageBackendError(
        'Payload too large',
        413,
        'The object exceeded the maximum allowed size'
      )
    }

    return objectMetadata
  }

  protected async incomingFileInfo(request: FastifyRequest) {
    const contentType = request.headers['content-type']
    const fileSizeLimit = await getFileSizeLimit(request.tenantId)

    let body: NodeJS.ReadableStream
    let mimeType: string
    let isTruncated: () => boolean

    let cacheControl: string
    if (contentType?.startsWith('multipart/form-data')) {
      const formData = await request.file({ limits: { fileSize: fileSizeLimit } })
      // https://github.com/fastify/fastify-multipart/issues/162
      /* @ts-expect-error: https://github.com/aws/aws-sdk-js-v3/issues/2085 */
      const cacheTime = formData.fields.cacheControl?.value

      body = formData.file
      mimeType = formData.mimetype
      cacheControl = cacheTime ? `max-age=${cacheTime}` : 'no-cache'
      isTruncated = () => formData.file.truncated
    } else {
      // just assume its a binary file
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
