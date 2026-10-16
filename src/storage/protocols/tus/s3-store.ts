import { Readable } from 'node:stream'
import { type MetadataValue, type Options, S3Store as TusS3Store } from '@tus/s3-store'
import { TUS_RESUMABLE, type Upload } from '@tus/server'

export class S3Store extends TusS3Store {
  constructor(options: Options) {
    super(options)
    this.client.middlewareStack.remove('loggerMiddleware')
  }

  protected async uploadPart(
    metadata: MetadataValue,
    readStream: Readable | Buffer,
    partNumber: number
  ) {
    try {
      return await super.uploadPart(metadata, readStream, partNumber)
    } finally {
      // SDK checksum wrappers do not propagate destruction to the store's part reader.
      if (readStream instanceof Readable) readStream.destroy()
    }
  }

  protected async uploadIncompletePart(id: string, readStream: Readable) {
    try {
      return await super.uploadIncompletePart(id, readStream)
    } finally {
      readStream.destroy()
    }
  }

  async create(upload: Upload): Promise<Upload> {
    if (!upload.metadata?.contentEncoding) {
      return super.create(upload)
    }

    // Upstream's create() forwards ContentType and CacheControl but omits ContentEncoding.
    upload.creation_date = new Date().toISOString()

    const result = await this.client.createMultipartUpload({
      Bucket: this.bucket,
      Key: upload.id,
      Metadata: { 'tus-version': TUS_RESUMABLE },
      ContentType: upload.metadata.contentType || undefined,
      CacheControl: upload.metadata.cacheControl || undefined,
      ContentEncoding: upload.metadata.contentEncoding,
    })

    upload.storage = { type: 's3', path: result.Key as string, bucket: this.bucket }
    await this.saveMetadata(upload, result.UploadId as string)
    return upload
  }
}
