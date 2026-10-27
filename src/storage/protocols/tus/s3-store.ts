import { type Options, S3Store as TusS3Store } from '@tus/s3-store'
import { TUS_RESUMABLE, type Upload } from '@tus/server'

export class S3Store extends TusS3Store {
  constructor(options: Options) {
    super(options)
    this.client.middlewareStack.remove('loggerMiddleware')
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
