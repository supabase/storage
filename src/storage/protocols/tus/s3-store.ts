import { type Options, S3Store as TusS3Store } from '@tus/s3-store'
import type { Upload } from '@tus/server'

export class S3Store extends TusS3Store {
  constructor(options: Options) {
    super(options)
    this.client.middlewareStack.remove('loggerMiddleware')
  }

  /**
   * TUS treats `Upload-Length: 0` as immediately final and calls `onUploadFinish`
   * without `write()`. Upstream `S3Store.create` only starts a multipart upload,
   * so `HeadObject` in our finish hook 404s and clients see a bare Not Found.
   *
   * Complete the empty multipart here (upstream `finishMultipartUpload` already
   * uploads a zero-byte part when `parts` is empty) so the object exists before
   * finish runs.
   */
  async create(upload: Upload): Promise<Upload> {
    const created = await super.create(upload)

    if (upload.size === 0 && !upload.sizeIsDeferred) {
      const metadata = await this.getMetadata(upload.id)
      await this.finishMultipartUpload(metadata, [])
      await this.completeMetadata(metadata.file)
      await this.clearCache(upload.id)
    }

    return created
  }
}
