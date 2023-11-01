import { S3Store as BaseS3Store } from '@tus/s3-store'
import type { S3 } from 'aws-sdk'
import fs from 'node:fs'
import { Readable } from 'node:stream'
import { TUS_RESUMABLE, Upload } from '@tus/server'
import { S3UploadPart } from '../../../monitoring/metrics'
import { UploadId } from './upload-id'

interface Options {
  partSize?: number
  s3ClientConfig: S3.Types.ClientConfiguration & {
    bucket: string
  }
  uploadExpiryMilliseconds?: number
}

export class S3Store extends BaseS3Store {
  constructor(protected readonly options: Options) {
    super(options)
  }

  getExpiration(): number {
    return this.options.uploadExpiryMilliseconds || 0
  }

  /**
   * Saves upload metadata to a `${file_id}.info` file on S3.
   * Please note that the file is empty and the metadata is saved
   * on the S3 object's `Metadata` field, so that only a `headObject`
   * is necessary to retrieve the data.
   */
  protected async saveMetadata(upload: Upload, upload_id: string) {
    await this.client
      .putObject({
        Bucket: this.bucket,
        Key: `${upload.id}.info`,
        Body: '',
        Metadata: {
          file: JSON.stringify(upload),
          upload_id,
          tus_version: TUS_RESUMABLE,
          tus_completed: 'false',
        },
      })
      .promise()
  }

  protected async uploadPart(
    metadata: { file: Upload; upload_id: string; tus_version: string },
    readStream: fs.ReadStream | Readable,
    partNumber: number
  ): Promise<string> {
    const timer = S3UploadPart.startTimer()

    const result = await super.uploadPart(metadata, readStream, partNumber)

    timer()

    return result
  }

  protected async uploadIncompletePart(
    id: string,
    readStream: fs.ReadStream | Readable
  ): Promise<string> {
    const data = await this.client
      .putObject({
        Bucket: this.bucket,
        Key: id,
        Body: readStream,
        Tagging: 'tus_completed=false',
      })
      .promise()
    return data.ETag as string
  }
}
