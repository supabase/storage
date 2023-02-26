import { S3Store as BaseS3Store } from '@tus/s3-store'
import aws from 'aws-sdk'

interface Options {
  partSize?: number
  s3ClientConfig: aws.S3.Types.ClientConfiguration & {
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
}
