import { FileBackend, FileBackendOptions } from './file'
import { getS3DefaultClient, S3Backend, S3Options } from './s3'
import { getConfig } from '../../config'

export * from './s3'
export * from './file'
export * from './generic'

const { globalS3Bucket, storageBackendType } = getConfig()

export function createStorageBackend(options: S3Options | FileBackendOptions) {
  switch (storageBackendType) {
    case 'file':
      return new FileBackend({
        prefix: options.prefix,
        bucket: globalS3Bucket,
      })
    case 's3':
      const s3Options = options as S3Options
      return new S3Backend({
        ...s3Options,
        client: s3Options.client ?? getS3DefaultClient(),
      })
    default:
      throw new Error(`unknown storage backend type ${storageBackendType}`)
  }
}
