import { StorageDisk } from './adapter'
import { FileDisk } from './file'
import { createS3Client, S3Disk, S3DiskOptions } from './s3'
import { getConfig, StorageBackendType } from '../../config'

export * from './s3'
export * from './file'
export * from './adapter'

const {
  storageFilePath,
  storageS3Bucket,
  storageS3Region,
  storageS3Endpoint,
  storageS3ForcePathStyle,
  storageS3ClientTimeout,
} = getConfig()

export function createDefaultDisk<Type extends StorageBackendType>(
  type: Type,
  opts?: { name: string }
) {
  let storageBackend: StorageDisk

  switch (type) {
    case 'file':
      if (!storageFilePath) {
        throw new Error('storageFilePath is required for file storage disk')
      }
      storageBackend = new FileDisk({
        mountPoint: storageFilePath,
      })
      break
    case 's3':
      const defaultOptions: S3DiskOptions = {
        client: createS3Client({
          name: opts?.name || 's3_default',
          region: storageS3Region,
          endpoint: storageS3Endpoint,
          forcePathStyle: storageS3ForcePathStyle,
          requestTimeout: storageS3ClientTimeout,
        }),
        mountPoint: storageS3Bucket,
      }
      storageBackend = new S3Disk(defaultOptions)
      break
    default:
      throw new Error(`Unsupported storage backend type: ${type}`)
  }

  return storageBackend
}
