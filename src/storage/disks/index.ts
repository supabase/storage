import { FileSystemDisk } from './file'
import { S3Disk, S3DiskOptions } from './s3'
import { getConfig, StorageDiskType } from '../../config'

export * from './s3'
export * from './file'
export * from './disk'

const {
  storageFilePath,
  storageBackendType,
  storageS3Region,
  storageS3Endpoint,
  storageS3ForcePathStyle,
  storageS3Bucket,
} = getConfig()

type ConfigForStorage<Type extends StorageDiskType> = Type extends 's3' ? S3DiskOptions : never

export function createDisk<Type extends StorageDiskType>(
  type: Type,
  config: ConfigForStorage<Type>
) {
  switch (type) {
    case 'file':
      if (!storageFilePath) throw new Error('FILE_STORAGE_BACKEND_PATH is not set')
      return new FileSystemDisk({
        mountPath: storageFilePath,
      })
    case 's3':
      return new S3Disk(config)
    default:
      throw new Error(`Unknown storage disk type: ${type}`)
  }
}

type ConfigForDefaultStorage<Type extends StorageDiskType> = Type extends 's3'
  ? Pick<S3DiskOptions, 'httpAgent' | 'prefix'>
  : never

export function createDefaultDisk(options?: ConfigForDefaultStorage<StorageDiskType>) {
  return createDisk(storageBackendType, {
    ...(options || {}),
    bucket: storageS3Bucket,
    region: storageS3Region,
    endpoint: storageS3Endpoint,
    forcePathStyle: storageS3ForcePathStyle,
  })
}
