import { StorageBackendAdapter } from './generic'
import { FileBackend } from './file'
import { S3Backend, S3ClientOptions } from './s3'
import { getConfig, StorageBackendType } from '../../config'

export * from './s3'
export * from './file'
export * from './generic'

const { region, globalS3Endpoint, globalS3ForcePathStyle } = getConfig()

type ConfigForStorage<Type extends StorageBackendType> = Type extends 's3'
  ? S3ClientOptions
  : undefined

export function createStorageBackend<Type extends StorageBackendType>(
  type: Type,
  config?: ConfigForStorage<Type>
) {
  let storageBackend: StorageBackendAdapter

  if (type === 'file') {
    storageBackend = new FileBackend()
  } else {
    const defaultOptions: S3ClientOptions = {
      region: region,
      endpoint: globalS3Endpoint,
      forcePathStyle: globalS3ForcePathStyle,
      ...(config ? config : {}),
    }
    storageBackend = new S3Backend(defaultOptions)
  }

  return storageBackend
}
