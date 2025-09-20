import { StorageBackendAdapter } from './adapter'
import { FileBackend } from './file'
import { S3Backend, S3ClientOptions } from './s3/adapter'
import { getConfig, StorageBackendType } from '../../config'
import { S3Client } from '@aws-sdk/client-s3'

export * from './s3'
export * from './file'
export * from './adapter'

const { storageS3Region, storageS3Endpoint, storageS3ForcePathStyle, storageS3ClientTimeout } =
  getConfig()

type ConfigForStorage<Type extends StorageBackendType> = Type extends 's3'
  ? S3ClientOptions
  : undefined

type BackendAdapterForType<Type extends StorageBackendType> = Type extends 's3'
  ? StorageBackendAdapter<S3Client>
  : StorageBackendAdapter

export function createStorageBackend<Type extends StorageBackendType>(
  type: Type,
  config?: ConfigForStorage<Type>
) {
  let storageBackend: BackendAdapterForType<Type>

  if (type === 'file') {
    storageBackend = new FileBackend() as BackendAdapterForType<Type>
  } else {
    const defaultOptions: S3ClientOptions = {
      region: storageS3Region,
      endpoint: storageS3Endpoint,
      forcePathStyle: storageS3ForcePathStyle,
      requestTimeout: storageS3ClientTimeout,
      ...(config ? config : {}),
    }
    storageBackend = new S3Backend(defaultOptions)
  }

  return storageBackend
}
