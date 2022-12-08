import { StorageBackendAdapter } from './generic'
import { FileBackend } from './file'
import { OSSBackend } from "./oss";
import { S3Backend } from './s3'
import { getConfig } from '../../config'

export * from './s3'
export * from './file'
export * from './generic'

const {
  region,
  globalS3Endpoint,
  globalOSSEndpoint,
  ossAccessKey,
  ossAccessSecret,
  storageBackendType,
} = getConfig()

export function createStorageBackend() {
  let storageBackend: StorageBackendAdapter

  if (storageBackendType === 'file') {
    storageBackend = new FileBackend()
  } else if (storageBackendType === 's3') {
    storageBackend = new S3Backend(region, globalS3Endpoint)
  } else {
    storageBackend = new OSSBackend(globalOSSEndpoint, ossAccessKey, ossAccessSecret)
  }

  return storageBackend
}
