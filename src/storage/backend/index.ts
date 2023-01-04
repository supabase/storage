import { StorageBackendAdapter } from './generic'
import { FileBackend } from './file'
import { S3Backend } from './s3'
import { getConfig } from '../../config'

export * from './s3'
export * from './file'
export * from './generic'

const { region, globalS3Endpoint, globalS3ForcePathStyle, storageBackendType } = getConfig()

export function createStorageBackend() {
  let storageBackend: StorageBackendAdapter

  if (storageBackendType === 'file') {
    storageBackend = new FileBackend()
  } else {
    storageBackend = new S3Backend(region, globalS3Endpoint, globalS3ForcePathStyle)
  }

  return storageBackend
}
