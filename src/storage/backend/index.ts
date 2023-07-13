import { FileBackend } from './file'
import { getClient, S3Backend, S3Options } from './s3'
import { getConfig } from '../../config'
import { getTenantBackendProvider } from '../../database/tenant'

export * from './s3'
export * from './file'
export * from './generic'

const { storageBackendType } = getConfig()

export async function createStorageBackend(tenantId: string, options?: S3Options) {
  switch (storageBackendType) {
    case 'file':
      return new FileBackend()
    case 's3':
      const provider = await getTenantBackendProvider(tenantId)
      const s3Options = options || ({} as S3Options)
      return new S3Backend({
        ...s3Options,
        client: s3Options.client ?? getClient(provider),
      })
    default:
      throw new Error(`unknown storage backend type ${storageBackendType}`)
  }
}
