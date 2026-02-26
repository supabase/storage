import { createStorageBackend, StorageBackendAdapter } from '@storage/backend'
import { CdnCacheManager } from '@storage/cdn/cdn-cache-manager'
import { StorageKnexDB } from '@storage/database'
import { PassThroughLocation, TenantLocation } from '@storage/locator'
import { Storage } from '@storage/storage'
import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../../config'

declare module 'fastify' {
  interface FastifyRequest {
    storage: Storage
    backend: StorageBackendAdapter
    cdnCache: CdnCacheManager
  }
}

const { storageBackendType, storageS3Bucket } = getConfig()

const storageBackend = createStorageBackend(storageBackendType)

export const storage = fastifyPlugin(
  async function storagePlugin(fastify) {
    fastify.decorateRequest('storage', null)
    fastify.addHook('preHandler', async (request) => {
      const database = new StorageKnexDB(request.db, {
        tenantId: request.tenantId,
        host: request.headers['x-forwarded-host'] as string,
        reqId: request.id,
        latestMigration: request.latestMigration,
      })

      const location = request.isIcebergBucket
        ? new PassThroughLocation(request.internalIcebergBucketName!)
        : new TenantLocation(storageS3Bucket)

      request.backend = storageBackend
      request.storage = new Storage(storageBackend, database, location)
      request.cdnCache = new CdnCacheManager(request.storage)
    })

    fastify.addHook('onClose', async () => {
      storageBackend.close()
    })
  },
  { name: 'storage-init' }
)
