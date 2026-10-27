import { createStorageBackend, StorageBackendAdapter } from '@storage/backend'
import type { Database } from '@storage/database'
import { StoragePgDB } from '@storage/database'
import { PassThroughLocation, TenantLocation } from '@storage/locator'
import { Storage } from '@storage/storage'
import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../../config'

declare module 'fastify' {
  interface FastifyInstance {
    storageBackendOwner: StorageBackendOwner
  }

  interface FastifyRequest {
    storage: Storage
    backend: StorageBackendAdapter
  }
}

const { storageBackendType, storageS3Bucket } = getConfig()

/** Owns the backend shared by the HTTP apps belonging to one server. */
export class StorageBackendOwner {
  private backend?: StorageBackendAdapter

  get() {
    this.backend ??= createStorageBackend(storageBackendType)
    return this.backend
  }

  async close() {
    const backend = this.backend
    this.backend = undefined
    await backend?.close()
  }
}

// TenantLocation is immutable so singleton not to allocate per request.
const tenantLocation = new TenantLocation(storageS3Bucket)

export const storage = fastifyPlugin(
  async function storagePlugin(fastify) {
    const backend = fastify.storageBackendOwner.get()
    fastify.decorateRequest('storage')
    fastify.addHook('preHandler', (request, _reply, done) => {
      const databaseOptions = {
        tenantId: request.tenantId,
        host: request.headers['x-forwarded-host'] as string,
        reqId: request.id,
        sbReqId: request.sbReqId,
        latestMigration: request.latestMigration,
      }

      const database: Database = new StoragePgDB(request.db, databaseOptions)

      const location = request.isIcebergBucket
        ? new PassThroughLocation(request.internalIcebergBucketName!)
        : tenantLocation

      request.backend = backend
      request.storage = new Storage(backend, database, location)
      done()
    })
  },
  { name: 'storage-init', decorators: { fastify: ['storageBackendOwner'] } }
)
