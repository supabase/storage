import { createStorageBackend, StorageBackendAdapter } from '@storage/backend'
import type { Database } from '@storage/database'
import { StoragePgDB } from '@storage/database'
import { PassThroughLocation, TenantLocation } from '@storage/locator'
import { Storage } from '@storage/storage'
import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../../config'

declare module 'fastify' {
  interface FastifyRequest {
    storage: Storage
    backend: StorageBackendAdapter
  }
}

const { storageBackendType, storageS3Bucket } = getConfig()

let storageBackend: StorageBackendAdapter | undefined

/** Close the process-wide backend after all HTTP apps have stopped. */
export async function closeStorageBackend() {
  const backend = storageBackend
  storageBackend = undefined
  await backend?.close()
}

// TenantLocation is immutable so singleton not to allocate per request.
const tenantLocation = new TenantLocation(storageS3Bucket)

export const storage = fastifyPlugin(
  async function storagePlugin(fastify) {
    storageBackend ??= createStorageBackend(storageBackendType)
    const backend = storageBackend
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
  { name: 'storage-init' }
)
