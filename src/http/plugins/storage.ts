import fastifyPlugin from 'fastify-plugin'
import { StorageBackendAdapter, createStorageBackend } from '@storage/backend'
import { Storage } from '@storage/storage'
import { StorageKnexDB } from '@storage/database'
import { getConfig } from '../../config'

declare module 'fastify' {
  interface FastifyRequest {
    storage: Storage
    backend: StorageBackendAdapter
  }
}

const { storageBackendType } = getConfig()

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
      request.backend = storageBackend
      request.storage = new Storage(storageBackend, database)
    })

    fastify.addHook('onClose', async () => {
      storageBackend.close()
    })
  },
  { name: 'storage-init' }
)
