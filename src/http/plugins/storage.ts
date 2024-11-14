import fastifyPlugin from 'fastify-plugin'
import { StorageDisk, createDefaultDisk } from '@storage/disks'
import { Storage } from '@storage/storage'
import { StorageKnexDB } from '@storage/database'
import { getConfig } from '../../config'

declare module 'fastify' {
  interface FastifyRequest {
    storage: Storage
    disk: StorageDisk
  }
}

const { storageBackendType } = getConfig()

export const storage = fastifyPlugin(
  async function storagePlugin(fastify) {
    const defaultDisk = createDefaultDisk(storageBackendType)

    fastify.addHook('preHandler', async (request) => {
      const database = new StorageKnexDB(request.db, {
        tenantId: request.tenantId,
        host: request.headers['x-forwarded-host'] as string,
        reqId: request.id,
        latestMigration: request.latestMigration,
      })
      request.disk = defaultDisk.withPrefix(request.tenantId)
      request.storage = new Storage(request.disk, database)
    })

    fastify.addHook('onClose', async () => {
      defaultDisk.close()
    })
  },
  { name: 'storage-init' }
)
