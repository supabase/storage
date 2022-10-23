import fastifyPlugin from 'fastify-plugin'
import { FileBackend, S3Backend, GenericStorageBackend } from '../../storage/backend'
import { Storage } from '../../storage'
import { getConfig } from '../../config'
import { Database } from '../../storage/database'

declare module 'fastify' {
  interface FastifyRequest {
    storage: Storage
    backend: GenericStorageBackend
  }
}

const { region, globalS3Endpoint, storageBackendType } = getConfig()

export const storage = fastifyPlugin(async (fastify) => {
  let storageBackend: GenericStorageBackend

  if (storageBackendType === 'file') {
    storageBackend = new FileBackend()
  } else {
    storageBackend = new S3Backend(region, globalS3Endpoint)
  }

  fastify.decorateRequest('storage', undefined)
  fastify.addHook('preHandler', async (request) => {
    const database = new Database(request.tenantId, request.postgrest, request.superUserPostgrest)
    request.backend = storageBackend
    request.storage = new Storage(storageBackend, database)
  })
})
