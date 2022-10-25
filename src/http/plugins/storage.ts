import fastifyPlugin from 'fastify-plugin'
import { GenericStorageBackend, createStorageBackend } from '../../storage/backend'
import { Storage } from '../../storage'
import { Database } from '../../storage/database'

declare module 'fastify' {
  interface FastifyRequest {
    storage: Storage
    backend: GenericStorageBackend
  }
}

export const storage = fastifyPlugin(async (fastify) => {
  const storageBackend = createStorageBackend()

  fastify.decorateRequest('storage', undefined)
  fastify.addHook('preHandler', async (request) => {
    const database = new Database(request.postgrest, {
      tenantId: request.tenantId,
      host: request.headers['x-forwarded-host'] as string,
      superAdmin: request.superUserPostgrest,
    })
    request.backend = storageBackend
    request.storage = new Storage(storageBackend, database)
  })
})
