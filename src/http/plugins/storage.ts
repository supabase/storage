import fastifyPlugin from 'fastify-plugin'
import { StorageBackendAdapter, createStorageBackend, S3Backend } from '../../storage/backend'
import { Storage } from '../../storage'
import { RouteGenericInterface } from 'fastify/types/route'
import { decrypt } from '../../auth'
import { BucketWithCredentials } from '../../storage/schemas'
import { getConfig } from '../../config'

declare module 'fastify' {
  interface FastifyRequest<RouteGeneric extends RouteGenericInterface = RouteGenericInterface> {
    storage: Storage
    backend: StorageBackendAdapter
  }
}

const { globalS3Bucket } = getConfig()

export const storage = fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('backend', null)
  fastify.decorateRequest('storage', null)
  fastify.addHook('preHandler', async (request) => {
    let storageBackend: StorageBackendAdapter | undefined = undefined

    const parentBucket: BucketWithCredentials | undefined = request.bucket

    if (parentBucket && parentBucket.credential_id) {
      storageBackend = new S3Backend({
        bucket: parentBucket.id,
        client: {
          role: parentBucket.role,
          endpoint: parentBucket.endpoint,
          region: parentBucket.region,
          forcePathStyle: parentBucket.force_path_style,
          accessKey: parentBucket.access_key ? decrypt(parentBucket.access_key) : undefined,
          secretKey: parentBucket.secret_key ? decrypt(parentBucket.secret_key) : undefined,
        },
      })
    } else {
      storageBackend = createStorageBackend({
        prefix: request.tenantId,
        bucket: globalS3Bucket,
      })
    }

    request.backend = storageBackend
    request.storage = new Storage(request.backend, request.db)
  })
})
