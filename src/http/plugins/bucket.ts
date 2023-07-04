import fastifyPlugin from 'fastify-plugin'
import { RouteGenericInterface } from 'fastify/types/route'
import { BucketWithCredentials } from '../../storage/schemas'
import { StorageBackendError } from '../../storage'

declare module 'fastify' {
  interface FastifyRequest<RouteGeneric extends RouteGenericInterface = RouteGenericInterface> {
    bucket: BucketWithCredentials
  }

  interface FastifyContextConfig {
    getParentBucketId?: ((request: FastifyRequest<any>) => string) | false
  }
}

export const parentBucket = fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('bucket', null)
  fastify.addHook('preHandler', async (request) => {
    if (typeof request.routeConfig.getParentBucketId === 'undefined') {
      throw new Error(
        `getParentBucketId not defined in route ${request.routerPath} ${request.routerPath} config`
      )
    }

    if (request.routeConfig.getParentBucketId === false) {
      return
    }

    const bucketId = request.routeConfig.getParentBucketId(request)

    if (!bucketId) {
      throw new StorageBackendError('invalid_bucket', 400, 'bucket name is invalid or not provided')
    }

    const bucket = await request.db.asSuperUser().findBucketById(bucketId, '*', {
      includeCredentials: true,
    })

    request.bucket = bucket
  })
})
