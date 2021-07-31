import { FastifyInstance } from 'fastify'
import jwt from '../../plugins/jwt'
import { postgrest, superUserPostgrest } from '../../plugins/postgrest'
import createBucket from './createBucket'
import deleteBucket from './deleteBucket'
import emptyBucket from './emptyBucket'
import getAllBuckets from './getAllBuckets'
import getBucket from './getBucket'
import updateBucket from './updateBucket'

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  fastify.register(jwt)
  fastify.register(postgrest)

  fastify.register(createBucket)
  fastify.register(emptyBucket)
  fastify.register(getAllBuckets)
  fastify.register(getBucket)
  fastify.register(updateBucket)

  fastify.register(async (fastify) => {
    fastify.register(superUserPostgrest)

    fastify.register(deleteBucket)
  })
}
