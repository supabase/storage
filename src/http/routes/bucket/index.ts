import { FastifyInstance } from 'fastify'
import createBucket from './createBucket'
import deleteBucket from './deleteBucket'
import emptyBucket from './emptyBucket'
import getAllBuckets from './getAllBuckets'
import getBucket from './getBucket'
import updateBucket from './updateBucket'
import { postgrest, superUserPostgrest, storage, jwt } from '../../plugins'

export default async function routes(fastify: FastifyInstance) {
  fastify.register(jwt)
  fastify.register(postgrest, superUserPostgrest)
  fastify.register(storage)

  fastify.register(createBucket)
  fastify.register(emptyBucket)
  fastify.register(getAllBuckets)
  fastify.register(getBucket)
  fastify.register(updateBucket)
  fastify.register(deleteBucket)
}
