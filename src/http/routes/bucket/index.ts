import { FastifyInstance } from 'fastify'
import createBucket from './createBucket'
import deleteBucket from './deleteBucket'
import emptyBucket from './emptyBucket'
import getAllBuckets from './getAllBuckets'
import getBucket from './getBucket'
import updateBucket from './updateBucket'
import { storage, jwt, db } from '../../plugins'

export default async function routes(fastify: FastifyInstance) {
  fastify.register(jwt)
  fastify.register(db)
  fastify.register(storage)

  fastify.register(createBucket)
  fastify.register(emptyBucket)
  fastify.register(getAllBuckets)
  fastify.register(getBucket)
  fastify.register(updateBucket)
  fastify.register(deleteBucket)
}
