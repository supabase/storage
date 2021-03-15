import { FastifyInstance } from 'fastify'
import createBucket from './createBucket'
import deleteBucket from './deleteBucket'
import emptyBucket from './emptyBucket'
import getAllBuckets from './getAllBuckets'
import getBucket from './getBucket'

// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default async function routes(fastify: FastifyInstance) {
  createBucket(fastify)
  deleteBucket(fastify)
  emptyBucket(fastify)
  getAllBuckets(fastify)
  getBucket(fastify)
}
