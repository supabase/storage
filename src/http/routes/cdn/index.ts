import { FastifyInstance } from 'fastify'
import { db, jwt, requireTenantFeature, storage } from '../../plugins'
import purgeCache from './purgeCache'

export default async function routes(fastify: FastifyInstance) {
  fastify.register(async function authenticated(fastify) {
    fastify.register(jwt)
    fastify.register(db)
    fastify.register(storage)
    fastify.register(requireTenantFeature('purgeCache'))

    fastify.register(purgeCache)
  })
}
