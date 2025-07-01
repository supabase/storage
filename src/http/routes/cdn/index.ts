import { FastifyInstance } from 'fastify'
import { db, enforceJwtRole, jwt, requireTenantFeature, storage } from '../../plugins'
import purgeCache from './purgeCache'
import { getConfig } from '../../../config'

const { dbServiceRole } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  fastify.register(async function authenticated(fastify) {
    fastify.register(jwt)
    fastify.register(enforceJwtRole, {
      roles: [dbServiceRole],
    })
    fastify.register(db)
    fastify.register(storage)
    fastify.register(requireTenantFeature('purgeCache'))

    fastify.register(purgeCache)
  })
}
