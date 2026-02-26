import { FastifyInstance } from 'fastify'
import { getConfig } from '../../../config'
import { db, jwt, requireTenantFeature, storage } from '../../plugins'
import purgeCache from './purgeCache'

const { dbServiceRole } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  fastify.register(async function authenticated(fastify) {
    fastify.register(jwt, {
      enforceJwtRoles: [dbServiceRole],
    })
    fastify.register(requireTenantFeature('purgeCache'))

    fastify.register(db)
    fastify.register(storage)

    fastify.register(purgeCache)
  })
}
