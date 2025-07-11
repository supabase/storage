import { FastifyInstance } from 'fastify'
import { db, jwt, requireTenantFeature, storage } from '../../plugins'
import purgeCache from './purgeCache'
import { getConfig } from '../../../config'

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
