import { FastifyInstance } from 'fastify'
import { getConfig } from '../../../config'
import { db, registerJwtAuth, requireTenantFeature, storage } from '../../plugins'
import purgeCache from './purgeCache'

const { dbServiceRole } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  fastify.register(async function authenticated(fastify) {
    registerJwtAuth(fastify, {
      enforceJwtRoles: [dbServiceRole],
    })
    fastify.register(requireTenantFeature('purgeCache'))

    fastify.register(db)
    fastify.register(storage)

    fastify.register(purgeCache)
  })
}
