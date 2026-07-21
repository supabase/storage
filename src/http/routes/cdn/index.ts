import { FastifyInstance } from 'fastify'
import { getConfig } from '../../../config'
import { registerJwtAuth, requireTenantFeature } from '../../plugins'
import purgeCache from './purgeCache'

const { dbServiceRole } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  fastify.register(async function authenticated(fastify) {
    registerJwtAuth(fastify, {
      enforceJwtRoles: [dbServiceRole],
    })
    fastify.register(requireTenantFeature('purgeCache'))

    fastify.register(purgeCache)
  })
}
