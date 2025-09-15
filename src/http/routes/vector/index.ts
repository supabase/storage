import { FastifyInstance } from 'fastify'
import { db, jwt, s3vector } from '../../plugins'
import { getConfig } from '../../../config'

import createVectorIndex from './create-index'

const { dbServiceRole } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  fastify.register(async function authenticated(fastify) {
    fastify.register(jwt, {
      enforceJwtRoles: [dbServiceRole],
    })
    // fastify.register(requireTenantFeature('icebergCatalog'))

    fastify.register(db)
    fastify.register(s3vector)
    fastify.register(createVectorIndex)
  })
}
