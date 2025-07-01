import { FastifyInstance } from 'fastify'
import { db, enforceJwtRole, icebergRestCatalog, jwt, storage } from '../../plugins'
import catalogue from './catalog'
import namespace from './namespace'
import table from './table'
import { setErrorHandler } from '../../error-handler'
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
    fastify.register(icebergRestCatalog)
    fastify.register(catalogue)
    fastify.register(namespace)
    fastify.register(table)

    setErrorHandler(fastify, {
      respectStatusCode: true,
      formatter: (e) => {
        return {
          error: {
            message: e.message,
            type: e.code,
            code: parseInt(e.statusCode, 10),
          },
        }
      },
    })
  })
}
