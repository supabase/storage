import { FastifyInstance } from 'fastify'
import renderPublicImage from './renderPublicImage'
import renderAuthenticatedImage from './renderAuthenticatedImage'
import { jwt } from '../../plugins/jwt'
import { postgrest, superUserPostgrest } from '../../plugins/postgrest'
import { storage } from '../../plugins/storage'

export default async function routes(fastify: FastifyInstance) {
  fastify.register(async function authorizationContext(fastify) {
    fastify.register(jwt)
    fastify.register(postgrest, superUserPostgrest)
    fastify.register(storage)
    fastify.register(renderAuthenticatedImage)
  })

  fastify.register(async (fastify) => {
    fastify.register(superUserPostgrest)
    fastify.register(storage)
    fastify.register(renderPublicImage)
  })
}
