import { FastifyInstance } from 'fastify'
import { postgrest, superUserPostgrest } from '../../plugins/postgrest'
import renderPublicImage from './renderPublicImage'
import renderAuthenticatedImage from './renderAuthenticatedImage'
import jwt from '../../plugins/jwt'

export default async function routes(fastify: FastifyInstance) {
  fastify.register(async function authorizationContext(fastify) {
    fastify.register(jwt)
    fastify.register(postgrest)

    fastify.register(renderAuthenticatedImage)
  })

  fastify.register(async (fastify) => {
    fastify.register(superUserPostgrest)

    fastify.register(renderPublicImage)
  })
}
