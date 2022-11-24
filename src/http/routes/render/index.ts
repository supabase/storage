import { FastifyInstance } from 'fastify'
import renderPublicImage from './renderPublicImage'
import renderAuthenticatedImage from './renderAuthenticatedImage'
import renderSignedImage from './renderSignedImage'
import { jwt, postgrest, superUserPostgrest, storage } from '../../plugins'
import { getConfig } from '../../../config'

const { enableImageTransformation } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  if (!enableImageTransformation) {
    return
  }

  fastify.register(async function authorizationContext(fastify) {
    fastify.register(jwt)
    fastify.register(postgrest)
    fastify.register(superUserPostgrest)
    fastify.register(storage)
    fastify.register(renderAuthenticatedImage)
  })

  fastify.register(async (fastify) => {
    fastify.register(superUserPostgrest)
    fastify.register(storage)
    fastify.register(renderSignedImage)
    fastify.register(renderPublicImage)
  })
}
