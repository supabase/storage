import { FastifyInstance } from 'fastify'
import renderPublicImage from './renderPublicImage'
import renderAuthenticatedImage from './renderAuthenticatedImage'
import renderSignedImage from './renderSignedImage'
import { jwt, storage, requireTenantFeature, db, dbSuperUser } from '../../plugins'
import { getConfig } from '../../../config'
import { rateLimiter } from './rate-limiter'

const { enableImageTransformation, enableRateLimiter } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  if (!enableImageTransformation) {
    return
  }

  fastify.register(async function authorizationContext(fastify) {
    fastify.register(requireTenantFeature('imageTransformation'))

    if (enableRateLimiter) {
      fastify.register(rateLimiter)
    }

    fastify.register(jwt)
    fastify.register(db)
    fastify.register(storage)
    fastify.register(renderAuthenticatedImage)
  })

  fastify.register(async (fastify) => {
    fastify.register(requireTenantFeature('imageTransformation'))

    if (enableRateLimiter) {
      fastify.register(rateLimiter)
    }

    fastify.register(dbSuperUser)
    fastify.register(storage)
    fastify.register(renderSignedImage)
    fastify.register(renderPublicImage)
  })
}
