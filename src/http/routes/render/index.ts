import { FastifyInstance } from 'fastify'
import { getConfig } from '../../../config'
import { db, dbSuperUser, jwt, requireTenantFeature, storage } from '../../plugins'
import { rateLimiter } from './rate-limiter'
import renderAuthenticatedImage from './renderAuthenticatedImage'
import renderPublicImage from './renderPublicImage'
import renderSignedImage from './renderSignedImage'

const { imageTransformationEnabled, rateLimiterEnabled } = getConfig()

export default async function routes(fastify: FastifyInstance) {
  if (!imageTransformationEnabled) {
    return
  }

  fastify.register(async function authorizationContext(fastify) {
    fastify.register(requireTenantFeature('imageTransformation'))

    if (rateLimiterEnabled) {
      fastify.register(rateLimiter)
    }

    fastify.register(jwt)
    fastify.register(db)
    fastify.register(storage)

    fastify.register(renderAuthenticatedImage)
  })

  fastify.register(async (fastify) => {
    fastify.register(requireTenantFeature('imageTransformation'))

    if (rateLimiterEnabled) {
      fastify.register(rateLimiter)
    }

    fastify.register(dbSuperUser)
    fastify.register(storage)

    fastify.register(renderSignedImage)
    fastify.register(renderPublicImage)
  })
}
