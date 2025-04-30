import fastifyPlugin from 'fastify-plugin'
import { JWTPayload } from 'jose'

import { verifyJWTWithCache, verifyJWT } from '@internal/auth'
import { getJwtSecret } from '@internal/database'
import { ERRORS } from '@internal/errors'
import { getConfig } from '../../config'

declare module 'fastify' {
  interface FastifyRequest {
    isAuthenticated: boolean
    jwt: string
    jwtPayload?: JWTPayload & { role?: string }
    owner?: string
  }

  interface FastifyContextConfig {
    allowInvalidJwt?: boolean
  }
}

const { jwtCachingEnabled } = getConfig()

const BEARER = /^Bearer\s+/i

export const jwt = fastifyPlugin(
  async (fastify) => {
    fastify.decorateRequest('jwt', '')
    fastify.decorateRequest('jwtPayload', undefined)

    fastify.addHook('preHandler', async (request) => {
      request.jwt = (request.headers.authorization || '').replace(BEARER, '')

      if (!request.jwt && request.routeOptions.config.allowInvalidJwt) {
        request.jwtPayload = { role: 'anon' }
        request.isAuthenticated = false
        return
      }

      const { secret, jwks } = await getJwtSecret(request.tenantId)

      try {
        const payload = await (jwtCachingEnabled
          ? verifyJWTWithCache(request.jwt, secret, jwks || null)
          : verifyJWT(request.jwt, secret, jwks || null))

        request.jwtPayload = payload
        request.owner = payload.sub
        request.isAuthenticated = true
      } catch (e) {
        request.jwtPayload = { role: 'anon' }
        request.isAuthenticated = false

        if (request.routeOptions.config.allowInvalidJwt) {
          return
        }
        const err = e as Error
        throw ERRORS.AccessDenied(err.message, err)
      }
    })
  },
  { name: 'auth-jwt' }
)
