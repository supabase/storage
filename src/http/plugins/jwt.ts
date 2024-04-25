import fastifyPlugin from 'fastify-plugin'
import { verifyJWT } from '../../auth'
import { getJwtSecret } from '../../database'
import { JwtPayload } from 'jsonwebtoken'
import { ERRORS } from '../../storage'

declare module 'fastify' {
  interface FastifyRequest {
    jwt: string
    jwtPayload?: JwtPayload & { role?: string }
    owner?: string
  }
}

const BEARER = /^Bearer\s+/i

export const jwt = fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('jwt', '')
  fastify.decorateRequest('jwtPayload', undefined)

  fastify.addHook('preHandler', async (request, reply) => {
    request.jwt = (request.headers.authorization || '').replace(BEARER, '')

    const { secret, jwks } = await getJwtSecret(request.tenantId)

    try {
      const payload = await verifyJWT(request.jwt, secret, jwks || null)
      request.jwtPayload = payload
      request.owner = payload.sub
    } catch (err: any) {
      throw ERRORS.AccessDenied(err.message, err)
    }
  })
})
