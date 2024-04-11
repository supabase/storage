import fastifyPlugin from 'fastify-plugin'
import { createResponse } from '../generic-routes'
import { verifyJWT } from '../../auth'
import { getJwtSecret } from '../../database'
import { JwtPayload } from 'jsonwebtoken'

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
      request.log.error({ error: err }, 'unable to get owner')
      return reply.status(400).send(createResponse(err.message, '400', err.message))
    }
  })
})
