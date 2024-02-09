import fastifyPlugin from 'fastify-plugin'
import { createResponse } from '../generic-routes'
import { getOwner } from '../../auth'
import { getJwtSecret } from '../../database/tenant'

declare module 'fastify' {
  interface FastifyRequest {
    jwt: string
    owner?: string
  }
}

const BEARER = /^Bearer\s+/i

export const jwt = fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('jwt', '')
  fastify.addHook('preHandler', async (request, reply) => {
    request.jwt = (request.headers.authorization || '').replace(BEARER, '')

    const { secret, jwks } = await getJwtSecret(request.tenantId)

    try {
      const owner = await getOwner(request.jwt, secret, jwks || null)
      request.owner = owner
    } catch (err: any) {
      request.log.error({ error: err }, 'unable to get owner')
      return reply.status(400).send(createResponse(err.message, '400', err.message))
    }
  })
})
