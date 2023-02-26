import fastifyPlugin from 'fastify-plugin'
import { createResponse } from '../generic-routes'
import { getJwtSecret, getOwner } from '../../auth'

declare module 'fastify' {
  interface FastifyRequest {
    jwt: string
    owner?: string
  }
}

export const jwt = fastifyPlugin(async (fastify) => {
  fastify.decorateRequest('jwt', '')
  fastify.addHook('preHandler', async (request, reply) => {
    request.jwt = (request.headers.authorization || '').substring('Bearer '.length)

    const jwtSecret = await getJwtSecret(request.tenantId)
    try {
      const owner = await getOwner(request.jwt, jwtSecret)
      request.owner = owner
    } catch (err: any) {
      request.log.error({ error: err }, 'unable to get owner')
      return reply.status(400).send(createResponse(err.message, '400', err.message))
    }
  })
})
