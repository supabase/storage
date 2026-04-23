import { getSbReqId } from '@internal/monitoring'
import fastifyPlugin from 'fastify-plugin'

declare module 'fastify' {
  interface FastifyRequest {
    sbReqId?: string
  }
}

export const requestContext = fastifyPlugin(
  async (fastify) => {
    fastify.decorateRequest('sbReqId', undefined)
    fastify.addHook('onRequest', async (request) => {
      request.sbReqId = getSbReqId(request.headers)
    })
  },
  { name: 'request-context' }
)
