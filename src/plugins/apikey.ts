import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../utils/config'

export default fastifyPlugin(async (fastify) => {
  const { apiKey } = getConfig()
  fastify.addHook('onRequest', async (request, reply) => {
    if (request.headers.apikey !== apiKey) {
      reply.status(401).send()
    }
  })
})
