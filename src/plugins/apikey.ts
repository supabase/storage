import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../utils/config'

export default fastifyPlugin(async (fastify) => {
  const { adminApiKey } = getConfig()
  fastify.addHook('onRequest', async (request, reply) => {
    if (request.headers.apikey !== adminApiKey) {
      reply.status(401).send()
    }
  })
})
