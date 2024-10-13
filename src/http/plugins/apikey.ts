import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../../config'

export default fastifyPlugin(
  async (fastify) => {
    const { adminApiKeys } = getConfig()
    const apiKeys = new Set(adminApiKeys.split(','))
    fastify.addHook('onRequest', async (request, reply) => {
      if (typeof request.headers.apikey !== 'string' || !apiKeys.has(request.headers.apikey)) {
        reply.status(401).send()
      }
    })
  },
  { name: 'auth-admin-api-key' }
)
