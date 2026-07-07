import { FastifyInstance } from 'fastify'
import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../../config'

const apiKeyPlugin = fastifyPlugin(
  async (fastify) => {
    const { adminApiKeys } = getConfig()
    const apiKeys = new Set(adminApiKeys.split(','))
    fastify.addHook('onRequest', async (request, reply) => {
      if (typeof request.headers.apikey !== 'string' || !apiKeys.has(request.headers.apikey)) {
        return reply.status(401).send()
      }
    })
  },
  { name: 'auth-admin-api-key' }
)

export function registerApiKeyAuth(fastify: FastifyInstance) {
  fastify.addHook('onRoute', (routeOptions) => {
    routeOptions.schema = routeOptions.schema || {}
    routeOptions.schema.security = [{ apiKeyAuth: [] }]

    // Every route behind this plugin can reject with an empty-bodied 401 when the
    // `apikey` header is missing or doesn't match one of the configured admin API keys.
    // Routes that don't declare their own response schema fall back to @fastify/swagger's
    // own "200: Default Response" placeholder - preserve that instead of losing it, since
    // setting `schema.response` at all opts a route out of that fallback.
    const hadResponseSchema = Boolean(routeOptions.schema.response)
    routeOptions.schema.response = {
      ...(hadResponseSchema ? undefined : { 200: { description: 'Default Response' } }),
      401: { description: 'Missing or invalid API key' },
      ...(routeOptions.schema.response as object | undefined),
    }
  })
  fastify.register(apiKeyPlugin)
}
