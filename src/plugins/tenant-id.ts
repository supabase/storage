import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../utils/config'

declare module 'fastify' {
  interface FastifyRequest {
    tenantId: string
  }
}

export default fastifyPlugin(async (fastify) => {
  const { isMultitenant, tenantId, xForwardedHostRegExp } = getConfig()
  fastify.decorateRequest('tenantId', tenantId)
  fastify.addHook('onRequest', async (request) => {
    if (!isMultitenant || !xForwardedHostRegExp) return
    const xForwardedHost = request.headers['x-forwarded-host']
    if (typeof xForwardedHost !== 'string') return
    const result = xForwardedHost.match(xForwardedHostRegExp)
    if (!result) return
    request.tenantId = result[1]
  })
})
