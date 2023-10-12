import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../../config'

declare module 'fastify' {
  interface FastifyRequest {
    tenantId: string
  }
}

export const tenantId = fastifyPlugin(async (fastify) => {
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

export const adminTenantId = fastifyPlugin(async (fastify) => {
  fastify.addHook('onRequest', async (request) => {
    const tenantId = (request.params as Record<string, undefined | string>).tenantId
    if (!tenantId) return

    request.tenantId = tenantId
  })
})
