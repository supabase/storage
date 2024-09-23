import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../../config'

declare module 'fastify' {
  interface FastifyRequest {
    tenantId: string
  }
}

const {
  version,
  isMultitenant,
  tenantId: defaultTenantId,
  requestXForwardedHostRegExp,
} = getConfig()

export const tenantId = fastifyPlugin(
  async (fastify) => {
    fastify.decorateRequest('tenantId', defaultTenantId)
    fastify.addHook('onRequest', async (request) => {
      if (!isMultitenant || !requestXForwardedHostRegExp) return
      const xForwardedHost = request.headers['x-forwarded-host']
      if (typeof xForwardedHost !== 'string') return
      const result = xForwardedHost.match(requestXForwardedHostRegExp)
      if (!result) return

      request.tenantId = result[1]
    })

    fastify.addHook('onRequest', async (request, reply) => {
      reply.log = request.log = request.log.child({
        tenantId: request.tenantId,
        project: request.tenantId,
        reqId: request.id,
        appVersion: version,
      })
    })
  },
  { name: 'tenant-id' }
)

export const adminTenantId = fastifyPlugin(
  async (fastify) => {
    fastify.addHook('onRequest', async (request) => {
      const tenantId = (request.params as Record<string, undefined | string>).tenantId
      if (!tenantId) return

      request.tenantId = tenantId
    })

    fastify.addHook('onRequest', async (request, reply) => {
      reply.log = request.log = request.log.child({
        tenantId: request.tenantId,
        project: request.tenantId,
        reqId: request.id,
      })
    })
  },
  { name: 'admin-tenant-id' }
)
