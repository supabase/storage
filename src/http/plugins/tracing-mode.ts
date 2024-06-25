import fastifyPlugin from 'fastify-plugin'
import { getTenantConfig } from '@internal/database'

import { getConfig } from '../../config'

declare module 'fastify' {
  interface FastifyRequest {
    tracingMode?: string
  }
}

const { isMultitenant, tracingMode: defaultTracingMode } = getConfig()

export const tracingMode = fastifyPlugin(async function tracingMode(fastify) {
  fastify.addHook('onRequest', async (request, reply) => {
    if (isMultitenant) {
      const tenantConfig = await getTenantConfig(request.tenantId)
      request.tracingMode = tenantConfig.tracingMode
    } else {
      request.tracingMode = defaultTracingMode
    }
  })
})
