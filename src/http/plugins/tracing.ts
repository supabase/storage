import fastifyPlugin from 'fastify-plugin'
import { getTenantConfig } from '@internal/database'

import { getConfig } from '../../config'
import { logSchema } from '@internal/monitoring'

declare module 'fastify' {
  interface FastifyRequest {
    tracingMode?: string
    serverTimings?: { spanName: string; duration: number }[]
  }
}

const { isMultitenant, tracingEnabled, tracingMode: defaultTracingMode } = getConfig()

export const tracing = fastifyPlugin(
  async function tracingMode(fastify) {
    if (!tracingEnabled) {
      return
    }

    fastify.addHook('onRequest', async (request) => {
      try {
        if (isMultitenant && request.tenantId) {
          const tenantConfig = await getTenantConfig(request.tenantId)
          request.tracingMode = tenantConfig.tracingMode
        } else {
          request.tracingMode = defaultTracingMode
        }
      } catch (e) {
        logSchema.error(request.log, 'failed setting tracing mode', { error: e, type: 'tracing' })
      }
    })
  },
  { name: 'tracing-mode' }
)
