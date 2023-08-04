import fastifyPlugin from 'fastify-plugin'
import { MetricsRegistrar, RequestErrors } from '../../monitoring/metrics'
import fastifyMetrics from 'fastify-metrics'
import { getConfig } from '../../config'

const { region, enableDefaultMetrics } = getConfig()

interface MetricsOptions {
  enabledEndpoint?: boolean
}

export const metrics = ({ enabledEndpoint }: MetricsOptions) =>
  fastifyPlugin(async (fastify) => {
    fastify.register(fastifyMetrics, {
      endpoint: enabledEndpoint ? '/metrics' : null,
      defaultMetrics: {
        enabled: enableDefaultMetrics,
        register: MetricsRegistrar,
        prefix: 'storage_api_',
        labels: {
          region,
        },
      },
      routeMetrics: {
        enabled: enableDefaultMetrics,
        routeBlacklist: ['/metrics', '/status'],
        overrides: {
          summary: {
            name: 'storage_api_http_request_summary_seconds',
          },
          histogram: {
            name: 'storage_api_http_request_duration_seconds',
          },
        },
        registeredRoutesOnly: true,
        groupStatusCodes: true,
        customLabels: {
          tenant_id: (req) => {
            return req.tenantId
          },
        },
      },
    })

    // Errors
    fastify.addHook('onResponse', async (request, reply) => {
      const error = (reply.raw as any).executionError || reply.executionError

      if (error) {
        RequestErrors.inc({
          name: error.name || error.constructor.name,
          tenant_id: request.tenantId,
          path: request.routerPath,
          method: request.routerMethod,
          status: reply.statusCode,
        })
      }
    })
  })
