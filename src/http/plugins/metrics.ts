import fastifyPlugin from 'fastify-plugin'
import { MetricsRegistrar } from '../../monitoring/metrics'
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
      },
    })
  })
