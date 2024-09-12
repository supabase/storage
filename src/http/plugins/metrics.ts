import fastifyPlugin from 'fastify-plugin'
import { MetricsRegistrar } from '@internal/monitoring/metrics'
import fastifyMetrics from 'fastify-metrics'
import { getConfig } from '../../config'

const { region, defaultMetricsEnabled } = getConfig()

interface MetricsOptions {
  enabledEndpoint?: boolean
}

export const metrics = ({ enabledEndpoint }: MetricsOptions) =>
  fastifyPlugin(
    async (fastify) => {
      fastify.register(fastifyMetrics, {
        endpoint: enabledEndpoint ? '/metrics' : null,
        defaultMetrics: {
          enabled: defaultMetricsEnabled,
          register: MetricsRegistrar,
          prefix: 'storage_api_',
          labels: {
            region,
          },
        },
        routeMetrics: {
          enabled: defaultMetricsEnabled,
          routeBlacklist: ['/metrics', '/status', '/health'],
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
    },
    { name: 'prometheus-metrics' }
  )
