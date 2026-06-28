import { recordHttpRequestMetrics } from '@internal/monitoring/metrics'
import { handleMetricsRequest } from '@internal/monitoring/otel-metrics'
import fastifyPlugin from 'fastify-plugin'
import { getConfig } from '../../config'

const { prometheusMetricsEnabled } = getConfig()

function parseMetricSizeHeader(value: number | string | string[] | undefined): number | undefined {
  let size: number | undefined

  if (typeof value === 'number') {
    size = value
  } else if (typeof value === 'string') {
    size = parseInt(value, 10)
  }

  return typeof size === 'number' && Number.isFinite(size) && size > 0 ? size : undefined
}

interface MetricsOptions {
  enabledEndpoint?: boolean
  excludeRoutes?: string[]
}

export const metrics = (options: MetricsOptions = {}) =>
  fastifyPlugin(async (fastify) => {
    // Register HTTP metrics plugin
    fastify.register(httpMetrics(options))

    // Register metrics endpoint if enabled
    if (prometheusMetricsEnabled) {
      fastify.register(metricsEndpoint(options))
    }
  })

export const metricsEndpoint = ({ enabledEndpoint }: MetricsOptions) => {
  // Metrics endpoint plugin
  return fastifyPlugin(
    async (fastify) => {
      if (enabledEndpoint) {
        fastify.get('/metrics', handleMetricsRequest)
      }
    },
    { name: 'otel-metrics' }
  )
}

interface HttpMetricsOptions {
  /** Routes to exclude from metrics collection */
  excludeRoutes?: string[]
}

/**
 * Fastify plugin for collecting HTTP request metrics.
 * Records request duration (histogram) and request count (counter)
 * using OpenTelemetry with tenant support.
 */
export const httpMetrics = (options: HttpMetricsOptions = {}) =>
  fastifyPlugin(
    async (fastify) => {
      const excludeRoutes = options.excludeRoutes || [
        '/metrics',
        '/status',
        '/health',
        '/healthcheck',
      ]

      // Hook into request lifecycle to measure duration
      fastify.addHook('onRequest', (request, _reply, done) => {
        // Store start time on request for later use
        request.metricsStartTime = process.hrtime.bigint()
        done()
      })

      fastify.addHook('onResponse', (request, reply, done) => {
        const route = request.routeOptions?.url || 'unknown'

        // Skip excluded routes (match start of path)
        if (excludeRoutes.some((r) => route === r || route.startsWith(r + '/'))) {
          done()
          return
        }

        const startTime = request.metricsStartTime
        if (!startTime) {
          done()
          return
        }

        // Calculate duration in seconds
        const endTime = process.hrtime.bigint()
        const durationNs = endTime - startTime
        const durationSeconds = Number(durationNs) / 1e9

        const operation =
          request.operation?.type || request.routeOptions?.config?.operation?.type || 'unknown'

        // Record request size from content-length header
        const requestContentLength = request.headers['content-length']
        const requestSize = parseMetricSizeHeader(requestContentLength)

        // Record response size from content-length header
        const responseContentLength = reply.getHeader('content-length')
        const responseSize = parseMetricSizeHeader(responseContentLength)

        recordHttpRequestMetrics(
          durationSeconds,
          requestSize,
          responseSize,
          request.method,
          operation,
          reply.statusCode
        )
        done()
      })
    },
    { name: 'http-metrics' }
  )

// Extend FastifyRequest to include metricsStartTime
declare module 'fastify' {
  interface FastifyRequest {
    metricsStartTime?: bigint
  }
}
