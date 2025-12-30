import fastifyPlugin from 'fastify-plugin'
import { handleMetricsRequest } from '@internal/monitoring/otel-metrics'
import {
  httpRequestDuration,
  httpRequestsTotal,
  httpRequestSizeBytes,
  httpResponseSizeBytes,
} from '@internal/monitoring/metrics'
import { getConfig } from '../../config'

const { region, prometheusMetricsEnabled } = getConfig()

interface MetricsOptions {
  enabledEndpoint?: boolean
  excludeRoutes?: string[]
  groupStatusCodes?: boolean
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
  /** Whether to group status codes (2xx, 3xx, etc.) */
  groupStatusCodes?: boolean
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
      const groupStatusCodes = options.groupStatusCodes ?? true

      // Hook into request lifecycle to measure duration
      fastify.addHook('onRequest', async (request) => {
        // Store start time on request for later use
        request.metricsStartTime = process.hrtime.bigint()
      })

      fastify.addHook('onResponse', async (request, reply) => {
        const route = request.routeOptions?.url || request.url || 'unknown'

        // Skip excluded routes (match start of path)
        if (excludeRoutes.some((r) => route === r || route.startsWith(r + '/'))) {
          return
        }

        const startTime = request.metricsStartTime
        if (!startTime) return

        // Calculate duration in seconds
        const endTime = process.hrtime.bigint()
        const durationNs = endTime - startTime
        const durationSeconds = Number(durationNs) / 1e9

        const method = request.method
        const statusCode = groupStatusCodes
          ? `${Math.floor(reply.statusCode / 100)}xx`
          : String(reply.statusCode)
        const tenantId = request.tenantId || ''

        const attributes = {
          method,
          route,
          status_code: statusCode,
          tenantId,
          region,
        }

        // Record metrics
        httpRequestDuration.record(durationSeconds, attributes)
        httpRequestsTotal.add(1, attributes)

        // Record request size from content-length header
        const requestContentLength = request.headers['content-length']
        if (requestContentLength) {
          const requestSize = parseInt(requestContentLength, 10)
          if (!isNaN(requestSize) && requestSize > 0) {
            httpRequestSizeBytes.add(requestSize, attributes)
          }
        }

        // Record response size from content-length header
        const responseContentLength = reply.getHeader('content-length')
        if (responseContentLength) {
          const responseSize =
            typeof responseContentLength === 'string'
              ? parseInt(responseContentLength, 10)
              : typeof responseContentLength === 'number'
              ? responseContentLength
              : 0
          if (!isNaN(responseSize) && responseSize > 0) {
            httpResponseSizeBytes.add(responseSize, attributes)
          }
        }
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
