import fastifyPlugin from 'fastify-plugin'
import { isIP } from 'net'
import { getTenantConfig } from '@internal/database'

import { getConfig } from '../../config'
import { context, trace } from '@opentelemetry/api'
import { Span, traceCollector } from '@internal/monitoring/otel-processor'
import { ReadableSpan } from '@opentelemetry/sdk-trace-base'
import { logger, logSchema } from '@internal/monitoring'

declare module 'fastify' {
  interface FastifyRequest {
    tracingMode?: string
    serverTimings?: { spanName: string; duration: number }[]
  }
}

const {
  isMultitenant,
  tracingEnabled,
  tracingMode: defaultTracingMode,
  tracingReturnServerTimings,
  isProduction,
  tracingTimeMinDuration,
} = getConfig()

export const tracing = fastifyPlugin(
  async function tracingMode(fastify) {
    if (!tracingEnabled) {
      return
    }
    fastify.register(traceServerTime)

    fastify.addHook('onRequest', async (request) => {
      try {
        if (isMultitenant && request.tenantId) {
          const tenantConfig = await getTenantConfig(request.tenantId)
          request.tracingMode = tenantConfig.tracingMode
        } else {
          request.tracingMode = defaultTracingMode
        }

        const span = trace.getSpan(context.active())

        if (span) {
          // We collect logs only in full,logs,debug mode
          if (
            tracingEnabled &&
            request.tracingMode &&
            !['full', 'logs', 'debug'].includes(request.tracingMode)
          ) {
            traceCollector.clearTrace(span.spanContext().traceId)
          }
        }
      } catch (e) {
        logSchema.error(request.log, 'failed setting tracing mode', { error: e, type: 'tracing' })
      }
    })
  },
  { name: 'tracing-mode' }
)

export const traceServerTime = fastifyPlugin(
  async function traceServerTime(fastify) {
    if (!tracingEnabled) {
      return
    }
    fastify.addHook('onResponse', async (request, reply) => {
      try {
        const traceId = trace.getSpan(context.active())?.spanContext().traceId

        if (traceId) {
          const spans = traceCollector.getSpansForTrace(traceId)
          if (spans) {
            const serverTimingHeaders = spansToServerTimings(spans, reply.statusCode >= 500)

            request.serverTimings = serverTimingHeaders

            // Return Server-Timing if enabled
            if (tracingReturnServerTimings) {
              const httpServerTimes = serverTimingHeaders
                .flatMap((span) => {
                  return [span, ...span.children]
                })
                .map(({ spanName, duration }) => {
                  return `${spanName};dur=${duration.toFixed(3)}` // Convert to milliseconds
                })
                .join(',')
              reply.header('Server-Timing', httpServerTimes)
            }
            traceCollector.clearTrace(traceId)
          }
        }
      } catch (e) {
        logSchema.error(request.log, 'failed tracing on response', { error: e, type: 'tracing' })
      }
    })

    fastify.addHook('onRequestAbort', async (req) => {
      try {
        const span = trace.getSpan(context.active())
        const traceId = span?.spanContext().traceId

        span?.setAttribute('req_aborted', true)

        if (traceId) {
          const spans = traceCollector.getSpansForTrace(traceId)
          if (spans) {
            req.serverTimings = spansToServerTimings(spans, true)
          }
          traceCollector.clearTrace(traceId)
        }
      } catch (e) {
        logSchema.error(logger, 'failed parsing server times on abort', { error: e, type: 'otel' })
      }
    })
  },
  { name: 'tracing-server-times' }
)

function enrichSpanName(spanName: string, span: ReadableSpan) {
  if (span.attributes['knex.version']) {
    const queryOperation = (span.attributes['db.operation'] as string)?.split(' ').shift()
    return (
      `pg_query_` +
      queryOperation?.toUpperCase() +
      (span.attributes['db.sql.table'] ? '_' + span.attributes['db.sql.table'] : '_postgres')
    )
  }

  if (['GET', 'PUT', 'HEAD', 'DELETE', 'POST'].includes(spanName)) {
    return `HTTP_${spanName}`
  }

  return spanName
}

function spansToServerTimings(
  spans: Span[],
  includeChildren = false
): { spanName: string; duration: number; action?: any; host?: string; children: any[] }[] {
  const minLatency = isProduction ? tracingTimeMinDuration : 50

  return spans.flatMap((span) => {
    const duration = Math.max(span.item.duration[1], span.item.duration[0]) / 1e6 // Convert nanoseconds to milliseconds

    if (duration >= minLatency || includeChildren) {
      let spanName =
        span.item.name
          .split('->')
          .pop()
          ?.trimStart()
          .replaceAll('\n', '')
          .replaceAll(' ', '_')
          .replaceAll('-', '_')
          .replaceAll('___', '_')
          .replaceAll(':', '_')
          .replaceAll('_undefined', '') || 'UNKNOWN'

      spanName = enrichSpanName(spanName, span.item)
      const hostName = span.item.attributes['net.peer.name'] as string | undefined

      return [
        {
          spanName,
          duration,
          action: span.item.attributes['db.statement'],
          error: span.item.attributes.error,
          status: span.item.status,
          host: hostName
            ? isIP(hostName)
              ? hostName
              : hostName?.split('.').slice(-3).join('.')
            : undefined,
          children: spansToServerTimings(span.children, true),
        },
      ]
    } else {
      // If the span doesn't meet the minimum latency, only return its children
      return spansToServerTimings(span.children)
    }
  })
}
