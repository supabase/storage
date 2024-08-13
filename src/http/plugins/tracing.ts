import fastifyPlugin from 'fastify-plugin'
import { isIP } from 'net'
import { getTenantConfig } from '@internal/database'

import { getConfig } from '../../config'
import { context, trace } from '@opentelemetry/api'
import { traceCollector } from '@internal/monitoring/otel-processor'
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
} = getConfig()

export const tracing = fastifyPlugin(async function tracingMode(fastify) {
  if (!tracingEnabled) {
    return
  }
  fastify.register(traceServerTime)

  fastify.addHook('onRequest', async (request) => {
    if (isMultitenant && request.tenantId) {
      const tenantConfig = await getTenantConfig(request.tenantId)
      request.tracingMode = tenantConfig.tracingMode
    } else {
      request.tracingMode = defaultTracingMode
    }
  })
})

export const traceServerTime = fastifyPlugin(async function traceServerTime(fastify) {
  if (!tracingEnabled) {
    return
  }
  fastify.addHook('onResponse', async (request, reply) => {
    const traceId = trace.getSpan(context.active())?.spanContext().traceId

    if (traceId) {
      const spans = traceCollector.getSpansForTrace(traceId)
      if (spans) {
        try {
          const serverTimingHeaders = spansToServerTimings(spans)

          request.serverTimings = serverTimingHeaders

          // Return Server-Timing if enabled
          if (tracingReturnServerTimings) {
            const httpServerTimes = serverTimingHeaders
              .map(({ spanName, duration }) => {
                return `${spanName};dur=${duration.toFixed(3)}` // Convert to milliseconds
              })
              .join(',')
            reply.header('Server-Timing', httpServerTimes)
          }
        } catch (e) {
          logSchema.error(logger, 'failed parsing server times', { error: e, type: 'otel' })
        }

        traceCollector.clearTrace(traceId)
      }
    }
  })

  fastify.addHook('onRequestAbort', async (req) => {
    const traceId = trace.getSpan(context.active())?.spanContext().traceId

    if (traceId) {
      const spans = traceCollector.getSpansForTrace(traceId)
      if (spans) {
        req.serverTimings = spansToServerTimings(spans)
      }
      traceCollector.clearTrace(traceId)
    }
  })
})

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

function spansToServerTimings(spans: ReadableSpan[]) {
  return spans
    .sort((a, b) => {
      return a.startTime[1] - b.startTime[1]
    })
    .map((span) => {
      const duration = span.duration[1] // Duration in nanoseconds

      let spanName =
        span.name
          .split('->')
          .pop()
          ?.trimStart()
          .replaceAll('\n', '')
          .replaceAll('.', '_')
          .replaceAll(' ', '_')
          .replaceAll('-', '_')
          .replaceAll('___', '_')
          .replaceAll(':', '_')
          .replaceAll('_undefined', '') || 'UNKNOWN'

      spanName = enrichSpanName(spanName, span)
      const hostName = span.attributes['net.peer.name'] as string | undefined

      return {
        spanName,
        duration: duration / 1e6,
        action: span.attributes['db.statement'],
        host: hostName
          ? isIP(hostName)
            ? hostName
            : hostName?.split('.').slice(-3).join('.')
          : undefined,
      }
    })
}
