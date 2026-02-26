import { getConfig } from '../../config'

const {
  version,
  requestTraceHeader,
  isMultitenant,
  requestXForwardedHostRegExp,
  tenantId: defaultTenantId,
  region,
  storageS3InternalTracesEnabled,
} = getConfig()

import * as grpc from '@grpc/grpc-js'
import { logger, logSchema } from '@internal/monitoring/logger'
import { TenantSpanProcessor } from '@internal/monitoring/otel-instrumentation'
import { trace } from '@opentelemetry/api'
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-grpc'
import { registerInstrumentations } from '@opentelemetry/instrumentation'
import { CompressionAlgorithm } from '@opentelemetry/otlp-exporter-base'
import { resourceFromAttributes } from '@opentelemetry/resources'
import { NodeSDK } from '@opentelemetry/sdk-node'
import { BatchSpanProcessor, SpanExporter, SpanProcessor } from '@opentelemetry/sdk-trace-base'
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions'

const tracingEnabled = process.env.TRACING_ENABLED === 'true'
const headersEnv = process.env.OTEL_EXPORTER_OTLP_TRACES_HEADERS || ''

const exporterHeaders = headersEnv
  .split(',')
  .filter(Boolean)
  .reduce(
    (all, header) => {
      const [name, value] = header.split('=')
      all[name] = value
      return all
    },
    {} as Record<string, any>
  )

const grpcMetadata = new grpc.Metadata()
Object.keys(exporterHeaders).forEach((key) => {
  grpcMetadata.set(key, exporterHeaders[key])
})

const endpoint = process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
let traceExporter: SpanExporter | undefined = undefined

if (tracingEnabled && endpoint) {
  // Create an OTLP trace exporter
  traceExporter = new OTLPTraceExporter({
    url: endpoint,
    compression: process.env.OTEL_EXPORTER_OTLP_COMPRESSION as CompressionAlgorithm,
    headers: exporterHeaders,
    metadata: grpcMetadata,
  })
}

// Create a BatchSpanProcessor using the trace exporter
const batchProcessor = traceExporter ? new BatchSpanProcessor(traceExporter) : undefined

const spanProcessors: SpanProcessor[] = []

if (tracingEnabled) {
  spanProcessors.push(new TenantSpanProcessor())
}

if (batchProcessor) {
  spanProcessors.push(batchProcessor)
}

if (tracingEnabled && spanProcessors.length > 0) {
  // Configure the OpenTelemetry Node SDK
  const sdk = new NodeSDK({
    resource: resourceFromAttributes({
      [ATTR_SERVICE_NAME]: 'storage',
      [ATTR_SERVICE_VERSION]: version,
    }),
    spanProcessors,
    traceExporter,
    instrumentations: [
      getNodeAutoInstrumentations({
        '@opentelemetry/instrumentation-http': {
          enabled: true,
          ignoreIncomingRequestHook: (req) => {
            const ignoreRoutes = ['/metrics', '/status', '/health', '/healthcheck']
            return ignoreRoutes.some((url) => req.url?.includes(url)) ?? false
          },
          ignoreOutgoingRequestHook: (req) => {
            // Skip OTEL instrumentation for S3 Tables requests to avoid injecting
            // unsupported headers (baggage, traceparent, tracestate)
            const host = req.hostname || req.host || ''
            return host.includes('.s3tables.') || host.includes('--table-s3')
          },
          startIncomingSpanHook: (req) => {
            let tenantId = ''
            if (isMultitenant) {
              if (requestXForwardedHostRegExp) {
                const serverRequest = req
                const xForwardedHost = serverRequest.headers['x-forwarded-host']
                if (typeof xForwardedHost !== 'string') return {}
                const result = xForwardedHost.match(requestXForwardedHostRegExp)
                if (!result) return {}
                tenantId = result[1]
              }
            } else {
              tenantId = defaultTenantId
            }

            return {
              'tenant.ref': tenantId,
              region,
            }
          },
          headersToSpanAttributes: {
            client: {
              requestHeaders: requestTraceHeader ? [requestTraceHeader] : [],
            },
            server: {
              requestHeaders: requestTraceHeader ? [requestTraceHeader] : [],
            },
          },
        },
        '@opentelemetry/instrumentation-fs': {
          enabled: false,
        },
        '@opentelemetry/instrumentation-aws-sdk': {
          enabled: storageS3InternalTracesEnabled,
        },
        '@opentelemetry/instrumentation-pg': {
          enabled: true,
          requireParentSpan: true,
        },
        '@opentelemetry/instrumentation-fastify': {
          enabled: true,
          requestHook: (span, req) => {
            span.setAttribute('http.method', req.request.method)
            span.setAttribute('http.route', req.request.routerPath)
            span.setAttribute('tenant.ref', req.request.tenantId)
            span.setAttribute('http.operation', req.request.operation)
            span.setAttribute('trace.mode', req.request.tracingMode)
          },
        },
        '@opentelemetry/instrumentation-knex': {
          enabled: true,
        },
      }),
    ],
  })

  // Initialize the OpenTelemetry Node SDK
  sdk.start()

  // Load class instrumentations after SDK starts to avoid loading http/metrics.ts too early
  void import('./otel-class-instrumentations').then(({ classInstrumentations }) => {
    registerInstrumentations({
      tracerProvider: trace.getTracerProvider(),
      instrumentations: classInstrumentations,
    })
  })

  // Gracefully shutdown the SDK on process exit
  process.once('SIGTERM', () => {
    logSchema.info(logger, '[Otel] Stopping', {
      type: 'otel',
    })
    sdk
      .shutdown()
      .then(() => {
        logSchema.info(logger, '[Otel] Exited', {
          type: 'otel',
        })
      })
      .catch((error) =>
        logSchema.error(logger, '[Otel] Shutdown error', {
          type: 'otel',
          error,
        })
      )
  })
}
