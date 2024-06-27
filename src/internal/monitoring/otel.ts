import { getConfig } from '../../config'

const {
  version,
  requestTraceHeader,
  isMultitenant,
  requestXForwardedHostRegExp,
  tenantId: defaultTenantId,
  region,
} = getConfig()

import { NodeSDK } from '@opentelemetry/sdk-node'
import { Resource } from '@opentelemetry/resources'
import {
  SEMRESATTRS_SERVICE_NAME,
  SEMRESATTRS_SERVICE_VERSION,
} from '@opentelemetry/semantic-conventions'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-grpc'
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node'
import { CompressionAlgorithm } from '@opentelemetry/otlp-exporter-base'
import * as grpc from '@grpc/grpc-js'
import { HttpInstrumentation } from '@opentelemetry/instrumentation-http'
import { IncomingMessage } from 'http'
import { logger, logSchema } from '@internal/monitoring/logger'

const headersEnv = process.env.OTEL_EXPORTER_OTLP_TRACES_HEADERS || ''

const exporterHeaders = headersEnv
  .split(',')
  .filter(Boolean)
  .reduce((all, header) => {
    const [name, value] = header.split('=')
    all[name] = value
    return all
  }, {} as Record<string, any>)

const grpcMetadata = new grpc.Metadata()
Object.keys(exporterHeaders).forEach((key) => {
  grpcMetadata.set(key, exporterHeaders[key])
})

const endpoint = process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT

// Create an OTLP trace exporter
const traceExporter = new OTLPTraceExporter({
  url: endpoint,
  compression: process.env.OTEL_EXPORTER_OTLP_COMPRESSION as CompressionAlgorithm,
  headers: exporterHeaders,
  metadata: grpcMetadata,
})

// Configure the OpenTelemetry Node SDK
const sdk = new NodeSDK({
  resource: new Resource({
    [SEMRESATTRS_SERVICE_NAME]: 'storage',
    [SEMRESATTRS_SERVICE_VERSION]: version,
  }),
  traceExporter,
  instrumentations: [
    new HttpInstrumentation({
      enabled: true,
      ignoreIncomingRequestHook: (req) => {
        const ignoreRoutes = ['/metrics', '/status', '/health', '/healthcheck']
        return ignoreRoutes.some((url) => req.url?.includes(url)) ?? false
      },
      applyCustomAttributesOnSpan: (span, req) => {
        let tenantId = ''
        if (isMultitenant) {
          if (requestXForwardedHostRegExp) {
            const serverRequest = req as IncomingMessage
            const xForwardedHost = serverRequest.headers['x-forwarded-host']
            if (typeof xForwardedHost !== 'string') return
            const result = xForwardedHost.match(requestXForwardedHostRegExp)
            if (!result) return
            tenantId = result[1]
          }
        } else {
          tenantId = defaultTenantId
        }
        span.setAttribute('tenant.ref', tenantId)
        span.setAttribute('region', region)
      },
      headersToSpanAttributes: {
        client: {
          requestHeaders: requestTraceHeader ? [requestTraceHeader] : [],
        },
        server: {
          requestHeaders: requestTraceHeader ? [requestTraceHeader] : [],
        },
      },
    }),
    getNodeAutoInstrumentations({
      '@opentelemetry/instrumentation-http': {
        enabled: false,
      },
      '@opentelemetry/instrumentation-fs': {
        enabled: false,
      },
      '@opentelemetry/instrumentation-aws-sdk': {
        enabled: true,
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

if (process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) {
  // Initialize the OpenTelemetry Node SDK
  sdk.start()

  // Gracefully shutdown the SDK on process exit
  process.on('SIGTERM', () => {
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
          error: error,
        })
      )
  })
}
