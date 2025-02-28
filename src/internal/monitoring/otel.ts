import { getConfig } from '../../config'

const {
  version,
  requestTraceHeader,
  isMultitenant,
  requestXForwardedHostRegExp,
  tenantId: defaultTenantId,
  region,
} = getConfig()

import { S3Client } from '@aws-sdk/client-s3'
import { NodeSDK } from '@opentelemetry/sdk-node'
import { Resource } from '@opentelemetry/resources'
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions'
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-grpc'
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node'
import { CompressionAlgorithm } from '@opentelemetry/otlp-exporter-base'
import { SpanExporter, BatchSpanProcessor, SpanProcessor } from '@opentelemetry/sdk-trace-base'
import * as grpc from '@grpc/grpc-js'
import { HttpInstrumentation } from '@opentelemetry/instrumentation-http'
import { IncomingMessage } from 'node:http'
import { logger, logSchema } from '@internal/monitoring/logger'
import { traceCollector } from '@internal/monitoring/otel-processor'
import { ClassInstrumentation } from './otel-instrumentation'
import { ObjectStorage } from '@storage/object'
import { Uploader } from '@storage/uploader'
import { Storage } from '@storage/storage'
import { Event as QueueBaseEvent } from '@internal/queue'
import { S3Backend } from '@storage/backend'
import { StorageKnexDB } from '@storage/database'
import { TenantConnection } from '@internal/database'
import { S3Store } from '@tus/s3-store'
import { Upload } from '@aws-sdk/lib-storage'
import { StreamSplitter } from '@tus/server'
import { PgLock } from '@storage/protocols/tus'
import { Semaphore, Permit } from '@shopify/semaphore'

const tracingEnabled = process.env.TRACING_ENABLED === 'true'
const headersEnv = process.env.OTEL_EXPORTER_OTLP_TRACES_HEADERS || ''
const enableLogTraces = ['debug', 'logs'].includes(process.env.TRACING_MODE || '')

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

if (batchProcessor) {
  spanProcessors.push(batchProcessor)
}

if (enableLogTraces) {
  spanProcessors.push(traceCollector)
}

// Configure the OpenTelemetry Node SDK
const sdk = new NodeSDK({
  resource: new Resource({
    [ATTR_SERVICE_NAME]: 'storage',
    [ATTR_SERVICE_VERSION]: version,
  }),
  spanProcessors: spanProcessors,
  traceExporter,
  instrumentations: [
    new HttpInstrumentation({
      enabled: true,
      ignoreIncomingRequestHook: (req) => {
        const ignoreRoutes = ['/metrics', '/status', '/health', '/healthcheck']
        return ignoreRoutes.some((url) => req.url?.includes(url)) ?? false
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
    }),
    new ClassInstrumentation({
      targetClass: Storage,
      enabled: true,
      methodsToInstrument: [
        'findBucket',
        'listBuckets',
        'createBucket',
        'updateBucket',
        'countObjects',
        'deleteBucket',
        'emptyBucket',
        'healthcheck',
      ],
    }),
    new ClassInstrumentation({
      targetClass: ObjectStorage,
      enabled: true,
      methodsToInstrument: [
        'uploadNewObject',
        'uploadOverridingObject',
        'deleteObject',
        'deleteObjects',
        'updateObjectMetadata',
        'updateObjectOwner',
        'findObject',
        'findObjects',
        'copyObject',
        'moveObject',
        'searchObjects',
        'listObjectsV2',
        'signObjectUrl',
        'signObjectUrls',
        'signUploadObjectUrl',
        'verifyObjectSignature',
      ],
    }),
    new ClassInstrumentation({
      targetClass: Uploader,
      enabled: true,
      methodsToInstrument: ['canUpload', 'prepareUpload', 'upload', 'completeUpload'],
    }),
    new ClassInstrumentation({
      targetClass: QueueBaseEvent,
      enabled: true,
      methodsToInstrument: ['send', 'batchSend'],
      setName: (name, attrs, eventClass) => {
        if (attrs.constructor.name) {
          return name + '.' + eventClass.constructor.name
        }
        return name
      },
    }),
    new ClassInstrumentation({
      targetClass: S3Backend,
      enabled: true,
      methodsToInstrument: [
        'getObject',
        'putObject',
        'deleteObject',
        'listObjects',
        'copyObject',
        'headObject',
        'createMultipartUpload',
        'uploadPart',
        'completeMultipartUpload',
        'abortMultipartUpload',
        'listMultipartUploads',
        'listParts',
        'getSignedUrl',
        'createBucket',
        'deleteBucket',
        'listBuckets',
        'getBucketLocation',
        'getBucketVersioning',
        'putBucketVersioning',
        'getBucketLifecycleConfiguration',
        'putBucketLifecycleConfiguration',
        'deleteBucketLifecycle',
        'uploadObject',
        'privateAssetUrl',
      ],
    }),
    new ClassInstrumentation({
      targetClass: StorageKnexDB,
      enabled: true,
      methodsToInstrument: ['runQuery'],
      setName: (name, attrs) => {
        if (attrs.queryName) {
          return name + '.' + attrs.queryName
        }
        return name
      },
      setAttributes: {
        runQuery: (queryName) => {
          return {
            queryName,
          }
        },
      },
    }),
    new ClassInstrumentation({
      targetClass: TenantConnection,
      enabled: true,
      methodsToInstrument: ['transaction', 'setScope'],
    }),
    new ClassInstrumentation({
      targetClass: S3Store,
      enabled: true,
      methodsToInstrument: [
        'write',
        'create',
        'remove',
        'getUpload',
        'declareUploadLength',
        'uploadIncompletePart',
        'uploadPart',
        'downloadIncompletePart',
        'uploadParts',
      ],
      setName: (name) => 'Tus.' + name,
    }),
    new ClassInstrumentation({
      targetClass: StreamSplitter,
      enabled: true,
      methodsToInstrument: ['emitEvent'],
      setName: (name: string, attrs: any) => {
        if (attrs.event) {
          return name + '.' + attrs.event
        }
        return name
      },
      setAttributes: {
        emitEvent: function (event) {
          return {
            part: this.part as any,
            event,
          }
        },
      },
    }),
    new ClassInstrumentation({
      targetClass: PgLock,
      enabled: true,
      methodsToInstrument: ['lock', 'unlock', 'acquireLock'],
    }),
    new ClassInstrumentation({
      targetClass: Semaphore,
      enabled: true,
      methodsToInstrument: ['acquire'],
    }),
    new ClassInstrumentation({
      targetClass: Permit,
      enabled: true,
      methodsToInstrument: ['release'],
    }),
    new ClassInstrumentation({
      targetClass: S3Client,
      enabled: true,
      methodsToInstrument: ['send'],
      setAttributes: {
        send: (command) => {
          return {
            operation: command.constructor.name as string,
          }
        },
      },
      setName: (name, attrs) => 'S3.' + attrs.operation,
    }),
    new ClassInstrumentation({
      targetClass: Upload,
      enabled: true,
      methodsToInstrument: [
        'done',
        '__uploadUsingPut',
        '__createMultipartUpload',
        'markUploadAsAborted',
      ],
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

if (tracingEnabled && spanProcessors.length > 0) {
  // Initialize the OpenTelemetry Node SDK
  sdk.start()

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
          error: error,
        })
      )
  })
}
