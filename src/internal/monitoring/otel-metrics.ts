import * as grpc from '@grpc/grpc-js'
import { logger, logSchema } from '@internal/monitoring/logger'
import { StorageNodeInstrumentation } from '@internal/monitoring/system'
import { metrics } from '@opentelemetry/api'
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-grpc'
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus'
import { HostMetrics } from '@opentelemetry/host-metrics'
import { registerInstrumentations } from '@opentelemetry/instrumentation'
import { RuntimeNodeInstrumentation } from '@opentelemetry/instrumentation-runtime-node'
import { CompressionAlgorithm } from '@opentelemetry/otlp-exporter-base'
import { resourceFromAttributes } from '@opentelemetry/resources'
import {
  AggregationTemporality,
  AggregationType,
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics'
import { ATTR_SERVICE_NAME, ATTR_SERVICE_VERSION } from '@opentelemetry/semantic-conventions'
import { FastifyReply, FastifyRequest } from 'fastify'
import * as os from 'os'
import { getConfig } from '../../config'

const { version, otelMetricsExportIntervalMs, otelMetricsEnabled, otelMetricsTemporality, region } =
  getConfig()

let prometheusExporter: PrometheusExporter | undefined
let meterProvider: MeterProvider | undefined
let metricsShutdownPromise: Promise<void> | undefined
let runtimeNodeInstrumentation: RuntimeNodeInstrumentation | undefined
let storageNodeInstrumentation: StorageNodeInstrumentation | undefined

interface OTelMetricsGlobalState {
  __otelMetricsShutdown?: () => Promise<void>
}

/**
 * Explicit shutdown hook for test and process teardown.
 * Safe to call multiple times.
 */
export async function shutdownOtelMetrics(): Promise<void> {
  if (metricsShutdownPromise) {
    await metricsShutdownPromise
    return
  }

  if (!meterProvider) {
    return
  }

  const provider = meterProvider
  metricsShutdownPromise = (async () => {
    logSchema.info(logger, '[OTel Metrics] Stopping', {
      type: 'otel-metrics',
    })

    try {
      // Disable custom/system instrumentations first to clear background timers
      storageNodeInstrumentation?.disable()
      runtimeNodeInstrumentation?.disable()

      await provider.shutdown()
      logSchema.info(logger, '[OTel Metrics] Shutdown complete', {
        type: 'otel-metrics',
      })
    } catch (error) {
      logSchema.error(logger, '[OTel Metrics] Shutdown error', {
        type: 'otel-metrics',
        error,
      })
    } finally {
      if (meterProvider === provider) {
        meterProvider = undefined
      }
      prometheusExporter = undefined
      runtimeNodeInstrumentation = undefined
      storageNodeInstrumentation = undefined
    }
  })()

  await metricsShutdownPromise
}

;(globalThis as typeof globalThis & OTelMetricsGlobalState).__otelMetricsShutdown =
  shutdownOtelMetrics

/**
 * Handles the /metrics endpoint request using OTel Prometheus exporter
 */
export async function handleMetricsRequest(
  request: FastifyRequest,
  reply: FastifyReply
): Promise<void> {
  if (!prometheusExporter) {
    reply.status(404).send('Metrics not enabled')
    return
  }

  const req = request.raw
  const res = reply.raw

  reply.hijack()
  prometheusExporter.getMetricsRequestHandler(req, res)

  return Promise.resolve()
}

if (otelMetricsEnabled) {
  const headersEnv = process.env.OTEL_EXPORTER_OTLP_METRICS_HEADERS || ''
  const otlpEndpoint =
    process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT || process.env.OTEL_EXPORTER_OTLP_ENDPOINT

  const exporterHeaders = headersEnv
    .split(',')
    .filter(Boolean)
    .reduce(
      (all, header) => {
        const [name, value] = header.split('=')
        all[name] = value
        return all
      },
      {} as Record<string, string>
    )

  const grpcMetadata = new grpc.Metadata()
  Object.keys(exporterHeaders).forEach((key) => {
    grpcMetadata.set(key, exporterHeaders[key])
  })

  // =============================================================================
  // Initialize MeterProvider at import time (before other modules use metrics)
  // =============================================================================
  const instance = os.hostname()

  const resource = resourceFromAttributes({
    [ATTR_SERVICE_NAME]: 'storage_api',
    [ATTR_SERVICE_VERSION]: version,
    'metric.version': '1',
    region,
    instance,
  })

  const readers = []

  // Add OTLP exporter if endpoint is configured (for pushing to collector)
  if (otlpEndpoint) {
    const otlpExporter = new OTLPMetricExporter({
      url: otlpEndpoint,
      compression: process.env.OTEL_EXPORTER_OTLP_COMPRESSION as CompressionAlgorithm,
      headers: exporterHeaders,
      metadata: grpcMetadata,
      temporalityPreference:
        otelMetricsTemporality === 'DELTA'
          ? AggregationTemporality.DELTA
          : AggregationTemporality.CUMULATIVE,
    })

    readers.push(
      new PeriodicExportingMetricReader({
        exporter: otlpExporter,
        exportIntervalMillis: otelMetricsExportIntervalMs,
      })
    )
  }

  // Always add Prometheus exporter for /metrics endpoint
  prometheusExporter = new PrometheusExporter({
    prefix: 'storage_api',
    preventServerStart: true, // We'll handle the endpoint in Fastify
    withResourceConstantLabels: /^(region|instance|metric\.version)$/,
  })
  readers.push(prometheusExporter)

  // Bucket boundaries for duration histograms (in seconds)
  // Provides good resolution from 0.5ms to 10s
  const durationBuckets = [
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
  ]

  const histogramAggregation = {
    type: AggregationType.EXPLICIT_BUCKET_HISTOGRAM,
    options: { boundaries: durationBuckets },
  } as const

  meterProvider = new MeterProvider({
    resource,
    readers,
    views: [
      {
        meterName: 'storage-api',
        instrumentName: 'http_request_duration_seconds',
        aggregation: histogramAggregation,
      },
      {
        meterName: 'storage-api',
        instrumentName: 'database_query_performance_seconds',
        aggregation: histogramAggregation,
      },
      {
        meterName: 'storage-api',
        instrumentName: 'queue_job_scheduled_time_seconds',
        aggregation: histogramAggregation,
      },
      {
        meterName: 'storage-api',
        instrumentName: 's3_upload_part_seconds',
        aggregation: histogramAggregation,
      },
      {
        meterName: 'storage-api',
        instrumentName: 'db_connection_acquire_seconds',
        aggregation: histogramAggregation,
      },
    ],
  })

  // Register as global provider IMMEDIATELY so metrics.ts instruments work
  metrics.setGlobalMeterProvider(meterProvider)

  logger.info(
    { type: 'otel-metrics', otlpEndpoint, exportIntervalMs: otelMetricsExportIntervalMs },
    '[OTel Metrics] Initializing'
  )

  if (otlpEndpoint) {
    logSchema.info(logger, '[OTel Metrics] OTLP exporter configured', {
      type: 'otel-metrics',
    })
  }

  // Initialize host metrics for Node.js runtime metrics
  const hostMetrics = new HostMetrics({
    meterProvider,
    name: 'storage-api-host-metrics',
  })
  hostMetrics.start()

  // Register Node.js runtime instrumentations
  runtimeNodeInstrumentation = new RuntimeNodeInstrumentation()
  storageNodeInstrumentation = new StorageNodeInstrumentation({
    labels: { region, instance },
  })

  registerInstrumentations({
    meterProvider,
    instrumentations: [
      // Official OTel: event loop delay/time/utilization, GC, heap spaces
      runtimeNodeInstrumentation,
      // Custom: event loop lag, CPU, handles, process start time, external memory, file descriptors
      storageNodeInstrumentation,
    ],
  })

  logSchema.info(logger, '[OTel Metrics] Initialized successfully', {
    type: 'otel-metrics',
  })

  // Graceful shutdown
  const shutdown = () => {
    void shutdownOtelMetrics()
  }

  process.once('SIGTERM', shutdown)
  process.once('SIGINT', shutdown)
}
