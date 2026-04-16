interface OTelGlobalState {
  __otelMetricsShutdown?: () => Promise<void>
}

import { vi } from 'vitest'

const mockedMetricsModules = [
  '../../config',
  '@internal/monitoring/logger',
  '@internal/monitoring/system',
  '@opentelemetry/api',
  '@opentelemetry/exporter-metrics-otlp-grpc',
  '@opentelemetry/exporter-prometheus',
  '@opentelemetry/host-metrics',
  '@opentelemetry/instrumentation',
  '@opentelemetry/instrumentation-runtime-node',
  '@opentelemetry/resources',
  '@opentelemetry/sdk-metrics',
] as const

async function importOtelMetricsModule() {
  await import('./otel-metrics')
  return (globalThis as typeof globalThis & OTelGlobalState).__otelMetricsShutdown
}

describe('otel metrics', () => {
  const originalOtelExporterEndpoint = process.env.OTEL_EXPORTER_OTLP_ENDPOINT
  const originalOtelMetricsEndpoint = process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
  const originalOtelMetricsHeaders = process.env.OTEL_EXPORTER_OTLP_METRICS_HEADERS

  afterEach(async () => {
    const otelGlobalState = globalThis as typeof globalThis & OTelGlobalState

    if (otelGlobalState.__otelMetricsShutdown) {
      await otelGlobalState.__otelMetricsShutdown()
      delete otelGlobalState.__otelMetricsShutdown
    }

    if (originalOtelExporterEndpoint === undefined) {
      delete process.env.OTEL_EXPORTER_OTLP_ENDPOINT
    } else {
      process.env.OTEL_EXPORTER_OTLP_ENDPOINT = originalOtelExporterEndpoint
    }

    if (originalOtelMetricsEndpoint === undefined) {
      delete process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
    } else {
      process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT = originalOtelMetricsEndpoint
    }

    if (originalOtelMetricsHeaders === undefined) {
      delete process.env.OTEL_EXPORTER_OTLP_METRICS_HEADERS
    } else {
      process.env.OTEL_EXPORTER_OTLP_METRICS_HEADERS = originalOtelMetricsHeaders
    }

    for (const moduleId of mockedMetricsModules) {
      vi.doUnmock(moduleId)
    }

    vi.restoreAllMocks()
    vi.resetModules()
  })

  test('still shuts down meter provider when unregister throws', async () => {
    const shutdown = vi.fn().mockResolvedValue(undefined)
    const unregisterError = new Error('metrics unregister failed')
    const unregisterMetricInstrumentations = vi.fn(() => {
      throw unregisterError
    })
    const registerInstrumentations = vi.fn(() => unregisterMetricInstrumentations)
    const HostMetrics = vi.fn(function () {
      return {
        start: vi.fn(),
      }
    })
    const MeterProvider = vi.fn(function () {
      return {
        shutdown,
      }
    })
    const PrometheusExporter = vi.fn(function () {
      return {
        getMetricsRequestHandler: vi.fn(),
      }
    })
    const RuntimeNodeInstrumentation = vi.fn(function () {
      return {}
    })
    const StorageNodeInstrumentation = vi.fn(function () {
      return {}
    })
    const logger = {
      info: vi.fn(),
    }
    const logSchema = {
      error: vi.fn(),
      info: vi.fn(),
    }

    vi.doMock('../../config', () => ({
      getConfig: vi.fn(() => ({
        version: 'test-version',
        otelMetricsExportIntervalMs: 1000,
        otelMetricsEnabled: true,
        otelMetricsTemporality: 'CUMULATIVE',
        prometheusMetricsEnabled: true,
        region: 'local',
      })),
    }))
    vi.doMock('@internal/monitoring/logger', () => ({
      logger,
      logSchema,
    }))
    vi.doMock('@internal/monitoring/system', () => ({
      StorageNodeInstrumentation,
    }))
    vi.doMock('@opentelemetry/api', () => ({
      metrics: {
        setGlobalMeterProvider: vi.fn(),
      },
    }))
    vi.doMock('@opentelemetry/exporter-metrics-otlp-grpc', () => ({
      OTLPMetricExporter: vi.fn(function () {
        return {}
      }),
    }))
    vi.doMock('@opentelemetry/exporter-prometheus', () => ({
      PrometheusExporter,
    }))
    vi.doMock('@opentelemetry/host-metrics', () => ({
      HostMetrics,
    }))
    vi.doMock('@opentelemetry/instrumentation', () => ({
      registerInstrumentations,
    }))
    vi.doMock('@opentelemetry/instrumentation-runtime-node', () => ({
      RuntimeNodeInstrumentation,
    }))
    vi.doMock('@opentelemetry/resources', () => ({
      resourceFromAttributes: vi.fn(() => ({})),
    }))
    vi.doMock('@opentelemetry/sdk-metrics', () => ({
      AggregationTemporality: {
        CUMULATIVE: 'CUMULATIVE',
        DELTA: 'DELTA',
      },
      AggregationType: {
        DROP: 'DROP',
        EXPLICIT_BUCKET_HISTOGRAM: 'EXPLICIT_BUCKET_HISTOGRAM',
      },
      MeterProvider,
      PeriodicExportingMetricReader: vi.fn(),
    }))

    const shutdownOtelMetrics = await importOtelMetricsModule()

    await expect(shutdownOtelMetrics?.()).resolves.toBeUndefined()

    expect(unregisterMetricInstrumentations).toHaveBeenCalledTimes(1)
    expect(shutdown).toHaveBeenCalledTimes(1)
    expect(unregisterMetricInstrumentations.mock.invocationCallOrder[0]).toBeLessThan(
      shutdown.mock.invocationCallOrder[0]
    )
    expect(logSchema.error).toHaveBeenCalledWith(
      logger,
      '[OTel Metrics] Failed to unregister metric instrumentations',
      expect.objectContaining({ type: 'otel-metrics', error: unregisterError })
    )
  })

  test('does not create a Prometheus reader when Prometheus metrics are disabled', async () => {
    delete process.env.OTEL_EXPORTER_OTLP_ENDPOINT
    delete process.env.OTEL_EXPORTER_OTLP_METRICS_ENDPOINT

    const registerInstrumentations = vi.fn(() => vi.fn())
    const HostMetrics = vi.fn(function () {
      return {
        start: vi.fn(),
      }
    })
    const MeterProvider = vi.fn(function () {
      return {
        shutdown: vi.fn().mockResolvedValue(undefined),
      }
    })
    const PrometheusExporter = vi.fn(function () {
      return {
        getMetricsRequestHandler: vi.fn(),
      }
    })
    const RuntimeNodeInstrumentation = vi.fn(function () {
      return {}
    })
    const StorageNodeInstrumentation = vi.fn(function () {
      return {}
    })

    vi.doMock('../../config', () => ({
      getConfig: vi.fn(() => ({
        version: 'test-version',
        otelMetricsExportIntervalMs: 1000,
        otelMetricsEnabled: true,
        otelMetricsTemporality: 'CUMULATIVE',
        prometheusMetricsEnabled: false,
        region: 'local',
      })),
    }))
    vi.doMock('@internal/monitoring/logger', () => ({
      logger: { info: vi.fn() },
      logSchema: { error: vi.fn(), info: vi.fn() },
    }))
    vi.doMock('@internal/monitoring/system', () => ({
      StorageNodeInstrumentation,
    }))
    vi.doMock('@opentelemetry/api', () => ({
      metrics: {
        setGlobalMeterProvider: vi.fn(),
      },
    }))
    vi.doMock('@opentelemetry/exporter-metrics-otlp-grpc', () => ({
      OTLPMetricExporter: vi.fn(function () {
        return {}
      }),
    }))
    vi.doMock('@opentelemetry/exporter-prometheus', () => ({
      PrometheusExporter,
    }))
    vi.doMock('@opentelemetry/host-metrics', () => ({
      HostMetrics,
    }))
    vi.doMock('@opentelemetry/instrumentation', () => ({
      registerInstrumentations,
    }))
    vi.doMock('@opentelemetry/instrumentation-runtime-node', () => ({
      RuntimeNodeInstrumentation,
    }))
    vi.doMock('@opentelemetry/resources', () => ({
      resourceFromAttributes: vi.fn(() => ({})),
    }))
    vi.doMock('@opentelemetry/sdk-metrics', () => ({
      AggregationTemporality: {
        CUMULATIVE: 'CUMULATIVE',
        DELTA: 'DELTA',
      },
      AggregationType: {
        DROP: 'DROP',
        EXPLICIT_BUCKET_HISTOGRAM: 'EXPLICIT_BUCKET_HISTOGRAM',
      },
      MeterProvider,
      PeriodicExportingMetricReader: vi.fn(),
    }))

    await importOtelMetricsModule()

    expect(PrometheusExporter).not.toHaveBeenCalled()
    expect(MeterProvider).toHaveBeenCalledWith(
      expect.objectContaining({
        readers: [],
      })
    )
  })
})
