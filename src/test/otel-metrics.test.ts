interface OTelGlobalState {
  __otelMetricsShutdown?: () => Promise<void>
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

    jest.restoreAllMocks()
    jest.resetModules()
  })

  test('still shuts down meter provider when unregister throws', async () => {
    const shutdown = jest.fn().mockResolvedValue(undefined)
    const unregisterError = new Error('metrics unregister failed')
    const unregisterMetricInstrumentations = jest.fn(() => {
      throw unregisterError
    })
    const registerInstrumentations = jest.fn(() => unregisterMetricInstrumentations)
    const HostMetrics = jest.fn().mockImplementation(() => ({
      start: jest.fn(),
    }))
    const MeterProvider = jest.fn().mockImplementation(() => ({
      shutdown,
    }))
    const PrometheusExporter = jest.fn().mockImplementation(() => ({
      getMetricsRequestHandler: jest.fn(),
    }))
    const RuntimeNodeInstrumentation = jest.fn().mockImplementation(() => ({}))
    const StorageNodeInstrumentation = jest.fn().mockImplementation(() => ({}))
    const logger = {
      info: jest.fn(),
    }
    const logSchema = {
      error: jest.fn(),
      info: jest.fn(),
    }

    jest.doMock('../config', () => ({
      getConfig: jest.fn(() => ({
        version: 'test-version',
        otelMetricsExportIntervalMs: 1000,
        otelMetricsEnabled: true,
        otelMetricsTemporality: 'CUMULATIVE',
        prometheusMetricsEnabled: true,
        region: 'local',
      })),
    }))
    jest.doMock('@internal/monitoring/logger', () => ({
      logger,
      logSchema,
    }))
    jest.doMock('@internal/monitoring/system', () => ({
      StorageNodeInstrumentation,
    }))
    jest.doMock('@opentelemetry/api', () => ({
      metrics: {
        setGlobalMeterProvider: jest.fn(),
      },
    }))
    jest.doMock('@opentelemetry/exporter-metrics-otlp-grpc', () => ({
      OTLPMetricExporter: jest.fn(),
    }))
    jest.doMock('@opentelemetry/exporter-prometheus', () => ({
      PrometheusExporter,
    }))
    jest.doMock('@opentelemetry/host-metrics', () => ({
      HostMetrics,
    }))
    jest.doMock('@opentelemetry/instrumentation', () => ({
      registerInstrumentations,
    }))
    jest.doMock('@opentelemetry/instrumentation-runtime-node', () => ({
      RuntimeNodeInstrumentation,
    }))
    jest.doMock('@opentelemetry/resources', () => ({
      resourceFromAttributes: jest.fn(() => ({})),
    }))
    jest.doMock('@opentelemetry/sdk-metrics', () => ({
      AggregationTemporality: {
        CUMULATIVE: 'CUMULATIVE',
        DELTA: 'DELTA',
      },
      AggregationType: {
        DROP: 'DROP',
        EXPLICIT_BUCKET_HISTOGRAM: 'EXPLICIT_BUCKET_HISTOGRAM',
      },
      MeterProvider,
      PeriodicExportingMetricReader: jest.fn(),
    }))

    let shutdownOtelMetrics: (() => Promise<void>) | undefined

    await jest.isolateModulesAsync(async () => {
      await import('../internal/monitoring/otel-metrics')
      shutdownOtelMetrics = (globalThis as typeof globalThis & OTelGlobalState)
        .__otelMetricsShutdown
    })

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

    const registerInstrumentations = jest.fn(() => jest.fn())
    const HostMetrics = jest.fn().mockImplementation(() => ({
      start: jest.fn(),
    }))
    const MeterProvider = jest.fn().mockImplementation(() => ({
      shutdown: jest.fn().mockResolvedValue(undefined),
    }))
    const PrometheusExporter = jest.fn().mockImplementation(() => ({
      getMetricsRequestHandler: jest.fn(),
    }))
    const RuntimeNodeInstrumentation = jest.fn().mockImplementation(() => ({}))
    const StorageNodeInstrumentation = jest.fn().mockImplementation(() => ({}))

    jest.doMock('../config', () => ({
      getConfig: jest.fn(() => ({
        version: 'test-version',
        otelMetricsExportIntervalMs: 1000,
        otelMetricsEnabled: true,
        otelMetricsTemporality: 'CUMULATIVE',
        prometheusMetricsEnabled: false,
        region: 'local',
      })),
    }))
    jest.doMock('@internal/monitoring/logger', () => ({
      logger: { info: jest.fn() },
      logSchema: { error: jest.fn(), info: jest.fn() },
    }))
    jest.doMock('@internal/monitoring/system', () => ({
      StorageNodeInstrumentation,
    }))
    jest.doMock('@opentelemetry/api', () => ({
      metrics: {
        setGlobalMeterProvider: jest.fn(),
      },
    }))
    jest.doMock('@opentelemetry/exporter-metrics-otlp-grpc', () => ({
      OTLPMetricExporter: jest.fn(),
    }))
    jest.doMock('@opentelemetry/exporter-prometheus', () => ({
      PrometheusExporter,
    }))
    jest.doMock('@opentelemetry/host-metrics', () => ({
      HostMetrics,
    }))
    jest.doMock('@opentelemetry/instrumentation', () => ({
      registerInstrumentations,
    }))
    jest.doMock('@opentelemetry/instrumentation-runtime-node', () => ({
      RuntimeNodeInstrumentation,
    }))
    jest.doMock('@opentelemetry/resources', () => ({
      resourceFromAttributes: jest.fn(() => ({})),
    }))
    jest.doMock('@opentelemetry/sdk-metrics', () => ({
      AggregationTemporality: {
        CUMULATIVE: 'CUMULATIVE',
        DELTA: 'DELTA',
      },
      AggregationType: {
        DROP: 'DROP',
        EXPLICIT_BUCKET_HISTOGRAM: 'EXPLICIT_BUCKET_HISTOGRAM',
      },
      MeterProvider,
      PeriodicExportingMetricReader: jest.fn(),
    }))

    await jest.isolateModulesAsync(async () => {
      await import('../internal/monitoring/otel-metrics')
    })

    expect(PrometheusExporter).not.toHaveBeenCalled()
    expect(MeterProvider).toHaveBeenCalledWith(
      expect.objectContaining({
        readers: [],
      })
    )
  })
})
