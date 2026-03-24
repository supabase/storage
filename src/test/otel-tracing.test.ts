interface OTelGlobalState {
  __otelTracingShutdown?: () => Promise<void>
}

interface Deferred<T> {
  promise: Promise<T>
  resolve: (value: T) => void
}

function createDeferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise
  })

  return { promise, resolve }
}

describe('otel tracing bootstrap', () => {
  const originalTracingEnabled = process.env.TRACING_ENABLED
  const originalTraceEndpoint = process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT

  beforeEach(() => {
    jest.resetModules()
    jest.clearAllMocks()
    process.env.TRACING_ENABLED = 'true'
    process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = 'http://127.0.0.1:4317'
  })

  afterEach(async () => {
    const otelGlobalState = globalThis as typeof globalThis & OTelGlobalState

    if (otelGlobalState.__otelTracingShutdown) {
      await otelGlobalState.__otelTracingShutdown()
      delete otelGlobalState.__otelTracingShutdown
    }

    if (originalTracingEnabled === undefined) {
      delete process.env.TRACING_ENABLED
    } else {
      process.env.TRACING_ENABLED = originalTracingEnabled
    }

    if (originalTraceEndpoint === undefined) {
      delete process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
    } else {
      process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = originalTraceEndpoint
    }

    jest.restoreAllMocks()
  })

  test('does not let tracing sdk create a hidden metrics pipeline', async () => {
    const start = jest.fn()
    const shutdown = jest.fn().mockResolvedValue(undefined)
    const NodeSDK = jest.fn().mockImplementation(() => ({
      start,
      shutdown,
    }))
    const registerInstrumentations = jest.fn(() => jest.fn())
    const getNodeAutoInstrumentations = jest.fn().mockReturnValue([])
    const FastifyOtelInstrumentation = jest.fn().mockImplementation((config) => ({
      instrumentationName: '@fastify/otel',
      config,
    }))
    const OTLPTraceExporter = jest.fn().mockImplementation(() => ({}))

    jest.doMock('@opentelemetry/sdk-node', () => ({
      NodeSDK,
    }))
    jest.doMock('@opentelemetry/instrumentation', () => ({
      registerInstrumentations,
    }))
    jest.doMock('@opentelemetry/auto-instrumentations-node', () => ({
      getNodeAutoInstrumentations,
    }))
    jest.doMock('@fastify/otel', () => ({
      FastifyOtelInstrumentation,
    }))
    jest.doMock('@opentelemetry/exporter-trace-otlp-grpc', () => ({
      OTLPTraceExporter,
    }))

    await jest.isolateModulesAsync(async () => {
      await import('../internal/monitoring/otel-tracing')
    })

    expect(NodeSDK).toHaveBeenCalledWith(
      expect.objectContaining({
        metricReaders: [],
      })
    )

    expect(getNodeAutoInstrumentations).toHaveBeenCalledWith(
      expect.objectContaining({
        '@opentelemetry/instrumentation-runtime-node': expect.objectContaining({
          enabled: false,
        }),
      })
    )

    expect(registerInstrumentations).toHaveBeenCalled()
    expect(start).toHaveBeenCalled()
  })

  test('does not register class instrumentations after shutdown starts', async () => {
    const start = jest.fn()
    const shutdown = jest.fn().mockResolvedValue(undefined)
    const unregisterTracingInstrumentations = jest.fn()
    const unregisterClassInstrumentations = jest.fn()
    const NodeSDK = jest.fn().mockImplementation(() => ({
      start,
      shutdown,
    }))
    const registerInstrumentations = jest
      .fn()
      .mockReturnValueOnce(unregisterTracingInstrumentations)
      .mockReturnValueOnce(unregisterClassInstrumentations)
    const getNodeAutoInstrumentations = jest.fn().mockReturnValue([])
    const FastifyOtelInstrumentation = jest.fn().mockImplementation((config) => ({
      instrumentationName: '@fastify/otel',
      config,
    }))
    const OTLPTraceExporter = jest.fn().mockImplementation(() => ({}))
    const classInstrumentationsDeferred = createDeferred<{ classInstrumentations: unknown[] }>()

    jest.doMock('@opentelemetry/sdk-node', () => ({
      NodeSDK,
    }))
    jest.doMock('@opentelemetry/instrumentation', () => ({
      registerInstrumentations,
    }))
    jest.doMock('@opentelemetry/auto-instrumentations-node', () => ({
      getNodeAutoInstrumentations,
    }))
    jest.doMock('@fastify/otel', () => ({
      FastifyOtelInstrumentation,
    }))
    jest.doMock('@opentelemetry/exporter-trace-otlp-grpc', () => ({
      OTLPTraceExporter,
    }))
    jest.doMock('../internal/monitoring/otel-class-instrumentations', () => ({
      loadClassInstrumentations: jest.fn(() => classInstrumentationsDeferred.promise),
    }))

    let shutdownOtelTracing: (() => Promise<void>) | undefined

    await jest.isolateModulesAsync(async () => {
      await import('../internal/monitoring/otel-tracing')
      shutdownOtelTracing = (globalThis as typeof globalThis & OTelGlobalState)
        .__otelTracingShutdown
    })

    const shutdownPromise = shutdownOtelTracing?.()

    classInstrumentationsDeferred.resolve({ classInstrumentations: [] })

    await shutdownPromise

    expect(registerInstrumentations).toHaveBeenCalledTimes(1)
    expect(unregisterTracingInstrumentations).toHaveBeenCalledTimes(1)
    expect(unregisterClassInstrumentations).not.toHaveBeenCalled()
    expect(shutdown).toHaveBeenCalledTimes(1)
  })

  test('still shuts down sdk when unregister callbacks throw', async () => {
    const start = jest.fn()
    const shutdown = jest.fn().mockResolvedValue(undefined)
    const unregisterTracingError = new Error('tracing unregister failed')
    const unregisterClassError = new Error('class unregister failed')
    const unregisterTracingInstrumentations = jest.fn(() => {
      throw unregisterTracingError
    })
    const unregisterClassInstrumentations = jest.fn(() => {
      throw unregisterClassError
    })
    const NodeSDK = jest.fn().mockImplementation(() => ({
      start,
      shutdown,
    }))
    const registerInstrumentations = jest
      .fn()
      .mockReturnValueOnce(unregisterTracingInstrumentations)
      .mockReturnValueOnce(unregisterClassInstrumentations)
    const getNodeAutoInstrumentations = jest.fn().mockReturnValue([])
    const FastifyOtelInstrumentation = jest.fn().mockImplementation((config) => ({
      instrumentationName: '@fastify/otel',
      config,
    }))
    const OTLPTraceExporter = jest.fn().mockImplementation(() => ({}))
    const logSchema = {
      error: jest.fn(),
      info: jest.fn(),
      warning: jest.fn(),
    }

    jest.doMock('@opentelemetry/sdk-node', () => ({
      NodeSDK,
    }))
    jest.doMock('@opentelemetry/instrumentation', () => ({
      registerInstrumentations,
    }))
    jest.doMock('@opentelemetry/auto-instrumentations-node', () => ({
      getNodeAutoInstrumentations,
    }))
    jest.doMock('@fastify/otel', () => ({
      FastifyOtelInstrumentation,
    }))
    jest.doMock('@opentelemetry/exporter-trace-otlp-grpc', () => ({
      OTLPTraceExporter,
    }))
    jest.doMock('@internal/monitoring/logger', () => ({
      logger: {},
      logSchema,
    }))
    jest.doMock('../internal/monitoring/otel-class-instrumentations', () => ({
      loadClassInstrumentations: jest.fn(async () => []),
    }))

    let shutdownOtelTracing: (() => Promise<void>) | undefined

    await jest.isolateModulesAsync(async () => {
      await import('../internal/monitoring/otel-tracing')
      shutdownOtelTracing = (globalThis as typeof globalThis & OTelGlobalState)
        .__otelTracingShutdown
    })

    await expect(shutdownOtelTracing?.()).resolves.toBeUndefined()

    expect(unregisterClassInstrumentations).toHaveBeenCalledTimes(1)
    expect(unregisterTracingInstrumentations).toHaveBeenCalledTimes(1)
    expect(shutdown).toHaveBeenCalledTimes(1)
    expect(logSchema.error).toHaveBeenCalledWith(
      {},
      '[Otel] Failed to unregister class instrumentations',
      expect.objectContaining({ type: 'otel', error: unregisterClassError })
    )
    expect(logSchema.error).toHaveBeenCalledWith(
      {},
      '[Otel] Failed to unregister tracing instrumentations',
      expect.objectContaining({ type: 'otel', error: unregisterTracingError })
    )
  })
})
