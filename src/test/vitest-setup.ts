import { getConfig, setEnvPaths } from '../config'

setEnvPaths(['.env.test', '.env'])

interface OTelGlobalState {
  __otelMetricsShutdown?: () => Promise<void>
  __otelTracingShutdown?: () => Promise<void>
}

beforeAll(async () => {
  // Production boots the queue in start/server.ts before serving requests; tests build the app
  // straight from src/app.ts, so install one here — always the in-process sync queue (v1 parity:
  // produce runs handlers inline, no queue DB connection), even in files that set
  // PG_QUEUE_ENABLE=true to exercise queue-enabled app branches: test setup must never
  // construct a real pg-boss. Files that stub the queue (mockQueue/setQueueForTesting) still
  // override this instance as before. Deferred imports — static ones would hoist above
  // setEnvPaths and read config before .env.test applies.
  const [{ startSyncQueueForTesting }, { buildHandlers, storageQueues }] = await Promise.all([
    import('../internal/queue'),
    import('../storage/events'),
  ])
  await startSyncQueueForTesting({ queues: storageQueues, handlers: buildHandlers() })
})

beforeEach(() => {
  getConfig({ reload: true })
})

afterEach(() => {
  vi.useRealTimers()
})

afterAll(async () => {
  const otelGlobalState = globalThis as typeof globalThis & OTelGlobalState
  const shutdownOtelTracing = otelGlobalState.__otelTracingShutdown
  const shutdownOtelMetrics = otelGlobalState.__otelMetricsShutdown

  if (shutdownOtelTracing) {
    await shutdownOtelTracing()
  }

  if (shutdownOtelMetrics) {
    await shutdownOtelMetrics()
  }
})
