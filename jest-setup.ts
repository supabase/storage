import { getConfig, setEnvPaths } from './src/config'

setEnvPaths(['.env.test', '.env'])

interface OTelMetricsGlobalState {
  __otelMetricsShutdown?: () => Promise<void>
}

beforeEach(() => {
  getConfig({ reload: true })
})

afterAll(async () => {
  const shutdownOtelMetrics = (globalThis as typeof globalThis & OTelMetricsGlobalState)
    .__otelMetricsShutdown

  if (shutdownOtelMetrics) {
    await shutdownOtelMetrics()
  }
})
