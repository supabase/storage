import { afterAll, beforeEach } from 'vitest'
import { getConfig, setEnvPaths } from '../src/config'

// Load test env BEFORE anything else touches config. setEnvPaths only stores
// the paths; dotenv.config only runs inside getConfig(), so we *must* call
// getConfig here so process.env is populated by the time the first test file
// module body executes. Without this, tests that open a knex connection in
// beforeAll (e.g. database-protection) see an unset DATABASE_URL.
setEnvPaths(['.env.test', '.env'])
getConfig({ reload: true })

interface OTelGlobalState {
  __otelMetricsShutdown?: () => Promise<void>
  __otelTracingShutdown?: () => Promise<void>
}

beforeEach(() => {
  // Force a fresh config read so per-test env overrides (mergeConfig / process.env
  // mutations) are honored without leaking across tests.
  getConfig({ reload: true })
})

afterAll(async () => {
  const otelGlobalState = globalThis as typeof globalThis & OTelGlobalState
  await otelGlobalState.__otelTracingShutdown?.()
  await otelGlobalState.__otelMetricsShutdown?.()
})
