import { Pool } from 'pg'
import { getConfig } from '../../config'
import { describeAdapterConformance } from '../../internal/queue/adapters/conformance'
import { pgboss } from '../../internal/queue/adapters/pgboss'

// The portable adapter contract against a real Postgres, on a throwaway pg-boss schema that
// the first adapter's start() installs and the file drops at the end.
const SCHEMA = 'queue_pgboss_conformance_test'
const url =
  process.env.QUEUE_TEST_DATABASE_URL || getConfig().pgQueueConnectionURL || getConfig().databaseURL

// pg-boss floors a queue's heartbeat window at 10s, so that is the shortest liveness contract
// it can honour; the three redelivery tests each wait it out.
vi.setConfig({ testTimeout: 60_000 })

// Multigres cannot install pg-boss's schema (its DDL runs dynamic SQL inside PL/pgSQL).
describe.skipIf(getConfig().databaseEngine === 'multigres')('pgboss queue adapter', () => {
  const dropSchema = async () => {
    const pool = new Pool({ connectionString: url, max: 1 })
    try {
      await pool.query(`drop schema if exists ${SCHEMA} cascade`)
    } finally {
      await pool.end()
    }
  }
  beforeAll(dropSchema)
  afterAll(dropSchema)

  describeAdapterConformance({
    name: 'pgboss',
    // Adapter-owned, so pg-boss's own supervisor runs the expiry sweeps; on a 1s cadence.
    create: () =>
      pgboss({
        connectionString: url,
        bossOptions: { schema: SCHEMA, monitorIntervalSeconds: 1, superviseIntervalSeconds: 1 },
      }),
    capabilities: {
      // `retryAccounting` stays at its default ('explicit'): a release keeps the same attempt.
      // Only a nack or an expiry (crash, lost heartbeat) advances the attempt.
      // start_after is whole seconds.
      timing: { livenessMs: 10_000, waitMs: 8_000, quietMs: 150, delayMs: 1_000 },
    },
  })
})
