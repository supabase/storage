import { Pool } from 'pg'
import { getConfig } from '../../config'
import { pgboss } from '../../internal/queue/adapters/pgboss'
import { describeQueueIntegration } from './queue-integration'

// The manager end to end over pg-boss on a real Postgres, on a throwaway schema that the
// first adapter's start() installs and the file drops at the end.
const SCHEMA = 'queue_pgboss_integration_test'
const url =
  process.env.QUEUE_TEST_DATABASE_URL || getConfig().pgQueueConnectionURL || getConfig().databaseURL

// Multigres cannot install pg-boss's schema (its DDL runs dynamic SQL inside PL/pgSQL).
describe.skipIf(getConfig().databaseEngine === 'multigres')('pgboss queue', () => {
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

  describeQueueIntegration({
    name: 'pgboss',
    // Adapter-owned, so pg-boss's own supervisor runs the expiry sweeps; on a 1s cadence.
    createAdapter: () =>
      pgboss({
        connectionString: url,
        bossOptions: { schema: SCHEMA, monitorIntervalSeconds: 1, superviseIntervalSeconds: 1 },
      }),
    dedup: {},
    budgetFreeRelease: true,
    // pg-boss floors a queue's heartbeat window at 10s.
    livenessMs: 10_000,
  })
})
