import { logger, logSchema } from '@internal/monitoring'
import { runMigrations } from '@supabase-labs/wave-adapter-pgque'
import { Pool } from 'pg'
import { queueConnectionConfig } from './database'

/**
 * Provision the pgque adapter's whole database footprint before the wave starts: wave's
 * bundled sql/ steps create the `pgque` schema — engine, migration ledger, and membership
 * tables — idempotently, under an advisory lock (concurrent booting nodes serialize).
 *
 * Always runs on a DIRECT connection (`queueConnectionConfig().directUrl`) — never the
 * pooled queue URL: migrations must pin one backend connection, which a transaction pooler
 * cannot guarantee. The adapter itself starts with `skipMigrations` and may therefore sit
 * behind PgBouncer (see `createQueuePgPool` in database.ts).
 */
export async function runPgqueMigrations(): Promise<void> {
  const { directUrl } = queueConnectionConfig()

  const pool = new Pool({ connectionString: directUrl, max: 1 })
  // An unlistened pool 'error' (the client dropping outside a query) kills the process.
  pool.on('error', (error) => {
    logSchema.error(logger, '[Queue] pgque migration pool error', { type: 'queue', error })
  })
  try {
    await runMigrations(pool)
    logSchema.info(logger, '[Queue] pgque migrations applied', { type: 'queue' })
  } finally {
    await pool.end()
  }
}
