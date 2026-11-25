import { getServiceKeyUser } from '@internal/database'
import type { TenantConnectionOptions } from '@internal/database/pool'
import { getConfig } from '../config'

const { databaseURL, databasePoolURL, tenantId } = getConfig()

// The pools pipeline only with DATABASE_ENABLE_PIPELINING, read when the module loads.
async function loadPipeliningPgConnection() {
  vi.resetModules()
  const config = await import('../config')
  config.getConfig({ reload: true })
  config.mergeConfig({ databaseEnablePipelining: true })
  return import('../internal/database/pg-connection')
}

describe('Pg pipelined transactions', () => {
  let pgConnection: Awaited<ReturnType<typeof loadPipeliningPgConnection>>

  beforeAll(async () => {
    pgConnection = await loadPipeliningPgConnection()
  })

  afterAll(() => {
    vi.resetModules()
  })

  afterEach(async () => {
    await pgConnection.PgTenantConnection.stop()
  })

  async function createConnection(overrides: Partial<TenantConnectionOptions> = {}) {
    const superUser = await getServiceKeyUser(tenantId)
    const settings: TenantConnectionOptions = {
      tenantId,
      isExternalPool: true,
      maxConnections: 1,
      dbUrl: databasePoolURL || databaseURL,
      user: superUser,
      superUser,
      headers: { 'x-test-header': 'pg-pipelined' },
      method: 'GET',
      path: '/pg-pipelined',
      operation: () => 'pg-pipelined-test',
      ...overrides,
    }
    const pool = new pgConnection.PgPoolStrategy(settings)
    const connection = new pgConnection.PgTenantConnection(
      { value: pool, release: () => {} },
      settings
    )

    return { connection, pool, superUser }
  }

  it('runs a single statement with the scope and settings of a transaction', async () => {
    const { connection, pool, superUser } = await createConnection()

    try {
      const executor = connection.singleStatementExecutor?.({ timeout: 1234 })
      const result = await executor?.query<{
        role: string
        jwt_role: string
        request_path: string
        statement_timeout: string
        search_path: string
      }>(`
        SELECT
          current_setting('role') AS role,
          current_setting('request.jwt.claim.role', true) AS jwt_role,
          current_setting('request.path', true) AS request_path,
          current_setting('statement_timeout') AS statement_timeout,
          current_setting('search_path') AS search_path
      `)

      expect(result?.rows[0]).toMatchObject({
        role: superUser.payload.role,
        jwt_role: superUser.payload.role,
        request_path: '/pg-pipelined',
        statement_timeout: '1234ms',
      })
      expect(result?.rows[0].search_path).toContain('storage')

      // COMMIT ended the transaction: its local settings are gone and the client is idle.
      const after = await pool
        .acquire()
        .query<{ jwt_role: string | null }>(
          "SELECT current_setting('request.jwt.claim.role', true) AS jwt_role"
        )
      expect(after.rows[0].jwt_role || null).toBeNull()
      expect(pool.getPoolStats()).toEqual({ total: 1, used: 0 })
    } finally {
      await pool.dispose('destroy')
    }
  })

  it('rolls back a failing statement and keeps the client', async () => {
    const { connection, pool } = await createConnection()

    try {
      await expect(
        connection
          .singleStatementExecutor?.()
          .query('SELECT * FROM pg_pipelined_missing_table_for_rollback_test')
      ).rejects.toMatchObject({ code: '42P01' })

      expect(pool.getPoolStats()).toEqual({ total: 1, used: 0 })
      const next = await pool.acquire().query<{ n: number }>('SELECT 1 AS n')
      expect(next.rows[0].n).toBe(1)
    } finally {
      await pool.dispose('destroy')
    }
  })

  it('sends nothing behind a BEGIN that fails', async () => {
    const { pool } = await createConnection()
    const table = 'pg_pipelined_after_failed_begin'

    try {
      await pool.acquire().query(`CREATE TABLE ${table} (n int)`)

      // pg syncs every pipelined query: an INSERT sent along with the BEGIN would still
      // run, in a transaction of its own.
      await expect(
        pool
          .acquire()
          .runTransaction('BEGIN ISOLATION LEVEL NONE', [
            { text: `INSERT INTO ${table} VALUES (1)` },
          ])
      ).rejects.toMatchObject({ code: '42601' })

      const rows = await pool
        .acquire()
        .query<{ count: number }>(`SELECT count(*)::int AS count FROM ${table}`)
      expect(rows.rows[0].count).toBe(0)
    } finally {
      await pool.acquire().query(`DROP TABLE IF EXISTS ${table}`)
      await pool.dispose('destroy')
    }
  })

  it('switches between the super user and the request scope inside a batch', async () => {
    const superUser = await getServiceKeyUser(tenantId)
    const { connection, pool } = await createConnection({
      user: { jwt: 'request-jwt', payload: { role: 'authenticated', sub: 'pipelined-user' } },
      superUser,
    })

    try {
      const roleOf = { text: "SELECT current_setting('role') AS role" }
      const results = await connection.scopedBatch?.([
        { ...roleOf, asSuperUser: true },
        roleOf,
        { ...roleOf, asSuperUser: true },
      ])

      expect(results?.map((result) => result.rows[0].role)).toEqual([
        superUser.payload.role,
        'authenticated',
        superUser.payload.role,
      ])
      expect(pool.getPoolStats()).toEqual({ total: 1, used: 0 })
    } finally {
      await pool.dispose('destroy')
    }
  })
})
