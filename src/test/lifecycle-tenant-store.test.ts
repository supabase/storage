import { randomUUID } from 'node:crypto'
import {
  closeMultitenantPg,
  type DatabaseTransaction,
  LifecycleTenantStorePg,
  multitenantPgExecutor,
} from '@internal/database'
import { PgPoolExecutor } from '@internal/database/pg-connection'
import { Pool } from 'pg'
import { getConfig } from '../config'

describe('lifecycle tenant retry scheduling', () => {
  let transaction: DatabaseTransaction | undefined
  let store: LifecycleTenantStorePg
  const claimId = '49f3e225-7455-4b2e-bf49-2ca7abc80b49'

  beforeEach(async () => {
    transaction = await multitenantPgExecutor.beginTransaction()
    await transaction.query(`
      CREATE TEMP TABLE lifecycle_tenants (
        tenant_id text PRIMARY KEY,
        next_dispatch_at timestamptz NOT NULL,
        claim_id uuid,
        claim_until timestamptz,
        last_dispatched_at timestamptz,
        last_completed_at timestamptz,
        last_success_at timestamptz,
        failure_count integer NOT NULL DEFAULT 0,
        last_error jsonb
      ) ON COMMIT DROP
    `)
    store = new LifecycleTenantStorePg(transaction)
  })

  afterEach(async () => {
    await transaction?.rollback()
    transaction = undefined
  })

  afterAll(() => closeMultitenantPg())

  it.each([
    [0, 300],
    [1, 600],
    [6, 19200],
    [7, 21600],
    [100, 21600],
  ])('backs off %i prior failures by %i seconds', async (failureCount, delaySeconds) => {
    await transaction!.query({
      text: `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, failure_count)
        VALUES ('tenant-a', 'infinity', $1, $2)`,
      values: [claimId, failureCount],
    })

    await expect(store.failTenantDispatch('tenant-a', claimId, { code: 'offline' })).resolves.toBe(
      true
    )

    const result = await transaction!.query(`
      SELECT extract(epoch FROM next_dispatch_at - last_completed_at)::float8 AS delay,
        failure_count, claim_id, last_error
      FROM lifecycle_tenants
    `)
    expect(result.rows[0]).toMatchObject({
      failure_count: failureCount + 1,
      claim_id: null,
      last_error: { code: 'offline' },
    })
    expect(result.rows[0].delay).toBeCloseTo(delaySeconds, 1)
  })

  it('preserves a concurrent wake and ignores a stale claim', async () => {
    await transaction!.query({
      text: `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, failure_count)
        VALUES ('tenant-a', '2026-01-01T00:00:00Z', $1, 4)`,
      values: [claimId],
    })
    await expect(store.failTenantDispatch('tenant-a', claimId, { code: 'offline' })).resolves.toBe(
      true
    )
    await expect(store.failTenantDispatch('tenant-a', claimId, { code: 'stale' })).resolves.toBe(
      false
    )
    const result = await transaction!.query(`SELECT * FROM lifecycle_tenants`)
    expect(result.rows[0]).toMatchObject({
      next_dispatch_at: new Date('2026-01-01T00:00:00Z'),
      failure_count: 5,
      last_error: { code: 'offline' },
    })
  })

  it('preserves a second wake when a dispatcher consumed the pre-commit wake', async () => {
    await store.wakeTenant('tenant-a')
    const [claim] = await store.claimDueTenants(1, 60_000)
    expect(claim.tenantId).toBe('tenant-a')

    await store.wakeTenant('tenant-a')
    await expect(
      store.completeTenantDispatch('tenant-a', claim.claimId, '2099-01-01T00:00:00Z')
    ).resolves.toBe(true)

    const result = await transaction!.query(`
      SELECT next_dispatch_at <= clock_timestamp() AS due, claim_id
      FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(result.rows).toEqual([{ due: true, claim_id: null }])
  })

  it('reclaims an expired dispatch at infinity without another wake', async () => {
    await store.wakeTenant('tenant-a')
    const [first] = await store.claimDueTenants(1, 60_000)
    expect(first.tenantId).toBe('tenant-a')
    await transaction!.query(`
      UPDATE lifecycle_tenants SET claim_until = clock_timestamp() - interval '1 second'
      WHERE tenant_id = 'tenant-a'
    `)

    const [recovered] = await store.claimDueTenants(1, 60_000)
    expect(recovered.tenantId).toBe('tenant-a')
    expect(recovered.claimId).not.toBe(first.claimId)
    await expect(
      store.completeTenantDispatch('tenant-a', first.claimId, new Date().toISOString())
    ).resolves.toBe(false)
  })

  it('revisits a completed dispatch without another wake', async () => {
    await store.wakeTenant('tenant-a')
    const [first] = await store.claimDueTenants(1, 60_000)
    const revisitAt = new Date(Date.now() + 24 * 60 * 60 * 1000)
    await expect(
      store.completeTenantDispatch('tenant-a', first.claimId, revisitAt.toISOString())
    ).resolves.toBe(true)
    const result = await transaction!.query(`
      SELECT next_dispatch_at, claim_id FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(result.rows).toEqual([{ next_dispatch_at: revisitAt, claim_id: null }])
    await expect(store.claimDueTenants(1, 60_000)).resolves.toEqual([])

    // Make the saved revisit due without waiting for the real interval.
    await transaction!.query(`
      UPDATE lifecycle_tenants SET next_dispatch_at = clock_timestamp() - interval '1 second'
      WHERE tenant_id = 'tenant-a'
    `)
    const [revisited] = await store.claimDueTenants(1, 60_000)
    expect(revisited.tenantId).toBe('tenant-a')
    expect(revisited.claimId).not.toBe(first.claimId)
  })

  it('cancels a central wake blocked on a row lock', async () => {
    const schema = `wake_${randomUUID().replaceAll('-', '')}`
    const pool = new Pool({
      connectionString: getConfig().multitenantDatabaseUrl,
      options: `-c search_path=${schema} -c statement_timeout=5000`,
      max: 3,
    })
    const executor = new PgPoolExecutor(pool)
    let lock: DatabaseTransaction | undefined
    let wake: Promise<unknown> | undefined
    const controller = new AbortController()
    try {
      await pool.query(`CREATE SCHEMA ${schema}`)
      await pool.query(
        'CREATE TABLE lifecycle_tenants (tenant_id text PRIMARY KEY, next_dispatch_at timestamptz NOT NULL)'
      )
      const store = new LifecycleTenantStorePg(executor)
      await store.wakeTenant('tenant-a')
      lock = await executor.beginTransaction()
      await lock.query("SELECT * FROM lifecycle_tenants WHERE tenant_id = 'tenant-a' FOR UPDATE")
      wake = store.wakeTenant('tenant-a', controller.signal).then(
        () => ({ resolved: true }),
        (error: unknown) => error
      )
      await expect
        .poll(
          async () =>
            (
              await pool.query(
                "SELECT count(*)::int AS count FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%INSERT INTO lifecycle_tenants%'"
              )
            ).rows[0].count
        )
        .toBe(1)
      const reason = new Error('configuration deadline')
      controller.abort(reason)
      expect(await wake).toMatchObject({ name: 'AbortError', code: 'ABORT_ERR' })
      expect(lock.isCompleted()).toBe(false)
      await expect
        .poll(
          async () =>
            (
              await pool.query(
                "SELECT count(*)::int AS count FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%INSERT INTO lifecycle_tenants%'"
              )
            ).rows[0].count
        )
        .toBe(0)
    } finally {
      controller.abort()
      await lock?.rollback()
      await wake
      await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`)
      await pool.end()
    }
  })
})
