import { randomUUID } from 'node:crypto'
import { readdir, readFile } from 'node:fs/promises'
import {
  closeMultitenantPg,
  type DatabaseTransaction,
  LifecycleTenantStorePg,
  multitenantPgExecutor,
} from '@internal/database'
import { runMultitenantMigrations } from '@internal/database/migrations'
import { PgPoolExecutor } from '@internal/database/pg-connection'
import { Pool } from 'pg'
import { getConfig } from '../config'

describe('lifecycle tenant retry scheduling', () => {
  let transaction: DatabaseTransaction | undefined
  let store: LifecycleTenantStorePg
  const claimId = '49f3e225-7455-4b2e-bf49-2ca7abc80b49'

  beforeAll(() => runMultitenantMigrations())

  beforeEach(async () => {
    transaction = await multitenantPgExecutor.beginTransaction()
    await transaction.query(`
      INSERT INTO tenants (id, anon_key, database_url, jwt_secret, service_key)
      SELECT id, 'anon', 'url', 'jwt', 'service'
      FROM unnest(ARRAY['tenant-a', 'due-old', 'due-new', 'expired-old', 'expired-new', 'claimed', 'future']) AS id
    `)
    await transaction.query('DELETE FROM lifecycle_tenants')
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
      text: `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until, failure_count)
        VALUES ('tenant-a', 'infinity', $1, now() + interval '1 hour', $2)`,
      values: [claimId, failureCount],
    })

    await expect(store.failTenantDispatch('tenant-a', claimId, { code: 'offline' })).resolves.toBe(
      true
    )

    const result = await transaction!.query(`
      SELECT extract(epoch FROM next_dispatch_at - last_completed_at)::float8 AS delay,
        failure_count, claim_id, claim_until, last_error
      FROM lifecycle_tenants
    `)
    expect(result.rows[0]).toMatchObject({
      failure_count: failureCount + 1,
      claim_id: null,
      claim_until: null,
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
    await expect(store.claimDueTenants(1, 60_000)).resolves.toEqual([])
    await expect(
      store.completeTenantDispatch('tenant-a', claim.claimId, '2099-01-01T00:00:00Z')
    ).resolves.toBe(true)

    const result = await transaction!.query(`
      SELECT next_dispatch_at <= clock_timestamp() AS due, claim_id
      FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(result.rows).toEqual([{ due: true, claim_id: null }])
  })

  it('reclaims an expired dispatch at infinity keeping its claim id, so the pending job can still complete', async () => {
    await store.wakeTenant('tenant-a')
    const [first] = await store.claimDueTenants(1, 60_000)
    expect(first.tenantId).toBe('tenant-a')
    await transaction!.query(`
      UPDATE lifecycle_tenants SET claim_until = clock_timestamp() - interval '1 second'
      WHERE tenant_id = 'tenant-a'
    `)

    const [recovered] = await store.claimDueTenants(1, 60_000)
    expect(recovered.tenantId).toBe('tenant-a')
    expect(recovered.claimId).toBe(first.claimId)
    await expect(
      store.completeTenantDispatch('tenant-a', first.claimId, new Date().toISOString())
    ).resolves.toBe(true)
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

  it('completes after prior failures by resetting the failure state', async () => {
    await transaction!.query({
      text: `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, failure_count, last_error)
        VALUES ('tenant-a', 'infinity', $1, 3, '{"code":"offline"}')`,
      values: [claimId],
    })

    await expect(
      store.completeTenantDispatch('tenant-a', claimId, '2099-01-01T00:00:00Z')
    ).resolves.toBe(true)

    const result = await transaction!.query(`
      SELECT failure_count, last_error, claim_id, claim_until,
        last_success_at IS NOT NULL AS succeeded, next_dispatch_at
      FROM lifecycle_tenants
    `)
    expect(result.rows).toEqual([
      {
        failure_count: 0,
        last_error: null,
        claim_id: null,
        claim_until: null,
        succeeded: true,
        next_dispatch_at: new Date('2099-01-01T00:00:00Z'),
      },
    ])
  })

  it('fences health observations by claim, preserves stale observations and clears resolved health', async () => {
    await store.wakeTenant('tenant-a')
    const [claim] = await store.claimDueTenants(1, 60_000)
    const next = '2099-01-01T00:00:00Z'
    const health = { maxFailureCount: 7, oldestInFlightStartedAt: '2026-01-01T00:00:00Z' }
    const healthy = { maxFailureCount: 0, oldestInFlightStartedAt: null }
    const read = async () =>
      (
        await transaction!.query(
          'SELECT max_shard_failure_count, oldest_in_flight_started_at, shard_status_observed_at FROM lifecycle_tenants'
        )
      ).rows[0]
    expect(await store.completeTenantDispatch('tenant-a', claim.claimId, next, health)).toBe(true)
    const observed = await read()
    expect(observed).toEqual({
      max_shard_failure_count: 7,
      oldest_in_flight_started_at: new Date(health.oldestInFlightStartedAt),
      shard_status_observed_at: expect.any(Date),
    })
    expect(await store.completeTenantDispatch('tenant-a', claim.claimId, next, healthy)).toBe(false)
    await store.wakeTenant('tenant-a')
    const [unobserved] = await store.claimDueTenants(1, 60_000)
    await store.completeTenantDispatch('tenant-a', unobserved.claimId, next)
    expect(await read()).toEqual(observed)
    await store.wakeTenant('tenant-a')
    const [recovered] = await store.claimDueTenants(1, 60_000)
    await store.completeTenantDispatch('tenant-a', recovered.claimId, next, healthy)
    expect(await read()).toMatchObject({
      max_shard_failure_count: 0,
      oldest_in_flight_started_at: null,
    })
  })

  it('does not postpone an already due row when it is woken again', async () => {
    await transaction!.query(`
      INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at)
      VALUES ('tenant-a', '2000-01-01T00:00:00Z')
    `)

    await store.wakeTenant('tenant-a')

    const result = await transaction!.query('SELECT next_dispatch_at FROM lifecycle_tenants')
    expect(result.rows).toEqual([{ next_dispatch_at: new Date('2000-01-01T00:00:00Z') }])
  })

  it('claims the oldest due or expired rows first and leases them', async () => {
    await transaction!.query(`
      INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until) VALUES
        ('due-old', now() - interval '10 minutes', NULL, NULL),
        ('due-new', now() - interval '1 minute', NULL, NULL),
        ('expired-old', 'infinity', gen_random_uuid(), now() - interval '20 minutes'),
        ('expired-new', 'infinity', gen_random_uuid(), now() - interval '5 minutes'),
        ('claimed', 'infinity', gen_random_uuid(), now() + interval '1 hour'),
        ('future', now() + interval '1 hour', NULL, NULL)
    `)

    const first = await store.claimDueTenants(2, 60_000)
    const second = await store.claimDueTenants(5, 60_000)

    expect(first.map((claim) => claim.tenantId).sort()).toEqual(['due-old', 'expired-old'])
    expect(second.map((claim) => claim.tenantId).sort()).toEqual(['due-new', 'expired-new'])
    const result = await transaction!.query(`
      SELECT extract(epoch FROM claim_until - last_dispatched_at)::float8 AS lease,
        next_dispatch_at, claim_id
      FROM lifecycle_tenants WHERE tenant_id = 'due-old'
    `)
    expect(result.rows[0].lease).toBeCloseTo(60, 1)
    expect(result.rows[0].next_dispatch_at).toBe(Number.POSITIVE_INFINITY)
    expect(result.rows[0].claim_id).toBe(
      first.find((claim) => claim.tenantId === 'due-old')?.claimId
    )
  })

  it('skips rows locked by another session when claiming', async () => {
    const schema = `claim_${randomUUID().replaceAll('-', '')}`
    const pool = new Pool({
      connectionString: getConfig().multitenantDatabaseUrl,
      options: `-c search_path=${schema} -c statement_timeout=5000`,
      max: 3,
    })
    const executor = new PgPoolExecutor(pool)
    let lock: DatabaseTransaction | undefined
    try {
      await pool.query(`CREATE SCHEMA ${schema}`)
      await pool.query('CREATE TABLE tenants (id text PRIMARY KEY)')
      const directory = 'migrations/multitenant'
      const file = (await readdir(directory)).find((name) => name.startsWith('0031-'))!
      await pool.query(await readFile(`${directory}/${file}`, 'utf8'))
      await pool.query("INSERT INTO tenants VALUES ('locked'), ('free-a'), ('free-b')")
      await pool.query(`
        INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at) VALUES
          ('locked', now() - interval '3 minutes'),
          ('free-a', now() - interval '2 minutes'),
          ('free-b', now() - interval '1 minute')
      `)
      lock = await executor.beginTransaction()
      await lock.query("SELECT * FROM lifecycle_tenants WHERE tenant_id = 'locked' FOR UPDATE")

      const claims = await new LifecycleTenantStorePg(executor).claimDueTenants(2, 60_000)

      expect(claims.map((claim) => claim.tenantId).sort()).toEqual(['free-a', 'free-b'])
    } finally {
      await lock?.rollback()
      await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`)
      await pool.end()
    }
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
