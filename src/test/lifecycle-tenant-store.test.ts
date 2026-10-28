import { randomUUID } from 'node:crypto'
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
  const deadline = () => new Date(Date.now() + 60_000).toISOString()
  const seedClaimed = () =>
    transaction!.query({
      text: `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until)
        VALUES ('tenant-a', 'infinity', $1, now() + interval '1 hour')`,
      values: [claimId],
    })

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
    [5000, 21600],
  ])('backs off %i prior failures by %i seconds', async (failureCount, delaySeconds) => {
    await transaction!.query({
      text: `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until, failure_count)
        VALUES ('tenant-a', 'infinity', $1, now() + interval '1 hour', $2)`,
      values: [claimId, failureCount],
    })

    await expect(
      store.failTenantDispatch('tenant-a', claimId, { code: 'offline' }, deadline())
    ).resolves.toBe(true)

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
    await expect(
      store.failTenantDispatch('tenant-a', claimId, { code: 'offline' }, deadline())
    ).resolves.toBe(true)
    await expect(
      store.failTenantDispatch('tenant-a', claimId, { code: 'stale' }, deadline())
    ).resolves.toBe(false)
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
      store.completeTenantDispatch(
        'tenant-a',
        claim.claimId,
        '2099-01-01T00:00:00Z',
        claim.claimUntil.toISOString()
      )
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
    expect(first.reclaimed).toBe(false)
    await transaction!.query(`
      UPDATE lifecycle_tenants SET claim_until = clock_timestamp() - interval '1 second'
      WHERE tenant_id = 'tenant-a'
    `)

    const [recovered] = await store.claimDueTenants(1, 60_000)
    expect(recovered.tenantId).toBe('tenant-a')
    expect(recovered.claimId).toBe(first.claimId)
    expect(recovered.reclaimed).toBe(true)
    await expect(
      store.completeTenantDispatch(
        'tenant-a',
        first.claimId,
        new Date().toISOString(),
        recovered.claimUntil.toISOString()
      )
    ).resolves.toBe(true)
  })

  it('backs off expired leases by recorded failures without counting the reclaim', async () => {
    await store.wakeTenant('tenant-a')
    const [first] = await store.claimDueTenants(1, 180_000)
    for (const [failureCount, delaySeconds] of [
      [0, 300],
      [1, 600],
    ] as const) {
      await transaction!.query(`
        UPDATE lifecycle_tenants SET claim_until = clock_timestamp() - interval '1 second'
        WHERE tenant_id = 'tenant-a'
      `)
      const [recovered] = await store.claimDueTenants(1, 180_000)
      expect(recovered.claimId).toBe(first.claimId)
      const { rows } = await transaction!.query(`
        SELECT failure_count, last_error,
          extract(epoch FROM claim_until - clock_timestamp())::float8 AS delay
        FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
      `)
      expect(rows[0].failure_count).toBe(failureCount)
      if (failureCount === 0) expect(rows[0].last_error).toBeNull()
      expect(rows[0].delay).toBeGreaterThan(delaySeconds - 2)
      expect(rows[0].delay).toBeLessThanOrEqual(delaySeconds)
      await expect(store.claimDueTenants(1, 180_000)).resolves.toEqual([])
      await expect(store.recordExpiredTenantDispatch('tenant-a', first.claimId)).resolves.toBe(true)
    }
    expect(
      (
        await transaction!.query(
          "SELECT failure_count FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'"
        )
      ).rows
    ).toEqual([{ failure_count: 2 }])
    await transaction!.query(`
      UPDATE lifecycle_tenants SET claim_until = clock_timestamp() - interval '1 second', failure_count = 100
      WHERE tenant_id = 'tenant-a'
    `)
    const [capped] = await store.claimDueTenants(1, 180_000)
    expect(capped.claimId).toBe(first.claimId)
    expect(capped.claimUntil.getTime() - Date.now()).toBeGreaterThan(15 * 60_000 - 2000)
    expect(capped.claimUntil.getTime() - Date.now()).toBeLessThanOrEqual(15 * 60_000)
    await store.wakeTenant('tenant-a')
    await expect(store.claimDueTenants(1, 180_000)).resolves.toEqual([])
    await expect(
      store.completeTenantDispatch(
        'tenant-a',
        first.claimId,
        '2099-01-01T00:00:00Z',
        capped.claimUntil.toISOString()
      )
    ).resolves.toBe(true)
    const { rows } = await transaction!.query(`
      SELECT failure_count, last_error FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(rows).toEqual([{ failure_count: 0, last_error: null }])
  })

  it('returns the lease end of a claim', async () => {
    await store.wakeTenant('tenant-a')

    const [claim] = await store.claimDueTenants(1, 60_000)

    expect(claim.claimUntil.getTime() - Date.now()).toBeGreaterThan(55_000)
    expect(claim.claimUntil.getTime() - Date.now()).toBeLessThanOrEqual(60_000)
  })

  it('rejects a late enqueue failure of a reclaimed lease and keeps the new lease', async () => {
    await store.wakeTenant('tenant-a')
    const [first] = await store.claimDueTenants(1, 20)
    await new Promise((resolve) => setTimeout(resolve, 50))
    const [recovered] = await store.claimDueTenants(1, 60_000)
    expect(recovered.claimId).toBe(first.claimId)

    await expect(
      store.failTenantDispatch(
        'tenant-a',
        first.claimId,
        { code: 'tenant_dispatch_enqueue_failed' },
        first.claimUntil.toISOString()
      )
    ).resolves.toBe(false)

    const result = await transaction!.query(`
      SELECT claim_id, claim_until, failure_count FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(result.rows[0].claim_id).toBe(first.claimId)
    expect(result.rows[0].claim_until).toEqual(recovered.claimUntil)
    expect(result.rows[0].failure_count).toBe(0)
  })

  it('records an expired dispatch only for its claim', async () => {
    await seedClaimed()
    const read = async () =>
      (
        await transaction!.query(
          "SELECT failure_count, last_error FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'"
        )
      ).rows

    await expect(
      store.recordExpiredTenantDispatch('tenant-a', '00000000-0000-4000-8000-000000000000')
    ).resolves.toBe(false)
    expect(await read()).toEqual([{ failure_count: 0, last_error: null }])

    await expect(store.recordExpiredTenantDispatch('tenant-a', claimId)).resolves.toBe(true)
    expect(await read()).toEqual([
      {
        failure_count: 1,
        last_error: {
          code: 'tenant_dispatch_lease_expired',
          message: 'Tenant dispatch lease expired before completion',
        },
      },
    ])
  })

  it('reports whether the central feature flag is on', async () => {
    await transaction!.query(
      "UPDATE tenants SET feature_object_versioning = true WHERE id = 'tenant-a'"
    )

    await expect(store.hasObjectVersioningFeature('tenant-a')).resolves.toBe(true)
    await expect(store.hasObjectVersioningFeature('due-old')).resolves.toBe(false)
    await expect(store.hasObjectVersioningFeature('missing')).resolves.toBe(false)
  })

  it('keeps a wake that arrived before an expired claim was reclaimed', async () => {
    await store.wakeTenant('tenant-a')
    const [first] = await store.claimDueTenants(1, 60_000)
    await store.wakeTenant('tenant-a')
    await transaction!.query(`
      UPDATE lifecycle_tenants SET claim_until = clock_timestamp() - interval '1 second'
      WHERE tenant_id = 'tenant-a'
    `)

    const [recovered] = await store.claimDueTenants(1, 60_000)
    expect(recovered.claimId).toBe(first.claimId)
    await expect(
      store.completeTenantDispatch(
        'tenant-a',
        first.claimId,
        '2099-01-01T00:00:00Z',
        recovered.claimUntil.toISOString()
      )
    ).resolves.toBe(true)

    const result = await transaction!.query(`
      SELECT next_dispatch_at <= clock_timestamp() AS due FROM lifecycle_tenants
      WHERE tenant_id = 'tenant-a'
    `)
    expect(result.rows).toEqual([{ due: true }])
  })

  it('revisits a completed dispatch without another wake', async () => {
    await store.wakeTenant('tenant-a')
    const [first] = await store.claimDueTenants(1, 60_000)
    const revisitAt = new Date(Date.now() + 24 * 60 * 60 * 1000)
    await expect(
      store.completeTenantDispatch(
        'tenant-a',
        first.claimId,
        revisitAt.toISOString(),
        first.claimUntil.toISOString()
      )
    ).resolves.toBe(true)
    const result = await transaction!.query(`
      SELECT next_dispatch_at, claim_id FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(result.rows).toEqual([{ next_dispatch_at: revisitAt, claim_id: null }])
    await expect(store.claimDueTenants(1, 60_000)).resolves.toEqual([])

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
      store.completeTenantDispatch('tenant-a', claimId, '2099-01-01T00:00:00Z', deadline())
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
          'SELECT max_bucket_failure_count, oldest_in_flight_started_at, bucket_status_observed_at FROM lifecycle_tenants'
        )
      ).rows[0]
    expect(
      await store.completeTenantDispatch(
        'tenant-a',
        claim.claimId,
        next,
        claim.claimUntil.toISOString(),
        health
      )
    ).toBe(true)
    const observed = await read()
    expect(observed).toEqual({
      max_bucket_failure_count: 7,
      oldest_in_flight_started_at: new Date(health.oldestInFlightStartedAt),
      bucket_status_observed_at: expect.any(Date),
    })
    expect(
      await store.completeTenantDispatch(
        'tenant-a',
        claim.claimId,
        next,
        claim.claimUntil.toISOString(),
        healthy
      )
    ).toBe(false)
    await store.wakeTenant('tenant-a')
    const [unobserved] = await store.claimDueTenants(1, 60_000)
    await store.completeTenantDispatch(
      'tenant-a',
      unobserved.claimId,
      next,
      unobserved.claimUntil.toISOString()
    )
    expect(await read()).toEqual(observed)
    await store.wakeTenant('tenant-a')
    const [recovered] = await store.claimDueTenants(1, 60_000)
    await store.completeTenantDispatch(
      'tenant-a',
      recovered.claimId,
      next,
      recovered.claimUntil.toISOString(),
      healthy
    )
    expect(await read()).toMatchObject({
      max_bucket_failure_count: 0,
      oldest_in_flight_started_at: null,
    })
  })

  it('counts expired leases toward the limit but still reclaims them', async () => {
    await transaction!.query(`
      INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until) VALUES
        ('claimed', 'infinity', gen_random_uuid(), now() + interval '1 hour'),
        ('expired-old', 'infinity', gen_random_uuid(), now() - interval '1 minute')
    `)
    await transaction!.query(`
      INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at) VALUES
        ('due-old', now() - interval '1 day'),
        ('due-new', now() - interval '2 hours')
    `)

    const claims = await store.claimDueTenantsWithinLeaseLimit(3, 60_000)

    expect(
      claims
        .map(({ tenantId, reclaimed }) => ({ tenantId, reclaimed }))
        .sort((a, b) => a.tenantId.localeCompare(b.tenantId))
    ).toEqual([
      { tenantId: 'due-old', reclaimed: false },
      { tenantId: 'expired-old', reclaimed: true },
    ])
    await expect(store.claimDueTenantsWithinLeaseLimit(3, 60_000)).resolves.toEqual([])
  })

  it('admits no new tenants while expired leases fill the limit', async () => {
    await transaction!.query(`
      INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until) VALUES
        ('expired-old', 'infinity', gen_random_uuid(), now() - interval '2 minutes'),
        ('expired-new', 'infinity', gen_random_uuid(), now() - interval '1 minute')
    `)
    await transaction!.query(
      "INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at) VALUES ('due-old', now() - interval '1 day')"
    )

    const claims = await store.claimDueTenantsWithinLeaseLimit(2, 60_000)

    expect(
      claims
        .map(({ tenantId, reclaimed }) => ({ tenantId, reclaimed }))
        .sort((a, b) => a.tenantId.localeCompare(b.tenantId))
    ).toEqual([
      { tenantId: 'expired-new', reclaimed: true },
      { tenantId: 'expired-old', reclaimed: true },
    ])
    const row = await transaction!.query(
      "SELECT claim_id FROM lifecycle_tenants WHERE tenant_id = 'due-old'"
    )
    expect(row.rows).toEqual([{ claim_id: null }])
  })

  it('reclaims expired leases without admitting new tenants when leases exceed the limit', async () => {
    await transaction!.query(`
      INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until) VALUES
        ('claimed', 'infinity', gen_random_uuid(), now() + interval '1 hour'),
        ('expired-old', 'infinity', gen_random_uuid(), now() - interval '1 minute')
    `)
    await transaction!.query(
      "INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at) VALUES ('due-old', now() - interval '1 day')"
    )

    const claims = await store.claimDueTenantsWithinLeaseLimit(1, 60_000)

    expect(claims.map(({ tenantId, reclaimed }) => ({ tenantId, reclaimed }))).toEqual([
      { tenantId: 'expired-old', reclaimed: true },
    ])
    const row = await transaction!.query(
      "SELECT claim_id FROM lifecycle_tenants WHERE tenant_id = 'due-old'"
    )
    expect(row.rows).toEqual([{ claim_id: null }])
  })

  it('cancels a claim blocked on the scheduler advisory lock', async () => {
    const pool = new Pool({
      connectionString: getConfig().multitenantDatabaseUrl,
      options: '-c statement_timeout=5000',
      max: 3,
    })
    const executor = new PgPoolExecutor(pool)
    let holder: DatabaseTransaction | undefined
    let waiter: DatabaseTransaction | undefined
    try {
      holder = await executor.beginTransaction()
      await holder.query("SELECT pg_advisory_xact_lock(hashtext('lifecycle_tenants_claim'))")
      waiter = await executor.beginTransaction()
      const controller = new AbortController()
      const pending = new LifecycleTenantStorePg(waiter).claimDueTenantsWithinLeaseLimit(
        100,
        180_000,
        controller.signal
      )
      await expect
        .poll(async () => {
          const result = await pool.query(
            "SELECT count(*)::int AS count FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE '%lifecycle_tenants_claim%'"
          )
          return result.rows[0].count
        })
        .toBeGreaterThan(0)

      controller.abort()
      await expect(pending).rejects.toMatchObject({ name: 'AbortError' })
      expect(holder.isCompleted()).toBe(false)
    } finally {
      if (waiter && !waiter.isCompleted()) await waiter.rollback()
      if (holder && !holder.isCompleted()) await holder.rollback()
      await pool.end()
    }
  })

  it('serializes concurrent claimers so outstanding claims stay within the limit', async () => {
    const ids = [1, 2, 3].map(() => `lease-limit-${randomUUID()}`)
    const pool = new Pool({
      connectionString: getConfig().multitenantDatabaseUrl,
      options: '-c statement_timeout=5000',
      max: 3,
    })
    const executor = new PgPoolExecutor(pool)
    let first: DatabaseTransaction | undefined
    let second: DatabaseTransaction | undefined
    try {
      await pool.query(
        "INSERT INTO tenants (id, anon_key, database_url, jwt_secret, service_key) SELECT id, 'anon', 'url', 'jwt', 'service' FROM unnest($1::text[]) AS id",
        [ids]
      )
      await pool.query(
        "INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at) SELECT id, '2000-01-01' FROM unnest($1::text[]) AS id",
        [ids]
      )
      const baseline = (
        await pool.query(
          'SELECT count(*)::int AS n FROM lifecycle_tenants WHERE claim_until IS NOT NULL'
        )
      ).rows[0].n
      const limit = baseline + 2

      first = await executor.beginTransaction()
      const claims1 = await new LifecycleTenantStorePg(first).claimDueTenantsWithinLeaseLimit(
        limit,
        60_000
      )
      expect(claims1.filter((claim) => ids.includes(claim.tenantId))).toHaveLength(2)

      second = await executor.beginTransaction()
      let settled = false
      const pending = new LifecycleTenantStorePg(second)
        .claimDueTenantsWithinLeaseLimit(limit, 60_000)
        .finally(() => {
          settled = true
        })
      await new Promise((resolve) => setTimeout(resolve, 300))
      expect(settled).toBe(false)

      await first.commit()
      expect((await pending).filter((claim) => ids.includes(claim.tenantId))).toEqual([])
      await second.commit()

      const claimed = await pool.query(
        'SELECT count(*)::int AS n FROM lifecycle_tenants WHERE tenant_id = ANY($1::text[]) AND claim_id IS NOT NULL',
        [ids]
      )
      expect(claimed.rows[0].n).toBe(2)
    } finally {
      if (first && !first.isCompleted()) await first.rollback()
      if (second && !second.isCompleted()) await second.rollback()
      await pool.query('DELETE FROM lifecycle_tenants WHERE tenant_id = ANY($1::text[])', [ids])
      await pool.query('DELETE FROM tenants WHERE id = ANY($1::text[])', [ids])
      await pool.end()
    }
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

  it('releases a claim without counting a failure and keeps a wake from its lease', async () => {
    await store.wakeTenant('tenant-a')
    const [claim] = await store.claimDueTenants(1, 60_000)
    expect(claim.reclaimed).toBe(false)
    await transaction!.query(
      "UPDATE lifecycle_tenants SET failure_count = 2 WHERE tenant_id = 'tenant-a'"
    )

    await expect(
      store.releaseTenantDispatch(
        'tenant-a',
        claim.claimId,
        '2099-01-01T00:00:00Z',
        claim.claimUntil.toISOString()
      )
    ).resolves.toBe(true)
    const row = await transaction!.query(
      "SELECT claim_id, claim_until, next_dispatch_at, failure_count, last_error FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'"
    )
    expect(row.rows).toEqual([
      {
        claim_id: null,
        claim_until: null,
        next_dispatch_at: new Date('2099-01-01T00:00:00Z'),
        failure_count: 2,
        last_error: null,
      },
    ])

    await transaction!.query(
      "UPDATE lifecycle_tenants SET next_dispatch_at = clock_timestamp() WHERE tenant_id = 'tenant-a'"
    )
    const [again] = await store.claimDueTenants(1, 60_000)
    await store.wakeTenant('tenant-a')
    await expect(
      store.releaseTenantDispatch(
        'tenant-a',
        again.claimId,
        '2099-01-01T00:00:00Z',
        again.claimUntil.toISOString()
      )
    ).resolves.toBe(true)
    const due = await transaction!.query(
      "SELECT next_dispatch_at <= clock_timestamp() AS due FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'"
    )
    expect(due.rows).toEqual([{ due: true }])
  })

  it.each([
    [
      'complete',
      (deadline: string) =>
        store.completeTenantDispatch('tenant-a', claimId, new Date().toISOString(), deadline),
    ],
    [
      'release',
      (deadline: string) =>
        store.releaseTenantDispatch('tenant-a', claimId, new Date().toISOString(), deadline),
    ],
    [
      'fail',
      (deadline: string) =>
        store.failTenantDispatch('tenant-a', claimId, { code: 'offline' }, deadline),
    ],
  ])('%s writes only before the deadline', async (_name, write) => {
    await seedClaimed()
    const snapshot = async () =>
      (await transaction!.query("SELECT * FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'"))
        .rows
    const before = await snapshot()

    await expect(write(undefined as never)).resolves.toBe(false)
    expect(await snapshot()).toEqual(before)
    await expect(write(new Date(Date.now() - 1000).toISOString())).resolves.toBe(false)
    expect(await snapshot()).toEqual(before)

    await expect(write(new Date(Date.now() + 60_000).toISOString())).resolves.toBe(true)
  })

  it('checks the deadline after waiting for a row lock', async () => {
    const tenantId = `deadline-${randomUUID()}`
    const pool = new Pool({
      connectionString: getConfig().multitenantDatabaseUrl,
      options: '-c statement_timeout=5000',
      max: 3,
    })
    const executor = new PgPoolExecutor(pool)
    let reclaim: DatabaseTransaction | undefined
    try {
      await pool.query(
        "INSERT INTO tenants (id, anon_key, database_url, jwt_secret, service_key) VALUES ($1, 'anon', 'url', 'jwt', 'service')",
        [tenantId]
      )
      await pool.query(
        `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until)
          VALUES ($1, 'infinity', $2, now() + interval '1 minute')`,
        [tenantId, claimId]
      )
      reclaim = await executor.beginTransaction()
      await reclaim.query({
        text: "UPDATE lifecycle_tenants SET claim_until = now() + interval '3 minutes' WHERE tenant_id = $1",
        values: [tenantId],
      })

      const complete = new LifecycleTenantStorePg(executor).completeTenantDispatch(
        tenantId,
        claimId,
        new Date().toISOString(),
        new Date(Date.now() + 1000).toISOString()
      )
      await new Promise((resolve) => setTimeout(resolve, 1500))
      await reclaim.commit()

      await expect(complete).resolves.toBe(false)
      const row = await pool.query(
        "SELECT claim_id, claim_until > now() + interval '2 minutes' AS leased FROM lifecycle_tenants WHERE tenant_id = $1",
        [tenantId]
      )
      expect(row.rows).toEqual([{ claim_id: claimId, leased: true }])
    } finally {
      if (reclaim && !reclaim.isCompleted()) await reclaim.rollback()
      await pool.query('DELETE FROM lifecycle_tenants WHERE tenant_id = $1', [tenantId])
      await pool.query('DELETE FROM tenants WHERE id = $1', [tenantId])
      await pool.end()
    }
  })

  it.each([
    [
      'complete',
      (store: LifecycleTenantStorePg, tenantId: string, deadline: string) =>
        store.completeTenantDispatch(tenantId, claimId, new Date().toISOString(), deadline),
    ],
    [
      'release',
      (store: LifecycleTenantStorePg, tenantId: string, deadline: string) =>
        store.releaseTenantDispatch(tenantId, claimId, new Date().toISOString(), deadline),
    ],
    [
      'fail',
      (store: LifecycleTenantStorePg, tenantId: string, deadline: string) =>
        store.failTenantDispatch(tenantId, claimId, { message: 'boom' }, deadline),
    ],
  ])('checks the deadline after waiting for a lock-only holder (%s)', async (_name, write) => {
    const tenantId = `deadline-lock-${randomUUID()}`
    const pool = new Pool({
      connectionString: getConfig().multitenantDatabaseUrl,
      options: '-c statement_timeout=5000',
      max: 3,
    })
    const executor = new PgPoolExecutor(pool)
    let holder: DatabaseTransaction | undefined
    try {
      await pool.query(
        "INSERT INTO tenants (id, anon_key, database_url, jwt_secret, service_key) VALUES ($1, 'anon', 'url', 'jwt', 'service')",
        [tenantId]
      )
      await pool.query(
        `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until)
          VALUES ($1, 'infinity', $2, now() + interval '1 minute')`,
        [tenantId, claimId]
      )
      const before = await pool.query('SELECT * FROM lifecycle_tenants WHERE tenant_id = $1', [
        tenantId,
      ])
      holder = await executor.beginTransaction()
      await holder.query({
        text: 'SELECT 1 FROM lifecycle_tenants WHERE tenant_id = $1 FOR UPDATE',
        values: [tenantId],
      })

      const result = write(
        new LifecycleTenantStorePg(executor),
        tenantId,
        new Date(Date.now() + 1000).toISOString()
      )
      await new Promise((resolve) => setTimeout(resolve, 1500))
      await holder.commit()

      await expect(result).resolves.toBe(false)
      const after = await pool.query('SELECT * FROM lifecycle_tenants WHERE tenant_id = $1', [
        tenantId,
      ])
      expect(after.rows).toEqual(before.rows)
    } finally {
      if (holder && !holder.isCompleted()) await holder.rollback()
      await pool.query('DELETE FROM lifecycle_tenants WHERE tenant_id = $1', [tenantId])
      await pool.query('DELETE FROM tenants WHERE id = $1', [tenantId])
      await pool.end()
    }
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

  it('picks the earliest due unclaimed tenant when more are due than the limit', async () => {
    await transaction!.query(`
      INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at) VALUES
        ('due-new', now() - interval '1 minute'),
        ('due-old', now() - interval '10 minutes'),
        ('tenant-a', now() - interval '5 minutes')
    `)

    const claims = await store.claimDueTenants(1, 60_000)

    expect(claims.map((claim) => claim.tenantId)).toEqual(['due-old'])
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
      await pool.query(
        'CREATE TABLE lifecycle_tenants (LIKE public.lifecycle_tenants INCLUDING ALL)'
      )
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

  it('skips expired leases locked by another session when reclaiming', async () => {
    const schema = `reclaim_${randomUUID().replaceAll('-', '')}`
    const pool = new Pool({
      connectionString: getConfig().multitenantDatabaseUrl,
      options: `-c search_path=${schema} -c statement_timeout=5000`,
      max: 3,
    })
    const executor = new PgPoolExecutor(pool)
    let lock: DatabaseTransaction | undefined
    try {
      await pool.query(`CREATE SCHEMA ${schema}`)
      await pool.query(
        'CREATE TABLE lifecycle_tenants (LIKE public.lifecycle_tenants INCLUDING ALL)'
      )
      await pool.query(`
        INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until) VALUES
          ('locked', 'infinity', gen_random_uuid(), now() - interval '3 minutes'),
          ('free-a', 'infinity', gen_random_uuid(), now() - interval '2 minutes'),
          ('free-b', 'infinity', gen_random_uuid(), now() - interval '1 minute')
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

  it('reclaims a lease that expired after a very large number of redispatches', async () => {
    await transaction!.query({
      text: `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until, failure_count)
        VALUES ('tenant-a', 'infinity', $1, now() - interval '1 second', 5000)`,
      values: [claimId],
    })

    const [claim] = await store.claimDueTenants(1, 180_000)

    expect(claim.claimId).toBe(claimId)
    expect(claim.claimUntil.getTime() - Date.now()).toBeGreaterThan(15 * 60_000 - 2000)
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
