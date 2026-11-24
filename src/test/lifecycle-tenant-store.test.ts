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
    [9, 86_400, 86_400],
  ])('backs off %i prior failures by %i seconds', async (failureCount, delaySeconds, max?) => {
    await transaction!.query({
      text: `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until, failure_count)
        VALUES ('tenant-a', 'infinity', $1, now() + interval '1 hour', $2)`,
      values: [claimId, failureCount],
    })

    await expect(
      store.failTenantDispatch({
        tenantId: 'tenant-a',
        claimId,
        error: { code: 'offline' },
        deadline: deadline(),
        maxBackoffSeconds: max,
      })
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
      store.failTenantDispatch({
        tenantId: 'tenant-a',
        claimId,
        error: { code: 'offline' },
        deadline: deadline(),
      })
    ).resolves.toBe(true)
    await expect(
      store.failTenantDispatch({
        tenantId: 'tenant-a',
        claimId,
        error: { code: 'stale' },
        deadline: deadline(),
      })
    ).resolves.toBe(false)
    const result = await transaction!.query(`SELECT * FROM lifecycle_tenants`)
    expect(result.rows[0]).toMatchObject({
      next_dispatch_at: new Date('2026-01-01T00:00:00Z'),
      failure_count: 5,
      last_error: { code: 'offline' },
    })
  })

  it('arms wake_floor_at without delaying due tenants and survives a concurrent immediate wake and pre-commit scan', async () => {
    // 1. Inserts absent tenant at +5m with matching wake_floor_at.
    await store.wakeTenant('tenant-a', undefined, 300_000)
    const inserted = await transaction!.query(`
      SELECT extract(epoch FROM next_dispatch_at - clock_timestamp())::float8 AS delay,
        wake_floor_at = next_dispatch_at AS floor_matches, claim_id
      FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(inserted.rows[0].claim_id).toBeNull()
    expect(inserted.rows[0].floor_matches).toBe(true)
    expect(inserted.rows[0].delay).toBeGreaterThan(298)
    expect(inserted.rows[0].delay).toBeLessThanOrEqual(300)

    // 2. Does not delay an already-due tenant (prevents starvation by failing PUTs).
    const earlier = new Date('2026-01-01T00:00:00Z')
    await transaction!.query({
      text: "UPDATE lifecycle_tenants SET next_dispatch_at = $1, wake_floor_at = NULL WHERE tenant_id = 'tenant-a'",
      values: [earlier.toISOString()],
    })
    await store.wakeTenant('tenant-a', undefined, 300_000)
    const dueRow = await transaction!.query(`
      SELECT next_dispatch_at,
        extract(epoch FROM wake_floor_at - clock_timestamp())::float8 AS floor_delay
      FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(dueRow.rows[0].next_dispatch_at).toEqual(earlier)
    expect(dueRow.rows[0].floor_delay).toBeGreaterThan(298)
    expect(dueRow.rows[0].floor_delay).toBeLessThanOrEqual(300)

    // 3. Concurrent immediate wake + claim + scan completing before the floor still honors wake_floor_at.
    await store.wakeTenant('tenant-a')
    const [firstClaim] = await store.claimDueTenantsWithinLeaseLimit({
      maxLeased: 1,
      leaseMs: 60_000,
    })
    const firstStart = await store.startTenantDispatch({
      tenantId: 'tenant-a',
      claimId: firstClaim.claimId,
      deadline: firstClaim.claimUntil.toISOString(),
      leaseMs: 60_000,
    })
    expect(firstStart?.startedAt).toBeInstanceOf(Date)
    await expect(
      store.completeTenantDispatch({
        tenantId: 'tenant-a',
        claimId: firstStart!.claimId,
        nextDispatchAt: '2099-01-01T00:00:00Z',
        deadline: firstStart!.claimUntil.toISOString(),
        startedAt: firstStart!.startedAt.toISOString(),
      })
    ).resolves.toBe(true)

    const afterEarlyScan = await transaction!.query(`
      SELECT extract(epoch FROM next_dispatch_at - clock_timestamp())::float8 AS delay,
        wake_floor_at
      FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(afterEarlyScan.rows[0].delay).toBeGreaterThan(295)
    expect(afterEarlyScan.rows[0].delay).toBeLessThanOrEqual(300)
    expect(afterEarlyScan.rows[0].wake_floor_at).not.toBeNull()

    // 4. Once a scan starts at or after wake_floor_at, completion clears the floor and schedules normally.
    await transaction!.query(`
      UPDATE lifecycle_tenants
      SET next_dispatch_at = clock_timestamp() - interval '1 second',
          wake_floor_at = clock_timestamp() - interval '1 second'
      WHERE tenant_id = 'tenant-a'
    `)
    const [secondClaim] = await store.claimDueTenantsWithinLeaseLimit({
      maxLeased: 1,
      leaseMs: 60_000,
    })
    const secondStart = await store.startTenantDispatch({
      tenantId: 'tenant-a',
      claimId: secondClaim.claimId,
      deadline: secondClaim.claimUntil.toISOString(),
      leaseMs: 60_000,
    })
    const distant = new Date('2099-01-01T00:00:00Z')
    await expect(
      store.completeTenantDispatch({
        tenantId: 'tenant-a',
        claimId: secondStart!.claimId,
        nextDispatchAt: distant.toISOString(),
        deadline: secondStart!.claimUntil.toISOString(),
        startedAt: secondStart!.startedAt.toISOString(),
      })
    ).resolves.toBe(true)

    const afterFloorScan = await transaction!.query(`
      SELECT next_dispatch_at, wake_floor_at
      FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(afterFloorScan.rows[0]).toEqual({ next_dispatch_at: distant, wake_floor_at: null })
  })

  it('preserves a second wake when a dispatcher consumed the pre-commit wake', async () => {
    await store.wakeTenant('tenant-a')
    const [claim] = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 60_000 })
    expect(claim.tenantId).toBe('tenant-a')

    await store.wakeTenant('tenant-a')
    await expect(
      store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 60_000 })
    ).resolves.toEqual([])
    await expect(
      store.completeTenantDispatch({
        tenantId: 'tenant-a',
        claimId: claim.claimId,
        nextDispatchAt: '2099-01-01T00:00:00Z',
        deadline: claim.claimUntil.toISOString(),
      })
    ).resolves.toBe(true)

    const result = await transaction!.query(`
      SELECT next_dispatch_at <= clock_timestamp() AS due, claim_id
      FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(result.rows).toEqual([{ due: true, claim_id: null }])
  })

  it('reclaims an expired dispatch at infinity keeping its claim id, so the pending job can still complete', async () => {
    await store.wakeTenant('tenant-a')
    const [first] = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 60_000 })
    expect(first.tenantId).toBe('tenant-a')
    expect(first.reclaimed).toBe(false)
    await transaction!.query(`
      UPDATE lifecycle_tenants SET claim_until = clock_timestamp() - interval '1 second'
      WHERE tenant_id = 'tenant-a'
    `)

    const [recovered] = await store.claimDueTenantsWithinLeaseLimit({
      maxLeased: 1,
      leaseMs: 60_000,
    })
    expect(recovered.tenantId).toBe('tenant-a')
    expect(recovered.claimId).toBe(first.claimId)
    expect(recovered.reclaimed).toBe(true)
    await expect(
      store.completeTenantDispatch({
        tenantId: 'tenant-a',
        claimId: first.claimId,
        nextDispatchAt: new Date().toISOString(),
        deadline: recovered.claimUntil.toISOString(),
      })
    ).resolves.toBe(true)
  })

  it('reclaims a first-time expired lease with a 5m floor without counting a failure, and evicts started expired leases into backoff', async () => {
    await store.wakeTenant('tenant-a')
    const [first] = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 180_000 })
    await transaction!.query(`
      UPDATE lifecycle_tenants SET claim_until = clock_timestamp() - interval '1 second'
      WHERE tenant_id = 'tenant-a'
    `)

    const [recovered] = await store.claimDueTenantsWithinLeaseLimit({
      maxLeased: 1,
      leaseMs: 180_000,
    })
    expect(recovered.claimId).toBe(first.claimId)
    expect(recovered.reclaimed).toBe(true)
    const { rows } = await transaction!.query(`
      SELECT failure_count, last_error,
        extract(epoch FROM claim_until - clock_timestamp())::float8 AS delay
      FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(rows[0].failure_count).toBe(0)
    expect(rows[0].last_error).toBeNull()
    expect(rows[0].delay).toBeGreaterThan(298)
    expect(rows[0].delay).toBeLessThanOrEqual(300)
    await expect(
      store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 180_000 })
    ).resolves.toEqual([])

    const started = await store.startTenantDispatch({
      tenantId: 'tenant-a',
      claimId: recovered.claimId,
      deadline: recovered.claimUntil.toISOString(),
      leaseMs: 180_000,
      reclaimed: true,
    })
    expect(started).toBeDefined()
    await transaction!.query(`
      UPDATE lifecycle_tenants SET claim_until = clock_timestamp() - interval '1 second'
      WHERE tenant_id = 'tenant-a'
    `)
    // Once the re-sent run started, its expiry is evicted into backoff instead of re-sent.
    await expect(
      store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 180_000 })
    ).resolves.toEqual([])
    const evicted = await transaction!.query(`
      SELECT claim_id, claim_until, failure_count, last_error
      FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(evicted.rows[0]).toMatchObject({
      claim_id: null,
      claim_until: null,
      failure_count: 1,
      last_error: { code: 'tenant_dispatch_lease_expired' },
    })
  })

  it('returns the lease end of a claim', async () => {
    await store.wakeTenant('tenant-a')

    const [claim] = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 60_000 })

    const result = await transaction!.query(`
      SELECT claim_until,
        extract(epoch FROM claim_until - last_dispatched_at)::float8 AS lease
      FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(claim.claimUntil).toEqual(result.rows[0].claim_until)
    expect(result.rows[0].lease).toBeCloseTo(60, 1)
  })

  it('starts only one delivery of a claim and fences an expired delivery', async () => {
    await store.wakeTenant('tenant-a')
    const [claim] = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 60_000 })
    const expired = new Date(Date.now() - 5_000).toISOString()
    await expect(
      store.startTenantDispatch({
        tenantId: 'tenant-a',
        claimId: claim.claimId,
        deadline: expired,
        leaseMs: 180_000,
      })
    ).resolves.toBeUndefined()

    const started = await store.startTenantDispatch({
      tenantId: 'tenant-a',
      claimId: claim.claimId,
      deadline: claim.claimUntil.toISOString(),
      leaseMs: 180_000,
    })
    expect(started?.claimId).toBeDefined()
    expect(started?.claimId).not.toBe(claim.claimId)
    expect(started!.claimUntil.getTime() - Date.now()).toBeGreaterThan(175_000)
    await expect(
      store.startTenantDispatch({
        tenantId: 'tenant-a',
        claimId: claim.claimId,
        deadline: claim.claimUntil.toISOString(),
        leaseMs: 180_000,
      })
    ).resolves.toBeUndefined()
    await expect(
      store.completeTenantDispatch({
        tenantId: 'tenant-a',
        claimId: started!.claimId,
        nextDispatchAt: '2099-01-01T00:00:00Z',
        deadline: started!.claimUntil.toISOString(),
      })
    ).resolves.toBe(true)
  })

  it('admits only one of two concurrent deliveries of the same claim', async () => {
    const tenantId = `delivery-${randomUUID()}`
    const pool = new Pool({ connectionString: getConfig().multitenantDatabaseUrl, max: 3 })
    const concurrentStore = new LifecycleTenantStorePg(new PgPoolExecutor(pool))
    try {
      await pool.query(
        "INSERT INTO tenants (id, anon_key, database_url, jwt_secret, service_key) VALUES ($1, 'anon', 'url', 'jwt', 'service')",
        [tenantId]
      )
      await pool.query(
        "INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until) VALUES ($1, 'infinity', $2, now() + interval '1 minute')",
        [tenantId, claimId]
      )
      const deadline = new Date(Date.now() + 60_000).toISOString()
      const deliveries = await Promise.all([
        concurrentStore.startTenantDispatch({ tenantId, claimId, deadline, leaseMs: 180_000 }),
        concurrentStore.startTenantDispatch({ tenantId, claimId, deadline, leaseMs: 180_000 }),
      ])
      expect(deliveries.filter(Boolean)).toHaveLength(1)
    } finally {
      await pool.query('DELETE FROM tenants WHERE id = $1', [tenantId])
      await pool.end()
    }
  })

  it('fences the original execution after an expired claim is reclaimed', async () => {
    await store.wakeTenant('tenant-a')
    const [claim] = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 60_000 })
    const first = (await store.startTenantDispatch({
      tenantId: 'tenant-a',
      claimId: claim.claimId,
      deadline: claim.claimUntil.toISOString(),
      leaseMs: 180_000,
    }))!
    await transaction!.query(
      "UPDATE lifecycle_tenants SET claim_until = clock_timestamp() - interval '1 second' WHERE tenant_id = 'tenant-a'"
    )
    const [reclaimed] = await store.claimDueTenantsWithinLeaseLimit({
      maxLeased: 1,
      leaseMs: 180_000,
    })
    expect(reclaimed.claimId).toBe(first.claimId)
    const replacement = (await store.startTenantDispatch({
      tenantId: 'tenant-a',
      claimId: reclaimed.claimId,
      deadline: reclaimed.claimUntil.toISOString(),
      leaseMs: 180_000,
    }))!

    await expect(
      store.completeTenantDispatch({
        tenantId: 'tenant-a',
        claimId: first.claimId,
        nextDispatchAt: '2099-01-01T00:00:00Z',
        deadline: first.claimUntil.toISOString(),
      })
    ).resolves.toBe(false)
    await expect(
      store.completeTenantDispatch({
        tenantId: 'tenant-a',
        claimId: replacement.claimId,
        nextDispatchAt: '2099-01-01T00:00:00Z',
        deadline: replacement.claimUntil.toISOString(),
      })
    ).resolves.toBe(true)
  })

  it('rejects a late enqueue failure of a reclaimed lease and keeps the new lease', async () => {
    await store.wakeTenant('tenant-a')
    const [first] = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 20 })
    await new Promise((resolve) => setTimeout(resolve, 50))
    const [recovered] = await store.claimDueTenantsWithinLeaseLimit({
      maxLeased: 1,
      leaseMs: 60_000,
    })
    expect(recovered.claimId).toBe(first.claimId)

    await expect(
      store.failTenantDispatch({
        tenantId: 'tenant-a',
        claimId: first.claimId,
        error: { code: 'tenant_dispatch_enqueue_failed' },
        deadline: first.claimUntil.toISOString(),
      })
    ).resolves.toBe(false)

    const result = await transaction!.query(`
      SELECT claim_id, claim_until, failure_count FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(result.rows[0].claim_id).toBe(first.claimId)
    expect(result.rows[0].claim_until).toEqual(recovered.claimUntil)
    expect(result.rows[0].failure_count).toBe(0)
  })

  it('tags the rotated claimId when a reclaimed dispatch starts without counting a failure', async () => {
    await seedClaimed()
    const read = async () =>
      (
        await transaction!.query(
          "SELECT failure_count, last_error FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'"
        )
      ).rows
    const deadline = new Date(Date.now() + 60_000).toISOString()

    await expect(
      store.startTenantDispatch({
        tenantId: 'tenant-a',
        claimId: '00000000-0000-4000-8000-000000000000',
        deadline,
        leaseMs: 180_000,
        reclaimed: true,
      })
    ).resolves.toBeUndefined()
    const started = await store.startTenantDispatch({
      tenantId: 'tenant-a',
      claimId,
      deadline,
      leaseMs: 180_000,
    })
    expect(await read()).toEqual([{ failure_count: 0, last_error: null }])

    const restarted = await store.startTenantDispatch({
      tenantId: 'tenant-a',
      claimId: started!.claimId,
      deadline: started!.claimUntil.toISOString(),
      leaseMs: 180_000,
      reclaimed: true,
    })
    expect(await read()).toEqual([
      {
        failure_count: 0,
        last_error: {
          claimId: restarted!.claimId,
        },
      },
    ])

    await expect(
      store.releaseTenantDispatch({
        tenantId: 'tenant-a',
        claimId: restarted!.claimId,
        deadline: restarted!.claimUntil.toISOString(),
      })
    ).resolves.toBe(true)
    expect(await read()).toEqual([{ failure_count: 0, last_error: null }])
  })

  it('keeps the reclaim backoff lease when a reclaimed dispatch starts', async () => {
    const leaseSeconds = async (reclaimed: boolean) => {
      await transaction!.query('DELETE FROM lifecycle_tenants')
      await seedClaimed()
      await store.startTenantDispatch({
        tenantId: 'tenant-a',
        claimId,
        deadline: deadline(),
        leaseMs: 180_000,
        reclaimed,
      })
      const result = await transaction!.query(
        "SELECT extract(epoch FROM claim_until - clock_timestamp())::float8 AS lease FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'"
      )
      return result.rows[0].lease as number
    }

    expect(await leaseSeconds(true)).toBeGreaterThan(3_500)
    const fresh = await leaseSeconds(false)
    expect(fresh).toBeGreaterThan(175)
    expect(fresh).toBeLessThanOrEqual(180)
  })

  it('floors a reclaimed lease at the full lease length', async () => {
    await transaction!.query({
      text: `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until)
        VALUES ('tenant-a', 'infinity', $1, now() + interval '10 seconds')`,
      values: [claimId],
    })

    await store.startTenantDispatch({
      tenantId: 'tenant-a',
      claimId,
      deadline: deadline(),
      leaseMs: 180_000,
      reclaimed: true,
    })

    const result = await transaction!.query(
      "SELECT extract(epoch FROM claim_until - clock_timestamp())::float8 AS lease FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'"
    )
    expect(result.rows[0].lease).toBeGreaterThan(175)
    expect(result.rows[0].lease).toBeLessThanOrEqual(180)
  })

  it('releases a claim for immediate redispatch without counting a failure', async () => {
    await transaction!.query({
      text: `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until, failure_count, last_error)
        VALUES ('tenant-a', now() - interval '1 hour', $1, now() + interval '1 hour', 2, '{"code":"offline"}')`,
      values: [claimId],
    })

    await expect(
      store.releaseTenantDispatch({
        tenantId: 'tenant-a',
        claimId: randomUUID(),
        deadline: deadline(),
      })
    ).resolves.toBe(false)
    await expect(
      store.releaseTenantDispatch({
        tenantId: 'tenant-a',
        claimId,
        deadline: new Date(Date.now() - 1_000).toISOString(),
      })
    ).resolves.toBe(false)
    await expect(
      store.releaseTenantDispatch({ tenantId: 'tenant-a', claimId, deadline: deadline() })
    ).resolves.toBe(true)

    const result = await transaction!.query(
      "SELECT claim_id, claim_until, failure_count, last_error, next_dispatch_at < now() - interval '30 minutes' AS earlier FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'"
    )
    expect(result.rows[0]).toEqual({
      claim_id: null,
      claim_until: null,
      failure_count: 2,
      last_error: { code: 'offline' },
      earlier: true,
    })
  })

  it('releases only the batched claims that still match and are live', async () => {
    const [idA, idB, idC] = [randomUUID(), randomUUID(), randomUUID()]
    for (const [tenant, id] of [
      ['tenant-a', idA],
      ['due-old', idB],
      ['due-new', idC],
    ]) {
      await transaction!.query({
        text: `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until)
          VALUES ($1, now() + interval '1 hour', $2, now() + interval '1 hour')`,
        values: [tenant, id],
      })
    }
    const live = new Date(Date.now() + 60_000)

    await expect(
      store.releaseTenantDispatches([
        { tenantId: 'tenant-a', claimId: idA, claimUntil: live },
        { tenantId: 'due-old', claimId: randomUUID(), claimUntil: live },
        { tenantId: 'due-new', claimId: idC, claimUntil: new Date(Date.now() - 1_000) },
      ])
    ).resolves.toBe(1)

    const result = await transaction!.query(
      `SELECT tenant_id, claim_id, claim_until, failure_count, next_dispatch_at <= clock_timestamp() AS due
       FROM lifecycle_tenants ORDER BY tenant_id = 'tenant-a' DESC, tenant_id DESC`
    )
    expect(result.rows[0]).toEqual({
      tenant_id: 'tenant-a',
      claim_id: null,
      claim_until: null,
      failure_count: 0,
      due: true,
    })
    expect(result.rows[1].claim_id).toBe(idB)
    expect(result.rows[2].claim_id).toBe(idC)
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
    const [first] = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 60_000 })
    await store.wakeTenant('tenant-a')
    await transaction!.query(`
      UPDATE lifecycle_tenants SET claim_until = clock_timestamp() - interval '1 second'
      WHERE tenant_id = 'tenant-a'
    `)

    const [recovered] = await store.claimDueTenantsWithinLeaseLimit({
      maxLeased: 1,
      leaseMs: 60_000,
    })
    expect(recovered.claimId).toBe(first.claimId)
    await expect(
      store.completeTenantDispatch({
        tenantId: 'tenant-a',
        claimId: first.claimId,
        nextDispatchAt: '2099-01-01T00:00:00Z',
        deadline: recovered.claimUntil.toISOString(),
      })
    ).resolves.toBe(true)

    const result = await transaction!.query(`
      SELECT next_dispatch_at <= clock_timestamp() AS due FROM lifecycle_tenants
      WHERE tenant_id = 'tenant-a'
    `)
    expect(result.rows).toEqual([{ due: true }])
  })

  it('revisits a completed dispatch without another wake', async () => {
    await store.wakeTenant('tenant-a')
    const [first] = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 60_000 })
    const revisitAt = new Date(Date.now() + 24 * 60 * 60 * 1000)
    await expect(
      store.completeTenantDispatch({
        tenantId: 'tenant-a',
        claimId: first.claimId,
        nextDispatchAt: revisitAt.toISOString(),
        deadline: first.claimUntil.toISOString(),
      })
    ).resolves.toBe(true)
    const result = await transaction!.query(`
      SELECT next_dispatch_at, claim_id FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(result.rows).toEqual([{ next_dispatch_at: revisitAt, claim_id: null }])
    await expect(
      store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 60_000 })
    ).resolves.toEqual([])

    await transaction!.query(`
      UPDATE lifecycle_tenants SET next_dispatch_at = clock_timestamp() - interval '1 second'
      WHERE tenant_id = 'tenant-a'
    `)
    const [revisited] = await store.claimDueTenantsWithinLeaseLimit({
      maxLeased: 1,
      leaseMs: 60_000,
    })
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
      store.completeTenantDispatch({
        tenantId: 'tenant-a',
        claimId,
        nextDispatchAt: '2099-01-01T00:00:00Z',
        deadline: deadline(),
      })
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
    const [claim] = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 60_000 })
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
      await store.completeTenantDispatch({
        tenantId: 'tenant-a',
        claimId: claim.claimId,
        nextDispatchAt: next,
        deadline: claim.claimUntil.toISOString(),
        health,
      })
    ).toBe(true)
    const observed = await read()
    expect(observed).toEqual({
      max_bucket_failure_count: 7,
      oldest_in_flight_started_at: new Date(health.oldestInFlightStartedAt),
      bucket_status_observed_at: expect.any(Date),
    })
    expect(
      await store.completeTenantDispatch({
        tenantId: 'tenant-a',
        claimId: claim.claimId,
        nextDispatchAt: next,
        deadline: claim.claimUntil.toISOString(),
        health: healthy,
      })
    ).toBe(false)
    await store.wakeTenant('tenant-a')
    const [unobserved] = await store.claimDueTenantsWithinLeaseLimit({
      maxLeased: 1,
      leaseMs: 60_000,
    })
    await store.completeTenantDispatch({
      tenantId: 'tenant-a',
      claimId: unobserved.claimId,
      nextDispatchAt: next,
      deadline: unobserved.claimUntil.toISOString(),
    })
    expect(await read()).toEqual(observed)
    await store.wakeTenant('tenant-a')
    const [recovered] = await store.claimDueTenantsWithinLeaseLimit({
      maxLeased: 1,
      leaseMs: 60_000,
    })
    await store.completeTenantDispatch({
      tenantId: 'tenant-a',
      claimId: recovered.claimId,
      nextDispatchAt: next,
      deadline: recovered.claimUntil.toISOString(),
      health: healthy,
    })
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

    const claims = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 3, leaseMs: 60_000 })

    expect(
      claims
        .map(({ tenantId, reclaimed }) => ({ tenantId, reclaimed }))
        .sort((a, b) => a.tenantId.localeCompare(b.tenantId))
    ).toEqual([
      { tenantId: 'due-old', reclaimed: false },
      { tenantId: 'expired-old', reclaimed: true },
    ])
    await expect(
      store.claimDueTenantsWithinLeaseLimit({ maxLeased: 3, leaseMs: 60_000 })
    ).resolves.toEqual([])
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

    const claims = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 2, leaseMs: 60_000 })

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

    const claims = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 60_000 })

    expect(claims.map(({ tenantId, reclaimed }) => ({ tenantId, reclaimed }))).toEqual([
      { tenantId: 'expired-old', reclaimed: true },
    ])
    const row = await transaction!.query(
      "SELECT claim_id FROM lifecycle_tenants WHERE tenant_id = 'due-old'"
    )
    expect(row.rows).toEqual([{ claim_id: null }])
  })

  it('skips immediately when the scheduler advisory lock is already held', async () => {
    const pool = new Pool({
      connectionString: getConfig().multitenantDatabaseUrl,
      options: '-c statement_timeout=5000',
      max: 3,
    })
    const executor = new PgPoolExecutor(pool)
    let holder: DatabaseTransaction | undefined
    let peer: DatabaseTransaction | undefined
    try {
      holder = await executor.beginTransaction()
      await holder.query("SELECT pg_advisory_xact_lock(hashtext('lifecycle_tenants_claim'))")
      peer = await executor.beginTransaction()

      await expect(
        new LifecycleTenantStorePg(peer).claimDueTenantsWithinLeaseLimit({
          maxLeased: 100,
          leaseMs: 180_000,
        })
      ).resolves.toEqual([])
      expect(holder.isCompleted()).toBe(false)
    } finally {
      if (peer && !peer.isCompleted()) await peer.rollback()
      if (holder && !holder.isCompleted()) await holder.rollback()
      await pool.end()
    }
  })

  it('skips concurrent claimers so outstanding claims stay within the limit', async () => {
    const ids = [1, 2, 3].map(() => `lease-limit-${randomUUID()}`)
    const pool = new Pool({
      connectionString: getConfig().multitenantDatabaseUrl,
      options: '-c statement_timeout=5000',
      max: 3,
    })
    const executor = new PgPoolExecutor(pool)
    let first: DatabaseTransaction | undefined
    let second: DatabaseTransaction | undefined
    let third: DatabaseTransaction | undefined
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
      const claims1 = await new LifecycleTenantStorePg(first).claimDueTenantsWithinLeaseLimit({
        maxLeased: limit,
        leaseMs: 60_000,
      })
      expect(claims1.filter((claim) => ids.includes(claim.tenantId))).toHaveLength(2)

      second = await executor.beginTransaction()
      const claims2 = await new LifecycleTenantStorePg(second).claimDueTenantsWithinLeaseLimit({
        maxLeased: limit,
        leaseMs: 60_000,
      })
      expect(claims2).toEqual([])
      await second.commit()

      await first.commit()

      third = await executor.beginTransaction()
      const claims3 = await new LifecycleTenantStorePg(third).claimDueTenantsWithinLeaseLimit({
        maxLeased: limit,
        leaseMs: 60_000,
      })
      expect(claims3.filter((claim) => ids.includes(claim.tenantId))).toEqual([])
      await third.commit()

      const claimed = await pool.query(
        'SELECT count(*)::int AS n FROM lifecycle_tenants WHERE tenant_id = ANY($1::text[]) AND claim_id IS NOT NULL',
        [ids]
      )
      expect(claimed.rows[0].n).toBe(2)
    } finally {
      if (first && !first.isCompleted()) await first.rollback()
      if (second && !second.isCompleted()) await second.rollback()
      if (third && !third.isCompleted()) await third.rollback()
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

  it.each([
    [
      'complete',
      (deadline: string) =>
        store.completeTenantDispatch({
          tenantId: 'tenant-a',
          claimId,
          nextDispatchAt: new Date().toISOString(),
          deadline,
        }),
    ],
    [
      'fail',
      (deadline: string) =>
        store.failTenantDispatch({
          tenantId: 'tenant-a',
          claimId,
          error: { code: 'offline' },
          deadline,
        }),
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

      const complete = new LifecycleTenantStorePg(executor).completeTenantDispatch({
        tenantId,
        claimId,
        nextDispatchAt: new Date().toISOString(),
        deadline: new Date(Date.now() + 1000).toISOString(),
      })
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
      'start',
      async (store: LifecycleTenantStorePg, tenantId: string, deadline: string) =>
        (await store.startTenantDispatch({ tenantId, claimId, deadline, leaseMs: 180_000 })) !==
        undefined,
    ],
    [
      'complete',
      (store: LifecycleTenantStorePg, tenantId: string, deadline: string) =>
        store.completeTenantDispatch({
          tenantId,
          claimId,
          nextDispatchAt: new Date().toISOString(),
          deadline,
        }),
    ],
    [
      'fail',
      (store: LifecycleTenantStorePg, tenantId: string, deadline: string) =>
        store.failTenantDispatch({ tenantId, claimId, error: { message: 'boom' }, deadline }),
    ],
    [
      'release',
      (store: LifecycleTenantStorePg, tenantId: string, deadline: string) =>
        store.releaseTenantDispatch({ tenantId, claimId, deadline }),
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

  it('abandons a release blocked on a row lock when its signal aborts', async () => {
    const tenantId = `release-abort-${randomUUID()}`
    const pool = new Pool({ connectionString: getConfig().multitenantDatabaseUrl, max: 3 })
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

      const started = Date.now()
      await expect(
        new LifecycleTenantStorePg(executor).releaseTenantDispatch({
          tenantId,
          claimId,
          deadline: new Date(Date.now() + 60_000).toISOString(),
          signal: AbortSignal.timeout(200),
        })
      ).rejects.toMatchObject({ code: 'ABORT_ERR' })
      expect(Date.now() - started).toBeLessThan(2000)
      await expect
        .poll(
          async () =>
            (
              await pool.query(
                "SELECT count(*)::int AS count FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%UPDATE lifecycle_tenants AS target%'"
              )
            ).rows[0].count
        )
        .toBe(0)

      await holder.commit()
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
  }, 10_000)

  it('makes a released claim immediately claimable without counting a failure', async () => {
    await seedClaimed()

    await expect(
      store.releaseTenantDispatch({ tenantId: 'tenant-a', claimId, deadline: deadline() })
    ).resolves.toBe(true)

    const [next] = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 180_000 })
    expect(next).toMatchObject({ tenantId: 'tenant-a', reclaimed: false })
    expect(next.claimId).not.toBe(claimId)
    const result = await transaction!.query('SELECT failure_count FROM lifecycle_tenants')
    expect(result.rows).toEqual([{ failure_count: 0 }])
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

    const first = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 60_000 })
    const second = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 4, leaseMs: 60_000 })
    const third = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 5, leaseMs: 60_000 })

    expect(first.map((claim) => claim.tenantId)).toEqual(['expired-old'])
    expect(second.map((claim) => claim.tenantId).sort()).toEqual(['due-old', 'expired-new'])
    expect(third.map((claim) => claim.tenantId)).toEqual(['due-new'])
    const result = await transaction!.query(`
      SELECT extract(epoch FROM claim_until - last_dispatched_at)::float8 AS lease,
        next_dispatch_at, claim_id
      FROM lifecycle_tenants WHERE tenant_id = 'due-old'
    `)
    expect(result.rows[0].lease).toBeCloseTo(60, 1)
    expect(result.rows[0].next_dispatch_at).toBe(Number.POSITIVE_INFINITY)
    expect(result.rows[0].claim_id).toBe(
      second.find((claim) => claim.tenantId === 'due-old')?.claimId
    )
  })

  it('picks the earliest due unclaimed tenant when more are due than the limit', async () => {
    await transaction!.query(`
      INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at) VALUES
        ('due-new', now() - interval '1 minute'),
        ('due-old', now() - interval '10 minutes'),
        ('tenant-a', now() - interval '5 minutes')
    `)

    const claims = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 60_000 })

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
    let claimer: DatabaseTransaction | undefined
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

      claimer = await executor.beginTransaction()
      const claims = await new LifecycleTenantStorePg(claimer).claimDueTenantsWithinLeaseLimit({
        maxLeased: 2,
        leaseMs: 60_000,
      })
      await claimer.commit()

      expect(claims.map((claim) => claim.tenantId).sort()).toEqual(['free-a', 'free-b'])
    } finally {
      if (claimer && !claimer.isCompleted()) await claimer.rollback()
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
    let claimer: DatabaseTransaction | undefined
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

      claimer = await executor.beginTransaction()
      const claims = await new LifecycleTenantStorePg(claimer).claimDueTenantsWithinLeaseLimit({
        maxLeased: 2,
        leaseMs: 60_000,
      })
      await claimer.commit()

      expect(claims.map((claim) => claim.tenantId).sort()).toEqual(['free-a', 'free-b'])
    } finally {
      if (claimer && !claimer.isCompleted()) await claimer.rollback()
      await lock?.rollback()
      await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`)
      await pool.end()
    }
  })

  it('evicts expired failed leases into backoff and admits waiting tenants in the same poll', async () => {
    await transaction!.query(`
      INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until, failure_count) VALUES
        ('expired-old', 'infinity', gen_random_uuid(), now() - interval '1 minute', 1),
        ('expired-new', 'infinity', gen_random_uuid(), now() - interval '1 minute', 0)
    `)
    await transaction!.query(`
      UPDATE lifecycle_tenants SET last_error = jsonb_build_object('claimId', claim_id)
      WHERE tenant_id = 'expired-old'
    `)
    await transaction!.query(
      "INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at) VALUES ('due-old', now() - interval '1 day')"
    )

    const claims = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 2, leaseMs: 60_000 })

    expect(
      claims
        .map(({ tenantId, reclaimed }) => ({ tenantId, reclaimed }))
        .sort((a, b) => a.tenantId.localeCompare(b.tenantId))
    ).toEqual([
      { tenantId: 'due-old', reclaimed: false },
      { tenantId: 'expired-new', reclaimed: true },
    ])

    const { rows } = await transaction!.query(`
      SELECT claim_id, claim_until, failure_count, last_error,
        extract(epoch FROM next_dispatch_at - clock_timestamp())::float8 AS delay
      FROM lifecycle_tenants
      WHERE tenant_id = 'expired-old'
    `)
    expect(rows).toHaveLength(1)
    expect(rows[0].claim_id).toBeNull()
    expect(rows[0].claim_until).toBeNull()
    expect(rows[0].failure_count).toBe(2)
    expect(rows[0].last_error).toEqual({
      code: 'tenant_dispatch_lease_expired',
      message: 'Tenant dispatch lease expired before completion',
    })
    expect(rows[0].delay).toBeGreaterThan(595)
    expect(rows[0].delay).toBeLessThanOrEqual(600)
  })

  it('evicts an expired failed lease without overflow after a very large failure count', async () => {
    await transaction!.query({
      text: `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until, failure_count, last_error)
        VALUES ('tenant-a', 'infinity', $1, now() - interval '1 second', 5000, jsonb_build_object('claimId', $1::uuid::text))`,
      values: [claimId],
    })
    await transaction!.query(
      "INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at) VALUES ('due-old', now() - interval '1 minute')"
    )

    const claims = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 1, leaseMs: 180_000 })

    expect(claims.map(({ tenantId, reclaimed }) => ({ tenantId, reclaimed }))).toEqual([
      { tenantId: 'due-old', reclaimed: false },
    ])
    const { rows } = await transaction!.query(`
      SELECT claim_id, claim_until, failure_count,
        extract(epoch FROM next_dispatch_at - clock_timestamp())::float8 AS delay
      FROM lifecycle_tenants WHERE tenant_id = 'tenant-a'
    `)
    expect(rows[0].claim_id).toBeNull()
    expect(rows[0].claim_until).toBeNull()
    expect(rows[0].failure_count).toBe(5001)
    expect(rows[0].delay).toBeGreaterThan(21_595)
    expect(rows[0].delay).toBeLessThanOrEqual(21_600)
  })

  it('keeps reclaiming an unstarted expired lease of a tenant with an earlier failure instead of evicting it', async () => {
    const staleClaimId = randomUUID()
    await transaction!.query({
      text: `INSERT INTO lifecycle_tenants (tenant_id, next_dispatch_at, claim_id, claim_until, failure_count, last_error) VALUES
        ('expired-old', 'infinity', $1, now() - interval '2 minutes', 1, '{"code":"offline"}'),
        ('expired-new', 'infinity', $2, now() - interval '1 minute', 1, jsonb_build_object('claimId', $3::text))`,
      values: [claimId, randomUUID(), staleClaimId],
    })
    const { rows: seeded } = await transaction!.query(
      'SELECT tenant_id, claim_id, last_error FROM lifecycle_tenants ORDER BY tenant_id'
    )

    const claims = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 2, leaseMs: 60_000 })

    expect(
      claims
        .map(({ tenantId, claimId, reclaimed }) => ({ tenantId, claimId, reclaimed }))
        .sort((a, b) => a.tenantId.localeCompare(b.tenantId))
    ).toEqual(
      seeded.map((row) => ({ tenantId: row.tenant_id, claimId: row.claim_id, reclaimed: true }))
    )
    const { rows } = await transaction!.query(
      'SELECT tenant_id, claim_until, failure_count, last_error FROM lifecycle_tenants ORDER BY tenant_id'
    )
    expect(
      rows.map((row) => ({
        tenant_id: row.tenant_id,
        failure_count: row.failure_count,
        last_error: row.last_error,
        leased: row.claim_until !== null,
      }))
    ).toEqual(
      seeded.map((row) => ({
        tenant_id: row.tenant_id,
        failure_count: 1,
        last_error: row.last_error,
        leased: true,
      }))
    )

    await transaction!.query(
      "UPDATE lifecycle_tenants SET claim_until = clock_timestamp() - interval '1 second'"
    )
    const again = await store.claimDueTenantsWithinLeaseLimit({ maxLeased: 2, leaseMs: 60_000 })

    expect(again.map(({ claimId, reclaimed }) => ({ claimId, reclaimed }))).toEqual(
      expect.arrayContaining(seeded.map((row) => ({ claimId: row.claim_id, reclaimed: true })))
    )
    const { rows: counts } = await transaction!.query(
      'SELECT failure_count FROM lifecycle_tenants WHERE claim_until IS NOT NULL'
    )
    expect(counts).toEqual([{ failure_count: 1 }, { failure_count: 1 }])
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
        'CREATE TABLE lifecycle_tenants (tenant_id text PRIMARY KEY, next_dispatch_at timestamptz NOT NULL, wake_floor_at timestamptz)'
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
