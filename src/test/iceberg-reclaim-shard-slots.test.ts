import { randomUUID } from 'node:crypto'
import {
  type DatabaseTransaction,
  type DatabaseTransactionalExecutor,
  multitenantPgExecutor,
} from '@internal/database'
import { runMultitenantMigrations } from '@internal/database/migrations'
import { PgPoolExecutor } from '@internal/database/pg-connection'
import { PgShardStoreFactory, ShardCatalog } from '@internal/sharding'
import { IcebergError, IcebergErrorType } from '@storage/protocols/iceberg/catalog/errors'
import {
  IcebergShardSlotReclaimer,
  type ReclaimShardSlotsOptions,
} from '@storage/protocols/iceberg/catalog/reclaim-shard-slots'
import { IcebergCatalogReconciler } from '@storage/protocols/iceberg/catalog/reconciler'
import type { RestCatalogClient } from '@storage/protocols/iceberg/catalog/rest-catalog-client'
import { PgMetastore } from '@storage/protocols/iceberg/pg'
import { Pool } from 'pg'
import { getConfig } from '../config'

const missing = new IcebergError('missing', IcebergErrorType.NoSuchTableException, 404)

describe('Iceberg reclamation and restoration with PostgreSQL', () => {
  let pool: Pool
  let db: PgPoolExecutor
  let namespaceId: string
  let catalogId: string
  let tenantId: string
  let shardId: string
  let shardKey: string
  let options: { dryRun: boolean; shardId: string }
  const applicationName = `reclaim-test-${randomUUID()}`
  const upstream = {
    listNamespaces: vi.fn().mockResolvedValue({ namespaces: [] }),
    tableExists: vi.fn().mockRejectedValue(missing),
    loadTable: vi.fn().mockRejectedValue(missing),
  }
  beforeAll(async () => {
    await runMultitenantMigrations()
    const { multitenantDatabaseUrl } = getConfig()
    if (!multitenantDatabaseUrl) throw new Error('Multitenant test database URL is required')
    pool = new Pool({
      connectionString: multitenantDatabaseUrl,
      application_name: applicationName,
      max: 5,
    })
    db = new PgPoolExecutor(pool)
  })
  beforeEach(async () => {
    vi.clearAllMocks()
    upstream.tableExists.mockRejectedValue(missing)
    upstream.loadTable.mockRejectedValue(missing)
    upstream.listNamespaces.mockResolvedValue({ namespaces: [] })
    namespaceId = randomUUID()
    catalogId = randomUUID()
    tenantId = `reclaim-${catalogId}`
    shardKey = `reclaim-${randomUUID()}`
    const shard = await pool.query(
      "INSERT INTO shard (kind, shard_key, capacity, status, next_slot) VALUES ('iceberg-table', $1, 1000, 'active', 1000) RETURNING id",
      [shardKey]
    )
    shardId = String(shard.rows[0].id)
    options = { dryRun: false, shardId }
    await pool.query('INSERT INTO iceberg_catalogs (id, name, tenant_id) VALUES ($1,$2,$3)', [
      catalogId,
      catalogId,
      tenantId,
    ])
    await pool.query(
      'INSERT INTO iceberg_namespaces (id, tenant_id, bucket_name, name, catalog_id) VALUES ($1,$2,$3,$4,$5)',
      [namespaceId, tenantId, catalogId, namespaceId, catalogId]
    )
  })
  afterAll(async () => {
    await pool?.end()
  })
  afterEach(async () => {
    vi.restoreAllMocks()
    await pool.query('DELETE FROM iceberg_tables WHERE catalog_id = $1', [catalogId])
    await pool.query('DELETE FROM iceberg_namespaces WHERE catalog_id = $1', [catalogId])
    await pool.query('DELETE FROM iceberg_catalogs WHERE id = $1', [catalogId])
    await pool.query('DELETE FROM shard_reservation WHERE shard_id = $1', [shardId])
    await pool.query('DELETE FROM shard WHERE id = $1', [shardId])
  })

  function runBatch(opts: ReclaimShardSlotsOptions = options) {
    return new IcebergShardSlotReclaimer(db, upstream).runBatch(opts)
  }

  function getReservations(columns = '*') {
    return pool
      .query(`SELECT ${columns} FROM shard_reservation WHERE shard_id = $1 ORDER BY slot_no`, [
        shardId,
      ])
      .then((result) => result.rows)
  }

  function getReservationById(id: string, columns = '*') {
    return pool
      .query(`SELECT ${columns} FROM shard_reservation WHERE id = $1`, [id])
      .then((result) => result.rows)
  }

  function getSlots(columns = '*') {
    return pool
      .query(`SELECT ${columns} FROM shard_slots WHERE shard_id = $1 ORDER BY slot_no`, [shardId])
      .then((result) => result.rows)
  }

  async function snapshotAllocations() {
    return { reservations: await getReservations(), slots: await getSlots() }
  }

  function metastore(executor: DatabaseTransactionalExecutor | DatabaseTransaction = db) {
    return new PgMetastore(executor, { multiTenant: true, schema: 'public' })
  }

  function lockNamespace(executor: DatabaseTransactionalExecutor | DatabaseTransaction) {
    return metastore(executor).lockResource('namespace', `${tenantId}:${namespaceId}`)
  }

  async function waitForAdvisoryLockWait() {
    await vi.waitFor(
      async () => {
        const waits = await pool.query(
          "SELECT 1 FROM pg_stat_activity WHERE application_name = $1 AND wait_event = 'advisory'",
          [applicationName]
        )
        expect(waits.rowCount).toBe(1)
      },
      { timeout: 1000 }
    )
  }

  async function seed({
    namespace = namespaceId,
    slot = 0,
    tenant = tenantId,
    key = catalogId,
    status = 'confirmed',
  }: {
    namespace?: string
    slot?: number
    tenant?: string
    key?: string
    status?: string
  } = {}) {
    const id = randomUUID()
    const resource = `iceberg-table::${key}::${namespace}/table-${slot}`
    await pool.query(
      'INSERT INTO shard_slots (shard_id, slot_no, tenant_id, resource_id) VALUES ($1,$2,$3,$4)',
      [shardId, slot, tenant, resource]
    )
    await pool.query(
      `INSERT INTO shard_reservation (id,kind,tenant_id,resource_id,shard_id,slot_no,status,lease_expires_at)
      VALUES ($1,'iceberg-table',$2,$3,$6,$4,$5,now() + interval '1 day')`,
      [id, tenant, resource, slot, status, shardId]
    )
    return id
  }
  function insertLocal(
    name = 'table-0',
    executor: DatabaseTransactionalExecutor | DatabaseTransaction = db
  ) {
    return metastore(executor).createTable({
      tenantId,
      namespaceId,
      name,
      bucketId: catalogId,
      bucketName: catalogId,
      shardId,
      shardKey,
      location: `s3://${catalogId}/${name}`,
    })
  }

  async function restoreLegacyTable(
    targetNamespaceId: string,
    location: string | null = `s3://${shardKey}/table-0`,
    tableNames = ['table-0']
  ) {
    await pool.query('UPDATE iceberg_catalogs SET name = $1 WHERE id = $2', [
      'warehouse',
      catalogId,
    ])
    const { rows } = await pool.query('SELECT * FROM shard WHERE id = $1', [shardId])
    vi.spyOn(ShardCatalog.prototype, 'listShardByKind').mockResolvedValue(rows)
    vi.spyOn(multitenantPgExecutor, 'query').mockImplementation(db.query.bind(db))
    vi.spyOn(multitenantPgExecutor, 'beginTransaction').mockImplementation(
      db.beginTransaction.bind(db)
    )
    const namespace = `${tenantId}_${targetNamespaceId.replaceAll('-', '_')}`
    const catalog = {
      listNamespaces: vi.fn().mockResolvedValue({ namespaces: [[namespace]] }),
      listTables: vi.fn().mockResolvedValue({
        identifiers: tableNames.map((name) => ({ namespace: [namespace], name })),
      }),
      dropTable: vi.fn(),
      loadTable: vi.fn().mockImplementation(async ({ table }: { table: string }) => ({
        metadata: {
          location: table === 'table-0' ? location : `s3://${shardKey}/${table}`,
          'table-uuid': randomUUID(),
        },
      })),
    }
    await new IcebergCatalogReconciler(catalog as unknown as RestCatalogClient).reconcile()
    return catalog
  }

  it('restores each namespace to its local owner', async () => {
    const otherNamespaceId = randomUUID()
    const otherCatalogId = randomUUID()
    await pool.query('INSERT INTO iceberg_catalogs (id, name, tenant_id) VALUES ($1,$2,$3)', [
      otherCatalogId,
      'other-warehouse',
      tenantId,
    ])
    try {
      await pool.query(
        'INSERT INTO iceberg_namespaces (id, tenant_id, bucket_name, name, catalog_id) VALUES ($1,$2,$3,$4,$5)',
        [otherNamespaceId, tenantId, 'other-warehouse', otherNamespaceId, otherCatalogId]
      )
      await seed()
      await restoreLegacyTable(namespaceId, undefined, ['table-0'])
      await seed({ namespace: otherNamespaceId, slot: 1, key: otherCatalogId })
      await restoreLegacyTable(otherNamespaceId, undefined, ['table-1'])
      expect(
        (
          await pool.query(
            'SELECT name, catalog_id::text, bucket_name FROM iceberg_tables WHERE tenant_id = $1 ORDER BY name',
            [tenantId]
          )
        ).rows
      ).toEqual([
        { name: 'table-0', catalog_id: catalogId, bucket_name: 'warehouse' },
        { name: 'table-1', catalog_id: otherCatalogId, bucket_name: 'other-warehouse' },
      ])
      expect(await getReservations('id')).toHaveLength(2)
    } finally {
      await pool.query('DELETE FROM iceberg_catalogs WHERE id = $1', [otherCatalogId])
    }
  })

  it.each([
    'missing namespace',
    'deleted catalog',
    'namespace tenant',
    'catalog tenant',
  ])('preserves upstream and allocation when ownership is unresolved: %s', async (reason) => {
    await seed()
    if (reason === 'missing namespace') {
      await pool.query('DELETE FROM iceberg_namespaces WHERE id = $1', [namespaceId])
    } else if (reason === 'deleted catalog') {
      await pool.query('UPDATE iceberg_catalogs SET deleted_at = now() WHERE id = $1', [catalogId])
    } else if (reason === 'namespace tenant') {
      await pool.query('UPDATE iceberg_namespaces SET tenant_id = $1 WHERE id = $2', [
        `${tenantId}-other`,
        namespaceId,
      ])
    } else {
      await pool.query('UPDATE iceberg_catalogs SET tenant_id = $1 WHERE id = $2', [
        `${tenantId}-other`,
        catalogId,
      ])
    }
    const before = await snapshotAllocations()
    const upstreamCatalog = await restoreLegacyTable(namespaceId)
    expect(upstreamCatalog.dropTable).not.toHaveBeenCalled()
    expect(await snapshotAllocations()).toEqual(before)
    expect(
      (await pool.query('SELECT id FROM iceberg_tables WHERE namespace_id = $1', [namespaceId]))
        .rows
    ).toEqual([])
  })

  it('restores a legacy allocation on a full shard and frees it using the catalog ID', async () => {
    const reservationId = await seed({ key: 'warehouse' })
    await pool.query('UPDATE shard SET capacity = 1, next_slot = 1 WHERE id = $1', [shardId])
    await restoreLegacyTable(namespaceId)
    const resourceId = `iceberg-table::${catalogId}::${namespaceId}/table-0`
    expect(await getReservations('id, resource_id, slot_no')).toEqual([
      { id: reservationId, resource_id: resourceId, slot_no: 0 },
    ])
    expect(await getSlots('resource_id')).toEqual([{ resource_id: resourceId }])
    expect(
      (
        await pool.query('SELECT shard_id::text FROM iceberg_tables WHERE catalog_id = $1', [
          catalogId,
        ])
      ).rows
    ).toEqual([{ shard_id: shardId }])
    const sharder = new ShardCatalog(new PgShardStoreFactory(db))
    await sharder.freeByResource(shardId, {
      kind: 'iceberg-table',
      tenantId,
      bucketName: catalogId,
      logicalName: `${namespaceId}/table-0`,
    })
    expect(await getReservations('id')).toEqual([])
    expect(await getSlots('resource_id')).toEqual([{ resource_id: null }])
  })

  it('rolls back invalid restored metadata and restores the next table', async () => {
    const id = await seed({ key: 'warehouse' })
    await seed({ slot: 1, key: 'warehouse' })
    await restoreLegacyTable(namespaceId, null, ['table-0', 'table-1'])
    const resourceId = `iceberg-table::warehouse::${namespaceId}/table-0`
    expect(await getReservationById(id, 'resource_id')).toEqual([{ resource_id: resourceId }])
    expect(await getSlots('resource_id')).toEqual([
      { resource_id: resourceId },
      { resource_id: `iceberg-table::${catalogId}::${namespaceId}/table-1` },
    ])
    expect(
      (await pool.query('SELECT name FROM iceberg_tables WHERE catalog_id = $1', [catalogId])).rows
    ).toEqual([{ name: 'table-1' }])
    expect(await getReservations('resource_id')).toEqual([
      { resource_id: resourceId },
      { resource_id: `iceberg-table::${catalogId}::${namespaceId}/table-1` },
    ])
  })

  async function seedCanonicalConflict(status: string, leaseExpired = false) {
    const id = await seed({ slot: 1, status })
    await pool.query(
      'UPDATE shard_slots SET resource_id = NULL WHERE shard_id = $1 AND slot_no = 1',
      [shardId]
    )
    await pool.query(
      'UPDATE shard_reservation SET resource_id = $1, lease_expires_at = now() + $2::interval WHERE id = $3',
      [
        `iceberg-table::${catalogId}::${namespaceId}/table-0`,
        leaseExpired ? '-1 hour' : '1 hour',
        id,
      ]
    )
    return id
  }

  it.each([
    'pending',
    'confirmed',
  ])('preserves a conflicting canonical %s reservation', async (status) => {
    const id = await seed({ key: 'warehouse' })
    const canonicalId = await seedCanonicalConflict(status, status === 'confirmed')
    await restoreLegacyTable(namespaceId)
    expect(await getReservationById(id, 'resource_id')).toEqual([
      { resource_id: `iceberg-table::warehouse::${namespaceId}/table-0` },
    ])
    expect(
      (await pool.query('SELECT id FROM iceberg_tables WHERE catalog_id = $1', [catalogId])).rows
    ).toEqual([])
    expect(await getReservationById(canonicalId, 'status')).toEqual([{ status }])
  })

  it.each([
    'cancelled',
    'expired',
    'pending',
  ])('removes a stale canonical %s reservation before legacy migration', async (status) => {
    const id = await seed({ key: 'warehouse' })
    const staleId = await seedCanonicalConflict(status, status === 'pending')
    await restoreLegacyTable(namespaceId)
    const resourceId = `iceberg-table::${catalogId}::${namespaceId}/table-0`
    expect(await getReservations('id, resource_id, slot_no')).toEqual([
      { id, resource_id: resourceId, slot_no: 0 },
    ])
    expect(await getSlots('resource_id')).toEqual([
      { resource_id: resourceId },
      { resource_id: null },
    ])
    expect(
      (await pool.query('SELECT name FROM iceberg_tables WHERE catalog_id = $1', [catalogId])).rows
    ).toEqual([{ name: 'table-0' }])
    expect(await getReservationById(staleId, 'id')).toEqual([])
  })

  it.each([
    'tenant',
    'slot resource',
    'status',
  ])('does not migrate a legacy allocation with a mismatched %s', async (mismatch) => {
    const id = await seed({ key: 'warehouse' })
    await pool.query('UPDATE shard SET capacity = 1, next_slot = 1 WHERE id = $1', [shardId])
    if (mismatch === 'tenant') {
      await pool.query('UPDATE shard_reservation SET tenant_id = $1 WHERE id = $2', [
        'other-tenant',
        id,
      ])
    } else if (mismatch === 'slot resource') {
      await pool.query('UPDATE shard_slots SET resource_id = $1 WHERE shard_id = $2', [
        'other-resource',
        shardId,
      ])
    } else {
      await pool.query("UPDATE shard_reservation SET status = 'pending' WHERE id = $1", [id])
    }
    const before = await snapshotAllocations()
    await restoreLegacyTable(namespaceId)
    expect(await snapshotAllocations()).toEqual(before)
    expect(
      (await pool.query('SELECT id FROM iceberg_tables WHERE catalog_id = $1', [catalogId])).rows
    ).toEqual([])
  })

  it('reclaims historical ID and name keys atomically and reuses the freed capacity', async () => {
    await seed()
    await seed({ slot: 1, key: 'warehouse' })
    expect(await runBatch()).toMatchObject({ scanned: 2, reclaimed: 2 })
    expect(await getReservations()).toEqual([])
    expect(await getSlots('resource_id, tenant_id')).toEqual([
      { resource_id: null, tenant_id: null },
      { resource_id: null, tenant_id: null },
    ])
    const shardCatalog = new ShardCatalog(new PgShardStoreFactory(db))
    const resource = {
      kind: 'iceberg-table' as const,
      tenantId,
      bucketName: catalogId,
      logicalName: `${namespaceId}/replacement`,
    }
    const reservation = await shardCatalog.reserve({ ...resource, shardId: Number(shardId) })
    await shardCatalog.confirm(reservation.reservationId, resource)
    expect(reservation.slotNo).toBe(0)
    expect(
      (await pool.query('SELECT next_slot FROM shard WHERE id = $1', [shardId])).rows[0].next_slot
    ).toBe(1000)
    await insertLocal('replacement')
    expect(await runBatch()).toMatchObject({
      scanned: 1,
      localTable: 1,
      reclaimed: 0,
    })
  })

  it('reclaims fresh confirmed orphans while preserving live tables and pending reservations', async () => {
    await seed()
    await seed({ slot: 1 })
    const freshOrphanId = await seed({ slot: 2 })
    await seed({ slot: 3, status: 'pending' })
    await insertLocal()
    upstream.tableExists.mockImplementation(async ({ table }) => {
      if (table === 'table-2') throw missing
    })
    expect(await runBatch()).toMatchObject({
      scanned: 3,
      localTable: 1,
      upstreamTable: 1,
      reclaimed: 1,
      reclaimedReservationIds: [freshOrphanId],
    })
    expect(await getReservations()).toHaveLength(3)
  })

  it('honors tenant/shard filters and dry-run without changing rows', async () => {
    await seed()
    await seed({ slot: 1, tenant: `${tenantId}-other` })
    expect(await runBatch({ ...options, dryRun: true, tenantId, shardId })).toMatchObject({
      scanned: 1,
      reclaimable: 1,
      reclaimed: 0,
    })
    expect(await runBatch({ ...options, shardId: '0' })).toMatchObject({ scanned: 0 })
    expect(await getReservations()).toHaveLength(2)
  })

  it('continues after a full page without skipping the remaining allocations', async () => {
    await pool.query(
      `
      INSERT INTO shard_slots (shard_id, slot_no, tenant_id, resource_id)
      SELECT $1, n, $2, $3 || n FROM generate_series(0, 100) n
    `,
      [shardId, tenantId, `iceberg-table::${catalogId}::${namespaceId}/table-`]
    )
    await pool.query(
      `
      INSERT INTO shard_reservation
        (kind, tenant_id, resource_id, shard_id, slot_no, status, lease_expires_at)
      SELECT 'iceberg-table', tenant_id, resource_id, shard_id, slot_no,
        'confirmed', now() FROM shard_slots WHERE shard_id = $1
    `,
      [shardId]
    )
    const first = await runBatch()
    expect(first).toMatchObject({ scanned: 100, reclaimed: 100 })
    expect(first.nextAfterReservationId).toBeDefined()
    const second = await runBatch({
      ...options,
      afterReservationId: first.nextAfterReservationId,
    })
    expect(second).toMatchObject({ scanned: 1, reclaimed: 1 })
    expect(second.nextAfterReservationId).toBeUndefined()
    expect(await getReservations()).toEqual([])
  })

  it('rolls back when upstream absence cannot be established', async () => {
    await seed()
    upstream.tableExists.mockRejectedValue(new Error('timeout'))
    await expect(runBatch()).rejects.toThrow('timeout')
    expect(await getReservations()).toHaveLength(1)
    expect((await getSlots('resource_id'))[0].resource_id).not.toBeNull()
  })

  it.each([
    'present',
    'unconfirmed',
  ])('preserves both allocation rows when GET is %s after HEAD 404', async (result) => {
    await seed()
    const before = await snapshotAllocations()
    if (result === 'present') {
      upstream.loadTable.mockResolvedValueOnce({ metadata: {} })
      expect(await runBatch()).toMatchObject({ upstreamTable: 1, reclaimed: 0 })
    } else {
      const error = new Error('Unconfirmed Iceberg error response')
      upstream.loadTable.mockRejectedValueOnce(error)
      await expect(runBatch()).rejects.toBe(error)
    }
    expect(upstream.loadTable).toHaveBeenCalledOnce()
    expect(await snapshotAllocations()).toEqual(before)
  })

  it('does not free a reservation replaced after candidate discovery', async () => {
    const oldId = await seed()
    const originalBegin = db.beginTransaction.bind(db)
    vi.spyOn(db, 'beginTransaction').mockImplementationOnce(async () => {
      await pool.query('UPDATE shard_reservation SET id = $1 WHERE id = $2', [randomUUID(), oldId])
      return originalBegin()
    })
    expect(await runBatch()).toMatchObject({
      changed: 1,
      reclaimed: 0,
    })
    expect(upstream.tableExists).not.toHaveBeenCalled()
    expect(await getReservations()).toHaveLength(1)
  })

  it.each([
    'resource_id',
    'tenant_id',
  ])('preserves an allocation when reservation %s changes after discovery', async (field) => {
    const id = await seed()
    const resourceId = `iceberg-table::${catalogId}::${namespaceId}/table-0`
    const replacement =
      field === 'resource_id' ? `${resourceId}-replacement` : `${tenantId}-replacement`
    const originalBegin = db.beginTransaction.bind(db)
    vi.spyOn(db, 'beginTransaction').mockImplementationOnce(async () => {
      await pool.query(`UPDATE shard_reservation SET ${field} = $1 WHERE id = $2`, [
        replacement,
        id,
      ])
      return originalBegin()
    })

    expect(await runBatch()).toMatchObject({
      scanned: 1,
      changed: 1,
      reclaimed: 0,
    })
    expect(upstream.tableExists).not.toHaveBeenCalled()
    expect(await getReservationById(id, 'id, resource_id, tenant_id')).toEqual([
      {
        id,
        resource_id: field === 'resource_id' ? replacement : resourceId,
        tenant_id: field === 'tenant_id' ? replacement : tenantId,
      },
    ])
    expect(await getSlots('resource_id, tenant_id')).toEqual([
      {
        resource_id: resourceId,
        tenant_id: tenantId,
      },
    ])
  })

  it('skips a locked allocation and reclaims the next candidate after rollback', async () => {
    const ids = [await seed(), await seed({ slot: 1 })].sort()
    const blocker = await db.beginTransaction()
    try {
      await blocker.query({
        text: `SELECT sl.slot_no FROM shard_slots sl
          JOIN shard_reservation r USING (shard_id, slot_no)
          WHERE r.id = $1 FOR UPDATE OF sl`,
        values: [ids[0]],
      })
      expect(await runBatch()).toMatchObject({
        scanned: 2,
        skipped: 1,
        reclaimed: 1,
        reclaimedReservationIds: [ids[1]],
      })
      expect(await getReservations('id')).toEqual([{ id: ids[0] }])
      expect(upstream.tableExists).toHaveBeenCalledOnce()
    } finally {
      await blocker.rollback()
    }
    expect(await runBatch()).toMatchObject({
      scanned: 1,
      skipped: 0,
      reclaimed: 1,
    })
  })

  it('skips a namespace-lock timeout and reclaims an unrelated namespace', async () => {
    await seed()
    const nextId = await seed({ slot: 1 })
    const nextResource = `iceberg-table::${catalogId}::${randomUUID()}/next-table`
    await pool.query('UPDATE shard_reservation SET resource_id = $1 WHERE id = $2', [
      nextResource,
      nextId,
    ])
    await pool.query(
      'UPDATE shard_slots SET resource_id = $1 WHERE shard_id = $2 AND slot_no = 1',
      [nextResource, shardId]
    )
    const blocker = await db.beginTransaction()
    try {
      await lockNamespace(blocker)
      expect(await runBatch()).toMatchObject({
        scanned: 2,
        skipped: 1,
        reclaimed: 1,
        reclaimedReservationIds: [nextId],
      })
      expect(await getReservations('slot_no')).toEqual([{ slot_no: 0 }])
      expect(upstream.tableExists).toHaveBeenCalledOnce()
    } finally {
      await blocker.rollback()
    }
  })

  it('waits for concurrent namespace writes and preserves a recreated local table', async () => {
    await seed()
    const creator = await db.beginTransaction()
    await lockNamespace(creator)
    await insertLocal('table-0', creator)
    const work = runBatch()
    try {
      await waitForAdvisoryLockWait()
      await creator.commit()
      expect(await work).toMatchObject({ localTable: 1, reclaimed: 0 })
      expect(upstream.tableExists).not.toHaveBeenCalled()
    } finally {
      await creator.rollback()
      await work.catch(() => {})
    }
  })

  it('allows namespace writes during warehouse validation and rechecks local metadata', async () => {
    await seed()
    upstream.listNamespaces.mockImplementationOnce(async () => {
      const creator = await db.beginTransaction()
      try {
        await creator.query("SET LOCAL lock_timeout = '100ms'")
        await lockNamespace(creator)
        await insertLocal('table-0', creator)
        await creator.commit()
      } catch (error) {
        await creator.rollback()
        throw error
      }
      return { namespaces: [] }
    })

    expect(await runBatch()).toMatchObject({
      localTable: 1,
      reclaimed: 0,
    })
    expect(upstream.tableExists).not.toHaveBeenCalled()
  })

  it('blocks concurrent creation while GET confirms upstream absence after HEAD 404', async () => {
    await seed()
    let checked!: () => void
    const checking = new Promise<void>((resolve) => {
      checked = resolve
    })
    let missingTable!: () => void
    upstream.loadTable.mockImplementationOnce(
      () =>
        new Promise<void>((_resolve, reject) => {
          checked()
          missingTable = () => reject(missing)
        })
    )
    const work = runBatch()
    await checking
    const creator = await db.beginTransaction()
    const lock = lockNamespace(creator)
    try {
      await waitForAdvisoryLockWait()
      missingTable()
      expect(await work).toMatchObject({ reclaimed: 1 })
      await lock
      const shardCatalog = new ShardCatalog(new PgShardStoreFactory(creator))
      const reservation = await shardCatalog.reserve({
        kind: 'iceberg-table',
        shardId: Number(shardId),
        tenantId,
        bucketName: catalogId,
        logicalName: `${namespaceId}/table-0`,
      })
      expect(reservation.slotNo).toBe(0)
    } finally {
      missingTable()
      await work.catch(() => {})
      await lock.catch(() => {})
      await creator.rollback()
    }
  })
})
