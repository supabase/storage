import type { DatabaseTransactionalExecutor } from '@internal/database'
import { DatabaseError } from 'pg'
import { IcebergError, IcebergErrorType } from './errors'
import { IcebergShardSlotReclaimer, RECLAIM_BATCH_SIZE } from './reclaim-shard-slots'
import { BearerTokenAuth, RestCatalogClient } from './rest-catalog-client'

const namespaceId = '11111111-1111-4111-8111-111111111111'
const candidate = {
  id: '22222222-2222-4222-8222-222222222222',
  shard_id: '1',
  slot_no: 0,
  tenant_id: 'tenant-a',
  shard_key: 'shard-1',
  resource_id: `iceberg-table::catalog-id::${namespaceId}/table`,
}
const options = { dryRun: false }
const missing = new IcebergError('missing', IcebergErrorType.NoSuchTableException, 404)

function setup() {
  const query = vi.fn(async (statement: string | { text: string }) => {
    const sql = typeof statement === 'string' ? statement : statement.text
    return { rows: sql.includes('SELECT r.id') ? [{ id: candidate.id }] : [] }
  })
  const tnx = { query, commit: vi.fn(), rollback: vi.fn(), isCompleted: () => false }
  const db = {
    query: vi.fn().mockResolvedValue({ rows: [candidate] }),
    beginTransaction: vi.fn().mockResolvedValue(tnx),
  }
  const catalog = {
    listNamespaces: vi.fn().mockResolvedValue({ namespaces: [] }),
    tableExists: vi.fn().mockRejectedValue(missing),
    loadTable: vi.fn().mockRejectedValue(missing),
  }
  const executor = db as DatabaseTransactionalExecutor
  const reclaimer = new IcebergShardSlotReclaimer(executor, catalog)
  const writes = () =>
    query.mock.calls.filter(([s]) => typeof s !== 'string' && s.text.includes('updated_slots'))
  return { db, tnx, catalog, reclaimer, writes, executor }
}

describe('IcebergShardSlotReclaimer', () => {
  it.each([
    'catalog-id',
    'warehouse-name',
  ])('reclaims a verified orphan keyed by %s', async (key) => {
    const t = setup()
    t.db.query.mockResolvedValue({
      rows: [{ ...candidate, resource_id: `iceberg-table::${key}::${namespaceId}/table` }],
    })
    expect(await t.reclaimer.runBatch(options)).toMatchObject({ reclaimed: 1, scanned: 1 })
    expect(t.catalog.tableExists).toHaveBeenCalledWith({
      warehouse: 'shard-1',
      namespace: `tenant-a_${namespaceId.replaceAll('-', '_')}`,
      table: 'table',
    })
    expect(t.writes()).toHaveLength(1)
    expect(t.tnx.commit).toHaveBeenCalledOnce()
  })

  it.each([
    {},
    { dryRun: true },
  ])('reports dry-run candidates without changing allocations for %j', async (dryRunOptions) => {
    const t = setup()
    expect(await t.reclaimer.runBatch(dryRunOptions)).toMatchObject({
      reclaimable: 1,
      reclaimed: 0,
      reclaimedReservationIds: [candidate.id],
    })
    expect(t.writes()).toHaveLength(0)
  })

  it('keeps local tables without checking upstream table existence', async () => {
    const t = setup()
    t.tnx.query.mockResolvedValue({ rows: [{ id: candidate.id }] })
    expect(await t.reclaimer.runBatch(options)).toMatchObject({ localTable: 1 })
    expect(t.catalog.tableExists).not.toHaveBeenCalled()
    expect(t.writes()).toHaveLength(0)
  })

  it('validates the warehouse before opening a transaction, once per shard per batch', async () => {
    const t = setup()
    t.db.query.mockResolvedValue({ rows: [candidate, { ...candidate, slot_no: 1 }] })
    t.catalog.listNamespaces.mockImplementation(async () => {
      expect(t.db.beginTransaction).not.toHaveBeenCalled()
      return { namespaces: [] }
    })

    expect(await t.reclaimer.runBatch(options)).toMatchObject({ reclaimed: 2 })
    expect(t.catalog.listNamespaces).toHaveBeenCalledExactlyOnceWith({
      warehouse: candidate.shard_key,
      pageSize: 1,
    })
    expect(t.tnx.commit.mock.invocationCallOrder[0]).toBeLessThan(
      t.db.beginTransaction.mock.invocationCallOrder[1]
    )
  })

  it('keeps upstream tables with missing local metadata', async () => {
    const t = setup()
    t.catalog.tableExists.mockResolvedValue(undefined)
    expect(await t.reclaimer.runBatch(options)).toMatchObject({ upstreamTable: 1 })
    expect(t.catalog.loadTable).not.toHaveBeenCalled()
    expect(t.writes()).toHaveLength(0)
  })

  describe('HEAD 404 confirmation', () => {
    afterEach(() => vi.unstubAllGlobals())

    function setupHttpConfirmation() {
      const t = setup()
      const fetchMock = vi
        .fn<typeof fetch>()
        .mockResolvedValueOnce(Response.json({ namespaces: [] }))
        .mockResolvedValueOnce(new Response(null, { status: 404 }))
      vi.stubGlobal('fetch', fetchMock)
      const client = new RestCatalogClient({
        catalogUrl: 'https://catalog.example/v1',
        auth: new BearerTokenAuth({ token: 'test' }),
      })
      const reclaimer = new IcebergShardSlotReclaimer(t.executor, client)
      return { ...t, reclaimer, fetchMock, client }
    }

    it('keeps a table when GET succeeds after HEAD returned 404', async () => {
      const t = setupHttpConfirmation()
      t.fetchMock.mockResolvedValueOnce(Response.json({ metadata: {} }))

      expect(await t.reclaimer.runBatch(options)).toMatchObject({
        upstreamTable: 1,
        reclaimable: 0,
        reclaimed: 0,
      })
      expect(t.fetchMock.mock.calls.map(([, init]) => init?.method)).toEqual(['GET', 'HEAD', 'GET'])
      expect(t.fetchMock.mock.calls[2][0]).toEqual(t.fetchMock.mock.calls[1][0])
      expect(t.writes()).toHaveLength(0)
      expect(t.tnx.commit).toHaveBeenCalledOnce()
    })

    it.each([
      IcebergErrorType.NoSuchTableException,
      IcebergErrorType.NoSuchNamespaceException,
    ])('accepts a structured GET 404 with %s', async (type) => {
      for (const dryRun of [true, false]) {
        const t = setupHttpConfirmation()
        t.fetchMock.mockResolvedValueOnce(
          Response.json({ error: { message: 'missing', type, code: 404 } }, { status: 404 })
        )

        expect(await t.reclaimer.runBatch({ dryRun })).toMatchObject({
          reclaimable: dryRun ? 1 : 0,
          reclaimed: dryRun ? 0 : 1,
        })
        expect(t.writes()).toHaveLength(dryRun ? 0 : 1)
        expect(t.tnx.commit).toHaveBeenCalledOnce()
      }
    })

    it.each([
      { name: 'empty 404', status: 404, body: null, contentType: 'application/json' },
      { name: 'HTML 404', status: 404, body: '<html>Not found</html>', contentType: 'text/html' },
      { name: 'malformed JSON 404', status: 404, body: '{', contentType: 'application/json' },
      { name: 'unstructured JSON 404', status: 404, body: '{}', contentType: 'application/json' },
      {
        name: 'HTTP 500 with a missing-table envelope',
        status: 500,
        body: JSON.stringify({
          error: { message: 'missing', type: 'NoSuchTableException', code: 404 },
        }),
        contentType: 'application/json',
      },
      {
        name: 'HTTP 404 with an internal-error envelope',
        status: 404,
        body: JSON.stringify({
          error: { message: 'failed', type: 'InternalServerError', code: 500 },
        }),
        contentType: 'application/json',
      },
    ])('preserves the allocation on $name', async ({ status, body, contentType }) => {
      const t = setupHttpConfirmation()
      t.fetchMock.mockResolvedValueOnce(
        new Response(body, { status, headers: { 'Content-Type': contentType } })
      )

      await expect(t.reclaimer.runBatch(options)).rejects.toThrow('Iceberg catalog error envelope')
      expect(t.writes()).toHaveLength(0)
      expect(t.tnx.rollback).toHaveBeenCalledOnce()
      expect(t.tnx.commit).not.toHaveBeenCalled()
    })

    it('preserves the allocation when the confirmation request fails', async () => {
      const t = setupHttpConfirmation()
      t.fetchMock.mockRejectedValueOnce(new Error('timeout'))

      await expect(t.reclaimer.runBatch(options)).rejects.toThrow()
      expect(t.writes()).toHaveLength(0)
      expect(t.tnx.rollback).toHaveBeenCalledOnce()
    })

    it('does not treat an authorization error as confirmed table absence', async () => {
      const t = setupHttpConfirmation()
      vi.spyOn(t.client.auth, 'authorize').mockImplementation((request) => {
        if (request.method === 'GET' && request.url.includes('/tables/')) throw missing
        return request
      })

      await expect(t.reclaimer.runBatch(options)).rejects.toThrow('Failed to authorize')
      expect(t.fetchMock).toHaveBeenCalledTimes(2)
      expect(t.writes()).toHaveLength(0)
      expect(t.tnx.rollback).toHaveBeenCalledOnce()
    })
  })

  it.each([
    new Error('timeout'),
    new IcebergError('denied', IcebergErrorType.NotAuthorizedException, 403),
    new IcebergError('internal', IcebergErrorType.InternalServerError, 500),
    new IcebergError('ambiguous', IcebergErrorType.InternalServerError, 404),
  ])('does not reclaim on upstream error: %s', async (error) => {
    const t = setup()
    t.catalog.tableExists.mockRejectedValue(error)
    await expect(t.reclaimer.runBatch(options)).rejects.toBe(error)
    expect(t.writes()).toHaveLength(0)
    expect(t.tnx.rollback).toHaveBeenCalledOnce()
  })

  it('does not treat a missing warehouse as a missing table', async () => {
    const t = setup()
    t.catalog.listNamespaces.mockRejectedValue(missing)
    await expect(t.reclaimer.runBatch(options)).rejects.toBe(missing)
    expect(t.db.beginTransaction).not.toHaveBeenCalled()
    expect(t.catalog.tableExists).not.toHaveBeenCalled()
    expect(t.writes()).toHaveLength(0)
  })

  it('skips a reservation changed since discovery', async () => {
    const t = setup()
    t.tnx.query.mockResolvedValue({ rows: [] })
    expect(await t.reclaimer.runBatch(options)).toMatchObject({ changed: 1 })
    expect(t.catalog.tableExists).not.toHaveBeenCalled()
    expect(t.writes()).toHaveLength(0)
  })

  it.each(['55P03', '57014'])('rolls back and skips %s, then continues the batch', async (code) => {
    const t = setup()
    const next = { ...candidate, id: '33333333-3333-4333-8333-333333333333', slot_no: 1 }
    t.db.query.mockResolvedValue({ rows: [candidate, next] })
    t.tnx.query.mockRejectedValueOnce(
      Object.assign(new DatabaseError('contention', 0, 'error'), { code })
    )

    expect(await t.reclaimer.runBatch(options)).toMatchObject({
      scanned: 2,
      skipped: 1,
      reclaimed: 1,
      reclaimedReservationIds: [next.id],
    })
    expect(t.tnx.rollback).toHaveBeenCalledOnce()
    expect(t.tnx.rollback.mock.invocationCallOrder[0]).toBeLessThan(
      t.db.beginTransaction.mock.invocationCallOrder[1]
    )
    expect(t.tnx.commit).toHaveBeenCalledOnce()
    expect(t.writes()).toHaveLength(1)
  })

  it('propagates other SQL errors', async () => {
    const t = setup()
    const error = Object.assign(new DatabaseError('connection lost', 0, 'error'), { code: '08006' })
    t.tnx.query.mockRejectedValueOnce(error)
    await expect(t.reclaimer.runBatch(options)).rejects.toBe(error)
    expect(t.tnx.rollback).toHaveBeenCalledOnce()
  })

  it('does not skip when rollback fails', async () => {
    const t = setup()
    const contentionError = Object.assign(new DatabaseError('contention', 0, 'error'), {
      code: '55P03',
    })
    t.tnx.query.mockRejectedValueOnce(contentionError)
    const rollbackError = new Error('rollback failed')
    t.tnx.rollback.mockRejectedValueOnce(rollbackError)
    await expect(t.reclaimer.runBatch(options)).rejects.toBe(rollbackError)
    expect(rollbackError.cause).toBe(contentionError)
    expect(t.tnx.commit).not.toHaveBeenCalled()
  })

  it('skips malformed resource identities', async () => {
    const t = setup()
    t.db.query.mockResolvedValue({ rows: [{ ...candidate, resource_id: 'invalid' }] })
    expect(await t.reclaimer.runBatch(options)).toMatchObject({ unsupported: 1 })
    expect(t.db.beginTransaction).not.toHaveBeenCalled()
  })

  it('passes filters and cursor to discovery and returns a continuation for a full batch', async () => {
    const t = setup()
    t.db.query.mockResolvedValue({
      rows: Array.from({ length: RECLAIM_BATCH_SIZE }, () => ({
        ...candidate,
        resource_id: 'invalid',
      })),
    })
    const result = await t.reclaimer.runBatch({
      ...options,
      tenantId: 'tenant-a',
      shardId: '1',
      afterReservationId: candidate.id,
    })
    expect(t.db.query.mock.calls[0][0].values).toEqual([
      'tenant-a',
      '1',
      candidate.id,
      RECLAIM_BATCH_SIZE,
    ])
    expect(result.nextAfterReservationId).toBe(candidate.id)
  })
})
