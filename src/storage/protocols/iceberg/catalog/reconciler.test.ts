import { type DatabaseTransaction, multitenantPgExecutor } from '@internal/database'
import { ShardCatalog } from '@internal/sharding'
import { IcebergCatalogReconciler } from './reconciler'
import type { RestCatalogClient } from './rest-catalog-client'

function createReconciler() {
  return new IcebergCatalogReconciler({} as RestCatalogClient) as unknown as {
    findCatalogByName: (
      tnx: { query: ReturnType<typeof vi.fn> },
      tenantId: string,
      catalogName: string
    ) => Promise<unknown>
    findFirstCatalog: (
      tnx: { query: ReturnType<typeof vi.fn> },
      tenantId: string
    ) => Promise<unknown>
  }
}

function getLastStatement(query: ReturnType<typeof vi.fn>): string {
  const [statement] = query.mock.calls.at(-1) || []

  if (!statement || typeof statement === 'string') {
    throw new Error('Expected a DatabaseStatement query')
  }

  return String((statement as { text: string }).text)
}

describe('IcebergCatalogReconciler', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it.each([
    true,
    false,
  ])('uses catalog IDs when an allocation exists: %s', async (hasAllocation) => {
    const shard = {
      id: 1,
      kind: 'iceberg-table' as const,
      shard_key: 'shard-1',
      capacity: 1000,
      next_slot: 1,
      status: 'active' as const,
      created_at: '2026-01-01T00:00:00Z',
    }
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [{ id: 'catalog-id', name: 'warehouse' }] })
      .mockResolvedValue({ rows: [] })
    const commit = vi.fn().mockResolvedValue(undefined)
    const rollback = vi.fn().mockResolvedValue(undefined)
    vi.spyOn(multitenantPgExecutor, 'beginTransaction').mockResolvedValue({
      query,
      commit,
      rollback,
    } as unknown as DatabaseTransaction)
    vi.spyOn(multitenantPgExecutor, 'query').mockResolvedValue({
      rows: [],
      rowCount: 0,
      command: 'SELECT',
      oid: 0,
      fields: [],
    })
    vi.spyOn(ShardCatalog.prototype, 'listShardByKind').mockResolvedValue([shard])
    const find = vi
      .spyOn(ShardCatalog.prototype, 'findShardByResourceId')
      .mockImplementation(async (resource) =>
        hasAllocation && resource.bucketName === 'catalog-id' ? shard : null
      )
    const reserve = vi.spyOn(ShardCatalog.prototype, 'reserve').mockResolvedValue({
      reservationId: 'reservation-id',
      shardId: '1',
      shardKey: 'shard-1',
      slotNo: 0,
      leaseExpiresAt: '2099-01-01T00:00:00Z',
    })
    const confirm = vi.spyOn(ShardCatalog.prototype, 'confirm').mockResolvedValue(undefined)
    const restCatalog = {
      listNamespaces: vi.fn().mockResolvedValue({ namespaces: [['tenant-id_namespace_id']] }),
      listTables: vi.fn().mockResolvedValue({
        identifiers: [{ namespace: ['tenant-id_namespace_id'], name: 'table' }],
      }),
      loadNamespaceMetadata: vi
        .fn()
        .mockResolvedValue({ properties: { 'bucket-name': 'warehouse' } }),
      loadTable: vi.fn().mockResolvedValue({
        metadata: { location: 's3://shard-1/table', 'table-uuid': 'remote-table-id' },
      }),
    }

    await new IcebergCatalogReconciler(restCatalog as unknown as RestCatalogClient).reconcile()

    const resource = {
      kind: 'iceberg-table',
      tenantId: 'tenant-id',
      bucketName: 'catalog-id',
      logicalName: 'namespace-id/table',
    }
    expect(find).toHaveBeenCalledExactlyOnceWith(resource)
    if (hasAllocation) {
      expect(reserve).not.toHaveBeenCalled()
      expect(confirm).not.toHaveBeenCalled()
    } else {
      expect(reserve).toHaveBeenCalledExactlyOnceWith({ ...resource, shardId: 1 })
      expect(confirm).toHaveBeenCalledExactlyOnceWith('reservation-id', resource)
    }
    expect(getLastStatement(query)).toContain('INSERT INTO iceberg_tables')
    expect(commit).toHaveBeenCalledOnce()
    expect(rollback).not.toHaveBeenCalled()
  })

  it('ignores soft-deleted catalogs when finding an upstream orphan catalog by name', async () => {
    const query = vi.fn().mockResolvedValue({ rows: [] })
    const reconciler = createReconciler()

    await reconciler.findCatalogByName({ query }, 'tenant-id', 'catalog-name')

    expect(getLastStatement(query)).toContain('deleted_at IS NULL')
  })

  it('ignores soft-deleted catalogs when falling back to the first tenant catalog', async () => {
    const query = vi.fn().mockResolvedValue({ rows: [] })
    const reconciler = createReconciler()

    await reconciler.findFirstCatalog({ query }, 'tenant-id')

    expect(getLastStatement(query)).toContain('deleted_at IS NULL')
  })
})
