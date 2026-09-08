import { type DatabaseTransaction, multitenantPgExecutor } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { ShardCatalog } from '@internal/sharding'
import { IcebergError, IcebergErrorType } from './errors'
import { IcebergCatalogReconciler } from './reconciler'
import type { RestCatalogClient } from './rest-catalog-client'

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
    {
      hasAllocation: true,
      legacyAllocation: true,
      secondTableError: undefined,
      restoredNames: ['table', 'second-table', 'third-table'],
    },
    {
      hasAllocation: true,
      secondTableError: undefined,
      restoredNames: ['table', 'second-table', 'third-table'],
    },
    {
      hasAllocation: false,
      secondTableError: undefined,
      restoredNames: ['table', 'second-table', 'third-table'],
    },
    {
      hasAllocation: false,
      secondTableError: new IcebergError(
        'table disappeared',
        IcebergErrorType.NoSuchTableException,
        404
      ),
      restoredNames: ['table', 'third-table'],
    },
    {
      hasAllocation: false,
      secondTableError: new IcebergError(
        'ambiguous 404',
        IcebergErrorType.InternalServerError,
        404
      ),
      restoredNames: ['table', 'third-table'],
    },
    {
      hasAllocation: false,
      secondTableError: new Error('timeout'),
      restoredNames: ['table', 'third-table'],
    },
    {
      hasAllocation: false,
      secondTableError: new Error('timeout'),
      rollbackError: new Error('rollback failed'),
      restoredNames: ['table'],
    },
    {
      hasAllocation: false,
      secondTableError: new IcebergError(
        'table disappeared',
        IcebergErrorType.NoSuchTableException,
        404
      ),
      rollbackError: new Error('rollback failed'),
      restoredNames: ['table'],
    },
  ])('restores $restoredNames with allocation=$hasAllocation, second table error=$secondTableError', async ({
    hasAllocation,
    legacyAllocation,
    secondTableError,
    rollbackError,
    restoredNames,
  }) => {
    const shard = {
      id: 1,
      kind: 'iceberg-table' as const,
      shard_key: 'shard-1',
      capacity: 1000,
      next_slot: 1,
      status: 'active' as const,
      created_at: '2026-01-01T00:00:00Z',
    }
    const query = vi.fn(async (statement: string | { text: string }) => {
      const sql = typeof statement === 'string' ? statement : statement.text
      return {
        rows: sql.includes('FROM iceberg_namespaces n')
          ? [{ id: 'catalog-id', name: 'warehouse' }]
          : legacyAllocation && sql.includes('renamed_reservation')
            ? [{ slot_no: 0 }]
            : [],
      }
    })
    const commit = vi.fn().mockResolvedValue(undefined)
    const rollback = vi.fn().mockResolvedValue(undefined)
    if (rollbackError) rollback.mockRejectedValue(rollbackError)
    const logError = vi.spyOn(logSchema, 'error').mockImplementation(() => {})
    vi.spyOn(logSchema, 'warning').mockImplementation(() => {})
    const begin = vi.spyOn(multitenantPgExecutor, 'beginTransaction').mockResolvedValue({
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
        hasAllocation && !legacyAllocation && resource.bucketName === 'catalog-id' ? shard : null
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
        identifiers: ['table', 'second-table', 'third-table'].map((name) => ({
          namespace: ['tenant-id_namespace_id'],
          name,
        })),
      }),
      loadNamespaceMetadata: vi.fn(),
      loadTable: vi.fn().mockImplementation(async ({ table }) => {
        expect(begin.mock.calls.length).toBe(
          commit.mock.calls.length + rollback.mock.calls.length + 1
        )
        expect(getLastStatement(query)).toContain('pg_advisory_xact_lock')
        if (secondTableError && table === 'second-table') {
          throw secondTableError
        }
        return { metadata: { location: 's3://shard-1/table', 'table-uuid': 'remote-table-id' } }
      }),
    }

    const work = new IcebergCatalogReconciler(
      restCatalog as unknown as RestCatalogClient
    ).reconcile()
    if (rollbackError) {
      await expect(work).rejects.toMatchObject({ cause: rollbackError })
    } else {
      await work
    }

    expect(restCatalog.loadNamespaceMetadata).not.toHaveBeenCalled()

    for (const name of restoredNames) {
      const resource = {
        kind: 'iceberg-table',
        tenantId: 'tenant-id',
        bucketName: 'catalog-id',
        logicalName: `namespace-id/${name}`,
      }
      expect(find).toHaveBeenCalledWith(resource)
      if (hasAllocation) {
        expect(reserve).not.toHaveBeenCalled()
        expect(confirm).not.toHaveBeenCalled()
      } else {
        expect(reserve).toHaveBeenCalledWith({ ...resource, shardId: 1 })
        expect(confirm).toHaveBeenCalledWith('reservation-id', resource)
      }
    }
    const inserts = query.mock.calls.filter(
      ([statement]) =>
        typeof statement !== 'string' && statement.text.includes('INSERT INTO iceberg_tables')
    )
    expect(inserts).toHaveLength(restoredNames.length)
    expect(begin).toHaveBeenCalledTimes(restoredNames.length + (secondTableError ? 1 : 0))
    expect(commit).toHaveBeenCalledTimes(restoredNames.length)
    expect(rollback).toHaveBeenCalledTimes(secondTableError ? 1 : 0)
    if (
      secondTableError &&
      (rollbackError ||
        !(
          secondTableError instanceof IcebergError &&
          secondTableError.type === IcebergErrorType.NoSuchTableException
        ))
    ) {
      expect(logError).toHaveBeenCalledExactlyOnceWith(
        logger,
        '[IcebergCatalogReconciler] Failed to restore table',
        expect.objectContaining({
          error: rollbackError
            ? expect.objectContaining({ cause: rollbackError })
            : secondTableError,
          project: 'tenant-id',
          metadata: JSON.stringify({
            shardId: 1,
            shardKey: 'shard-1',
            namespaceId: 'namespace-id',
            table: 'second-table',
          }),
        })
      )
    } else {
      expect(logError).not.toHaveBeenCalled()
    }
  })
})
