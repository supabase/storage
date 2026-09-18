import { type DatabaseTransaction, multitenantPgExecutor } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { PgShardStoreFactory, ShardCatalog, type ShardResource, ShardRow } from '@internal/sharding'
import {
  ListTableResponse,
  RestCatalogClient,
} from '@storage/protocols/iceberg/catalog/rest-catalog-client'
import { TableIndex } from '@storage/protocols/iceberg/metastore'
import { PgMetastore } from '@storage/protocols/iceberg/pg'
import { IcebergCatalog } from '@storage/schemas'
import { IcebergError, IcebergErrorType } from './errors'

type NamespaceWithShardInfo = TableIndex & { shard_id?: string; shard_key?: string }
type CatalogRow = Pick<IcebergCatalog, 'id' | 'name'>
type ReconcilerTransaction = DatabaseTransaction

class ReconciliationRollbackError extends Error {}

function rethrowRollbackFailure(results: PromiseSettledResult<unknown>[]) {
  for (const result of results) {
    if (result.status === 'rejected' && result.reason instanceof ReconciliationRollbackError) {
      throw result.reason
    }
  }
}

/**
 * Highly experimental reconciler for iceberg catalogs
 * It will try to ensure that the local database and the upstream catalog
 * are in sync, by performing the following actions:
 * - Deleting local tables that do not exist upstream
 * - Creating local tables for upstream tables that do not exist locally
 * - Deleting upstream namespaces that are empty
 */
export class IcebergCatalogReconciler {
  constructor(private readonly restCatalog: RestCatalogClient) {}

  async reconcile() {
    const namespaces = await this.listNamespacesWithShardInfo()

    await this.syncOrphanTables()
    await this.deleteUpstreamEmptyNamespaces(namespaces)
  }

  private async syncOrphanTables() {
    const sharding = this.createShardCatalog()
    const shards = await sharding.listShardByKind('iceberg-table')

    const shardResults = await Promise.allSettled(
      shards.map(async (shard) => {
        const namespaces = this.listNamespaces(shard.shard_key)

        for await (const nsBatch of namespaces) {
          for (const namespace of nsBatch) {
            const tables = this.listTables(namespace[0], shard.shard_key)

            for await (const tableBatch of tables) {
              const tenantId = namespace[0].split('_').shift()
              if (!tenantId) {
                continue
              }
              const dbNamespaceId = namespace[0].split('_').slice(1).join('-')
              if (!dbNamespaceId) {
                continue
              }

              const dbTables = await this.listDbTablesForBatch(
                dbNamespaceId,
                shard.shard_key,
                tableBatch.map((t) => t.name)
              )

              const results = await Promise.allSettled([
                this.deleteLocalOrphanTables(shard, dbTables, tableBatch),
                this.syncUpstreamOrphanTables(shard, tenantId, dbNamespaceId, dbTables, tableBatch),
              ])
              rethrowRollbackFailure(results)
            }
          }
        }
      })
    )
    rethrowRollbackFailure(shardResults)
  }

  private async deleteLocalOrphanTables(
    shard: ShardRow,
    dbTables: TableIndex[],
    tableBatch: ListTableResponse['identifiers']
  ) {
    const tablesToDeleteInDb = dbTables.filter(
      (dbt) => !tableBatch.find((t) => t.name === dbt.name)
    )

    if (tablesToDeleteInDb.length === 0) {
      return
    }

    await multitenantPgExecutor.query({
      text: `
        DELETE FROM iceberg_tables
        WHERE name = ANY($1::text[])
          AND shard_key = $2
      `,
      values: [tablesToDeleteInDb.map((t) => t.name), shard.shard_key],
    })
  }

  private async syncUpstreamOrphanTables(
    shard: ShardRow,
    tenantId: string,
    namespaceId: string,
    dbTables: TableIndex[],
    tableBatch: ListTableResponse['identifiers']
  ) {
    const shardCatalog = this.createShardCatalog()
    // Find tables that are in the catalog but not in the database
    const tablesMissing = tableBatch.filter((t) => !dbTables.find((dbt) => dbt.name === t.name))

    if (tablesMissing.length === 0) {
      return
    }

    for (const table of tablesMissing) {
      try {
        await this.withTransaction(async (tnx) => {
          // Serialize metadata restoration with create/drop and orphan allocation reclamation.
          await new PgMetastore(tnx, { multiTenant: true, schema: 'public' }).lockResource(
            'namespace',
            `${tenantId}:${namespaceId}`
          )
          // Re-read the table under the writer lock so a concurrent drop cannot
          // turn a prefetched response into stale restored metadata.
          const tableResp = await this.restCatalog.loadTable({
            warehouse: shard.shard_key,
            namespace: table.namespace[0],
            table: table.name,
          })

          const catalog = await this.findNamespaceCatalog(tnx, tenantId, namespaceId)
          if (!catalog) {
            throw new Error(
              'Cannot restore table without an active catalog owning the local namespace'
            )
          }

          const sharder = shardCatalog.withTnx(tnx)
          const resource: ShardResource = {
            kind: 'iceberg-table',
            tenantId,
            bucketName: catalog.id,
            logicalName: `${namespaceId}/${table.name}`,
          }
          const existingShard = await sharder.findShardByResourceId(resource)

          if (
            !existingShard &&
            !(await this.migrateLegacyAllocation(
              tnx,
              shard.id,
              tenantId,
              catalog,
              resource.logicalName
            ))
          ) {
            // Reserve a shard for this table
            const { reservationId } = await sharder.reserve({
              ...resource,
              shardId: shard.id,
            })

            await sharder.confirm(reservationId, resource)
          }

          await this.insertIcebergTable(tnx, {
            name: table.name,
            namespace_id: namespaceId,
            location: tableResp.metadata.location as string,
            catalog_id: catalog.id,
            bucket_name: catalog.name,
            tenant_id: tenantId,
            shard_id: shard.id,
            shard_key: shard.shard_key,
            remote_table_id: tableResp.metadata['table-uuid'],
          })
        })
      } catch (error) {
        // A table can disappear between listing and acquiring its namespace lock.
        if (
          error instanceof IcebergError &&
          error.code === 404 &&
          error.type === IcebergErrorType.NoSuchTableException
        )
          continue
        logSchema.error(logger, '[IcebergCatalogReconciler] Failed to restore table', {
          type: 'iceberg-reconciliation',
          project: tenantId,
          error,
          metadata: JSON.stringify({
            shardId: shard.id,
            shardKey: shard.shard_key,
            namespaceId,
            table: table.name,
          }),
        })
        if (error instanceof ReconciliationRollbackError) throw error
      }
    }
  }

  private async deleteUpstreamEmptyNamespaces(namespaces: NamespaceWithShardInfo[]) {
    await Promise.allSettled(
      namespaces.map(async (namespace) => {
        const namespaceName = `${namespace.tenant_id}_${namespace.id.replaceAll('-', '_')}`

        if (!namespace.shard_key) {
          return
        }

        const tables = await this.restCatalog.listTables({
          namespace: namespaceName,
          pageSize: 1,
          warehouse: namespace.shard_key,
        })

        if (tables.identifiers.length === 0) {
          await this.restCatalog.dropNamespace({
            namespace: namespaceName,
            warehouse: namespace.shard_key,
          })
        }
      })
    )
  }

  private createShardCatalog() {
    return new ShardCatalog(new PgShardStoreFactory(multitenantPgExecutor))
  }

  private async migrateLegacyAllocation(
    tnx: ReconcilerTransaction,
    shardId: number,
    tenantId: string,
    catalog: CatalogRow,
    logicalName: string
  ): Promise<boolean> {
    if (catalog.name === catalog.id) return false
    const legacyResourceId = `iceberg-table::${catalog.name}::${logicalName}`
    const resourceId = `iceberg-table::${catalog.id}::${logicalName}`
    const store = new PgShardStoreFactory(tnx).autocommit()
    // The caller holds the namespace lock. Also serialize with reservations under
    // either key, including older writers that still use the catalog name.
    for (const key of [legacyResourceId, resourceId].sort()) {
      await store.advisoryLockByString(key)
    }

    // Confirmed reservations keep their original lease timestamp. Only pending
    // leases expire; remove obsolete reservation metadata without freeing slots.
    await tnx.query({
      text: `
        DELETE FROM shard_reservation
        WHERE tenant_id = $1 AND kind = 'iceberg-table' AND resource_id = $2
          AND (
            status IN ('cancelled', 'expired')
            OR (status = 'pending' AND lease_expires_at < now())
          )
      `,
      values: [tenantId, resourceId],
    })

    const result = await tnx.query({
      text: `
        WITH legacy AS (
          SELECT r.id, r.shard_id, r.slot_no
          FROM shard_reservation r
          JOIN shard_slots sl ON sl.shard_id = r.shard_id AND sl.slot_no = r.slot_no
            AND sl.tenant_id = r.tenant_id AND sl.resource_id = r.resource_id
          WHERE r.kind = 'iceberg-table' AND r.status = 'confirmed'
            AND r.shard_id = $1 AND r.tenant_id = $2 AND r.resource_id = $3
          FOR UPDATE OF sl, r
        ), renamed_reservation AS (
          UPDATE shard_reservation r SET resource_id = $4
          FROM legacy
          WHERE r.id = legacy.id
          RETURNING r.shard_id, r.slot_no
        )
        UPDATE shard_slots sl SET resource_id = $4
        FROM renamed_reservation r
        WHERE sl.shard_id = r.shard_id AND sl.slot_no = r.slot_no
        RETURNING sl.slot_no
      `,
      values: [shardId, tenantId, legacyResourceId, resourceId],
    })
    return result.rows.length > 0
  }

  private async listNamespacesWithShardInfo(): Promise<NamespaceWithShardInfo[]> {
    const result = await multitenantPgExecutor.query<NamespaceWithShardInfo>(`
      SELECT DISTINCT
        iceberg_namespaces.*,
        iceberg_tables.shard_id AS shard_id,
        iceberg_tables.shard_key AS shard_key
      FROM iceberg_namespaces
      JOIN iceberg_tables ON iceberg_namespaces.id = iceberg_tables.namespace_id
    `)

    return result.rows
  }

  private async listDbTablesForBatch(
    namespaceId: string,
    shardKey: string,
    tableNames: string[]
  ): Promise<TableIndex[]> {
    if (tableNames.length === 0) {
      return []
    }

    const result = await multitenantPgExecutor.query<TableIndex>({
      text: `
        SELECT iceberg_tables.name, iceberg_tables.tenant_id
        FROM iceberg_tables
        JOIN iceberg_namespaces ON iceberg_tables.namespace_id = iceberg_namespaces.id
        WHERE iceberg_tables.name = ANY($1::text[])
          AND iceberg_tables.shard_key = $2
          AND iceberg_namespaces.id = $3
      `,
      values: [tableNames, shardKey, namespaceId],
    })

    return result.rows
  }

  private async withTransaction<T>(
    callback: (tnx: ReconcilerTransaction) => Promise<T>
  ): Promise<T> {
    const tnx = await multitenantPgExecutor.beginTransaction()
    try {
      const result = await callback(tnx)
      await tnx.commit()
      return result
    } catch (e) {
      try {
        await tnx.rollback()
      } catch (rollbackError) {
        logSchema.warning(logger, '[IcebergCatalogReconciler] Failed to rollback transaction', {
          type: 'db',
          error: rollbackError,
          metadata: JSON.stringify({ originalError: String(e) }),
        })
        throw new ReconciliationRollbackError('Failed to rollback reconciliation transaction', {
          cause: rollbackError,
        })
      }
      throw e
    }
  }

  private async findNamespaceCatalog(
    tnx: ReconcilerTransaction,
    tenantId: string,
    namespaceId: string
  ): Promise<CatalogRow | undefined> {
    const result = await tnx.query<CatalogRow>({
      text: `
        SELECT c.id, c.name
        FROM iceberg_namespaces n
        JOIN iceberg_catalogs c ON c.id = n.catalog_id
        WHERE n.id = $1::uuid AND n.tenant_id = $2 AND c.tenant_id = $2
          AND c.deleted_at IS NULL
        FOR SHARE OF n, c
      `,
      values: [namespaceId, tenantId],
    })
    return result.rows[0]
  }

  private async insertIcebergTable(
    tnx: ReconcilerTransaction,
    table: {
      name: string
      namespace_id: string
      location: string
      catalog_id: string
      bucket_name: string
      tenant_id: string
      shard_id: number
      shard_key: string
      remote_table_id?: string
    }
  ): Promise<void> {
    const entries = Object.entries(table)
    await tnx.query({
      text: `
        INSERT INTO iceberg_tables (${entries.map(([column]) => column).join(', ')})
        VALUES (${entries.map((_, index) => `$${index + 1}`).join(', ')})
      `,
      values: entries.map(([, value]) => value),
    })
  }

  private async *listNamespaces(shardKey: string) {
    let restToken: string | undefined
    do {
      const resp = await this.restCatalog.listNamespaces({
        warehouse: shardKey,
        pageSize: 1000,
        pageToken: restToken,
      })
      yield resp.namespaces
      restToken = resp['next-page-token']
    } while (restToken)
  }

  private async *listTables(namespaceName: string, shardKey: string) {
    let restToken: string | undefined
    do {
      const resp = await this.restCatalog.listTables({
        warehouse: shardKey,
        namespace: namespaceName,
        pageSize: 1000,
        pageToken: restToken,
      })
      yield resp.identifiers
      restToken = resp['next-page-token']
    } while (restToken)
  }
}
