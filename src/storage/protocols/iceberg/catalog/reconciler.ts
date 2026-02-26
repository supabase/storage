import { multitenantKnex } from '@internal/database'
import { KnexShardStoreFactory, ShardCatalog, ShardRow } from '@internal/sharding'
import {
  ListTableResponse,
  RestCatalogClient,
} from '@storage/protocols/iceberg/catalog/rest-catalog-client'
import { TableIndex } from '@storage/protocols/iceberg/knex'
import { IcebergCatalog } from '@storage/schemas'

type NamespaceWithShardInfo = TableIndex & { shard_id?: string; shard_key?: string }

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
    const namespaces = await multitenantKnex
      .table('iceberg_namespaces')
      .select(
        'iceberg_namespaces.*',
        'iceberg_tables.shard_id as shard_id',
        'iceberg_tables.shard_key as shard_key'
      )
      .join('iceberg_tables', 'iceberg_namespaces.id', 'iceberg_tables.namespace_id')
      .distinct<NamespaceWithShardInfo[]>('iceberg_namespaces.name')

    await this.syncOrphanTables()
    await this.deleteUpstreamEmptyNamespaces(namespaces)
  }

  private async syncOrphanTables() {
    const sharding = new ShardCatalog(new KnexShardStoreFactory(multitenantKnex))
    const shards = await sharding.listShardByKind('iceberg-table')

    await Promise.allSettled(
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

              // List tables in the database for this namespace and shard
              const dbTables = await multitenantKnex
                .table('iceberg_tables')
                .whereIn(
                  'name',
                  tableBatch.map((t) => t.name)
                )
                .where('shard_key', shard.shard_key)
                .join('iceberg_namespaces', 'iceberg_tables.namespace_id', 'iceberg_namespaces.id')
                .where('iceberg_namespaces.id', dbNamespaceId)
                .select<TableIndex[]>(
                  'iceberg_tables.name',
                  'iceberg_namespaces.name',
                  'iceberg_tables.tenant_id'
                )

              await Promise.allSettled([
                this.deleteLocalOrphanTables(shard, dbTables, tableBatch),
                this.syncUpstreamOrphanTables(shard, tenantId, dbNamespaceId, dbTables, tableBatch),
              ])
            }
          }
        }
      })
    )
  }

  private async deleteLocalOrphanTables(
    shard: ShardRow,
    dbTables: TableIndex[],
    tableBatch: ListTableResponse['identifiers']
  ) {
    const tablesToDeleteInDb = dbTables.filter(
      (dbt) => !tableBatch.find((t) => t.name === dbt.name)
    )

    await multitenantKnex
      .table('iceberg_tables')
      .whereIn(
        'name',
        tablesToDeleteInDb.map((t) => t.name)
      )
      .where('shard_key', shard.shard_key)
      .del()
  }

  private async syncUpstreamOrphanTables(
    shard: ShardRow,
    tenantId: string,
    namespaceId: string,
    dbTables: TableIndex[],
    tableBatch: ListTableResponse['identifiers']
  ) {
    const shardCatalog = new ShardCatalog(new KnexShardStoreFactory(multitenantKnex))
    // Find tables that are in the catalog but not in the database
    const tablesMissing = tableBatch.filter((t) => !dbTables.find((dbt) => dbt.name === t.name))

    if (tablesMissing.length === 0) {
      return
    }

    await multitenantKnex.transaction(async (tnx) => {
      await Promise.all(
        tablesMissing.map(async (table) => {
          const namespaceResp = await this.restCatalog.loadNamespaceMetadata({
            warehouse: shard.shard_key,
            namespace: table.namespace[0],
          })

          const tableResp = await this.restCatalog.loadTable({
            warehouse: shard.shard_key,
            namespace: table.namespace[0],
            table: table.name,
          })

          let catalogId = namespaceResp.properties?.['bucket-name'] as string | undefined

          if (!catalogId) {
            const firstCatalog = await multitenantKnex
              .table<IcebergCatalog>('iceberg_catalogs')
              .select('name')
              .where('tenant_id', tenantId)
              .first()

            if (!firstCatalog) {
              // There is no catalog in the user database, meaning that the only thing we can do
              // is delete the table from the upstream catalog
              await this.restCatalog.dropTable({
                warehouse: shard.shard_key,
                namespace: table.namespace[0],
                table: table.name,
              })

              // Also special case here, since the tenant has no catalog, we can free up the shard slots
              await tnx.raw(
                `
                WITH shard_slots AS (
                  UPDATE shard_slots
                    SET resource_id = null, tenant_id = null
                    WHERE shard_id = ? AND tenant_id = ?
                    RETURNING shard_id, slot_no
                ),
                deleted_reservations AS (
                   DELETE FROM shard_reservation
                     WHERE shard_id = ?
                       AND tenant_id = ?
                )
                SELECT 1;
              `,
                [shard.id, tenantId, shard.id, tenantId]
              )
              return
            }

            catalogId = firstCatalog.name
          }

          const sharder = shardCatalog.withTnx(tnx)
          const existingShard = await sharder.findShardByResourceId({
            kind: 'iceberg-table',
            tenantId,
            bucketName: catalogId,
            logicalName: `${namespaceId}/${table.name}`,
          })

          if (!existingShard) {
            // Reserve a shard for this table
            const { reservationId } = await sharder.reserve({
              kind: 'iceberg-table',
              tenantId,
              bucketName: catalogId,
              logicalName: `${namespaceId}/${table.name}`,
              shardId: shard.id,
            })

            await sharder.confirm(reservationId, {
              kind: 'iceberg-table',
              tenantId,
              bucketName: catalogId,
              logicalName: `${namespaceId}/${table.name}`,
            })
          }

          await tnx.table('iceberg_tables').insert({
            name: table.name,
            namespace_id: namespaceId,
            location: tableResp.metadata.location,
            bucket_id: catalogId,
            tenant_id: tenantId,
            shard_id: shard.id,
            shard_key: shard.shard_key,
            remote_table_id: tableResp.metadata['table-uuid'],
          })
        })
      )
    })
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

  private async *listNamespaces(shardKey: string) {
    let restToken: string | undefined = undefined
    while (true) {
      const resp = await this.restCatalog.listNamespaces({
        warehouse: shardKey,
        pageSize: 1000,
        pageToken: restToken,
      })

      yield resp.namespaces

      if (!resp['next-page-token']) {
        break
      }

      restToken = resp['next-page-token']
    }
  }

  private async *listTables(namespaceName: string, shardKey: string) {
    let restToken: string | undefined = undefined
    while (true) {
      const resp = await this.restCatalog.listTables({
        warehouse: shardKey,
        namespace: namespaceName,
        pageSize: 1000,
        pageToken: restToken,
      })

      yield resp.identifiers

      if (!resp['next-page-token']) {
        break
      }

      restToken = resp['next-page-token']
    }
  }
}
