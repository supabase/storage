import { multitenantPgExecutor } from '@internal/database'
import { ERRORS } from '@internal/errors'
import type { BasePayload, WirePayload } from '@internal/queue'
import { PgShardStoreFactory, ShardCatalog } from '@internal/sharding'
import { getCatalogAuthStrategy, RestCatalogClient } from '@storage/protocols/iceberg/catalog'
import { IcebergError } from '@storage/protocols/iceberg/catalog/errors'
import { PgMetastore } from '@storage/protocols/iceberg/pg'
import type { Storage } from '@storage/storage'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { getConfig } from '../../../config'
import { createStorage, DEDUP_TTL_12H, storageEvent } from '../base'
import { systemRetry, TOPICS } from '../topics'

const { icebergCatalogUrl, icebergCatalogAuthType, isMultitenant, pgQueueConcurrentTasksPerQueue } =
  getConfig()

const catalogAuthType = getCatalogAuthStrategy(icebergCatalogAuthType)

export interface DeleteIcebergResourcesPayload extends BasePayload {
  catalogId: string
}

export class DeleteIcebergResources extends storageEvent<DeleteIcebergResourcesPayload>({
  type: 'DeleteIcebergResources',
  idempotencyKey: (data) => data.catalogId,
  idempotencyTtlMs: DEDUP_TTL_12H,
  // v1: never runs in-process; skipped when the queue is disabled.
  allowSync: false,
}) {}

export class DeleteIcebergResourcesHandler extends TopicHandler(DeleteIcebergResources) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    retry: systemRetry(TOPICS.deleteIcebergResources),
  }

  async handle(ctx: JobContext<WirePayload<DeleteIcebergResourcesPayload>>): Promise<void> {
    const { data } = ctx.message
    let eventStorage: Storage | undefined

    try {
      try {
        eventStorage = await createStorage(data)
      } catch (e) {
        // don't require tenant db for multitenant
        // if the tenant was removed we can still cleanup the resources and multitenant db
        if (!isMultitenant) {
          throw e
        }
      }

      const metastore = new PgMetastore(
        isMultitenant ? multitenantPgExecutor : eventStorage!.db.connection,
        {
          multiTenant: isMultitenant,
          schema: isMultitenant ? 'public' : 'storage',
        }
      )

      const restCatalog = new RestCatalogClient({
        catalogUrl: icebergCatalogUrl,
        auth: catalogAuthType,
      })

      await metastore.transaction(async (store) => {
        await store.lockResource('catalog', data.catalogId)

        const catalog = await store.findCatalogById({
          id: data.catalogId,
          deleted: true,
          tenantId: data.tenant.ref,
        })

        if (!catalog.deleted_at) {
          throw ERRORS.UnableToEmptyBucket(
            data.catalogId,
            `Catalog ${data.catalogId} is not marked for deletion`
          )
        }

        const namespaces = await store.listNamespaces({
          catalogId: data.catalogId,
          tenantId: data.tenant.ref,
        })

        // Delete all tables and namespaces in the catalog
        await Promise.all(
          namespaces.map(async (ns) => {
            const tables = await store.listTables({
              namespaceId: ns.id,
              pageSize: 1000,
              tenantId: data.tenant.ref,
            })

            for (const table of tables) {
              if (!table.shard_key || !table.shard_id) {
                continue
              }

              try {
                await restCatalog.dropTable({
                  namespace: ns.name,
                  table: table.name,
                  purgeRequested: true,
                  warehouse: table.shard_key,
                })
              } catch (e) {
                if (e instanceof IcebergError && e.code === 404) {
                  // Table not found in remote catalog, continue to delete metadata
                } else {
                  throw e
                }
              }

              await store.dropTable({
                name: table.name,
                namespaceId: ns.id, // namespace_id UUID
                catalogId: data.catalogId,
                tenantId: data.tenant.ref,
              })

              const listTables = await restCatalog.listTables({
                namespace: `${data.tenant.ref}_${ns.id.replaceAll('-', '_')}`,
                warehouse: table.shard_key,
                pageSize: 1,
              })

              if (listTables.identifiers.length === 0) {
                await restCatalog.dropNamespace({
                  namespace: ns.name,
                  warehouse: table.shard_key,
                })
                // Delete the namespace metadata after removing it from remote catalog
                await store.dropNamespace({
                  namespace: ns.name,
                  catalogId: data.catalogId,
                  tenantId: data.tenant.ref,
                })
              }

              if (isMultitenant) {
                const sharding = new ShardCatalog(new PgShardStoreFactory(multitenantPgExecutor))
                const sharder = sharding.withTnx(store.getTnx())
                await sharder.freeByResource(table.shard_id, {
                  kind: 'iceberg-table',
                  tenantId: data.tenant.ref,
                  bucketName: data.catalogId,
                  logicalName: `${ns.id}/${table.name}`,
                })
              }
            }
          })
        )

        // Finally, drop the catalog
        // Child rows are already deleted, so this won't trigger cascading deletes
        await store.dropCatalog({
          bucketId: data.catalogId,
          tenantId: data.tenant.ref,
          soft: false,
        })

        if (isMultitenant && eventStorage) {
          // Delete the underlying bucket
          await eventStorage.db.deleteAnalyticsBucket(data.catalogId)
        }
      })
    } finally {
      if (eventStorage) {
        eventStorage.db.destroyConnection()
      }
    }
  }
}
