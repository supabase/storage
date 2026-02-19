import { getConfig } from '../../../config'
import { getTenantConfig } from '@internal/database'
import { runMigrationsOnTenant } from '@internal/database/migrations'
import { UpgradeBaseEvent, UpgradeBaseEventPayload } from './base-event'
import { Knex } from 'knex'
import { logger, logSchema } from '@internal/monitoring'

type SyncCatalogIdsPayload = UpgradeBaseEventPayload

const { icebergShards } = getConfig()

export class SyncCatalogIds extends UpgradeBaseEvent<SyncCatalogIdsPayload> {
  static queueName = 'sync-iceberg-catalog-ids'

  static async handleUpgrade(tnx: Knex.Transaction) {
    const tenantCatalogs = await tnx
      .table('iceberg_catalogs')
      .select<{ tenant_id: string; catalog_names: string[] }[]>(
        'tenant_id',
        tnx.raw('ARRAY_AGG(name) AS catalog_names')
      )
      .groupBy('tenant_id')

    let updatedCount = 0
    let hardFail: unknown | undefined = undefined

    logSchema.info(
      logger,
      `[Upgrade][SyncCatalogIds] Found ${tenantCatalogs.length} catalogs to sync IDs for`,
      {
        type: 'upgradeEvent',
      }
    )

    await Promise.all(
      tenantCatalogs.map(async (catalog) => {
        try {
          const config = await getTenantConfig(catalog.tenant_id)

          await runMigrationsOnTenant({
            tenantId: catalog.tenant_id,
            databaseUrl: config.databaseUrl,
            waitForLock: true,
          })

          const storage = await this.createStorage({
            tenant: {
              ref: catalog.tenant_id,
              host: '',
            },
          })

          const tenantBuckets = await storage.listAnalyticsBuckets({
            columns: 'id,name',
            options: {
              limit: 1000,
            },
          })

          logSchema.info(
            logger,
            `[Upgrade][SyncCatalogIds] Found ${tenantBuckets.length} buckets to sync for tenant ${catalog.tenant_id}`,
            {
              type: 'upgradeEvent',
            }
          )

          if (hardFail) {
            throw hardFail
          }

          try {
            for (const bucket of tenantBuckets) {
              const updated = await tnx
                .table('iceberg_catalogs')
                .where('tenant_id', catalog.tenant_id)
                .where('name', bucket.name)
                .whereNull('deleted_at')
                .update({
                  id: bucket.id,
                  updated_at: new Date(),
                })
                .returning('id')

              logSchema.info(
                logger,
                `[Upgrade][SyncCatalogIds] Updated ${updated.length} records for bucket ${bucket.name} for tenant_id ${catalog.tenant_id} using catalog-id ${bucket.id}`,
                {
                  type: 'upgradeEvent',
                }
              )

              if (updated.length === 0) {
                // insert catalog if it does not exist
                await tnx.table('iceberg_catalogs').insert({
                  id: bucket.id,
                  name: bucket.name,
                  tenant_id: catalog.tenant_id,
                  created_at: new Date(),
                  updated_at: new Date(),
                })
              }

              updatedCount += updated.length
            }
          } catch (e) {
            logSchema.error(
              logger,
              `[Upgrade][SyncCatalogIds] Failed to update bucket for ${catalog.tenant_id}`,
              {
                type: 'upgradeEvent',
                error: e,
              }
            )

            if (hardFail) {
              hardFail = e
            }
            throw e
          }
        } catch (e) {
          // no-op
          logSchema.error(
            logger,
            `[Upgrade][SyncCatalogIds] Error interacting with tenant ${catalog.tenant_id}, skipping...`,
            {
              error: e,
              type: 'upgradeEvent',
            }
          )
        }
      })
    )

    if (hardFail) {
      throw hardFail
    }

    if (updatedCount > 0) {
      await this.refillShards(tnx)
    }

    logSchema.info(logger, `[Upgrade][SyncCatalogIds] Completed updated ${updatedCount} catalogs`, {
      type: 'upgradeEvent',
    })
  }

  protected static async refillShards(tnx: Knex.Transaction) {
    if (icebergShards.length === 0) {
      return
    }

    await tnx.raw(`DELETE FROM shard_reservation where resource_id LIKE 'iceberg-table::%'`)
    await tnx.raw(`DELETE FROM shard_slots where resource_id LIKE 'iceberg-table::%'`)

    const query = `
        WITH all_iceberg_tables as (
            SELECT t.id, t.tenant_id, t.name, t.shard_key, t.shard_id, t.namespace_id, t.catalog_id, row_number() OVER () as seq_num
            FROM iceberg_tables t
        ),
        set_shard_reservation AS (
             INSERT INTO shard_reservation (tenant_id, shard_id, kind, resource_id, lease_expires_at, slot_no, status)
                 SELECT it.tenant_id, s.id, 'iceberg-table', ('iceberg-table::' || it.catalog_id || '::' || it.namespace_id || '/' || it.name), now() + interval '5 minutes', it.seq_num - 1, 'confirmed'
                 FROM all_iceberg_tables it
                          JOIN shard s ON s.kind = 'iceberg-table' AND s.shard_key = it.shard_key
         ),
         shard_slot AS (
             INSERT INTO shard_slots (tenant_id, shard_id, resource_id, slot_no)
                 SELECT it.tenant_id, it.shard_id, ('iceberg-table::' || it.catalog_id || '::' || it.namespace_id || '/' || it.name), it.seq_num - 1
                 FROM all_iceberg_tables it
                 RETURNING slot_no
         )
        UPDATE shard
        SET next_slot = (SELECT COALESCE((SELECT MAX(slot_no) + 1 FROM shard_slot), next_slot))
        WHERE shard.kind = 'iceberg-table'
          AND shard.shard_key = ?;
    `

    await tnx.raw(query, icebergShards[0])
  }
}
