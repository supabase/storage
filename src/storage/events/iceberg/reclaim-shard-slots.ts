import { multitenantPgExecutor } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, StorageJob } from '@internal/queue'
import type { SubscribeOptions } from '@internal/queue/manager'
import { QueueHandler } from '@internal/queue/manager'
import { getCatalogAuthStrategy, RestCatalogClient } from '@storage/protocols/iceberg/catalog'
import {
  IcebergShardSlotReclaimer,
  type ReclaimShardSlotsOptions,
} from '@storage/protocols/iceberg/catalog/reclaim-shard-slots'
import { getConfig } from '../../../config'
import { storageEvent } from '../base'

export interface ReclaimIcebergShardSlotsPayload extends BasePayload, ReclaimShardSlotsOptions {
  runId: string
}

export class ReclaimIcebergShardSlots extends storageEvent<ReclaimIcebergShardSlotsPayload>({
  type: 'ReclaimIcebergShardSlots',
  // One job per page of a run: the continuation carries the next cursor, so it never collides
  // with the page that produced it.
  idempotencyKey: (data) => `${data.runId}:${data.afterReservationId ?? 'start'}`,
  // Never runs in-process; skipped when the queue is disabled.
  allowSync: false,
}) {}

export class ReclaimIcebergShardSlotsHandler extends QueueHandler(ReclaimIcebergShardSlots) {
  override readonly options: SubscribeOptions = {
    // One page at a time: a run walks the reservations in cursor order.
    prefetch: 1,
  }

  async handle(ctx: StorageJob<ReclaimIcebergShardSlotsPayload>): Promise<void> {
    const { data } = ctx.message
    const config = getConfig()
    if (!config.isMultitenant)
      throw new Error('Iceberg shard reclamation requires multitenant mode')
    const catalog = new RestCatalogClient({
      catalogUrl: config.icebergCatalogUrl,
      auth: getCatalogAuthStrategy(config.icebergCatalogAuthType),
      timeoutMs: 3000,
    })
    const result = await new IcebergShardSlotReclaimer(multitenantPgExecutor, catalog).runBatch(
      data
    )

    if (result.nextAfterReservationId) {
      // A failed append throws (the event is `allowSync: false`, so there is no inline
      // fallback) and this page is retried, which re-produces the same continuation key.
      await ReclaimIcebergShardSlots.send({
        ...data,
        afterReservationId: result.nextAfterReservationId,
      })
    }

    logSchema.info(logger, '[Iceberg] Shard reclamation batch completed', {
      type: 'iceberg-shard-reclamation',
      sbReqId: data.sbReqId,
      metadata: JSON.stringify({
        runId: data.runId,
        dryRun: data.dryRun !== false,
        tenantId: data.tenantId,
        shardId: data.shardId,
        afterReservationId: data.afterReservationId,
        ...result,
      }),
    })
  }
}
