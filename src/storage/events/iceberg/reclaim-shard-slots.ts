import { multitenantPgExecutor } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { type BasePayload, Event } from '@internal/queue'
import { getCatalogAuthStrategy, RestCatalogClient } from '@storage/protocols/iceberg/catalog'
import {
  IcebergShardSlotReclaimer,
  type ReclaimShardSlotsOptions,
} from '@storage/protocols/iceberg/catalog/reclaim-shard-slots'
import type { Job, SendOptions } from 'pg-boss'
import { getConfig } from '../../../config'

export interface ReclaimIcebergShardSlotsPayload extends BasePayload, ReclaimShardSlotsOptions {
  runId: string
}

export class ReclaimIcebergShardSlots extends Event<ReclaimIcebergShardSlotsPayload> {
  static allowSync = false
  static queueName = 'reclaim-iceberg-shard-slots'

  static getQueueOptions() {
    return { name: this.queueName, policy: 'exactly_once' } as const
  }

  static getWorkerOptions() {
    return { includeMetadata: true, concurrentTaskCount: 1 }
  }

  static getSendOptions(payload: ReclaimIcebergShardSlotsPayload): SendOptions {
    return {
      singletonKey: `${payload.runId}:${payload.afterReservationId ?? 'start'}`,
      expireInMinutes: 15,
      retryLimit: 3,
      retryDelay: 30,
      retryBackoff: true,
    }
  }

  static async handle(job: Job<ReclaimIcebergShardSlotsPayload>) {
    const config = getConfig()
    if (!config.isMultitenant)
      throw new Error('Iceberg shard reclamation requires multitenant mode')
    const catalog = new RestCatalogClient({
      catalogUrl: config.icebergCatalogUrl,
      auth: getCatalogAuthStrategy(config.icebergCatalogAuthType),
      timeoutMs: 3000,
    })
    const result = await new IcebergShardSlotReclaimer(multitenantPgExecutor, catalog).runBatch(
      job.data
    )

    if (result.nextAfterReservationId) {
      const nextJobId = await this.send({
        ...job.data,
        afterReservationId: result.nextAfterReservationId,
      })
      if (nextJobId === undefined) throw new Error('Reclamation continuation was not queued')
    }

    logSchema.info(logger, '[Iceberg] Shard reclamation batch completed', {
      type: 'iceberg-shard-reclamation',
      sbReqId: job.data.sbReqId,
      metadata: JSON.stringify({
        runId: job.data.runId,
        dryRun: job.data.dryRun !== false,
        tenantId: job.data.tenantId,
        shardId: job.data.shardId,
        afterReservationId: job.data.afterReservationId,
        ...result,
      }),
    })
  }
}
