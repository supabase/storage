import { logger, logSchema } from '@internal/monitoring'
import { lifecycleRunDuration, lifecycleShardRuns } from '@internal/monitoring/metrics'
import { BasePayload } from '@internal/queue'
import { JobWithMetadata, Queue, SendOptions, WorkOptions } from 'pg-boss'
import { getConfig } from '../../../config'
import { NoncurrentLifecycleExecutor } from '../../lifecycle/executor'
import { isLifecycleEnabledForTenant } from '../../lifecycle/guards'
import type { LifecycleScanKind } from '../../schemas'
import { BaseEvent } from '../base-event'

const config = getConfig()

const LIFECYCLE_SHARD_CONCURRENCY = 8

export interface LifecycleBucketShardRunPayload extends BasePayload {
  bucketId: string
  scanKind: LifecycleScanKind
  shardEpoch: string
  shardId: number
}

export class LifecycleBucketShardRun extends BaseEvent<LifecycleBucketShardRunPayload> {
  static queueName = 'lifecycle:bucket-shard:run-v1'
  static allowSync = false

  static getQueueOptions(): Queue {
    return { name: this.queueName, policy: 'exactly_once' } as const
  }

  static getSendOptions(payload: LifecycleBucketShardRunPayload): SendOptions {
    return {
      singletonKey: [
        payload.tenant.ref,
        payload.bucketId,
        payload.scanKind,
        payload.shardEpoch,
        payload.shardId,
      ].join('/'),
      expireInMinutes: Math.max(
        1,
        Math.ceil(
          (config.storageLifecycleJobBudgetMs + config.storageLifecycleClaimLeaseMs) / 60_000
        )
      ),
      retryLimit: 0,
    }
  }

  static getWorkerOptions(): WorkOptions & { concurrentTaskCount: number } {
    return {
      includeMetadata: true,
      concurrentTaskCount: LIFECYCLE_SHARD_CONCURRENCY,
      pollingIntervalSeconds: 5,
    }
  }

  static async shouldSend(): Promise<boolean> {
    // Durable shard state, not the generic event suppression list, owns retries.
    return true
  }

  static async handle(job: JobWithMetadata<LifecycleBucketShardRunPayload>): Promise<void> {
    const storage = await this.createStorage(job.data)
    const startedAt = performance.now()
    let status = 'ERROR'
    try {
      const result = await new NoncurrentLifecycleExecutor(storage).run(
        {
          bucketId: job.data.bucketId,
          scanKind: job.data.scanKind,
          shardEpoch: job.data.shardEpoch,
          shardId: job.data.shardId,
        },
        {
          tenantLifecycleEnabled: await isLifecycleEnabledForTenant(job.data.tenant.ref),
        }
      )
      status = result.status
      logSchema.info(logger, '[Lifecycle] Shard delivery completed', {
        type: 'event',
        project: job.data.tenant.ref,
        metadata: JSON.stringify({
          bucketId: job.data.bucketId,
          runId: result.runId,
          scanKind: job.data.scanKind,
          shardEpoch: job.data.shardEpoch,
          shardId: job.data.shardId,
          status: result.status,
        }),
      })
    } finally {
      const attributes = { status, scan_kind: job.data.scanKind }
      try {
        lifecycleShardRuns.add(1, attributes)
        lifecycleRunDuration.record((performance.now() - startedAt) / 1000, attributes)
      } finally {
        storage.db.destroyConnection()
      }
    }
  }
}
