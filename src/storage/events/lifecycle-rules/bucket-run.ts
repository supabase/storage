import { logger, logSchema } from '@internal/monitoring'
import { lifecycleRunDuration } from '@internal/monitoring/metrics'
import { BasePayload } from '@internal/queue'
import { JobWithMetadata, Queue, SendOptions, WorkOptions } from 'pg-boss'
import { getConfig } from '../../../config'
import { NoncurrentLifecycleExecutor } from '../../lifecycle/executor'
import { isLifecycleEnabledForTenant } from '../../lifecycle/guards'
import { BaseEvent } from '../base-event'

const config = getConfig()

const LIFECYCLE_BUCKET_CONCURRENCY = 8

export interface LifecycleBucketRunPayload extends BasePayload {
  bucketId: string
}

export class LifecycleBucketRun extends BaseEvent<LifecycleBucketRunPayload> {
  static queueName = 'lifecycle:bucket:run-v1'
  static allowSync = false

  static getQueueOptions(): Queue {
    return { name: this.queueName, policy: 'exactly_once' } as const
  }

  static getSendOptions(payload: LifecycleBucketRunPayload): SendOptions {
    return {
      singletonKey: `${payload.tenant.ref}/${payload.bucketId}`,
      expireInMinutes: Math.ceil(
        (config.storageLifecycleJobBudgetMs + config.storageLifecycleClaimLeaseMs) / 60_000
      ),
      retryLimit: 0,
    }
  }

  static getWorkerOptions(): WorkOptions & { concurrentTaskCount: number } {
    return {
      includeMetadata: true,
      concurrentTaskCount: LIFECYCLE_BUCKET_CONCURRENCY,
      batchSize: LIFECYCLE_BUCKET_CONCURRENCY,
    }
  }

  static async shouldSend(): Promise<boolean> {
    return true
  }

  static async handle(
    job: JobWithMetadata<LifecycleBucketRunPayload>,
    opts?: { signal?: AbortSignal }
  ): Promise<void> {
    const startedAt = performance.now()
    let status = 'ERROR'
    let storage: Awaited<ReturnType<typeof LifecycleBucketRun.createStorage>> | undefined
    try {
      storage = await this.createStorage(job.data)
      const result = await new NoncurrentLifecycleExecutor(storage).run(job.data.bucketId, {
        tenantLifecycleEnabled: await isLifecycleEnabledForTenant(job.data.tenant.ref),
        signal: opts?.signal,
      })
      status = result.status
      logSchema.info(logger, '[Lifecycle] Bucket delivery completed', {
        type: 'event',
        project: job.data.tenant.ref,
        metadata: JSON.stringify({
          bucketId: job.data.bucketId,
          runId: result.runId,
          status: result.status,
        }),
      })
    } finally {
      lifecycleRunDuration.record((performance.now() - startedAt) / 1000, { status })
      storage?.db.destroyConnection()
    }
  }
}
