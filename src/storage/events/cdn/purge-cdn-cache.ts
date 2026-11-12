import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, StorageJob } from '@internal/queue'
import type { SubscribeOptions } from '@internal/queue/manager'
import { QueueHandler } from '@internal/queue/manager'
import { CdnCacheManager, PurgeCacheInput } from '@storage/cdn/cdn-cache-manager'
import { getConfig } from '../../../config'
import { storageEvent } from '../base'

const { pgQueueConcurrentTasksPerQueue } = getConfig()

export interface PurgeCdnCachePayload extends BasePayload {
  purgeOptions: PurgeCacheInput
}

const cdnCacheManager = new CdnCacheManager()

export class PurgeCdnCache extends storageEvent<PurgeCdnCachePayload>({
  type: 'PurgeCdnCache',
  // v1 singletonKey — on the queue's `exclusive` policy, one purge per cache scope queued.
  // v1 also sent with priority 10; the queue carries no per-message priority.
  idempotencyKey: ({ purgeOptions }) =>
    [
      purgeOptions.type,
      purgeOptions.tenant,
      'bucket' in purgeOptions ? purgeOptions.bucket : undefined,
      'objectName' in purgeOptions ? purgeOptions.objectName : undefined,
    ]
      .filter(Boolean)
      .join('/'),
}) {}

export class PurgeCdnCacheHandler extends QueueHandler(PurgeCdnCache) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue * 2,
    parallelism: pgQueueConcurrentTasksPerQueue,
    pollIdleIntervalMs: 1_000,
    consumeTimeout: 15_000, // above the purge client's 10s abort budget
  }

  async handle(ctx: StorageJob<PurgeCdnCachePayload>): Promise<void> {
    const { data } = ctx.message

    if (!cdnCacheManager.isConfigured()) {
      // exit early if cdn cache manager is not configured (missing CDN_PURGE_ENDPOINT_URL)
      return
    }

    try {
      await cdnCacheManager.purge(data.purgeOptions)
    } catch (e) {
      logSchema.error(logger, '[CDN] Failed to purge cache', {
        type: 'cdn',
        error: e,
        project: data.tenant.ref,
        sbReqId: data.sbReqId,
        metadata: JSON.stringify(data.purgeOptions),
      })
      throw e
    }
  }
}
