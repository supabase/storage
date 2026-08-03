import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, WirePayload } from '@internal/queue'
import { CdnCacheManager, PurgeCacheInput } from '@storage/cdn/cdn-cache-manager'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { getConfig } from '../../../config'
import { storageEvent } from '../base'
import { backupRetry, TOPICS } from '../topics'

const { pgQueueConcurrentTasksPerQueue } = getConfig()

export interface PurgeCdnCachePayload extends BasePayload {
  purgeOptions: PurgeCacheInput
}

const cdnCacheManager = new CdnCacheManager()

export class PurgeCdnCache extends storageEvent<PurgeCdnCachePayload>({
  type: 'PurgeCdnCache',
  // v1 singletonKey — on the topic's `exclusive` policy, one purge per cache scope queued.
  // v1 also sent with priority 10; wave carries no per-message priority.
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

export class PurgeCdnCacheHandler extends TopicHandler(PurgeCdnCache) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    parallelism: pgQueueConcurrentTasksPerQueue,
    // v1 retryLimit 5, retryDelay 5 — the same posture as backup.
    retry: backupRetry(TOPICS.purgeCdnCache),
  }

  async handle(ctx: JobContext<WirePayload<PurgeCdnCachePayload>>): Promise<void> {
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
