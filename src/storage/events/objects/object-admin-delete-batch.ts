import type { BasePayload, WirePayload } from '@internal/queue'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { getConfig } from '../../../config'
import { getObjectDeletionKeys, TenantLocation } from '../../locator'
import { deleteBackendObjects } from '../../object-cleanup'
import { getOrCreateStorageBackend, storageEvent } from '../base'
import { defaultRetry, TOPICS } from '../topics'

const { storageS3Bucket, pgQueueConcurrentTasksPerQueue } = getConfig()

export interface ObjectDeleteBatchEvent extends BasePayload {
  bucketId: string
  objects: { name: string; version?: string | null }[]
}

/** Backend cleanup for objects whose metadata is already gone. Its own topic keeps batch
 * payloads away from workers expecting a single object. */
export class ObjectAdminDeleteBatch extends storageEvent<ObjectDeleteBatchEvent>({
  type: 'ObjectAdminDeleteBatch',
  // Never gated by per-tenant disableEvents: skipping it would leak the deleted objects' bytes.
  disableKeys: () => [],
}) {}

export class ObjectAdminDeleteBatchHandler extends TopicHandler(ObjectAdminDeleteBatch) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue * 2,
    parallelism: pgQueueConcurrentTasksPerQueue,
    retry: defaultRetry(TOPICS.objectAdminDeleteBatch),
    pollIdleIntervalMs: 1_000,
    consumeTimeout: 25_000, // under the topic's pgboss expireInSeconds: 30
    livenessTimeoutMs: 60_000,
  }

  async handle(ctx: JobContext<WirePayload<ObjectDeleteBatchEvent>>): Promise<void> {
    const backend = getOrCreateStorageBackend()
    const location = new TenantLocation(storageS3Bucket)
    const { tenant, bucketId, objects } = ctx.message.data
    const keys = getObjectDeletionKeys(location, { tenantId: tenant.ref, bucketId, objects })
    await deleteBackendObjects(backend, location.getRootLocation(), keys)
  }
}
