import type { BasePayload, WirePayload } from '@internal/queue'
import type { JobContext, SubscribeOptions } from '@internal/queue/manager'
import { QueueHandler } from '@internal/queue/manager'
import { getConfig } from '../../../config'
import { getObjectDeletionKeys, TenantLocation } from '../../locator'
import { deleteBackendObjects } from '../../object-cleanup'
import { getOrCreateStorageBackend, storageEvent } from '../base'
import { defaultRetry, QUEUES } from '../registry'

const { storageS3Bucket, pgQueueConcurrentTasksPerQueue } = getConfig()

export interface ObjectDeleteBatchEvent extends BasePayload {
  bucketId: string
  objects: { name: string; version?: string | null }[]
}

/** Backend cleanup for objects whose metadata is already gone. Its own queue keeps batch
 * payloads away from workers expecting a single object. */
export class ObjectAdminDeleteBatch extends storageEvent<ObjectDeleteBatchEvent>({
  type: 'ObjectAdminDeleteBatch',
  // Never gated by per-tenant disableEvents: skipping it would leak the deleted objects' bytes.
  disableKeys: () => [],
}) {}

export class ObjectAdminDeleteBatchHandler extends QueueHandler(ObjectAdminDeleteBatch) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue * 2,
    parallelism: pgQueueConcurrentTasksPerQueue,
    retry: defaultRetry(QUEUES.objectAdminDeleteBatch),
    pollIdleIntervalMs: 1_000,
    consumeTimeout: 25_000, // under the queue's pgboss expireInSeconds: 30
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
