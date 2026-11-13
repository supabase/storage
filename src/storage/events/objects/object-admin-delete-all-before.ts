import { logger } from '@internal/monitoring'
import type { BasePayload, StorageJob } from '@internal/queue'
import type { SubscribeOptions } from '@internal/queue/manager'
import { QueueHandler } from '@internal/queue/manager'
import { metadataSize } from '@storage/schemas'
import { getConfig } from '../../../config'
import { Storage } from '../../index'
import { MAX_OBJECTS_PER_DELETE_BATCH } from '../../limits'
import { getObjectDeletionKeys } from '../../locator'
import { deleteBackendObjects, deleteObjectContent } from '../../object-cleanup'
import { createStorage, storageEvent } from '../base'
import { ObjectRemoved } from '../webhooks/lifecycle-events'

const DELETE_JOB_TIME_LIMIT_MS = 10_000

export interface ObjectDeleteAllBeforeEvent extends BasePayload {
  before: string
  bucketId: string
}

const { storageS3Bucket, pgQueueConcurrentTasksPerQueue } = getConfig()

export class ObjectAdminDeleteAllBefore extends storageEvent<ObjectDeleteAllBeforeEvent>({
  type: 'ObjectAdminDeleteAllBefore',
  // v1 singletonKey — on the queue's `singleton` policy, one job per (tenant, bucket) queued.
  idempotencyKey: (data) => `${data.tenant.ref}/${data.bucketId}`,
}) {}

export class ObjectAdminDeleteAllBeforeHandler extends QueueHandler(ObjectAdminDeleteAllBefore) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue * 2,
    parallelism: pgQueueConcurrentTasksPerQueue,
    pollIdleIntervalMs: 1_000,
    consumeTimeout: 25_000, // bounds the ~10s DELETE_JOB_TIME_LIMIT_MS loop; under the queue's pgboss expireInSeconds: 30
  }

  async handle(ctx: StorageJob<ObjectDeleteAllBeforeEvent>): Promise<void> {
    const { id, data } = ctx.message
    let storage: Storage | undefined = undefined

    const tenantId = data.tenant.ref
    const bucketId = data.bucketId
    const before = new Date(data.before)

    try {
      storage = await createStorage(data)

      const batchLimit = MAX_OBJECTS_PER_DELETE_BATCH

      let moreObjectsToDelete = false
      const start = Date.now()
      while (Date.now() - start < DELETE_JOB_TIME_LIMIT_MS) {
        moreObjectsToDelete = false
        const objects = await storage.db.listObjects(bucketId, 'id, name', batchLimit + 1, before)

        const backend = storage.backend
        if (objects && objects.length > 0) {
          if (objects.length > batchLimit) {
            objects.pop()
            moreObjectsToDelete = true
          }

          const deleted = await storage.db.deleteObjects(
            bucketId,
            objects.map(({ id: objectId }) => objectId!),
            'id'
          )

          const committedAt = Date.now()

          const prefixes = getObjectDeletionKeys(storage.location, {
            tenantId,
            bucketId,
            objects: deleted,
          })

          try {
            await deleteObjectContent(
              {
                tenant: data.tenant,
                bucketId,
                objects: deleted.map(({ name, version }) => ({ name, version })),
                reqId: data.reqId,
                sbReqId: data.sbReqId,
              },
              () => deleteBackendObjects(backend, storageS3Bucket, prefixes)
            )
          } finally {
            // One send to the queue for the whole page, not one per object.
            await ObjectRemoved.sendWebhookBatch(
              deleted.map((object) => ({
                payload: {
                  tenant: data.tenant,
                  name: object.name,
                  bucketId,
                  reqId: data.reqId,
                  sbReqId: data.sbReqId,
                  version: object.version ?? null,
                  metadata: object.metadata,
                  bytesChanged: -metadataSize(object.metadata) || 0,
                },
                applyTime: committedAt,
              }))
            )
          }
        }

        if (!moreObjectsToDelete) {
          break
        }
      }

      if (moreObjectsToDelete) {
        // delete next batch
        await ObjectAdminDeleteAllBefore.send({
          before: before.toISOString(),
          bucketId,
          tenant: data.tenant,
          reqId: data.reqId,
          sbReqId: data.sbReqId,
        })
      }
    } catch (e) {
      logger.error(
        {
          error: e,
          jobId: id,
          type: 'event',
          event: 'ObjectAdminDeleteAllBefore',
          payload: JSON.stringify(data),
          objectPath: bucketId,
          tenantId,
          project: tenantId,
          reqId: data.reqId,
          sbReqId: data.sbReqId,
        },
        `[Admin]: ObjectAdminDeleteAllBefore ${bucketId} ${before.toUTCString()} - FAILED`
      )
      throw e
    } finally {
      if (storage) {
        storage.db.destroyConnection()
      }
    }
  }
}
