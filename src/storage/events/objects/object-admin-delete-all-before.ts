import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, WirePayload } from '@internal/queue'
import { withOptionalVersion } from '@storage/backend'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { getConfig } from '../../../config'
import { Storage } from '../../index'
import { MAX_OBJECTS_PER_DELETE_BATCH } from '../../limits'
import { createStorage, storageEvent } from '../base'
import { getStorageQueue } from '../queue'
import { defaultRetry, TOPICS } from '../topics'
import { ObjectRemoved } from '../webhooks/lifecycle-events'

const DELETE_JOB_TIME_LIMIT_MS = 10_000

export interface ObjectDeleteAllBeforeEvent extends BasePayload {
  before: string
  bucketId: string
}

const { storageS3Bucket, pgQueueConcurrentTasksPerQueue } = getConfig()

export class ObjectAdminDeleteAllBefore extends storageEvent<ObjectDeleteAllBeforeEvent>({
  type: 'ObjectAdminDeleteAllBefore',
  // v1 singletonKey — on the topic's `singleton` policy, one job per (tenant, bucket) queued.
  idempotencyKey: (data) => `${data.tenant.ref}/${data.bucketId}`,
}) {}

export class ObjectAdminDeleteAllBeforeHandler extends TopicHandler(ObjectAdminDeleteAllBefore) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    retry: defaultRetry(TOPICS.objectAdminDeleteAllBefore),
  }

  async handle(ctx: JobContext<WirePayload<ObjectDeleteAllBeforeEvent>>): Promise<void> {
    const { id, data } = ctx.message
    let storage: Storage | undefined = undefined

    const tenantId = data.tenant.ref
    const bucketId = data.bucketId
    const before = new Date(data.before)

    try {
      storage = await createStorage(data)

      logSchema.event(
        logger,
        `[Admin]: ObjectAdminDeleteAllBefore ${bucketId} ${before.toUTCString()}`,
        {
          jobId: id,
          type: 'event',
          event: 'ObjectAdminDeleteAllBefore',
          payload: JSON.stringify(data),
          objectPath: bucketId,
          tenantId,
          project: tenantId,
          reqId: data.reqId,
          sbReqId: data.sbReqId,
        }
      )

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

          await storage.db.withTransaction(async (trx) => {
            const deleted = await trx.deleteObjects(
              bucketId,
              objects.map(({ id: objectId }) => objectId!),
              'id'
            )

            if (deleted && deleted.length > 0) {
              const prefixes: string[] = []

              for (const { name, version } of deleted) {
                const fileName = withOptionalVersion(`${tenantId}/${bucketId}/${name}`, version)
                prefixes.push(fileName)
                prefixes.push(fileName + '.info')
              }

              await backend.deleteObjects(storageS3Bucket, prefixes)

              await Promise.allSettled(
                deleted.map((object) =>
                  ObjectRemoved.sendWebhook({
                    tenant: data.tenant,
                    name: object.name,
                    bucketId,
                    reqId: data.reqId,
                    sbReqId: data.sbReqId,
                    version: object.version,
                    metadata: object.metadata,
                  })
                )
              )
            }
          })
        }

        if (!moreObjectsToDelete) {
          break
        }
      }

      if (moreObjectsToDelete) {
        // delete next batch
        await getStorageQueue().produce(
          new ObjectAdminDeleteAllBefore({
            before: before.toISOString(),
            bucketId,
            tenant: data.tenant,
            reqId: data.reqId,
            sbReqId: data.sbReqId,
          })
        )
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
