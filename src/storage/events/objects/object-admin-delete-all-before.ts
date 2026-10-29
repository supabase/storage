import { logger, logSchema } from '@internal/monitoring'
import { BasePayload } from '@internal/queue'
import { metadataSize } from '@storage/schemas'
import { Job, SendOptions, WorkOptions } from 'pg-boss'
import { getConfig } from '../../../config'
import { Storage } from '../../index'
import { MAX_OBJECTS_PER_DELETE_BATCH } from '../../limits'
import { getObjectDeletionKeys } from '../../locator'
import { deleteBackendObjects, deleteObjectContent } from '../../object-cleanup'
import { BaseEvent } from '../base-event'
import { ObjectRemoved } from '../lifecycle/object-removed'

const DELETE_JOB_TIME_LIMIT_MS = 10_000

export interface ObjectDeleteAllBeforeEvent extends BasePayload {
  before: string
  bucketId: string
}

const { storageS3Bucket } = getConfig()

export class ObjectAdminDeleteAllBefore extends BaseEvent<ObjectDeleteAllBeforeEvent> {
  static queueName = 'object:admin:delete-all-before'

  static getWorkerOptions(): WorkOptions {
    return {}
  }

  static getSendOptions(payload: ObjectDeleteAllBeforeEvent): SendOptions {
    return {
      singletonKey: `${payload.tenant.ref}/${payload.bucketId}`,
      priority: 10,
      expireInSeconds: 30,
    }
  }

  static async handle(job: Job<ObjectDeleteAllBeforeEvent>) {
    let storage: Storage | undefined = undefined

    const tenantId = job.data.tenant.ref
    const bucketId = job.data.bucketId
    const before = new Date(job.data.before)

    try {
      storage = await this.createStorage(job.data)

      logSchema.event(
        logger,
        `[Admin]: ObjectAdminDeleteAllBefore ${bucketId} ${before.toUTCString()}`,
        {
          jobId: job.id,
          type: 'event',
          event: 'ObjectAdminDeleteAllBefore',
          payload: JSON.stringify(job.data),
          objectPath: bucketId,
          tenantId,
          project: tenantId,
          reqId: job.data.reqId,
          sbReqId: job.data.sbReqId,
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

          const deleted = await storage.db.deleteObjects(
            bucketId,
            objects.map(({ id }) => id!),
            'id'
          )

          const prefixes = getObjectDeletionKeys(storage.location, {
            tenantId,
            bucketId,
            objects: deleted,
          })

          try {
            await deleteObjectContent(
              {
                tenant: job.data.tenant,
                bucketId,
                objects: deleted.map(({ name, version }) => ({ name, version })),
                reqId: job.data.reqId,
                sbReqId: job.data.sbReqId,
              },
              () => deleteBackendObjects(backend, storageS3Bucket, prefixes)
            )
          } finally {
            await Promise.allSettled(
              deleted.map((object) =>
                ObjectRemoved.sendWebhook({
                  tenant: job.data.tenant,
                  name: object.name,
                  bucketId,
                  reqId: job.data.reqId,
                  sbReqId: job.data.sbReqId,
                  version: object.version ?? null,
                  metadata: object.metadata,
                  bytesChanged: -metadataSize(object.metadata) || 0,
                })
              )
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
          tenant: job.data.tenant,
          reqId: job.data.reqId,
          sbReqId: job.data.sbReqId,
        })
      }
    } catch (e) {
      logger.error(
        {
          error: e,
          jobId: job.id,
          type: 'event',
          event: 'ObjectAdminDeleteAllBefore',
          payload: JSON.stringify(job.data),
          objectPath: bucketId,
          tenantId,
          project: tenantId,
          reqId: job.data.reqId,
          sbReqId: job.data.sbReqId,
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
