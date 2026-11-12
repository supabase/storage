import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, StorageJob } from '@internal/queue'
import type { SubscribeOptions } from '@internal/queue/manager'
import { QueueHandler } from '@internal/queue/manager'
import { getConfig } from '../../../config'
import { Storage } from '../../index'
import { getObjectDeletionKeys } from '../../locator'
import { deleteBackendObjects } from '../../object-cleanup'
import { createStorage, storageEvent } from '../base'

export interface ObjectDeleteEvent extends BasePayload {
  name: string
  bucketId: string
  version?: string
}

const { storageS3Bucket, pgQueueConcurrentTasksPerQueue } = getConfig()

export class ObjectAdminDelete extends storageEvent<ObjectDeleteEvent>({
  type: 'ObjectAdminDelete',
}) {}

export class ObjectAdminDeleteHandler extends QueueHandler(ObjectAdminDelete) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue * 2,
    parallelism: pgQueueConcurrentTasksPerQueue,
    pollIdleIntervalMs: 1_000,
    consumeTimeout: 25_000, // under the queue's pgboss expireInSeconds: 30
  }

  async handle(ctx: StorageJob<ObjectDeleteEvent>): Promise<void> {
    const { id, data } = ctx.message
    let storage: Storage | undefined = undefined

    try {
      storage = await createStorage(data)
      const s3Key = storage.location.getKeyLocation({
        tenantId: data.tenant.ref,
        bucketId: data.bucketId,
        objectName: data.name,
      })

      logSchema.event(logger, `[Admin]: ObjectAdminDelete ${s3Key}`, {
        jobId: id,
        type: 'event',
        event: 'ObjectAdminDelete',
        payload: JSON.stringify(data),
        objectPath: s3Key,
        resources: [`${data.bucketId}/${data.name}`],
        tenantId: data.tenant.ref,
        project: data.tenant.ref,
        reqId: data.reqId,
        sbReqId: data.sbReqId,
      })

      const keys = getObjectDeletionKeys(storage.location, {
        tenantId: data.tenant.ref,
        bucketId: data.bucketId,
        objects: [data],
      })
      await deleteBackendObjects(storage.backend, storageS3Bucket, keys)
    } catch (e) {
      const s3Key = `${data.tenant.ref}/${data.bucketId}/${data.name}`

      logger.error(
        {
          error: e,
          jobId: id,
          type: 'event',
          event: 'ObjectAdminDelete',
          payload: JSON.stringify(data),
          objectPath: s3Key,
          objectVersion: data.version,
          tenantId: data.tenant.ref,
          project: data.tenant.ref,
          reqId: data.reqId,
          sbReqId: data.sbReqId,
        },
        `[Admin]: ObjectAdminDelete ${s3Key} - FAILED`
      )
      throw e
    } finally {
      if (storage) {
        storage.db.destroyConnection()
      }
    }
  }
}
