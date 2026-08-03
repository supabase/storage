import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, WirePayload } from '@internal/queue'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { getConfig } from '../../../config'
import { withOptionalVersion } from '../../backend'
import { Storage } from '../../index'
import { createStorage, storageEvent } from '../base'
import { defaultRetry, TOPICS } from '../topics'

export interface ObjectDeleteEvent extends BasePayload {
  name: string
  bucketId: string
  version?: string
}

const { storageS3Bucket, pgQueueConcurrentTasksPerQueue } = getConfig()

export class ObjectAdminDelete extends storageEvent<ObjectDeleteEvent>({
  type: 'ObjectAdminDelete',
}) {}

export class ObjectAdminDeleteHandler extends TopicHandler(ObjectAdminDelete) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    parallelism: pgQueueConcurrentTasksPerQueue,
    retry: defaultRetry(TOPICS.objectAdminDelete),
  }

  async handle(ctx: JobContext<WirePayload<ObjectDeleteEvent>>): Promise<void> {
    const { id, data } = ctx.message
    let storage: Storage | undefined = undefined

    try {
      storage = await createStorage(data)
      const version = data.version

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

      await storage.backend.deleteObjects(storageS3Bucket, [
        withOptionalVersion(s3Key, version),
        withOptionalVersion(s3Key, version) + '.info',
      ])
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
