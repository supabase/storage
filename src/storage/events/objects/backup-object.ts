import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, WirePayload } from '@internal/queue'
import { S3Backend } from '@storage/backend'
import type { JobContext, SubscribeOptions } from '@supabase-labs/wave-core'
import { TopicHandler } from '@supabase-labs/wave-core'
import { getConfig } from '../../../config'
import { createStorage, storageEvent } from '../base'
import { backupRetry, TOPICS } from '../topics'

const { storageS3Bucket, pgQueueConcurrentTasksPerQueue } = getConfig()

export interface BackupObjectEventPayload extends BasePayload {
  name: string
  bucketId: string
  version: string
  size: number
  deleteOriginal?: boolean
}

export class BackupObjectEvent extends storageEvent<BackupObjectEventPayload>({
  type: 'BackupObjectEvent',
  // v1 singletonKey — on the topic's `singleton` policy, one job per object version queued.
  idempotencyKey: (data) => `${data.tenant.ref}/${data.bucketId}/${data.name}/${data.version}`,
}) {}

export class BackupObjectHandler extends TopicHandler(BackupObjectEvent) {
  override readonly options: SubscribeOptions = {
    prefetch: pgQueueConcurrentTasksPerQueue,
    retry: backupRetry(TOPICS.backupObject),
  }

  async handle(ctx: JobContext<WirePayload<BackupObjectEventPayload>>): Promise<void> {
    const { id, data } = ctx.message
    const tenantId = data.tenant.ref
    const storage = await createStorage(data)

    if (!(storage.backend instanceof S3Backend)) {
      storage.db.destroyConnection()
      return
    }

    const s3Key = storage.location.getKeyLocation({
      tenantId,
      bucketId: data.bucketId,
      objectName: data.name,
    })

    try {
      logSchema.event(logger, `[Admin]: BackupObject ${s3Key}`, {
        jobId: id,
        type: 'event',
        event: 'BackupObject',
        payload: JSON.stringify(data),
        objectPath: s3Key,
        resources: [`${data.bucketId}/${data.name}`],
        tenantId: data.tenant.ref,
        project: data.tenant.ref,
        reqId: data.reqId,
        sbReqId: data.sbReqId,
      })

      await storage.backend.backup({
        sourceBucket: storageS3Bucket,
        destinationBucket: storageS3Bucket,
        sourceKey: `${s3Key}/${data.version}`,
        destinationKey: `__internal/${s3Key}/${data.version}`,
        size: data.size,
      })

      if (data.deleteOriginal) {
        logSchema.event(logger, `[Admin]: DeleteOriginalObject ${s3Key}`, {
          jobId: id,
          type: 'event',
          event: 'BackupObject',
          payload: JSON.stringify(data),
          objectPath: s3Key,
          resources: [`${data.bucketId}/${data.name}`],
          tenantId: data.tenant.ref,
          project: data.tenant.ref,
          reqId: data.reqId,
          sbReqId: data.sbReqId,
        })

        await storage.backend.deleteObject(
          storageS3Bucket,
          storage.location.getKeyLocation({
            tenantId,
            bucketId: data.bucketId,
            objectName: data.name,
          }),
          data.version
        )
      }
    } catch (e) {
      logger.error(
        {
          error: e,
          jobId: id,
          type: 'event',
          event: 'BackupObject',
          payload: JSON.stringify(data),
          objectPath: s3Key,
          objectVersion: data.version,
          tenantId: data.tenant.ref,
          project: data.tenant.ref,
          reqId: data.reqId,
          sbReqId: data.sbReqId,
        },
        `[Admin]: BackupObjectEvent ${s3Key} - FAILED`
      )
    } finally {
      storage.db.destroyConnection()
    }
  }
}
