import { isS3Error } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import { type BasePayload, isSyncDispatch, type WirePayload } from '@internal/queue'
import { isMissingBackendObject, S3Backend } from '@storage/backend'
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
    parallelism: pgQueueConcurrentTasksPerQueue,
    retry: backupRetry(TOPICS.backupObject),
  }

  async handle(ctx: JobContext<WirePayload<BackupObjectEventPayload>>): Promise<void> {
    const { id, data } = ctx.message
    const tenantId = data.tenant.ref
    const storage = await createStorage(data)

    const s3Key = storage.location.getKeyLocation({
      tenantId,
      bucketId: data.bucketId,
      objectName: data.name,
    })
    const backupKey = `__internal/${s3Key}/${data.version}`
    const logContext = {
      jobId: id,
      type: 'event' as const,
      event: 'BackupObject',
      objectPath: s3Key,
      objectVersion: data.version,
      backupKey,
      resources: [`${data.bucketId}/${data.name}`],
      tenantId,
      project: tenantId,
      reqId: data.reqId,
      sbReqId: data.sbReqId,
      payload: JSON.stringify(data),
    }

    try {
      if (!(storage.backend instanceof S3Backend)) {
        return
      }

      logSchema.event(logger, `[Admin]: BackupObject ${s3Key}`, logContext)

      try {
        await storage.backend.backup({
          sourceBucket: storageS3Bucket,
          destinationBucket: storageS3Bucket,
          sourceKey: `${s3Key}/${data.version}`,
          destinationKey: backupKey,
          size: data.size,
        })
      } catch (error) {
        if (
          !data.deleteOriginal ||
          !isS3Error(error) ||
          error.name !== 'NoSuchKey' ||
          error.$metadata.httpStatusCode !== 404
        ) {
          throw error
        }

        // A previous attempt may have deleted the source before losing its response.
        const backup = await storage.backend
          .headObject(storageS3Bucket, backupKey, undefined, { confirmMissing: true })
          .catch((backupError) => {
            if (!isMissingBackendObject(backupError)) throw backupError
          })
        if (!backup) {
          logger.warn(
            { ...logContext, outcome: 'source_and_backup_missing' },
            `[Admin]: BackupObjectEvent ${s3Key} - SKIPPED: source and backup are missing`
          )
          return
        }
        logger.info(
          { ...logContext, outcome: 'already_backed_up' },
          `[Admin]: BackupObjectEvent ${s3Key} - ALREADY BACKED UP`
        )
        return
      }

      if (data.deleteOriginal) {
        logSchema.event(logger, `[Admin]: DeleteOriginalObject ${s3Key}`, logContext)

        await storage.backend.deleteObject(storageS3Bucket, s3Key, data.version)
      }
    } catch (e) {
      logger.error({ ...logContext, error: e }, `[Admin]: BackupObjectEvent ${s3Key} - FAILED`)
      if (!isSyncDispatch(ctx.message)) throw e
    } finally {
      storage.db.destroyConnection()
    }
  }
}
