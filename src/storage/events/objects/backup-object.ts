import { BaseEvent } from '../base-event'
import { JobWithMetadata, Queue, SendOptions, WorkOptions } from 'pg-boss'
import { BasePayload } from '@internal/queue'
import { S3Adapter } from '@storage/backend'
import { getConfig } from '../../../config'
import { logger, logSchema } from '@internal/monitoring'

const { storageS3Bucket } = getConfig()

interface BackupObjectEventPayload extends BasePayload {
  name: string
  bucketId: string
  version: string
  size: number
  deleteOriginal?: boolean
}

export class BackupObjectEvent extends BaseEvent<BackupObjectEventPayload> {
  static queueName = 'backup-object'

  static getWorkerOptions(): WorkOptions {
    return {
      includeMetadata: true,
    }
  }

  static getQueueOptions(): Queue {
    return {
      name: this.queueName,
      policy: 'singleton',
    } as const
  }

  static getSendOptions(payload: BackupObjectEventPayload): SendOptions {
    return {
      singletonKey: `${payload.tenant.ref}/${payload.bucketId}/${payload.name}/${payload.version}`,
      retryLimit: 5,
      retryDelay: 5,
      priority: 10,
    }
  }

  static async handle(job: JobWithMetadata<BackupObjectEventPayload>) {
    const tenantId = job.data.tenant.ref
    const storage = await this.createStorage(job.data)

    if (!(storage.backend instanceof S3Adapter)) {
      return
    }

    const s3Key = storage.location.getKeyLocation({
      tenantId,
      bucketId: job.data.bucketId,
      objectName: job.data.name,
    })

    try {
      logSchema.event(logger, `[Admin]: BackupObject ${s3Key}`, {
        jodId: job.id,
        type: 'event',
        event: 'BackupObject',
        payload: JSON.stringify(job.data),
        objectPath: s3Key,
        resources: [`${job.data.bucketId}/${job.data.name}`],
        tenantId: job.data.tenant.ref,
        project: job.data.tenant.ref,
        reqId: job.data.reqId,
      })

      await storage.backend.backup({
        sourceBucket: storageS3Bucket,
        destinationBucket: storageS3Bucket,
        sourceKey: `${s3Key}/${job.data.version}`,
        destinationKey: `__internal/${s3Key}/${job.data.version}`,
        size: job.data.size,
      })

      if (job.data.deleteOriginal) {
        logSchema.event(logger, `[Admin]: DeleteOriginalObject ${s3Key}`, {
          jodId: job.id,
          type: 'event',
          event: 'BackupObject',
          payload: JSON.stringify(job.data),
          objectPath: s3Key,
          resources: [`${job.data.bucketId}/${job.data.name}`],
          tenantId: job.data.tenant.ref,
          project: job.data.tenant.ref,
          reqId: job.data.reqId,
        })

        await storage.backend.remove({
          bucket: storageS3Bucket,
          key: storage.location.getKeyLocation({
            tenantId,
            bucketId: job.data.bucketId,
            objectName: job.data.name,
          }),
          version: job.data.version,
        })
      }
    } catch (e) {
      logger.error(
        {
          error: e,
          jodId: job.id,
          type: 'event',
          event: 'ObjectAdminDelete',
          payload: JSON.stringify(job.data),
          objectPath: s3Key,
          objectVersion: job.data.version,
          tenantId: job.data.tenant.ref,
          project: job.data.tenant.ref,
          reqId: job.data.reqId,
        },
        `[Admin]: BackupObjectEvent ${s3Key} - FAILED`
      )
    } finally {
      storage.db
        .destroyConnection()
        .then(() => {
          // no-op
        })
        .catch((e) => {
          logger.error(
            { error: e },
            `[Admin]: BackupObjectEvent ${tenantId} - FAILED DISPOSING CONNECTION`
          )
        })
    }
  }
}
