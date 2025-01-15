import { BaseEvent } from './base-event'
import { JobWithMetadata, SendOptions, WorkOptions } from 'pg-boss'
import { BasePayload } from '@internal/queue'
import { S3Backend } from '@storage/backend'
import { getConfig } from '../../config'
import { logger, logSchema } from '@internal/monitoring'

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
      teamSize: 10,
      teamConcurrency: 5,
      includeMetadata: true,
    }
  }

  static getQueueOptions(payload: BackupObjectEventPayload): SendOptions {
    return {
      singletonKey: payload.tenant.ref,
      retryLimit: 5,
      retryDelay: 5,
      priority: 10,
    }
  }

  static async handle(job: JobWithMetadata<BackupObjectEventPayload>) {
    const tenantId = job.data.tenant.ref
    const storage = await this.createStorage(job.data)

    if (!(storage.backend instanceof S3Backend)) {
      return
    }

    const { storageS3Bucket, storageS3Region, storageS3BackupBucket } = getConfig()

    if (!storageS3BackupBucket) {
      throw new Error('Backup bucket not configured')
    }

    const s3Key = `${tenantId}/${job.data.bucketId}/${job.data.name}`

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
        destinationBucket: storageS3BackupBucket,
        sourceKey: `${s3Key}/${job.data.version}`,
        destinationKey: `${storageS3Region}/${s3Key}/${job.data.version}`,
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

        await storage.backend.deleteObject(
          storageS3Bucket,
          `${tenantId}/${job.data.bucketId}/${job.data.name}`,
          job.data.version
        )
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
