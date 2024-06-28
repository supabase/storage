import { BaseEvent } from './base-event'
import { getConfig } from '../../config'
import { Job, SendOptions, WorkOptions } from 'pg-boss'
import { withOptionalVersion } from '../backend'
import { logger, logSchema } from '@internal/monitoring'
import { Storage } from '../index'
import { BasePayload } from '@internal/queue'

export interface ObjectDeleteEvent extends BasePayload {
  name: string
  bucketId: string
  version?: string
}

const { storageS3Bucket, adminDeleteQueueTeamSize, adminDeleteConcurrency } = getConfig()

export class ObjectAdminDelete extends BaseEvent<ObjectDeleteEvent> {
  static queueName = 'object:admin:delete'

  static getWorkerOptions(): WorkOptions {
    return {
      teamSize: adminDeleteQueueTeamSize,
      teamConcurrency: adminDeleteConcurrency,
    }
  }

  static getQueueOptions(): SendOptions {
    return {
      priority: 10,
    }
  }

  static async handle(job: Job<ObjectDeleteEvent>) {
    let storage: Storage | undefined = undefined

    try {
      storage = await this.createStorage(job.data)
      const version = job.data.version

      const s3Key = `${job.data.tenant.ref}/${job.data.bucketId}/${job.data.name}`

      logSchema.event(logger, `[Admin]: ObjectAdminDelete ${s3Key}`, {
        jodId: job.id,
        type: 'event',
        event: 'ObjectAdminDelete',
        payload: JSON.stringify(job.data),
        objectPath: s3Key,
        resources: [`${job.data.bucketId}/${job.data.name}`],
        tenantId: job.data.tenant.ref,
        project: job.data.tenant.ref,
        reqId: job.data.reqId,
      })

      await storage.backend.deleteObjects(storageS3Bucket, [
        withOptionalVersion(s3Key, version),
        withOptionalVersion(s3Key, version) + '.info',
      ])
    } catch (e) {
      const s3Key = `${job.data.tenant.ref}/${job.data.bucketId}/${job.data.name}`

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
        `[Admin]: ObjectAdminDelete ${s3Key} - FAILED`
      )
      throw e
    } finally {
      if (storage) {
        const tenant = storage.db.tenant()
        storage.db
          .destroyConnection()
          .then(() => {
            // no-op
          })
          .catch((e) => {
            logger.error(
              { error: e },
              `[Admin]: ObjectAdminDelete ${tenant.ref} - FAILED DISPOSING CONNECTION`
            )
          })
      }
    }
  }
}
