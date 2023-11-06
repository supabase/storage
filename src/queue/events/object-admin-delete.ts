import { BaseEvent, BasePayload } from './base-event'
import { getConfig } from '../../config'
import { Job, WorkOptions } from 'pg-boss'
import { withOptionalVersion } from '../../storage/backend'
import { logger, logSchema } from '../../monitoring'
import { Storage } from '../../storage'

export interface ObjectDeleteEvent extends BasePayload {
  name: string
  bucketId: string
  version?: string
}

const { globalS3Bucket, adminDeleteQueueTeamSize, adminDeleteConcurrency } = getConfig()

export class ObjectAdminDelete extends BaseEvent<ObjectDeleteEvent> {
  static queueName = 'object:admin:delete'

  static getWorkerOptions(): WorkOptions {
    return {
      teamSize: adminDeleteQueueTeamSize,
      teamConcurrency: adminDeleteConcurrency,
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
        tenantId: job.data.tenant.ref,
        project: job.data.tenant.ref,
        reqId: job.data.reqId,
      })

      await storage.backend.deleteObjects(globalS3Bucket, [
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
