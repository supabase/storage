import { BaseEvent, BasePayload } from './base-event'
import { getConfig } from '../../config'
import { Job } from 'pg-boss'
import { withOptionalVersion } from '../../storage/backend'
import { logger } from '../../monitoring'

export interface ObjectDeleteEvent extends BasePayload {
  name: string
  bucketId: string
  version?: string
}

const { storageS3Bucket } = getConfig()

export class ObjectAdminDelete extends BaseEvent<ObjectDeleteEvent> {
  static queueName = 'object:admin:delete'

  static async handle(job: Job<ObjectDeleteEvent>) {
    logger.info({ job: JSON.stringify(job) }, 'Handling ObjectAdminDelete')

    try {
      const storage = await this.createStorage(job.data)
      const version = job.data.version

      const s3Key = `${job.data.tenant.ref}/${job.data.bucketId}/${job.data.name}`

      await storage.backend.deleteObjects(storageS3Bucket, [
        withOptionalVersion(s3Key, version),
        withOptionalVersion(s3Key, version) + '.info',
      ])
    } catch (e) {
      console.error(e)
      logger.error(
        {
          error: JSON.stringify(e),
        },
        'Error Deleting files from queue'
      )
      throw e
    }
  }
}
