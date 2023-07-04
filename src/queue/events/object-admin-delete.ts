import { BaseEvent, BasePayload } from './base-event'
import { Job } from 'pg-boss'
import { withOptionalVersion } from '../../storage/backend'
import { logger } from '../../monitoring'

export interface ObjectDeleteEvent extends BasePayload {
  name: string
  bucketId: string
  version?: string
}

export class ObjectAdminDelete extends BaseEvent<ObjectDeleteEvent> {
  static queueName = 'object:admin:delete'

  static getBucketId(payload: ObjectDeleteEvent): string | undefined {
    return payload.bucketId
  }

  static async handle(job: Job<ObjectDeleteEvent>) {
    logger.info({ job: JSON.stringify(job) }, 'Handling ObjectAdminDelete')

    try {
      const storage = await this.createStorage(job.data)
      const bucketStore = await storage.fromBucketId(job.data.bucketId)

      const version = job.data.version

      const s3Key = bucketStore.computeObjectPath(job.data.name)

      await storage.backend.deleteObjects([
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
