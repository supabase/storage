import { BaseEvent, BasePayload } from './base-event'
import { getConfig } from '../../config'
import { Job, SendOptions } from 'pg-boss'
import { withOptionalVersion } from '../../storage/backend'

export interface ObjectDeleteEvent extends BasePayload {
  name: string
  bucketId: string
  version?: string
}

const { globalS3Bucket } = getConfig()

export class ObjectAdminDelete extends BaseEvent<ObjectDeleteEvent> {
  static queueName = 'object:admin:delete'

  static getQueueOptions(): SendOptions | undefined {
    return {}
  }

  static async handle(job: Job<ObjectDeleteEvent>) {
    try {
      const storage = await this.createStorage(job.data)
      const version = job.data.version

      const s3Key = `${job.data.tenant.ref}/${job.data.bucketId}/${job.data.name}`

      await storage.backend.deleteObjects(globalS3Bucket, [
        withOptionalVersion(s3Key, version),
        withOptionalVersion(s3Key, version) + '.info',
      ])
    } catch (e) {
      console.error(e)
      throw e
    }
  }
}
