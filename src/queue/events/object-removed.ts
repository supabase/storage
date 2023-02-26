import { BaseEvent, BasePayload } from './base-event'
import { Job } from 'pg-boss'
import { getConfig } from '../../config'

export interface ObjectRemovedEvent extends BasePayload {
  name: string
  bucketId: string
}

export class ObjectRemoved extends BaseEvent<ObjectRemovedEvent> {
  protected static queueName = 'object-deleted'

  static eventName() {
    return `ObjectRemoved:Delete`
  }
}

export class ObjectRemovedMove extends BaseEvent<ObjectRemovedEvent> {
  protected static queueName = 'object-deleted'

  static eventName() {
    return `ObjectRemoved:Move`
  }
}

export interface ObjectDeleteEvent extends BasePayload {
  name: string
  bucketId: string
  version?: string
}

const { globalS3Bucket } = getConfig()

export class AdminDeleteObject extends BaseEvent<ObjectDeleteEvent> {
  static queueName = 'admin:delete:object'

  static async handle(job: Job<ObjectDeleteEvent>) {
    try {
      const storage = await this.createStorage(job.data)

      const s3Key = `${job.data.tenant.ref}/${job.data.bucketId}/${job.data.name}`
      await storage.backend.deleteObject(globalS3Bucket, s3Key, job.data.version)
      await storage.backend.deleteObject(globalS3Bucket, s3Key, job.data.version + '.info')
      console.log('deleted from s3', job.data.version)
    } catch (e) {
      console.error(e)
      throw e
    }
  }
}
