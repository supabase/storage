import { BaseEvent, BasePayload } from './base-event'
import { Job } from 'pg-boss'
import { getConfig } from '../../config'
import { S3Backend } from '../../storage/backend'
import { isS3Error } from '../../storage'

interface UploadCompleted extends BasePayload {
  bucketName: string
  objectName: string
  version: string
}

const { globalS3Bucket } = getConfig()

export class UploadCompletedEvent extends BaseEvent<UploadCompleted> {
  static queueName = 'object-uploaded'

  static async handle(job: Job<UploadCompleted>) {
    try {
      const storage = await this.createStorage(job.data)
      const version = job.data.version

      const s3Key = `${job.data.tenant.ref}/${job.data.bucketName}/${job.data.objectName}/${version}`

      if (storage.backend instanceof S3Backend) {
        await storage.backend.updateObjectInfoMetadata(globalS3Bucket, s3Key)
      }
    } catch (e) {
      if (isS3Error(e) && e.$metadata.httpStatusCode === 404) {
        return
      }
      throw e
    }
  }
}
