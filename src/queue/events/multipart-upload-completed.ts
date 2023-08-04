import { BaseEvent, BasePayload } from './base-event'
import { Job } from 'pg-boss'
import { S3Backend } from '../../storage/backend'
import { isS3Error } from '../../storage'

interface UploadCompleted extends BasePayload {
  bucketName: string
  objectName: string
  version: string
}

export class MultiPartUploadCompleted extends BaseEvent<UploadCompleted> {
  static queueName = 'multipart:upload:completed'

  static getBucketId(payload: UploadCompleted): string | undefined {
    return payload.bucketName
  }

  static async handle(job: Job<UploadCompleted>) {
    try {
      const storage = await this.createStorage(job.data)
      const version = job.data.version

      const bucketStore = await storage.fromBucketId(job.data.bucketName)

      const s3Key = `${bucketStore.computeObjectPath(job.data.objectName)}/${version}`

      if (storage.backend instanceof S3Backend) {
        await storage.backend.setMetadataToCompleted(s3Key)
      }
    } catch (e) {
      if (isS3Error(e) && e.$metadata.httpStatusCode === 404) {
        return
      }
      throw e
    }
  }
}
