import { BaseEvent, BasePayload } from './base-event'
import { Job } from 'pg-boss'
import { getConfig } from '../../config'
import { S3Backend } from '../../storage/backend'
import { isS3Error, Storage } from '../../storage'
import { logger } from '../../monitoring'

interface UploadCompleted extends BasePayload {
  bucketName: string
  objectName: string
  version: string
}

const { globalS3Bucket } = getConfig()

export class MultiPartUploadCompleted extends BaseEvent<UploadCompleted> {
  static queueName = 'multipart:upload:completed'

  static async handle(job: Job<UploadCompleted>) {
    let storage: Storage | undefined = undefined
    try {
      storage = await this.createStorage(job.data)
      const version = job.data.version

      const s3Key = `${job.data.tenant.ref}/${job.data.bucketName}/${job.data.objectName}/${version}`

      if (storage.backend instanceof S3Backend) {
        await storage.backend.setMetadataToCompleted(globalS3Bucket, s3Key)
      }
    } catch (e) {
      if (isS3Error(e) && e.$metadata.httpStatusCode === 404) {
        return
      }
      logger.error({ error: e }, 'multi part uploaded completed failed')
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
              `[Admin]: MultiPartUploadCompleted ${tenant.ref} - FAILED DISPOSING CONNECTION`
            )
          })
      }
    }
  }
}
