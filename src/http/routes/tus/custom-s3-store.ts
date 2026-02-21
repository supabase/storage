import { S3Store, S3StoreOptions } from '@tus/s3-store'
import { Upload } from '@tus/server'
import http from 'http'
import { HeadObjectCommand } from '@aws-sdk/client-s3'

export class CustomS3Store extends S3Store {
  constructor(options: S3StoreOptions) {
    super(options)
  }

  async finish(req: http.IncomingMessage, id: string, offset: number): Promise<Upload> {
    try {
      return await super.finish(req, id, offset)
    } catch (error: any) {
      // Check if the error is related to multipart completion failure
      // RustFS or some S3 backends might return 500 or 400 even if the file is persisted
      if (
        (error.statusCode === 500 || error.statusCode === 400) &&
        (error.message?.includes('One or more of the specified parts could not be found') ||
          error.message?.includes('Internal Server Error'))
      ) {
        // Attempt to check if the object actually exists
        try {
          const bucket = this.bucket
          const key = (this as any).key(id)

          const headCommand = new HeadObjectCommand({
            Bucket: bucket,
            Key: key,
          })

          const data = await (this as any).client.send(headCommand)

          // If we find the object, we assume success
          return {
            id,
            offset,
            size: data.ContentLength,
            metadata: {}, // We might lose some metadata here but the upload is recovered
          }
        } catch (headError) {
          // If HeadObject also fails, throw the original error
          throw error
        }
      }

      throw error
    }
  }
}
