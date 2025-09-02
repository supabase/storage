import { Transform, TransformCallback } from 'stream'
import { ERRORS } from '@internal/errors'

export class ByteLimitTransformStream extends Transform {
  bytesProcessed = 0

  constructor(
    private readonly limit: number,
    private readonly bucketContext?: { name: string; fileSizeLimit?: number | null }
  ) {
    super()
  }

  _transform(chunk: Buffer, encoding: BufferEncoding, callback: TransformCallback) {
    this.bytesProcessed += chunk.length

    if (this.bytesProcessed > this.limit) {
      const context = this.bucketContext ? {
        bucketName: this.bucketContext.name,
        bucketLimit: this.bucketContext.fileSizeLimit || undefined
      } : undefined
      callback(ERRORS.EntityTooLarge(undefined, 'object', context))
    } else {
      callback(null, chunk)
    }
  }
}
