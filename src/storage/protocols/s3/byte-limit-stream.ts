import { Transform, TransformCallback } from 'stream'
import { ERRORS } from '@internal/errors'

export class ByteLimitTransformStream extends Transform {
  bytesProcessed = 0

  constructor(private readonly limit: number) {
    super()
  }

  _transform(chunk: Buffer, encoding: BufferEncoding, callback: TransformCallback) {
    this.bytesProcessed += chunk.length

    if (this.bytesProcessed > this.limit) {
      callback(ERRORS.EntityTooLarge())
    } else {
      callback(null, chunk)
    }
  }
}
