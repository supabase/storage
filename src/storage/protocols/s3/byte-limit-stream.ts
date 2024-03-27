import { Transform, TransformCallback } from 'stream'
import { ERRORS } from '../../errors'

export class ByteLimitTransformStream extends Transform {
  bytesProcessed = BigInt(0)

  constructor(private readonly limit: bigint) {
    super()
  }

  _transform(chunk: Buffer, encoding: BufferEncoding, callback: TransformCallback) {
    this.bytesProcessed += BigInt(chunk.length)

    if (this.bytesProcessed > this.limit) {
      callback(ERRORS.EntityTooLarge())
    } else {
      callback(null, chunk)
    }
  }
}
