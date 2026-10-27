import { Transform, TransformCallback } from 'node:stream'
import { ERRORS } from '@internal/errors'

export class ByteCounterStream extends Transform {
  bytes = 0

  constructor(
    private readonly limit = Infinity,
    private readonly minimum = 0
  ) {
    super()
  }

  // Fastify reads this property from streams returned by preParsing hooks.
  get receivedEncodedLength() {
    return this.bytes
  }

  _transform(chunk: Buffer, _enc: BufferEncoding, cb: TransformCallback): void {
    this.bytes += chunk.length

    if (this.bytes > this.limit) {
      cb(ERRORS.EntityTooLarge())
    } else {
      cb(null, chunk)
    }
  }

  _flush(cb: TransformCallback): void {
    if (this.bytes < this.minimum) {
      cb(ERRORS.EntityTooSmall())
    } else {
      cb()
    }
  }
}
