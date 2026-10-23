import { ERRORS } from '@internal/errors'
import { createHash, timingSafeEqual } from 'crypto'
import { Transform, TransformCallback } from 'stream'

export class ContentMD5TransformStream extends Transform {
  private readonly hash = createHash('md5')
  private readonly expected: Buffer

  constructor(contentMD5: string) {
    super()

    const decoded = Buffer.from(contentMD5, 'base64')
    if (decoded.length !== 16 || decoded.toString('base64') !== contentMD5) {
      throw ERRORS.InvalidDigest()
    }
    this.expected = decoded
  }

  _transform(chunk: Buffer, _encoding: BufferEncoding, callback: TransformCallback) {
    this.hash.update(chunk)
    callback(null, chunk)
  }

  _flush(callback: TransformCallback) {
    if (!timingSafeEqual(this.hash.digest(), this.expected)) {
      callback(ERRORS.BadDigest())
      return
    }
    callback()
  }
}
