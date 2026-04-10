import { Transform, TransformCallback } from 'stream'

/**
 * A transform stream that buffers data until it has at least minChunkSize bytes
 * before pushing downstream. This is useful for S3 Tables which requires
 * chunks to be at least 8KB (except for the last chunk).
 */
export class MinChunkTransform extends Transform {
  private buffer: Buffer = Buffer.alloc(0)

  constructor(private readonly minChunkSize: number) {
    super()
  }

  _transform(chunk: Buffer, encoding: BufferEncoding, callback: TransformCallback) {
    try {
      this.buffer = Buffer.concat([this.buffer, chunk])

      // Push complete chunks of minChunkSize
      while (this.buffer.length >= this.minChunkSize) {
        this.push(this.buffer.subarray(0, this.minChunkSize))
        this.buffer = this.buffer.subarray(this.minChunkSize)
      }

      callback()
    } catch (err) {
      callback(err as Error)
    }
  }

  _flush(callback: TransformCallback) {
    // Push remaining data (last chunk can be smaller than minChunkSize)
    if (this.buffer.length > 0) {
      this.push(this.buffer)
    }
    callback()
  }
}
