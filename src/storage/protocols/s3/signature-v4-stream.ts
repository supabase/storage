// ChunkSignatureParser.ts

import crypto from 'crypto'
import { Transform, TransformCallback, TransformOptions } from 'stream'

type ParserState = 'HEADER' | 'DATA' | 'FOOTER' | 'TRAILER'

/**
 * Represents the different types of V4 streaming algorithms supported.
 *
 * The `V4StreamingAlgorithm` type defines a series of constants that specify the
 * streaming algorithms used for AWS signature version 4 signing in streaming scenarios.
 * These algorithms dictate the method for signing and authenticating streamed payloads.
 *
 * Available algorithms:
 * - 'STREAMING-UNSIGNED-PAYLOAD-TRAILER': Indicates an unsigned payload with a trailer.
 * - 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD': Indicates AWS signature version 4 with HMAC-SHA256 for streamed payload signing.
 * - 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER': Indicates AWS signature version 4 with HMAC-SHA256 for streamed payload signing, including a trailer.
 */
export type V4StreamingAlgorithm =
  | 'STREAMING-UNSIGNED-PAYLOAD-TRAILER'
  | 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD'
  | 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER'

/**
 * Represents the configuration options for parsing chunk signatures in a
 * streaming operation. This interface extends the TransformOptions interface
 * and adds additional options specific to chunk signature validation and
 * processing.
 *
 * Properties:
 * - `maxHeaderLength` (optional): Specifies the maximum number of bytes allowed
 *   for the header line. Default is 128 bytes.
 * - `signaturePattern` (optional): A regular expression used to validate the
 *   signature for each chunk. The default pattern ensures 64 hexadecimal characters.
 * - `streamingAlgorithm`: Specifies the streaming algorithm to be used. This
 *   property is required.
 * - `trailerHeaderNames` (optional): A list of header names included in the
 *   trailer, as specified in the `x-amz-trailer` header.
 * - `maxChunkSize`: Specifies the maximum allowable size for each chunk. The
 *   default is 8MB.
 */
export interface ChunkSignatureParserOptions extends TransformOptions {
  /** Max bytes of header line; default 128 */
  maxHeaderLength?: number
  /** Regex for validating per-chunk signature; default 64 hex chars */
  signaturePattern?: RegExp
  /** Choose streaming algorithm */
  streamingAlgorithm: V4StreamingAlgorithm
  /** Names of trailer headers as specified in x-amz-trailer */
  trailerHeaderNames?: string[]
  /** Max chunk size; default 8MB */
  maxChunkSize: number
}

/**
 * ChunkSignatureV4Parser is a Transform stream implementation designed to parse and validate
 * chunked data streams with signatures as part of the AWS Signature Version 4 streaming protocol.
 * See: http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
 *
 * It processes data in sequential phases (HEADER, DATA, FOOTER, TRAILER) and validates the integrity
 * and authenticity of chunked payloads. The parser supports multiple streaming algorithms and verifies
 * chunk signatures when required.
 *
 * A chunk has the following format:
 *
 * <chunk-size-as-hex> + ";chunk-signature=" + <signature-as-hex> + "\r\n" + <payload> + "\r\n"
 *
 */
export class ChunkSignatureV4Parser extends Transform {
  private buffer = Buffer.alloc(0)
  private state: ParserState = 'HEADER'
  private bytesRemaining = 0

  private dataRead = 0
  private currentSignature?: string
  private currentChunkSize = 0
  private currentHash!: crypto.Hash
  private previousSignature?: string

  private readonly maxHeaderLength: number
  private readonly signaturePattern: RegExp
  private readonly alg: V4StreamingAlgorithm

  constructor(private opts: ChunkSignatureParserOptions) {
    super(opts)
    this.validateStreamingAlgorithm(opts.streamingAlgorithm)

    this.maxHeaderLength = opts.maxHeaderLength ?? 128
    this.signaturePattern = opts.signaturePattern ?? /^[0-9a-fA-F]{64}$/
    this.alg = opts.streamingAlgorithm
  }

  _transform(chunk: Buffer | string, encoding: BufferEncoding, cb: TransformCallback) {
    const data = typeof chunk === 'string' ? Buffer.from(chunk, encoding) : chunk
    this.buffer = Buffer.concat([this.buffer, data])

    try {
      let madeProgress: boolean

      do {
        madeProgress = false
        switch (this.state) {
          case 'HEADER':
            madeProgress = this.consumeHeader()
            break
          case 'DATA':
            madeProgress = this.consumeData()
            break
          case 'FOOTER':
            madeProgress = this.consumeFooter()
            break
          case 'TRAILER':
            madeProgress = this.consumeTrailer()
            break
        }
      } while (madeProgress)

      cb()
    } catch (err) {
      cb(err as Error)
    }
  }

  private validateStreamingAlgorithm(alg: string) {
    const validAlgorithms = [
      'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
      'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
      'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
    ]
    if (!validAlgorithms.includes(alg)) {
      throw new Error(`Invalid streaming algorithm: ${alg}`)
    }
  }

  /**
   * Extracts size and signature from a header line according to the current algorithm.
   */
  private parseHeaderLine(line: string): { size: number; signature?: string } {
    const delim = ';chunk-signature='
    let size: number
    let sig: string | undefined

    if (line.includes(delim)) {
      const [sizeHex, signature] = line.split(delim)
      size = parseInt(sizeHex, 16)
      sig = signature
      if (isNaN(size) || !this.signaturePattern.test(sig)) {
        throw new Error(`Invalid header: "${line}"`)
      }
    } else {
      size = parseInt(line, 16)
      if (isNaN(size)) {
        throw new Error(`Invalid chunk size: "${line}"`)
      }
      // signature required for signed algorithms
      if (
        this.alg === 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' ||
        this.alg === 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER'
      ) {
        throw new Error(`Missing chunk signature: "${line}"`)
      }
    }
    return { size, signature: sig }
  }

  private consumeHeader(): boolean {
    if (this.buffer.length === 0) return false

    const idx = this.buffer.indexOf('\r\n')
    if (idx < 0) {
      if (this.buffer.length > this.maxHeaderLength) {
        throw new Error(`Header exceeds ${this.maxHeaderLength} bytes`)
      }
      return false
    }
    const line = this.buffer.subarray(0, idx).toString('utf8')
    const { size, signature } = this.parseHeaderLine(line)

    if (size > this.opts.maxChunkSize) {
      throw new Error(`Chunk size exceeds ${this.opts.maxChunkSize} bytes`)
    }

    this.currentChunkSize = size
    this.currentSignature = signature
    this.bytesRemaining = size
    this.currentHash = crypto.createHash('sha256')

    this.buffer = this.buffer.subarray(idx + 2)
    this.state = 'DATA'
    return true
  }

  private consumeData(): boolean {
    const want = this.bytesRemaining
    const piece = this.buffer.length <= want ? this.buffer : this.buffer.subarray(0, want)

    this.currentHash.update(piece)

    if (piece.length === 0) {
      const isLastChunk = this.currentChunkSize === 0
      if (isLastChunk) {
        this.state = 'TRAILER'
      }
      return isLastChunk
    }

    this.push(piece)

    this.buffer = this.buffer.subarray(piece.length)
    this.bytesRemaining -= piece.length
    this.dataRead += piece.length

    if (this.dataRead > this.opts.maxChunkSize) {
      throw new Error(`Chunk size exceeds ${this.opts.maxChunkSize} bytes`)
    }

    if (this.bytesRemaining === 0) {
      this.dataRead = 0
      this.state = 'FOOTER'
    }
    return true
  }

  private consumeFooter(): boolean {
    if (this.buffer.length < 2) return false
    if (this.buffer[0] !== 0x0d || this.buffer[1] !== 0x0a) {
      throw new Error('Missing CRLF after chunk data')
    }

    const sig = this.currentSignature
    const size = this.currentChunkSize
    const prev = this.previousSignature

    if (this.alg !== 'STREAMING-UNSIGNED-PAYLOAD-TRAILER') {
      const hash = this.currentHash.digest('hex')
      this.emit('signatureReadyForVerification', sig, size, hash, prev)
    }

    this.previousSignature = sig
    this.buffer = this.buffer.subarray(2)
    this.state = size === 0 && this.hasTrailer() ? 'TRAILER' : 'HEADER'

    return true
  }

  private consumeTrailer(): boolean {
    const dbl = this.buffer.indexOf('\r\n\r\n')
    if (dbl < 0) return false

    const block = this.buffer.subarray(0, dbl).toString('utf8')
    this.buffer = this.buffer.subarray(dbl + 4)

    const parsed: Record<string, string> = {}
    block.split('\r\n').forEach((line) => {
      const [k, v] = line.split(/: ?/, 2)
      if (k && v !== undefined) parsed[k.toLowerCase()] = v
    })

    const names = this.opts.trailerHeaderNames || []
    const trailers: Record<string, string> = {}
    names.forEach((n) => {
      const key = n.toLowerCase()
      if (parsed[key] !== undefined) trailers[key] = parsed[key]
    })

    this.emit('trailer', trailers)
    this.state = 'HEADER'

    return true
  }

  private hasTrailer(): boolean {
    return this.alg.endsWith('-TRAILER')
  }
}
