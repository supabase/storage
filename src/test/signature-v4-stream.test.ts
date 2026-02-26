import {
  ChunkSignatureParserOptions,
  ChunkSignatureV4Parser,
} from '@storage/protocols/s3/signature-v4-stream'
import { Buffer } from 'buffer'
import crypto from 'crypto'

describe('ChunkSignatureV4Parser', () => {
  const makeParser = (opts: Partial<ChunkSignatureParserOptions> = {}) => {
    const defaultOpts: ChunkSignatureParserOptions = {
      streamingAlgorithm: 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
      maxChunkSize: 1024,
      ...opts,
    }
    return new ChunkSignatureV4Parser(defaultOpts)
  }

  test('constructor throws on invalid algorithm', () => {
    expect(
      () => new ChunkSignatureV4Parser({ streamingAlgorithm: 'INVALID' as any, maxChunkSize: 1 })
    ).toThrow(/Invalid streaming algorithm/)
  })

  test('parseHeaderLine accepts signed header and rejects malformed signature', () => {
    const parser = makeParser()
    const parse = (parser as any).parseHeaderLine.bind(parser)
    const validSig = 'a'.repeat(64)
    const { size, signature } = parse(`5;chunk-signature=${validSig}`)
    expect(size).toBe(5)
    expect(signature).toBe(validSig)
    expect(() => parse('5;chunk-signature=xyz')).toThrow(/Invalid header/)
  })

  test('parseHeaderLine rejects invalid chunk size', () => {
    const parser = makeParser()
    const parse = (parser as any).parseHeaderLine.bind(parser)
    expect(() => parse('zz;chunk-signature=' + 'a'.repeat(64))).toThrow(/Invalid header/)
    expect(() => parse('zz')).toThrow(/Invalid chunk size/)
  })

  test('missing signature for signed algorithm emits error', () => {
    const parser = makeParser({ maxChunkSize: 10 })
    return new Promise<void>((resolve) => {
      parser.on('error', (err) => {
        expect(err).toBeInstanceOf(Error)
        expect(err.message).toMatch(/Missing chunk signature/)
        resolve()
      })
      parser.write('5\r\n')
    })
  })

  test('header exceeding maxHeaderLength emits error', () => {
    const parser = makeParser({ maxHeaderLength: 2 })
    return new Promise<void>((resolve) => {
      parser.on('error', (err) => {
        expect(err.message).toMatch(/Header exceeds 2 bytes/)
        resolve()
      })
      parser.write('abc')
    })
  })

  test('chunk size exceeds maxChunkSize emits error', () => {
    const parser = makeParser({ maxChunkSize: 1 })
    const sig = 'f'.repeat(64)
    return new Promise<void>((resolve) => {
      parser.on('error', (err) => {
        expect(err.message).toMatch(/Chunk size exceeds 1 bytes/)
        resolve()
      })
      parser.write(`2;chunk-signature=${sig}\r\n`)
    })
  })

  test('missing CRLF after chunk data emits error', () => {
    const sig = '0'.repeat(64)
    const parser = makeParser({ maxChunkSize: 10 })
    return new Promise<void>((resolve) => {
      parser.on('error', (err) => {
        expect(err.message).toMatch(/Missing CRLF after chunk data/)
        resolve()
      })
      parser.write(`1;chunk-signature=${sig}\r\n`)
      parser.write('a')
      // write invalid footer prefix to trigger error
      parser.write('xx')
    })
  })

  test('emits signatureReadyForVerification and data for single signed chunk', () => {
    const sig = '0'.repeat(64)
    const parser = makeParser({ maxChunkSize: 10 })
    const dataChunks: Buffer[] = []
    const sigEvents: Array<{ sig: string; size: number; hash: string; prev: string | undefined }> =
      []

    parser.on('data', (chunk: Buffer) => dataChunks.push(chunk))
    parser.on('signatureReadyForVerification', (sigVal, size, hash, prev) => {
      sigEvents.push({ sig: sigVal, size, hash, prev })
    })

    const header = `5;chunk-signature=${sig}\r\n`
    const payload = 'hello'
    const footer = '\r\n'
    const endChunk = `0;chunk-signature=${sig}\r\n\r\n`

    parser.end(header + payload + footer + endChunk)

    return new Promise<void>((resolve) => {
      parser.on('end', () => {
        expect(Buffer.concat(dataChunks).toString()).toBe('hello')
        // only one signature event for the data chunk
        expect(sigEvents).toHaveLength(1)
        expect(sigEvents[0].size).toBe(5)
        const expectedHash = crypto.createHash('sha256').update(payload).digest('hex')
        expect(sigEvents[0].hash).toBe(expectedHash)
        expect(sigEvents[0].sig).toBe(sig)
        expect(sigEvents[0].prev).toBeUndefined()
        resolve()
      })
    })
  })

  test('supports unsigned payload algorithm and emits trailer', () => {
    const trailerKey = 'x-amz-meta-foo'
    const trailerValue = 'bar'
    const opts: ChunkSignatureParserOptions = {
      streamingAlgorithm: 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
      maxChunkSize: 1024,
      trailerHeaderNames: [trailerKey],
    }
    const parser = new ChunkSignatureV4Parser(opts)
    const dataChunks: Buffer[] = []
    let trailerObj: Record<string, string> | undefined

    parser.on('data', (chunk: Buffer) => dataChunks.push(chunk))
    parser.on('trailer', (t) => {
      trailerObj = t
    })

    const header = `3\r\n`
    const payload = 'hey'
    const footer = '\r\n'
    const endChunk = `0\r\n`
    const trailerBlock = `${trailerKey}: ${trailerValue}\r\nother: ignore\r\n\r\n`

    parser.end(header + payload + footer + endChunk + trailerBlock)

    return new Promise<void>((resolve) => {
      parser.on('end', () => {
        expect(Buffer.concat(dataChunks).toString()).toBe('hey')
        expect(trailerObj).toEqual({ [trailerKey]: trailerValue })
        resolve()
      })
    })
  })

  test('dataRead exceeding maxChunkSize emits error', () => {
    const sig = 'a'.repeat(64)
    const parser = makeParser({ maxChunkSize: 1 })
    return new Promise<void>((resolve) => {
      parser.on('error', (err) => {
        expect(err.message).toMatch(/Chunk size exceeds 1 bytes/)
        resolve()
      })
      parser.write(`2;chunk-signature=${sig}\r\n`)
      parser.write('ab')
    })
  })

  test('honors custom signaturePattern', () => {
    const parser = makeParser({
      streamingAlgorithm: 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
      maxChunkSize: 10,
      signaturePattern: /^[0-9]+$/,
    })
    const parse = (parser as any).parseHeaderLine.bind(parser)
    expect(parse('3;chunk-signature=123').signature).toBe('123')
    expect(() => parse('3;chunk-signature=abc')).toThrow()
  })
})
