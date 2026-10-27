import { PassThrough, Readable } from 'node:stream'
import { buffer } from 'node:stream/consumers'
import { pipeline } from 'node:stream/promises'
import { ErrorCode } from '@internal/errors'
import { ByteCounterStream } from './byte-counter'

describe('ByteCounterStream', () => {
  it('preserves binary and UTF-8 bytes with a running count', async () => {
    const counter = new ByteCounterStream()
    const output = buffer(counter)
    const binary = Buffer.from([0, 255, 13, 10])
    const text = 'object bytes 🦥'

    expect(counter.bytes).toBe(0)
    expect(counter.receivedEncodedLength).toBe(0)
    counter.write(binary)
    expect(counter.bytes).toBe(binary.length)
    expect(counter.receivedEncodedLength).toBe(binary.length)
    counter.end(text)

    expect(await output).toEqual(Buffer.concat([binary, Buffer.from(text)]))
    expect(counter.bytes).toBe(binary.length + Buffer.byteLength(text))
    expect(counter.receivedEncodedLength).toBe(counter.bytes)
  })

  it.each([
    { size: 0, limit: 0, minimum: 0 },
    { size: 2, limit: 4, minimum: 2 },
    { size: 4, limit: 4, minimum: 2 },
  ])(
    'accepts $size bytes within inclusive bounds $minimum–$limit',
    async ({ size, limit, minimum }) => {
      const counter = new ByteCounterStream(limit, minimum)
      const input = Buffer.alloc(size)

      expect(await buffer(Readable.from([input]).pipe(counter))).toEqual(input)
      expect(counter.bytes).toBe(size)
    }
  )

  it.each([
    { limit: 0, chunks: [Buffer.from('a')], expectedOutput: Buffer.alloc(0) },
    { limit: 3, chunks: [Buffer.from('ab'), Buffer.from('cd')], expectedOutput: Buffer.from('ab') },
  ])(
    'stops before forwarding the chunk that exceeds $limit bytes',
    async ({ limit, chunks, expectedOutput }) => {
      const counter = new ByteCounterStream(limit)
      const output: Buffer[] = []
      counter.on('data', (chunk: Buffer) => output.push(chunk))

      await expect(pipeline(Readable.from(chunks), counter)).rejects.toMatchObject({
        code: ErrorCode.EntityTooLarge,
        httpStatusCode: 413,
      })

      expect(Buffer.concat(output)).toEqual(expectedOutput)
      expect(counter.bytes).toBe(Buffer.concat(chunks).length)
    }
  )

  it.each([0, 1])('rejects a %i-byte body below the minimum when it ends', async (size) => {
    const counter = new ByteCounterStream(4, 2)
    const output = buffer(counter)
    counter.write(Buffer.alloc(size))
    expect(counter.errored).toBeNull()

    counter.end()

    await expect(output).rejects.toMatchObject({
      code: ErrorCode.EntityTooSmall,
      httpStatusCode: 400,
    })
  })

  it('stops an open source when its limit is exceeded', async () => {
    const source = new PassThrough()
    const counter = new ByteCounterStream(1)
    const result = pipeline(source, counter)
    source.write(Buffer.from('ab'))

    await expect(result).rejects.toMatchObject({ code: ErrorCode.EntityTooLarge })
    expect(source.destroyed).toBe(true)
    expect(source.errored).toBe(counter.errored)
  })
})
