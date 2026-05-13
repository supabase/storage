import { Readable } from 'node:stream'
import { describe, expect, test } from 'vitest'
import { createByteCounterStream } from './byte-counter'

describe('createByteCounterStream', () => {
  test('counts UTF-8 bytes for string chunks', async () => {
    const byteCounter = createByteCounterStream()
    const stream = Readable.from(['é😀']).pipe(byteCounter.transformStream)

    for await (const _chunk of stream) {
      // Drain the stream so the byte counter observes the source chunks.
    }

    expect(byteCounter.bytes).toBe(Buffer.byteLength('é😀'))
  })
})
