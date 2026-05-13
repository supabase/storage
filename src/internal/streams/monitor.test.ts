import { Readable } from 'node:stream'
import { describe, expect, test } from 'vitest'
import { createByteCounterStream } from './byte-counter'
import { monitorStream } from './monitor'

describe('monitorStream', () => {
  test('uses a caller-provided byte counter', async () => {
    const byteCounter = createByteCounterStream()
    const stream = monitorStream(Readable.from(['hello']), byteCounter)

    for await (const _chunk of stream) {
      // Drain stream so the monitor pipeline consumes the source body.
    }

    expect(byteCounter.bytes).toBe(5)
  })
})
