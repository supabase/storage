import { PassThrough, Readable } from 'node:stream'
import { buffer } from 'node:stream/consumers'
import { setImmediate } from 'node:timers/promises'
import { monitorStream } from './monitor'
import { monitorStreamSpeed } from './stream-speed'

describe.each([
  ['upload monitor', monitorStream],
  ['speed monitor', monitorStreamSpeed],
] as const)('%s lifecycle', (_name, monitor) => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval'] })
  })

  afterEach(() => vi.useRealTimers())

  it('preserves every byte and clears its timer on successful completion', async () => {
    const chunks = [Buffer.from('object bytes'), Buffer.from([0, 255, 13, 10])]
    const source = Readable.from(chunks)
    const output = monitor(source)

    expect(await buffer(output)).toEqual(Buffer.concat(chunks))
    await setImmediate()
    expect(vi.getTimerCount()).toBe(0)
  })

  it('stops the source and clears its timer when a consumer cancels', async () => {
    const source = new PassThrough()
    const output = monitor(source)

    try {
      expect(vi.getTimerCount()).toBe(1)
      output.destroy()
      await setImmediate()

      expect(source.destroyed).toBe(true)
      expect(vi.getTimerCount()).toBe(0)
    } finally {
      output.destroy()
      source.destroy()
      await setImmediate()
    }
  })

  it('propagates source errors to the consumer and clears its timer', async () => {
    const source = new PassThrough()
    const output = monitor(source)
    const error = new Error('upload source failed')
    const result = buffer(output)

    source.destroy(error)
    await expect(result).rejects.toBe(error)
    await setImmediate()
    expect(vi.getTimerCount()).toBe(0)
  })

  it('preserves a consumer failure while stopping the source and timer', async () => {
    const source = new PassThrough()
    const output = monitor(source)
    const error = new Error('upload consumer failed')
    const result = buffer(output)

    output.destroy(error)
    await expect(result).rejects.toBe(error)
    await setImmediate()

    expect(source.destroyed).toBe(true)
    expect(source.errored).toBe(error)
    expect(vi.getTimerCount()).toBe(0)
  })
})
