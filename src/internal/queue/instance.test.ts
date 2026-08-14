import { vi } from 'vitest'
import { isQueueStarted, setWaveForTesting, stopQueue } from './instance'

// The v1 `Queue.stop` timeout tests: a hung teardown must not wedge shutdown — the 25s race
// rejects and the cached instance is cleared so a later start can re-create the queue.
describe('stopQueue', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('times out a hung wave close and clears the cached instance', async () => {
    vi.useFakeTimers()

    const close = vi.fn(() => new Promise<void>(() => undefined))
    setWaveForTesting({
      start: vi.fn().mockResolvedValue(undefined),
      close,
    })
    expect(isQueueStarted()).toBe(true)

    const stopPromise = stopQueue()
    const stopErrorPromise = stopPromise.catch((error) => error)

    await vi.advanceTimersByTimeAsync(25_000)

    expect(await stopErrorPromise).toEqual(
      expect.objectContaining({
        message: 'Queue stop timeout',
      })
    )
    expect(close).toHaveBeenCalledTimes(1)
    expect(isQueueStarted()).toBe(false)
  })

  it('is a no-op when the queue was never started', async () => {
    expect(isQueueStarted()).toBe(false)

    await expect(stopQueue()).resolves.toBeUndefined()
  })

  it('returns the in-flight stop promise instead of racing a second teardown', async () => {
    vi.useFakeTimers()

    const close = vi.fn(() => new Promise<void>(() => undefined))
    setWaveForTesting({
      start: vi.fn().mockResolvedValue(undefined),
      close,
    })

    const settled = Promise.allSettled([stopQueue(), stopQueue()])

    await vi.advanceTimersByTimeAsync(25_000)
    const [first, second] = await settled

    // Both callers share the one in-flight teardown: same rejection, one close().
    expect(first).toEqual({ status: 'rejected', reason: new Error('Queue stop timeout') })
    expect(second).toEqual(first)
    expect(close).toHaveBeenCalledTimes(1)
  })
})
