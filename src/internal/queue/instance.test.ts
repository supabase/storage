import { queueJobSyncInvocations } from '@internal/monitoring/metrics'
import { vi } from 'vitest'
import { isQueueStarted, setQueueForTesting, startSyncQueueForTesting, stopQueue } from './instance'
import { defineMessage, defineQueues, QueueHandler, queue } from './manager'

vi.mock('@internal/monitoring/metrics', async (importOriginal) => ({
  ...(await importOriginal<object>()),
  queueJobSyncInvocations: { add: vi.fn() },
}))

describe('sync invocation metric', () => {
  class Ping extends defineMessage<{ n: number }>() {
    static readonly eventType = 'Ping'
    readonly type = 'Ping'
  }
  const queues = defineQueues({ pings: queue(Ping) })

  afterEach(async () => {
    vi.mocked(queueJobSyncInvocations.add).mockClear()
    await stopQueue()
  })

  it('records an explicit invoke as reason invoke, and a produce as queue_disabled', async () => {
    const handled: number[] = []
    class PingHandler extends QueueHandler(Ping) {
      handle(ctx: { message: { data: { n: number } } }) {
        handled.push(ctx.message.data.n)
      }
    }
    const q = await startSyncQueueForTesting({ queues, handlers: [new PingHandler()] })

    await q.invoke(new Ping({ n: 1 }))
    await q.produce(new Ping({ n: 2 }))

    expect(handled).toEqual([1, 2])
    expect(vi.mocked(queueJobSyncInvocations.add).mock.calls).toEqual([
      [1, { name: 'pings', reason: 'invoke', status: 'ok' }],
      [1, { name: 'pings', reason: 'queue_disabled', status: 'ok' }],
    ])
  })

  it('records invoke by queue name and invokeMany as one invoke call each', async () => {
    const handled: number[] = []
    class PingHandler extends QueueHandler(Ping) {
      handle(ctx: { message: { data: { n: number } } }) {
        handled.push(ctx.message.data.n)
      }
    }
    const q = await startSyncQueueForTesting({ queues, handlers: [new PingHandler()] })

    await q.invoke('pings', { n: 1 })
    await q.invokeMany('pings', [{ data: { n: 2 } }, { data: { n: 3 } }])

    expect(handled).toEqual([1, 2, 3])
    expect(vi.mocked(queueJobSyncInvocations.add).mock.calls).toEqual([
      [1, { name: 'pings', reason: 'invoke', status: 'ok' }],
      [1, { name: 'pings', reason: 'invoke', status: 'ok' }],
    ])
  })
})

// The v1 `Queue.stop` timeout tests: a hung teardown must not wedge shutdown — the 25s race
// rejects and the cached instance is cleared so a later start can re-create the queue.
describe('stopQueue', () => {
  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  it('times out a hung queue close and clears the cached instance', async () => {
    vi.useFakeTimers()

    const close = vi.fn(() => new Promise<void>(() => undefined))
    setQueueForTesting({
      start: vi.fn().mockResolvedValue(undefined),
      close,
    })
    expect(isQueueStarted()).toBe(true)

    const stopPromise = stopQueue()
    const stopErrorPromise = stopPromise.catch((error) => error)

    await vi.advanceTimersByTimeAsync(25_000)

    expect(await stopErrorPromise).toEqual(
      expect.objectContaining({
        message: 'Queue stop timed out after 25000ms',
      })
    )
    expect(close).toHaveBeenCalledTimes(1)
    expect(isQueueStarted()).toBe(false)
  })

  it('bounds the stop by an earlier deadline and still runs onStop when it is hit', async () => {
    vi.useFakeTimers()

    const close = vi.fn(() => new Promise<void>(() => undefined))
    const onStop = vi.fn().mockResolvedValue(undefined)
    setQueueForTesting({ close })

    const stopError = stopQueue(onStop, Date.now() + 5_000).catch((error) => error)
    await vi.advanceTimersByTimeAsync(4_999)
    expect(isQueueStarted()).toBe(true)
    await vi.advanceTimersByTimeAsync(1)

    expect(await stopError).toEqual(
      expect.objectContaining({ message: 'Queue stop timed out after 5000ms' })
    )
    expect(onStop).toHaveBeenCalledOnce()
    expect(isQueueStarted()).toBe(false)
  })

  it('runs onStop once the queue has closed and clears the cached instance', async () => {
    const calls: string[] = []
    setQueueForTesting({
      close: vi.fn(async () => {
        calls.push('close')
      }),
    })

    await expect(
      stopQueue(async () => {
        calls.push('onStop')
      })
    ).resolves.toBeUndefined()

    expect(calls).toEqual(['close', 'onStop'])
    expect(isQueueStarted()).toBe(false)
  })

  it('is a no-op when the queue was never started', async () => {
    expect(isQueueStarted()).toBe(false)

    await expect(stopQueue()).resolves.toBeUndefined()
  })

  it('returns the in-flight stop promise instead of racing a second teardown', async () => {
    vi.useFakeTimers()

    const close = vi.fn(() => new Promise<void>(() => undefined))
    setQueueForTesting({
      start: vi.fn().mockResolvedValue(undefined),
      close,
    })

    const settled = Promise.allSettled([stopQueue(), stopQueue()])

    await vi.advanceTimersByTimeAsync(25_000)
    const [first, second] = await settled

    // Both callers share the one in-flight teardown: same rejection, one close().
    expect(first).toEqual({
      status: 'rejected',
      reason: new Error('Queue stop timed out after 25000ms'),
    })
    expect(second).toEqual(first)
    expect(close).toHaveBeenCalledTimes(1)
  })
})
