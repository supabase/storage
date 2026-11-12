import {
  type BatchContext,
  createSyncQueue,
  defineMessage,
  defineQueues,
  type JobContext,
  jsonText,
  type Message,
  NotSupportedError,
  QueueHandler,
  type QueueMiddleware,
  queue,
} from './index'
import { deferred, rejection } from './test-support'

interface Ev {
  readonly n: number
}

class Logged extends defineMessage<Ev>({
  key: (d) => `k-${d.n}`,
  idempotencyKey: (d) => `n-${d.n}`,
}) {
  readonly type = 'logged'
}

const queues = defineQueues({
  jobs: queue<Ev>(),
  chain: queue<{ hops: number }>(),
  stamps: queue<{ at: Date }>(),
  logged: queue(Logged),
})

const batchOf = (...ns: number[]): { data: Ev }[] => ns.map((n) => ({ data: { n } }))

describe('createSyncQueue', () => {
  it('produce IS execution: runs the attached handler inline with a synthetic delivery', async () => {
    const q = createSyncQueue({ queues })
    const seen: JobContext<Ev>[] = []
    q.subscribe('jobs', async (ctx) => {
      await ctx.heartbeat()
      seen.push(ctx)
    })

    await q.produce('jobs', { n: 7 }, { key: 'k', headers: { h: '1' } })

    expect(seen).toHaveLength(1)
    expect(seen[0].queue).toBe('jobs')
    expect(seen[0].message).toEqual({
      id: expect.any(String),
      data: { n: 7 },
      key: 'k',
      headers: { h: '1' },
      timestamp: expect.any(Number),
      attempt: 1,
    })
  })

  it('round-trips every dispatched payload through the codec, on produce and invoke', async () => {
    const q = createSyncQueue({ queues, codec: jsonText() })
    const seen: unknown[] = []
    q.subscribe('stamps', ({ message }) => void seen.push(message.data.at))
    const at = new Date('2026-01-02T03:04:05.000Z')

    await q.produce('stamps', { at })
    await q.invoke('stamps', { at })

    expect(seen).toEqual(['2026-01-02T03:04:05.000Z', '2026-01-02T03:04:05.000Z'])
  })

  it('attaches config handlers at start(), including message-class handlers', async () => {
    const seen: string[] = []
    class LogHandler extends QueueHandler(Logged) {
      async handle(ctx: JobContext<Ev>): Promise<void> {
        seen.push(`${ctx.message.type}:${ctx.message.key}:${ctx.message.data.n}`)
      }
    }
    const q = createSyncQueue({ queues, handlers: [new LogHandler()] })

    await expect(q.produce(new Logged({ n: 1 }))).rejects.toThrow(
      'queue: sync queue — no local handler on queue "logged" (config handlers attach at start())'
    )
    await q.start()
    await q.start() // idempotent
    await q.produce(new Logged({ n: 2 }))
    await q.invoke(new Logged({ n: 3 }))

    expect(seen).toEqual(['logged:k-2:2', 'logged:k-3:3'])
  })

  it('start() attaches the config handlers all or nothing', async () => {
    class Stray extends defineMessage<{ x: number }>() {}
    const ran = vi.fn()
    const q = createSyncQueue({
      queues,
      handlers: [{ queue: 'jobs', handle: ran }, { messages: [Stray], handle: () => {} } as never],
    })

    await expect(q.start()).rejects.toThrow(/"Stray" is not bound to any queue/)
    // The good entry did not slip in ahead of the bad one.
    await expect(q.produce('jobs', { n: 1 })).rejects.toThrow(/no local handler/)
    expect(ran).not.toHaveBeenCalled()
  })

  it('allows a nested produce or invoke from inside a handler: no depth cap', async () => {
    const q = createSyncQueue({ queues })
    const hops: number[] = []
    const jobs: number[] = []
    q.subscribe('jobs', ({ message }) => void jobs.push(message.data.n))
    q.subscribe('chain', async ({ message }) => {
      const n = message.data.hops
      hops.push(n)
      if (n > 0)
        await q.produce('chain', { hops: n - 1 }) // the self-resend pattern
      else await q.invoke('jobs', { n: 99 })
    })

    await q.produce('chain', { hops: 3 })

    expect(hops).toEqual([3, 2, 1, 0])
    expect(jobs).toEqual([99])
  })

  it('accepts delayMs (runs immediately) and idempotencyKey (no dedup) for produce parity', async () => {
    const q = createSyncQueue({ queues })
    let runs = 0
    q.subscribe('jobs', () => void runs++)

    await q.produce('jobs', { n: 1 }, { delayMs: 60_000, idempotencyKey: 'same' })
    await q.produce('jobs', { n: 1 }, { idempotencyKey: 'same' })

    expect(runs).toBe(2) // every produce runs: at-most-once per call, no window
  })

  it('applies produce and consume stages and the produce hook exactly as a real manager', async () => {
    const order: string[] = []
    const both: QueueMiddleware = {
      produce: (next) => async (call) => {
        order.push(`produce:before:${call.messages[0].headers?.traceparent}`)
        await next(call)
        order.push('produce:after')
      },
      consume: (next) => async (call) => {
        order.push('consume:before')
        const result = await next(call)
        order.push(`consume:after:${result.outcome}`)
        return result
      },
    }
    const q = createSyncQueue({
      queues,
      middleware: [both],
      events: {
        produce: ({ headers }) => {
          headers.traceparent = '00-abc'
        },
      },
    })
    q.subscribe('jobs', ({ message }) => void order.push(`handler:${message.headers.traceparent}`))

    await q.produce('jobs', { n: 1 })

    expect(order).toEqual([
      'produce:before:00-abc',
      'consume:before',
      'handler:00-abc',
      'consume:after:ok',
      'produce:after',
    ])
  })

  it('a batch produce reaches a batch-shaped handler WHOLE, in one call', async () => {
    const calls: number[][] = []
    const q = createSyncQueue({ queues })
    q.subscribe({
      queue: 'jobs',
      handleBatch: (ctx: BatchContext<Ev>) => void calls.push(ctx.messages.map((m) => m.data.n)),
    })

    await q.produce('jobs', batchOf(1, 2, 3))
    await q.produce('jobs', { n: 4 })

    expect(calls).toEqual([[1, 2, 3], [4]])
  })

  it('a single-message handler gets a batch one at a time in send order, fail-fast', async () => {
    const q = createSyncQueue({ queues })
    const seen: number[] = []
    const stop = new Error('stop here')
    q.subscribe('jobs', ({ message }) => {
      if (message.data.n === 3) throw stop
      seen.push(message.data.n)
    })

    expect(await rejection(q.produce('jobs', batchOf(1, 2, 3, 4)))).toBe(stop)
    expect(seen).toEqual([1, 2]) // 3 failed, 4 never ran
  })

  it('a handler failure rejects the produce with that error; a middleware can swallow it', async () => {
    const boom = new Error('boom')
    const strict = createSyncQueue({ queues })
    strict.subscribe('jobs', () => {
      throw boom
    })
    expect(await rejection(strict.produce('jobs', { n: 1 }))).toBe(boom)
    expect(await rejection(strict.invoke('jobs', { n: 1 }))).toBe(boom)

    const lenient = createSyncQueue({
      queues,
      middleware: [
        {
          produce: (next) => async (call) => {
            await next(call).catch(() => {}) // swallow: produce resolves
          },
        },
      ],
    })
    lenient.subscribe('jobs', () => {
      throw boom
    })
    await expect(lenient.produce('jobs', { n: 1 })).resolves.toBeUndefined()
  })

  it("honours the handler's consumeTimeout", async () => {
    const q = createSyncQueue({ queues })
    q.subscribe({ queue: 'jobs', consumeTimeout: 20 }, () => new Promise(() => {}))
    await expect(q.produce('jobs', { n: 1 })).rejects.toThrow(
      'queue: sync queue — invocation on queue "jobs" timed out after 20ms'
    )
  })

  it("dispatches to the first handler attached; a worker's stop() detaches it", async () => {
    const q = createSyncQueue({ queues })
    const runs: string[] = []
    const a = q.subscribe('jobs', () => void runs.push('a'))
    const b = q.subscribe('jobs', () => void runs.push('b'))

    await q.produce('jobs', { n: 1 })
    await q.invoke('jobs', { n: 1 }, { key: 'k' })
    expect(runs).toEqual(['a', 'a'])

    expect(a.closed).toBe(false)
    await a.stop()
    expect(a.closed).toBe(true)
    await q.produce('jobs', { n: 1 })
    expect(runs).toEqual(['a', 'a', 'b'])

    await b.stop()
    await expect(q.produce('jobs', { n: 1 })).rejects.toThrow(
      /sync queue — no local handler on queue "jobs"/
    )
  })

  it('has no backend to sample or maintain, and exposes the declared registry', async () => {
    const q = createSyncQueue({ queues })
    const err = await rejection(q.stats())
    expect(err).toBeInstanceOf(NotSupportedError)
    expect((err as NotSupportedError).capability).toBe('stats')

    q.maintenance.start({ intervalMs: 1 })
    await q.maintenance.runOnce()
    await q.maintenance.stop()

    expect(q.getQueue('jobs')).toBe(queues.get('jobs'))
    expect(q.getQueues()).toBe(queues)

    const never = (): boolean => false
    if (never()) {
      // @ts-expect-error — no backend, so no produce context
      void q.produce('jobs', { n: 1 }, { ctx: {} })
      // @ts-expect-error — undeclared queue
      void q.produce('nope', { n: 1 })
    }
  })

  it('close() aborts an in-flight dispatch and refuses every later one', async () => {
    const q = createSyncQueue({ queues })
    const entered = deferred()
    const seen: Message<Ev>[] = []
    let aborted = false
    q.subscribe('jobs', ({ message, signal }) => {
      seen.push(message)
      signal.addEventListener('abort', () => {
        aborted = true
      })
      entered.resolve()
      return new Promise(() => {})
    })

    const pending = rejection(q.produce('jobs', { n: 1 }))
    await entered.promise
    await q.close()

    expect(((await pending) as Error).message).toBe(
      'queue: sync queue — invocation on queue "jobs" aborted by shutdown'
    )
    expect(aborted).toBe(true)
    await expect(q.produce('jobs', { n: 2 })).rejects.toThrow('queue: sync queue is closed')
    await expect(q.invoke('jobs', { n: 3 })).rejects.toThrow('queue: sync queue is closed')
    expect(seen).toHaveLength(1)
  })
})
