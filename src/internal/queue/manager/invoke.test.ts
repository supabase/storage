import { memory } from '../adapters/memory'
import {
  type BatchContext,
  type Codec,
  createQueue,
  defineMessage,
  defineQueues,
  type JobContext,
  type Message,
  type QueueMiddleware,
  queue,
} from './index'
import { deferred, rejection, until } from './test-support'

interface Ev {
  readonly n: number
}

class OrderPlaced extends defineMessage<{ orderId: string }>({ key: (o) => o.orderId }) {
  readonly type = 'order.placed'
}

const queues = defineQueues({
  jobs: queue<Ev>(),
  other: queue<Ev>(),
  stamps: queue<{ at: Date }>(),
  orders: queue(OrderPlaced),
})

const fast = { pollIdleIntervalMs: 5, pollBatchIntervalMs: 0 }

describe('invoke', () => {
  it('runs the locally attached handler inline with a synthetic delivery, sending nothing', async () => {
    const adapter = memory()
    const send = vi.spyOn(adapter, 'send')
    const q = createQueue(adapter, { queues, ...fast })
    await q.start()
    const seen: JobContext<Ev>[] = []
    q.subscribe('jobs', async (ctx) => {
      await ctx.heartbeat() // no lease to renew: a no-op, never a throw
      seen.push(ctx)
    })

    await q.invoke('jobs', { n: 7 }, { key: 'k', headers: { h: '1' } })
    await q.invoke('jobs', { n: 8 })

    expect(seen).toHaveLength(2)
    const [{ queue: name, message, signal }, bare] = seen
    expect(name).toBe('jobs')
    expect(message).toEqual({
      id: expect.any(String),
      data: { n: 7 },
      key: 'k',
      headers: { h: '1' },
      timestamp: expect.any(Number),
      attempt: 1,
    })
    expect(message.id).not.toBe(bare.message.id)
    expect(bare.message.headers).toEqual({})
    expect('key' in bare.message).toBe(false)
    expect(signal.aborted).toBe(false)
    expect(send).not.toHaveBeenCalled()
    await q.close()
  })

  it('round-trips the payload through the codec, so the handler sees the wire shape', async () => {
    // A Date survives an in-process object graph but not a stringifying wire format; inline
    // execution must not hide that difference from the handler.
    const jsonString: Codec<unknown> = {
      contentType: 'application/json',
      encode: (value) => JSON.stringify(value),
      decode: (wire) => JSON.parse(wire as string),
    }
    const q = createQueue(memory(), { queues, ...fast, codec: jsonString })
    await q.start()
    let at: unknown
    q.subscribe('stamps', ({ message }) => {
      at = message.data.at
    })

    await q.invoke('stamps', { at: new Date('2026-01-02T03:04:05.000Z') })

    expect(at).toBe('2026-01-02T03:04:05.000Z')
    await q.close()
  })

  it('is exactly one execution: the first handler attached to the queue runs', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await q.start()
    const ran: string[] = []
    const first = q.subscribe('jobs', () => void ran.push('first'))
    q.subscribe('jobs', () => void ran.push('second'))
    q.subscribe('other', () => void ran.push('other'))

    await q.invoke('jobs', { n: 1 })
    expect(ran).toEqual(['first'])

    // Once its worker stops, the next attached handler stands in.
    await first.stop()
    await q.invoke('jobs', { n: 2 })
    expect(ran).toEqual(['first', 'second'])
    await q.close()
  })

  it('throws when the queue has no locally attached handler', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await q.start()
    q.subscribe('other', () => {})
    await expect(q.invoke('jobs', { n: 1 })).rejects.toThrow(
      'queue: invoke — no local handler on queue "jobs" (config handlers attach at start())'
    )
    await expect(q.invoke(42 as never)).rejects.toThrow(TypeError)
    await q.close()
  })

  it('targets config handlers only once start() has attached them', async () => {
    const ran: number[] = []
    const q = createQueue(memory(), {
      queues,
      ...fast,
      handlers: [
        { queue: 'jobs', handle: (ctx: JobContext<Ev>) => void ran.push(ctx.message.data.n) },
      ],
    })
    await expect(q.invoke('jobs', { n: 1 })).rejects.toThrow(/attach at start\(\)/)
    await q.start()
    await q.invoke('jobs', { n: 2 })
    expect(ran).toEqual([2])
    await q.close()
  })

  it("no longer targets a stopped worker's handler", async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await q.start()
    const worker = q.subscribe('jobs', () => {})
    await worker.stop()
    await expect(q.invoke('jobs', { n: 1 })).rejects.toThrow(/no local handler/)
    await q.close()
  })

  it('hands a batch-shaped handler the one message as a batch', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await q.start()
    const calls: number[][] = []
    q.subscribe({
      queue: 'jobs',
      handleBatch: (ctx: BatchContext<Ev>) => void calls.push(ctx.messages.map((m) => m.data.n)),
    })
    await q.invoke('jobs', { n: 5 })
    expect(calls).toEqual([[5]])
    await q.close()
  })

  it('dispatches a message instance: queue, key and type resolve from the class', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await q.start()
    const seen: Message<{ orderId: string }>[] = []
    q.subscribe('orders', ({ message }) => void seen.push(message))

    await q.invoke(new OrderPlaced({ orderId: 'o-1' }, { headers: { h: '1' } }))

    expect(seen).toHaveLength(1)
    expect(seen[0]).toMatchObject({
      data: { orderId: 'o-1' },
      key: 'o-1',
      type: 'order.placed',
      headers: { h: '1', 'message-type': 'order.placed' },
    })

    const never = (): boolean => false
    if (never()) {
      // @ts-expect-error — the message-instance form takes no options
      void q.invoke(new OrderPlaced({ orderId: 'o-1' }), { key: 'k' })
      // @ts-expect-error — wrong payload type for the queue
      void q.invoke('jobs', { n: 'one' })
      // @ts-expect-error — a delay is not an invoke option: nothing is sent
      void q.invoke('jobs', { n: 1 }, { delayMs: 10 })
    }
    await q.close()
  })
})

describe('invoke failures', () => {
  it('a handler failure rejects with THAT error, unwrapped', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await q.start()
    const boom = new Error('boom')
    q.subscribe('jobs', () => {
      throw boom
    })
    expect(await rejection(q.invoke('jobs', { n: 1 }))).toBe(boom)
    await q.close()
  })

  it("honours the handler's consumeTimeout: a timeout is a failure, not a redelivery", async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await q.start()
    let aborted: unknown
    q.subscribe({ queue: 'jobs', consumeTimeout: 20 }, ({ signal }) => {
      signal.addEventListener('abort', () => {
        aborted = signal.reason
      })
      return new Promise(() => {})
    })

    const err = await rejection(q.invoke('jobs', { n: 1 }))

    expect(err).toBeInstanceOf(Error)
    expect((err as Error).message).toBe(
      'queue: invoke — invocation on queue "jobs" timed out after 20ms'
    )
    expect((aborted as DOMException).name).toBe('TimeoutError')
    await q.close()
  })

  it('runs the consume chain around the dispatch: manager-level outside attachment-level', async () => {
    const order: string[] = []
    const stage = (name: string): QueueMiddleware => ({
      consume: (next) => async (call) => {
        order.push(`${name}:before:${call.queue}`)
        const result = await next(call)
        order.push(`${name}:after:${result.outcome}`)
        return result
      },
    })
    const q = createQueue(memory(), { queues, ...fast, middleware: [stage('manager')] })
    await q.start()
    q.subscribe({ queue: 'jobs', middleware: [stage('worker')] }, () => void order.push('handler'))

    await q.invoke('jobs', { n: 1 })

    expect(order).toEqual([
      'manager:before:jobs',
      'worker:before:jobs',
      'handler',
      'worker:after:ok',
      'manager:after:ok',
    ])
    await q.close()
  })

  it('a middleware rejection fails the invoke: with its error, or a queue: invoke error without one', async () => {
    const blocked = new Error('blocked')
    let withError = true
    const guard: QueueMiddleware = {
      consume: (next) => async (call) => {
        for (const m of [...call.messages]) {
          if (withError) call.reject(m, blocked)
          else call.reject(m)
        }
        return next(call)
      },
    }
    const q = createQueue(memory(), { queues, ...fast, middleware: [guard] })
    await q.start()
    const seen: unknown[] = []
    q.subscribe('jobs', ({ message }) => void seen.push(message.data))

    expect(await rejection(q.invoke('jobs', { n: 1 }))).toBe(blocked)
    withError = false
    await expect(q.invoke('jobs', { n: 2 })).rejects.toThrow(
      'queue: invoke — message rejected on queue "jobs"'
    )
    expect(seen).toEqual([]) // a rejected message never reaches the handler
    await q.close()
  })

  it('a middleware that never calls next, or throws, fails the invoke loudly', async () => {
    const thrown = new Error('stage blew up')
    let mode: 'skip' | 'throw' = 'skip'
    const broken: QueueMiddleware = {
      consume: () => async (call) => {
        if (mode === 'throw') throw thrown
        return { outcome: 'ok', size: call.messages.length, ok: 1, released: 0, failures: [] }
      },
    }
    const q = createQueue(memory(), { queues, ...fast, middleware: [broken] })
    await q.start()
    const handler = vi.fn()
    q.subscribe('jobs', handler)

    await expect(q.invoke('jobs', { n: 1 })).rejects.toThrow(
      'queue: invoke — a consume middleware resolved without calling next on queue "jobs"'
    )
    mode = 'throw'
    expect(await rejection(q.invoke('jobs', { n: 1 }))).toBe(thrown)
    expect(handler).not.toHaveBeenCalled()
    await q.close()
  })

  it('a middleware cannot hide a handler failure behind another result', async () => {
    const boom = new Error('boom')
    const masking: QueueMiddleware = {
      consume: (next) => async (call) => {
        await next(call)
        return { outcome: 'ok', size: 1, ok: 1, released: 0, failures: [] }
      },
    }
    const q = createQueue(memory(), { queues, ...fast, middleware: [masking] })
    await q.start()
    q.subscribe('jobs', () => {
      throw boom
    })
    expect(await rejection(q.invoke('jobs', { n: 1 }))).toBe(boom)
    await q.close()
  })

  it('aborts in-flight invocations on close(), rejecting the caller', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await q.start()
    const entered = deferred()
    let reason: unknown
    q.subscribe('jobs', ({ signal }) => {
      signal.addEventListener('abort', () => {
        reason = signal.reason
      })
      entered.resolve()
      // Never settles: a handler that completes on abort would be honoured as ok.
      return new Promise(() => {})
    })

    const pending = rejection(q.invoke('jobs', { n: 1 }))
    await entered.promise
    await q.close()

    const err = await pending
    expect(err).toBeInstanceOf(Error)
    expect((err as Error).message).toBe(
      'queue: invoke — invocation on queue "jobs" aborted by shutdown'
    )
    expect((reason as DOMException).name).toBe('AbortError')
  })
})

describe('the invoke re-entrancy guard', () => {
  it('refuses invoke from inside an invoke-dispatched handler, on any queue or manager', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    const elsewhere = createQueue(memory(), { queues, ...fast })
    await Promise.all([q.start(), elsewhere.start()])
    const reached = vi.fn()
    q.subscribe('other', reached)
    elsewhere.subscribe('other', reached)
    const nested: unknown[] = []
    q.subscribe('jobs', async () => {
      nested.push(await rejection(q.invoke('other', { n: 2 })))
      // The guard survives an async hop and crosses manager instances.
      await new Promise((r) => setTimeout(r, 1))
      nested.push(await rejection(elsewhere.invoke('other', { n: 3 })))
    })

    await q.invoke('jobs', { n: 1 })

    expect(nested.map((e) => (e as Error).message)).toEqual([
      'queue: invoke called from inside an invoke-dispatched handler — refusing the inline cascade (depth cap 1)',
      'queue: invoke called from inside an invoke-dispatched handler — refusing the inline cascade (depth cap 1)',
    ])
    expect(reached).not.toHaveBeenCalled()

    // The guard is scoped to the dispatch: a later top-level invoke is unaffected.
    await q.invoke('other', { n: 4 })
    expect(reached).toHaveBeenCalledTimes(1)
    await Promise.all([q.close(), elsewhere.close()])
  })

  it('allows invoke from a handler a worker delivered: only inline dispatch is capped', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await q.start()
    const seen: number[] = []
    q.subscribe('other', ({ message }) => void seen.push(message.data.n))
    q.subscribe('jobs', async ({ message }) => {
      await q.invoke('other', { n: message.data.n * 10 })
    })

    await q.produce('jobs', { n: 4 })
    await until(() => seen.length === 1)

    expect(seen).toEqual([40])
    await q.close()
  })
})
