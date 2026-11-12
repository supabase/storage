import { memory } from '../adapters/memory'
import {
  createProducerQueue,
  createQueue,
  createSyncQueue,
  defineQueues,
  type JobContext,
  type Message,
  type ProduceCall,
  type QueueAdapter,
  type QueueMiddleware,
  queue,
} from './index'
import { recordingAdapter, rejection } from './test-support'

interface Ev {
  readonly n: number
}

const queues = defineQueues({
  jobs: queue<Ev>(),
  other: queue<Ev>(),
})

const fast = { pollIdleIntervalMs: 5, pollBatchIntervalMs: 0 }

const failing = (error: Error) => async (): Promise<void> => {
  throw error
}

/** The memory adapter with `send` failing unconditionally. */
const unreachable = (error: Error): QueueAdapter => ({ ...memory(), send: failing(error) })

/** The naive always-fallback stage: on any send failure, run the local handler instead. */
const fallback: QueueMiddleware = {
  produce: (next) => async (call) => {
    try {
      await next(call)
    } catch {
      for (const m of call.messages) {
        await call.invoke(call.queue, m.data, { key: m.key, headers: m.headers })
      }
    }
  },
}

describe('produce middleware', () => {
  it('sees the raw error, the queue and the wire-shaped batch; swallowing resolves the produce', async () => {
    const boom = new Error('connection refused')
    let got: ProduceCall | undefined
    let caught: unknown
    const { adapter } = recordingAdapter({ send: failing(boom) })
    const q = createProducerQueue(adapter, {
      queues,
      // The hook injects headers BEFORE the chain: middleware sees the batch as it would go
      // on the wire, not as the caller passed it.
      events: {
        produce: ({ headers }) => {
          headers.traceparent = '00-abc'
        },
      },
      middleware: [
        {
          produce: (next) => async (call) => {
            try {
              await next(call)
            } catch (error) {
              got = call
              caught = error
            }
          },
        },
      ],
    })

    await expect(
      q.produce('jobs', { n: 1 }, { key: 'k', headers: { h: '1' } })
    ).resolves.toBeUndefined()

    expect(caught).toBe(boom) // the exact instance, never wrapped
    expect(got?.queue).toBe('jobs')
    expect(got?.messages).toEqual([
      { data: { n: 1 }, key: 'k', headers: { h: '1', traceparent: '00-abc' } },
    ])
  })

  it('rethrowing surfaces the original error; a thrown replacement rejects with it', async () => {
    const boom = new Error('connection refused')
    const translated = new Error('backend down — event dropped')
    let mode: 'rethrow' | 'translate' = 'rethrow'
    const { adapter } = recordingAdapter({ send: failing(boom) })
    const q = createProducerQueue(adapter, {
      queues,
      middleware: [
        {
          produce: (next) => async (call) => {
            try {
              await next(call)
            } catch (error) {
              throw mode === 'rethrow' ? error : translated
            }
          },
        },
      ],
    })

    expect(await rejection(q.produce('jobs', { n: 1 }))).toBe(boom)
    mode = 'translate'
    expect(await rejection(q.produce('jobs', { n: 1 }))).toBe(translated)
  })

  it('without middleware a send error throws straight through', async () => {
    const boom = new Error('connection refused')
    const { adapter } = recordingAdapter({ send: failing(boom) })
    const q = createProducerQueue(adapter, { queues })
    expect(await rejection(q.produce('jobs', { n: 1 }))).toBe(boom)
  })

  it('runs once per produce call with the full batch, the caller signal and ctx', async () => {
    const calls: ProduceCall[] = []
    const { adapter, sends } = recordingAdapter<{ tx: number }>()
    const q = createProducerQueue(adapter, {
      queues,
      produceTimeout: 5_000,
      middleware: [
        {
          produce: (next) => async (call) => {
            calls.push(call)
            await next(call)
          },
        },
      ],
    })
    const ac = new AbortController()
    const ctx = { tx: 1 }

    await q.produce(
      'jobs',
      [{ data: { n: 1 }, key: 'a' }, { data: { n: 2 } }, { data: { n: 3 } }],
      {
        signal: ac.signal,
        ctx,
      }
    )
    await q.produce('jobs', { n: 4 })

    expect(calls).toHaveLength(2) // a batch is ONE send, so one pass through the chain
    expect(calls[0].messages.map((m) => [(m.data as Ev).n, m.key])).toEqual([
      [1, 'a'],
      [2, undefined],
      [3, undefined],
    ])
    expect(calls[0].signal).toBe(ac.signal) // the caller's, not the composed bound
    expect(calls[0].ctx).toBe(ctx)
    expect('signal' in calls[1]).toBe(false)
    expect('ctx' in calls[1]).toBe(false)
    expect(sends).toHaveLength(2)
  })

  it('call.getQueue resolves the declared descriptor from inside the chain', async () => {
    let resolved: unknown
    let missing: unknown = 'unset'
    const { adapter } = recordingAdapter()
    const q = createProducerQueue(adapter, {
      queues,
      middleware: [
        {
          produce: (next) => async (call) => {
            resolved = call.getQueue(call.queue)
            missing = call.getQueue('undeclared')
            await next(call)
          },
        },
      ],
    })

    await q.produce('jobs', { n: 1 })

    expect(resolved).toBe(queues.get('jobs')) // the registry's own descriptor, not a copy
    expect(missing).toBeUndefined()
  })

  it('composes first-declared outermost, skipping entries without a produce stage', async () => {
    const order: string[] = []
    const stage = (name: string): QueueMiddleware => ({
      produce: (next) => async (call) => {
        order.push(`${name}:before`)
        await next(call)
        order.push(`${name}:after`)
      },
    })
    const consumeOnly: QueueMiddleware = {
      consume: (next) => async (call) => {
        order.push('consume')
        return next(call)
      },
    }
    const { adapter } = recordingAdapter({
      send: async () => void order.push('send'),
    })
    const q = createProducerQueue(adapter, {
      queues,
      middleware: [stage('outer'), consumeOnly, stage('inner')],
    })

    await q.produce('jobs', { n: 1 })

    expect(order).toEqual(['outer:before', 'inner:before', 'send', 'inner:after', 'outer:after'])
  })

  it("a transforming middleware is encoding's input: what it passes down is what gets sent", async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, {
      queues,
      middleware: [
        {
          produce: (next) => async (call) =>
            next({
              ...call,
              queue: 'other',
              messages: call.messages.map((m) => ({
                ...m,
                data: { n: (m.data as Ev).n * 10 },
                headers: { ...m.headers, stamped: 'yes' },
              })),
            }),
        },
      ],
    })

    await q.produce('jobs', [{ data: { n: 4 } }, { data: { n: 5 }, key: 'k' }])

    expect(sends).toHaveLength(1)
    expect(sends[0].queue).toBe('other')
    expect(sends[0].messages).toEqual([
      { data: { n: 40 }, headers: { stamped: 'yes' } },
      { data: { n: 50 }, key: 'k', headers: { stamped: 'yes' } },
    ])
  })

  it('a middleware that never calls next drops the send and still resolves', async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, {
      queues,
      middleware: [{ produce: () => async () => {} }],
    })
    await q.produce('jobs', { n: 1 })
    expect(sends).toEqual([])
  })
})

describe('the call.invoke fallback seam', () => {
  it('full manager: a failed send falls back to the local handler and produce resolves', async () => {
    const seen: Message<Ev>[] = []
    const q = createQueue(unreachable(new Error('ECONNREFUSED')), {
      queues,
      ...fast,
      middleware: [fallback],
    })
    await q.start()
    q.subscribe('jobs', ({ message }) => void seen.push(message))

    await q.produce('jobs', [{ data: { n: 1 }, key: 'a', headers: { h: '1' } }, { data: { n: 2 } }])

    expect(seen.map((m) => [m.data.n, m.key, m.headers, m.attempt])).toEqual([
      [1, 'a', { h: '1' }, 1],
      [2, undefined, {}, 1],
    ])
    await q.close()
  })

  it('full manager: a timed-out send falls back too (the send may still land later)', async () => {
    const seen: number[] = []
    let caught: unknown
    const q = createQueue(
      { ...memory(), send: () => new Promise<never>(() => {}) },
      {
        queues,
        ...fast,
        produceTimeout: 20,
        middleware: [
          {
            produce: (next) => async (call) => {
              try {
                await next(call)
              } catch (error) {
                caught = error
                for (const m of call.messages) await call.invoke(call.queue, m.data)
              }
            },
          },
        ],
      }
    )
    await q.start()
    q.subscribe('jobs', ({ message }) => void seen.push(message.data.n))

    await q.produce('jobs', { n: 7 })

    expect((caught as DOMException).name).toBe('TimeoutError')
    expect(seen).toEqual([7])
    await q.close()
  })

  it('breaks the cascade: call.invoke inside an invoke-dispatched handler is refused', async () => {
    let cascade: unknown
    const q = createQueue(unreachable(new Error('down')), {
      queues,
      ...fast,
      middleware: [fallback],
    })
    await q.start()
    q.subscribe('other', () => {})
    q.subscribe('jobs', async () => {
      // A produce from inside an invoked handler re-enters the fallback with the backend
      // still down: exactly the unbounded inline cascade the guard exists for.
      cascade = await rejection(q.produce('other', { n: 2 }))
    })

    await q.invoke('jobs', { n: 1 })

    expect((cascade as Error).message).toContain('depth cap 1')
    await q.close()
  })

  it('producer-only manager: falls back to its config handlers', async () => {
    const handled: Ev[] = []
    const { adapter } = recordingAdapter({ send: failing(new Error('down')) })
    const q = createProducerQueue(adapter, {
      queues,
      handlers: [
        { queue: 'jobs', handle: (ctx: JobContext<Ev>) => void handled.push(ctx.message.data) },
      ],
      middleware: [fallback],
    })
    await q.start()

    await q.produce('jobs', { n: 7 })

    expect(handled).toEqual([{ n: 7 }])
    await q.close()
  })

  it('producer-only manager: loud when the queue has no config handler, never a silent no-op', async () => {
    const { adapter } = recordingAdapter({ send: failing(new Error('down')) })
    const q = createProducerQueue(adapter, { queues, middleware: [fallback] })
    await q.start()

    await expect(q.produce('jobs', { n: 1 })).rejects.toThrow(
      'queue: invoke — no local handler on queue "jobs" (config handlers attach at start())'
    )
    await q.close()
  })

  it('sync manager: call.invoke dispatches inline to the attached handler', async () => {
    const seen: unknown[] = []
    const q = createSyncQueue({
      queues,
      middleware: [
        {
          // Reroute: skip the terminal and hand another queue's handler the batch directly.
          produce: () => async (call) => {
            for (const m of call.messages) {
              await call.invoke('other', m.data, { key: 'rerouted', headers: m.headers })
            }
          },
        },
      ],
    })
    q.subscribe('jobs', () => void seen.push('jobs'))
    q.subscribe('other', ({ queue: name, message }) => {
      seen.push([name, message.data.n, message.key, message.headers])
    })

    await q.produce('jobs', { n: 3 }, { headers: { h: '1' } })

    expect(seen).toEqual([['other', 3, 'rerouted', { h: '1' }]])
    await q.close()
  })
})
