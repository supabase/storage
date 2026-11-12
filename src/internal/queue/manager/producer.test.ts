import {
  createProducerQueue,
  defineMessage,
  defineQueues,
  jsonText,
  MESSAGE_TYPE_HEADER,
  NotSupportedError,
  type QueueMiddleware,
  queue,
} from './index'
import { hangingSend, recordingAdapter, rejection, sleep } from './test-support'

interface Ev {
  readonly n: number
}

class OrderPlaced extends defineMessage<{ id: string }>({
  key: (o) => o.id,
  headers: { source: 'checkout' },
}) {
  readonly type = 'order.placed'
}
class OrderPaid extends defineMessage<{ id: string }>() {
  readonly type = 'order.paid'
}
class Shipment extends defineMessage<{ id: string }>() {}
class Unbound extends defineMessage<{ nope: boolean }>() {}

const queues = defineQueues({
  jobs: queue<Ev>(),
  nums: queue<number[]>(),
  wrapped: queue<{ data: number }[]>(),
  orders: queue([OrderPlaced, OrderPaid]),
  shipments: queue(Shipment),
})

describe('produce forms', () => {
  it('single form: one send carrying the payload and its per-message options', async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues })

    await q.produce('jobs', { n: 42 })
    await q.produce(
      'jobs',
      { n: 43 },
      {
        key: 'k',
        headers: { h: '1' },
        idempotencyKey: 'once',
        idempotencyTtlMs: 60_000,
        delayMs: 5_000,
      }
    )

    expect(sends.map((s) => [s.queue, s.messages])).toEqual([
      ['jobs', [{ data: { n: 42 }, headers: {} }]],
      [
        'jobs',
        [
          {
            data: { n: 43 },
            key: 'k',
            headers: { h: '1' },
            idempotencyKey: 'once',
            idempotencyTtlMs: 60_000,
            delayMs: 5_000,
          },
        ],
      ],
    ])
  })

  it('batch form: ONE send, in order, each message keeping its own options', async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues })

    await q.produce('jobs', [
      { data: { n: 1 }, key: 'a', idempotencyKey: 'k-1', idempotencyTtlMs: 30_000 },
      { data: { n: 2 }, delayMs: 250 },
      { data: { n: 3 } },
    ])

    expect(sends).toHaveLength(1)
    expect(sends[0].messages).toEqual([
      { data: { n: 1 }, key: 'a', headers: {}, idempotencyKey: 'k-1', idempotencyTtlMs: 30_000 },
      { data: { n: 2 }, headers: {}, delayMs: 250 },
      { data: { n: 3 }, headers: {} },
    ])
  })

  it('an empty batch is a no-op: nothing is sent', async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues })
    await q.produce('jobs', [])
    await expect(q.produce([])).resolves.toBeUndefined()
    expect(sends).toEqual([])
  })

  it('an array-typed payload is one message, not a batch', async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues })
    await q.produce('nums', [1, 2, 3])
    expect(sends[0].messages.map((m) => m.data)).toEqual([[1, 2, 3]])
  })

  it('an array of { data } items is a batch unless per-message options force the single form', async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues })

    // The one ambiguous shape: read as a batch, its items unwrapped...
    await q.produce('wrapped', [{ data: [{ data: 1 }] }])
    // ...while a per-message option can only belong to the single form.
    await q.produce('wrapped', [{ data: 2 }], { key: 'k' })
    // Per-call options (ctx/signal) do NOT force the single form.
    await q.produce('jobs', [{ data: { n: 1 } }, { data: { n: 2 } }], {
      signal: new AbortController().signal,
    })

    expect(sends.map((s) => s.messages)).toEqual([
      [{ data: [{ data: 1 }], headers: {} }],
      [{ data: [{ data: 2 }], key: 'k', headers: {} }],
      [
        { data: { n: 1 }, headers: {} },
        { data: { n: 2 }, headers: {} },
      ],
    ])
  })

  it('encodes each payload with the codec; a config codec overrides the adapter one', async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues, codec: jsonText() })
    await q.produce('jobs', [{ data: { n: 1 } }, { data: { n: 2 } }])
    expect(sends[0].messages.map((m) => m.data)).toEqual(['{"n":1}', '{"n":2}'])
  })
})

describe('produce: message instances', () => {
  it('resolves the queue from the class binding, with derived key, merged headers and type stamp', async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues })

    await q.produce(new OrderPlaced({ id: 'o-1' }))
    await q.produce(
      new OrderPlaced({ id: 'o-2' }, { key: 'custom', headers: { source: 'import' } })
    )

    expect(sends.map((s) => [s.queue, s.messages])).toEqual([
      [
        'orders',
        [
          {
            data: { id: 'o-1' },
            key: 'o-1',
            headers: { source: 'checkout', [MESSAGE_TYPE_HEADER]: 'order.placed' },
          },
        ],
      ],
      [
        'orders',
        [
          {
            data: { id: 'o-2' },
            key: 'custom',
            headers: { source: 'import', [MESSAGE_TYPE_HEADER]: 'order.placed' },
          },
        ],
      ],
    ])
  })

  it('omits the type stamp for a class without a type, and never overwrites an explicit one', async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues })

    await q.produce(new Shipment({ id: 's-1' }))
    await q.produce(
      new OrderPaid({ id: 'o-1' }, { headers: { [MESSAGE_TYPE_HEADER]: 'explicit' } })
    )

    expect(sends[0].messages[0].headers).toEqual({})
    expect(sends[1].messages[0].headers).toEqual({ [MESSAGE_TYPE_HEADER]: 'explicit' })
  })

  it('a batch of instances (mixed classes, one queue) is one send in order', async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues })

    await q.produce([new OrderPlaced({ id: 'a' }), new OrderPaid({ id: 'a' }, { delayMs: 10 })])

    expect(sends).toHaveLength(1)
    expect(sends[0].queue).toBe('orders')
    expect(
      sends[0].messages.map((m) => [m.headers?.[MESSAGE_TYPE_HEADER], m.key, m.delayMs])
    ).toEqual([
      ['order.placed', 'a', undefined],
      ['order.paid', undefined, 10],
    ])
  })

  it('rejects a batch resolving to two queues instead of silently splitting the send', async () => {
    const entered = vi.fn()
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, {
      queues,
      middleware: [
        {
          produce: (next) => async (call) => {
            entered()
            await next(call)
          },
        },
      ],
    })

    await expect(
      q.produce([new OrderPlaced({ id: 'a' }), new Shipment({ id: 'a' })])
    ).rejects.toThrow('queue: mixed-queue batch — "shipments" alongside "orders"')
    expect(sends).toEqual([])
    expect(entered).not.toHaveBeenCalled() // raised before the chain
  })

  it('rejects an instance of an unbound class, and anything that is not an instance', async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues })

    // @ts-expect-error — Unbound is not bound to any queue in this registry
    await expect(q.produce(new Unbound({ nope: true }))).rejects.toThrow(
      /message class "Unbound" is not bound to any queue/
    )
    await expect(q.produce({ data: { n: 1 } } as never)).rejects.toThrow(TypeError)
    await expect(
      q.produce([new OrderPlaced({ id: 'a' }), { data: { id: 'b' } }] as never)
    ).rejects.toThrow(/every element of a message batch must be a defineMessage instance/)
    expect(sends).toEqual([])
  })
})

describe('produce: headers and the produce event', () => {
  it('fires the produce event per message with mutable headers: the injection lands on the wire', async () => {
    const seen: unknown[] = []
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, {
      queues,
      events: {
        produce: (ctx) => {
          seen.push({ queue: ctx.queue, key: ctx.key, hadKey: 'key' in ctx })
          ctx.headers.traceparent = `00-abc-${ctx.queue}`
        },
      },
    })

    await q.produce('jobs', { n: 1 }, { headers: { 'x-req': 'r1' } })
    await q.produce('jobs', [{ data: { n: 2 }, key: 'a' }, { data: { n: 3 } }])
    await q.produce(new OrderPlaced({ id: 'o-1' }))

    expect(seen).toEqual([
      { queue: 'jobs', key: undefined, hadKey: false },
      { queue: 'jobs', key: 'a', hadKey: true },
      { queue: 'jobs', key: undefined, hadKey: false },
      { queue: 'orders', key: 'o-1', hadKey: true },
    ])
    expect(sends.flatMap((s) => s.messages.map((m) => m.headers))).toEqual([
      { 'x-req': 'r1', traceparent: '00-abc-jobs' },
      { traceparent: '00-abc-jobs' },
      { traceparent: '00-abc-jobs' },
      { source: 'checkout', [MESSAGE_TYPE_HEADER]: 'order.placed', traceparent: '00-abc-orders' },
    ])
  })

  it("copies headers: neither the hook nor a middleware can mutate the caller's objects", async () => {
    const tamper: QueueMiddleware = {
      produce: (next) => async (call) => {
        for (const m of call.messages) (m.headers as Record<string, string>).mw = 'yes'
        await next(call)
      },
    }
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, {
      queues,
      events: {
        produce: ({ headers }) => {
          headers.hook = 'yes'
        },
      },
      middleware: [tamper],
    })
    const single = Object.freeze({ mine: '1' })
    const batched = Object.freeze({ mine: '2' })
    const instance = new OrderPlaced({ id: 'o-1' }, { headers: { mine: '3' } })
    const instanceHeaders = { ...instance.headers }

    await q.produce('jobs', { n: 1 }, { headers: single })
    await q.produce('jobs', [{ data: { n: 2 }, headers: batched }])
    await q.produce(instance)

    expect(single).toEqual({ mine: '1' })
    expect(batched).toEqual({ mine: '2' })
    expect(instance.headers).toEqual(instanceHeaders)
    expect(sends.map((s) => s.messages[0].headers)).toEqual([
      { mine: '1', hook: 'yes', mw: 'yes' },
      { mine: '2', hook: 'yes', mw: 'yes' },
      { ...instanceHeaders, [MESSAGE_TYPE_HEADER]: 'order.placed', hook: 'yes', mw: 'yes' },
    ])
  })
})

describe('produce: the send context', () => {
  interface Tx {
    readonly transaction: { readonly id: string }
  }

  it("threads the caller's ctx verbatim, on every form, typed by the adapter", async () => {
    const { adapter, sends } = recordingAdapter<Tx>()
    const q = createProducerQueue(adapter, { queues })
    const tx: Tx = { transaction: { id: 'tx-1' } }

    await q.produce('jobs', { n: 1 }, { ctx: tx, key: 'k' })
    await q.produce('jobs', [{ data: { n: 2 } }, { data: { n: 3 } }], { ctx: tx })
    await q.produce(new OrderPlaced({ id: 'o-1' }), { ctx: tx })
    await q.produce([new OrderPaid({ id: 'o-2' })], { ctx: tx })

    expect(sends).toHaveLength(4)
    for (const send of sends) expect(send.context.ctx).toBe(tx)
    expect(sends[0].messages).toEqual([{ data: { n: 1 }, key: 'k', headers: {} }])
    expect(sends[1].messages).toHaveLength(2) // the ctx rides the whole batch

    const never = (): boolean => false
    if (never()) {
      // @ts-expect-error — not the context shape this adapter declares
      void q.produce('jobs', { n: 1 }, { ctx: { transaction: 'tx-1' } })
      // @ts-expect-error — wrong payload type inside a batch
      void q.produce('jobs', [{ data: { n: 'one' } }])
      // @ts-expect-error — per-message options do not belong on the batch form's options
      void q.produce('jobs', [{ data: { n: 1 } }], { key: 'k' })
    }
  })

  it('always hands the adapter the declared registry, and no ctx key when none was passed', async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues })

    await q.produce('jobs', { n: 1 })

    expect(sends[0].context.queues).toBe(queues)
    expect('ctx' in sends[0].context).toBe(false)
    expect(sends[0].signal).toBeUndefined() // no bound: nothing to forward
  })
})

describe('produce: delayed delivery is a declared capability', () => {
  it('an adapter without it rejects any delayed message before sending; the rest is unaffected', async () => {
    const { delayedDelivery: _omitted, ...overrides } = recordingAdapter().adapter
    void _omitted
    const sends: string[] = []
    const q = createProducerQueue(
      { ...overrides, send: async (name) => void sends.push(name) },
      { queues }
    )

    const single = await rejection(q.produce('jobs', { n: 1 }, { delayMs: 1_000 }))
    expect(single).toBeInstanceOf(NotSupportedError)
    expect((single as NotSupportedError).capability).toBe('delay')
    expect((single as Error).message).toMatch(/does not support delayed delivery/)
    await expect(
      q.produce('jobs', [{ data: { n: 1 } }, { data: { n: 2 }, delayMs: 0 }])
    ).rejects.toThrow(NotSupportedError)
    await expect(q.produce(new OrderPaid({ id: 'o' }, { delayMs: 5 }))).rejects.toThrow(
      NotSupportedError
    )
    expect(sends).toEqual([])

    await q.produce('jobs', { n: 3 }, { idempotencyKey: 'k' })
    expect(sends).toEqual(['jobs'])
  })

  it('a middleware catching the refusal sees it like any send failure', async () => {
    const { delayedDelivery: _omitted, ...bare } = recordingAdapter().adapter
    void _omitted
    let caught: unknown
    const q = createProducerQueue(bare, {
      queues,
      middleware: [
        {
          produce: (next) => async (call) => {
            await next(call).catch((error: unknown) => {
              caught = error
            })
          },
        },
      ],
    })
    await q.produce('jobs', { n: 1 }, { delayMs: 10 })
    expect(caught).toBeInstanceOf(NotSupportedError)
  })
})

describe('bounded produce', () => {
  it('a per-call signal bounds a hung send, rejecting with the signal reason', async () => {
    const { adapter } = recordingAdapter({ send: hangingSend })
    const q = createProducerQueue(adapter, { queues })

    const err = await rejection(q.produce('jobs', { n: 1 }, { signal: AbortSignal.timeout(20) }))

    expect(err).toBeInstanceOf(DOMException)
    expect((err as DOMException).name).toBe('TimeoutError')
  })

  it('produceTimeout bounds every form without touching call sites', async () => {
    const { adapter } = recordingAdapter({ send: hangingSend })
    const q = createProducerQueue(adapter, { queues, produceTimeout: 20 })

    const errors = await Promise.all([
      rejection(q.produce('jobs', { n: 1 })),
      rejection(q.produce('jobs', [{ data: { n: 1 } }])),
      rejection(q.produce(new OrderPaid({ id: 'o' }))),
    ])

    expect(errors.map((e) => (e as DOMException).name)).toEqual([
      'TimeoutError',
      'TimeoutError',
      'TimeoutError',
    ])
  })

  it("a caller's abort rejects with the caller's exact reason, ahead of a later produceTimeout", async () => {
    const { adapter } = recordingAdapter({ send: hangingSend })
    const q = createProducerQueue(adapter, { queues, produceTimeout: 5_000 })
    const reason = new Error('request closed')
    const ac = new AbortController()

    const pending = rejection(q.produce('jobs', { n: 1 }, { signal: ac.signal }))
    await sleep(5)
    ac.abort(reason)

    expect(await pending).toBe(reason)
  })

  it('a produce that completes in time is untouched by its bounds', async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues, produceTimeout: 5_000 })
    await q.produce('jobs', { n: 1 }, { signal: AbortSignal.timeout(5_000) })
    expect(sends).toHaveLength(1)
  })

  it('a send failure inside the bound rejects with the adapter error exactly as thrown', async () => {
    const boom = new Error('connection refused')
    const { adapter } = recordingAdapter({
      send: async () => {
        throw boom
      },
    })
    const q = createProducerQueue(adapter, { queues, produceTimeout: 5_000 })
    expect(await rejection(q.produce('jobs', { n: 1 }))).toBe(boom)
  })

  it('a signal dead before the call short-circuits: the send is never issued', async () => {
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues, produceTimeout: 5_000 })
    const reason = new Error('already gone')

    const err = await rejection(q.produce('jobs', { n: 1 }, { signal: AbortSignal.abort(reason) }))

    expect(err).toBe(reason)
    expect(sends).toEqual([]) // the one provably-not-sent abort
  })

  it('forwards the bound to the adapter: the caller signal alone, or composed with the timeout', async () => {
    const { adapter, sends } = recordingAdapter()
    const plain = createProducerQueue(adapter, { queues })
    const bounded = createProducerQueue(adapter, { queues, produceTimeout: 5_000 })
    const ac = new AbortController()

    await plain.produce('jobs', { n: 1 }, { signal: ac.signal })
    await bounded.produce('jobs', { n: 2 })
    await bounded.produce('jobs', { n: 3 }, { signal: ac.signal })

    const [own, timer, composed] = sends.map((s) => s.signal)
    expect(own).toBe(ac.signal)
    expect(timer).toBeInstanceOf(AbortSignal)
    expect(composed).toBeInstanceOf(AbortSignal)
    expect(composed).not.toBe(ac.signal)
    expect(composed?.aborted).toBe(false)
    ac.abort() // the caller's abort reaches the adapter through the composition
    expect(composed?.aborted).toBe(true)
    expect(timer?.aborted).toBe(false)
  })

  it("an abandoned send's late rejection surfaces nowhere", async () => {
    // The send outlives the bound, then rejects with nobody waiting. A dropped rejection
    // would fail this file as an unhandled rejection.
    const late = vi.fn()
    const { adapter } = recordingAdapter({
      send: async () => {
        await sleep(30)
        late()
        throw new Error('late failure of an abandoned send')
      },
    })
    const q = createProducerQueue(adapter, { queues })

    await expect(
      q.produce('jobs', { n: 1 }, { signal: AbortSignal.timeout(5) })
    ).rejects.toMatchObject({ name: 'TimeoutError' })
    expect(late).not.toHaveBeenCalled()

    await sleep(60)
    expect(late).toHaveBeenCalledTimes(1)
  })

  it('a retrying middleware gets a fresh produceTimeout per attempt, not the spent one', async () => {
    const signals: (AbortSignal | undefined)[] = []
    const failures: unknown[] = []
    const { adapter } = recordingAdapter({
      send: (_queue, _messages, _context, signal) => {
        signals.push(signal)
        return signals.length === 1 ? hangingSend() : Promise.resolve()
      },
    })
    const retryOnce: QueueMiddleware = {
      produce: (next) => async (call) => {
        // The timeout is NOT composed into the call's own signal.
        expect(call.signal).toBeUndefined()
        try {
          await next(call)
        } catch (error) {
          failures.push(error)
          await next(call)
        }
      },
    }
    const q = createProducerQueue(adapter, { queues, produceTimeout: 20, middleware: [retryOnce] })

    await q.produce('jobs', { n: 1 })

    expect(failures.map((e) => (e as DOMException).name)).toEqual(['TimeoutError'])
    expect(signals).toHaveLength(2)
    expect(signals[0]).not.toBe(signals[1])
    expect(signals[0]?.aborted).toBe(true)
    expect(signals[1]?.aborted).toBe(false) // a full bound for the second attempt
  })
})
