import { memory } from '../adapters/memory'
import {
  type BatchContext,
  createProducerQueue,
  createQueue,
  createSyncQueue,
  defineMessage,
  defineQueues,
  type JobContext,
  QueueBatchHandler,
  QueueHandler,
  type QueueOptions,
  QueueRegistry,
  queue,
  type SubscribeOptions,
} from './index'
import { recordingAdapter } from './test-support'

interface Order {
  readonly orderId: string
  readonly total: number
}
interface Payment {
  readonly orderId: string
  readonly amount: number
}
interface Shipment {
  readonly orderId: string
}

// Message classes carry NO queue name: the registry binds them below.
class OrderPlaced extends defineMessage<Order>({
  key: (o) => o.orderId,
  headers: { source: 'checkout' },
}) {
  readonly type = 'order.placed'
}
class OrderPaid extends defineMessage<Payment>({ key: (p) => p.orderId }) {
  readonly type = 'order.paid'
}
class ShipmentRequested extends defineMessage<Shipment>() {}
class Elsewhere extends defineMessage<Order>() {
  readonly type = 'order.elsewhere'
}

const orderOptions = { retention: 2 } as QueueOptions
const queues = defineQueues({
  orders: queue([OrderPlaced, OrderPaid], orderOptions), // polymorphic: one queue, two classes
  shipments: queue(ShipmentRequested),
  'orders-dead': queue<Order>(),
})

describe('defineMessage', () => {
  it('a class without defaults carries only its payload', () => {
    const m = new ShipmentRequested({ orderId: 'o-1' })
    expect({ ...m }).toEqual({ data: { orderId: 'o-1' } })
    expect(m.type).toBeUndefined()
  })

  it('derives key and idempotencyKey from the payload and applies class-level defaults', () => {
    class Logged extends defineMessage<Order>({
      key: (o) => o.orderId,
      headers: { source: 'log' },
      idempotencyKey: (o) => `log-${o.orderId}`,
      idempotencyTtlMs: 60_000,
    }) {
      readonly type = 'logged'
    }
    expect({ ...new Logged({ orderId: 'a', total: 1 }) }).toEqual({
      type: 'logged',
      data: { orderId: 'a', total: 1 },
      key: 'a',
      headers: { source: 'log' },
      idempotencyKey: 'log-a',
      idempotencyTtlMs: 60_000,
    })
  })

  it('per-instance init wins over every derivation and default; headers merge', () => {
    class Logged extends defineMessage<Order>({
      key: (o) => o.orderId,
      headers: { source: 'log', kept: 'yes' },
      idempotencyKey: (o) => `log-${o.orderId}`,
      idempotencyTtlMs: 60_000,
    }) {}
    const m = new Logged(
      { orderId: 'a', total: 1 },
      {
        key: 'custom',
        headers: { source: 'import', priority: 'rush' },
        idempotencyKey: 'explicit',
        idempotencyTtlMs: 30_000,
        delayMs: 750,
      }
    )
    expect({ ...m }).toEqual({
      data: { orderId: 'a', total: 1 },
      key: 'custom',
      headers: { source: 'import', kept: 'yes', priority: 'rush' },
      idempotencyKey: 'explicit',
      idempotencyTtlMs: 30_000,
      delayMs: 750,
    })
    // Instance headers alone still produce a headers object.
    expect(new ShipmentRequested({ orderId: 'o' }, { headers: { a: '1' } }).headers).toEqual({
      a: '1',
    })
  })

  it('class defaults reach the adapter through a message-instance produce', async () => {
    class Logged extends defineMessage<Order>({
      idempotencyKey: (o) => `log-${o.orderId}`,
      idempotencyTtlMs: 60_000,
    }) {
      readonly type = 'logged'
    }
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues: defineQueues({ log: queue(Logged) }) })

    await q.produce(new Logged({ orderId: 'z', total: 1 }, { delayMs: 100 }))

    expect(sends[0].messages[0]).toMatchObject({
      idempotencyKey: 'log-z',
      idempotencyTtlMs: 60_000,
      delayMs: 100,
    })
  })
})

describe('the queue registry', () => {
  it('binds classes and provisioning options to each declared queue', () => {
    expect(queues).toBeInstanceOf(QueueRegistry)
    expect(queues.names()).toEqual(['orders', 'shipments', 'orders-dead'])
    expect(queues.get('orders').options).toBe(orderOptions)
    expect(queues.get('orders').classes).toEqual([OrderPlaced, OrderPaid])
    expect(queues.get('shipments').classes).toEqual([ShipmentRequested])
    expect(queues.get('shipments').options).toBeUndefined()
    expect(queues.get('orders-dead')).toEqual({})
    expect(queue<Order>(orderOptions)).toEqual({ options: orderOptions })
    expect(queues.get('undeclared' as string)).toBeUndefined()
    expect(queues.toMap().orders).toBe(queues.get('orders'))
    expect([...queues.classIndex]).toEqual([
      [OrderPlaced, 'orders'],
      [OrderPaid, 'orders'],
      [ShipmentRequested, 'shipments'],
    ])
  })

  it('throws when one class is bound to two queues, before any manager exists', () => {
    expect(() =>
      defineQueues({ a: queue(OrderPlaced), b: queue([OrderPaid, OrderPlaced]) })
    ).toThrow('queue: message class "OrderPlaced" is bound to both "a" and "b"')
  })

  it('a subclass of a bound class resolves through the prototype chain', async () => {
    class RushOrderPlaced extends OrderPlaced {}
    const { adapter, sends } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues })

    await q.produce(new RushOrderPlaced({ orderId: 'o-1', total: 1 }))

    expect(sends[0].queue).toBe('orders')
    expect(sends[0].messages[0].key).toBe('o-1') // the base class's derivation still applies

    // The same resolution serves a handler built over the subclass.
    const seen: string[] = []
    class OnRush extends QueueHandler(RushOrderPlaced) {
      async handle(ctx: JobContext<Order>): Promise<void> {
        seen.push(`${ctx.queue}:${ctx.message.data.orderId}`)
      }
    }
    const sync = createSyncQueue({ queues })
    sync.subscribe(new OnRush())
    await sync.produce(new RushOrderPlaced({ orderId: 'o-2', total: 1 }))
    expect(seen).toEqual(['orders:o-2'])
  })
})

describe('handler classes', () => {
  it("resolves a handler's queue via the registry, with `this` bound across deliveries", async () => {
    class FulfillOrder extends QueueHandler(OrderPlaced) {
      readonly seen: string[] = []
      async handle(ctx: JobContext<Order>): Promise<void> {
        // `this` must be the instance: the attach seam may not detach the method.
        this.seen.push(`${ctx.queue}:${ctx.message.data.orderId}`)
      }
    }
    const q = createSyncQueue({ queues })
    const handler = new FulfillOrder()
    q.subscribe(handler)

    await q.produce(new OrderPlaced({ orderId: 'o-1', total: 10 }))
    await q.produce(new OrderPlaced({ orderId: 'o-2', total: 10 }))

    expect(handler.seen).toEqual(['orders:o-1', 'orders:o-2'])
  })

  it("applies the instance's own options; per-attachment overrides win", async () => {
    class Slow extends QueueHandler(ShipmentRequested) {
      override readonly options: SubscribeOptions = { consumeTimeout: 20 }
      handle(): Promise<void> {
        return new Promise(() => {})
      }
    }
    const own = createSyncQueue({ queues })
    own.subscribe(new Slow())
    await expect(own.invoke(new ShipmentRequested({ orderId: 'o' }))).rejects.toThrow(
      /timed out after 20ms/
    )

    const overridden = createSyncQueue({ queues })
    overridden.subscribe(new Slow(), { consumeTimeout: 30 })
    await expect(overridden.invoke(new ShipmentRequested({ orderId: 'o' }))).rejects.toThrow(
      /timed out after 30ms/
    )
  })

  it('handles a polymorphic queue with a multi-class handler, narrowed by message type', async () => {
    class OrderProjector extends QueueHandler(OrderPlaced, OrderPaid) {
      readonly seen: string[] = []
      async handle(ctx: JobContext<Order | Payment>): Promise<void> {
        this.seen.push(`${ctx.message.type}:${ctx.message.data.orderId}`)
      }
    }
    const q = createSyncQueue({ queues })
    const projector = new OrderProjector()
    q.subscribe(projector)

    await q.produce([
      new OrderPlaced({ orderId: 'o-1', total: 10 }),
      new OrderPaid({ orderId: 'o-1', amount: 10 }),
    ])

    expect(projector.seen).toEqual(['order.placed:o-1', 'order.paid:o-1'])
  })

  it('a QueueBatchHandler subclass receives a produced batch in one call', async () => {
    class OrderBatch extends QueueBatchHandler(OrderPlaced, OrderPaid) {
      readonly calls: (string | undefined)[][] = []
      async handleBatch(ctx: BatchContext<Order | Payment>): Promise<void> {
        this.calls.push(ctx.messages.map((m) => m.type))
      }
    }
    const q = createSyncQueue({ queues })
    const handler = new OrderBatch()
    q.subscribe(handler)

    await q.produce([
      new OrderPlaced({ orderId: 'o-1', total: 10 }),
      new OrderPaid({ orderId: 'o-1', amount: 10 }),
    ])

    expect(handler.calls).toEqual([['order.placed', 'order.paid']])
  })

  it('accepts plain { queue, handle } and { messages, handle } objects with an inferred ctx', async () => {
    const seen: unknown[] = []
    const q = createSyncQueue({ queues })
    q.subscribe({
      queue: 'orders-dead',
      options: { prefetch: 1 },
      handle: async (ctx) => {
        seen.push(ctx.message.data.total) // ctx inferred as JobContext<Order>
      },
    })
    q.subscribe({
      messages: [ShipmentRequested],
      handle: async (ctx) => {
        seen.push(ctx.message.data.orderId) // ctx inferred as JobContext<Shipment>
      },
    })

    await q.produce('orders-dead', { orderId: 'o-9', total: 3 })
    await q.produce(new ShipmentRequested({ orderId: 'o-7' }))

    expect(seen).toEqual([3, 'o-7'])
  })
})

describe('handler-shape guards', () => {
  // Every guard throws synchronously from subscribe, before any worker exists.
  const q = createQueue(memory(), { queues })

  it('refuses a handler object carrying both handle and handleBatch', () => {
    expect(() =>
      q.subscribe({ queue: 'orders-dead', handle: () => {}, handleBatch: () => {} } as never)
    ).toThrow(/carries handle OR handleBatch, never both/)
  })

  it("refuses batch: true on a handler object, in its options or the attachment's overrides", () => {
    const single = { queue: 'orders-dead' as const, handle: (_ctx: JobContext<Order>) => {} }
    expect(() => q.subscribe({ ...single, options: { batch: true } as SubscribeOptions })).toThrow(
      /batch: true is the INLINE form's opt-in/
    )
    expect(() => q.subscribe(single, { batch: true } as SubscribeOptions)).toThrow(
      /batch: true is the INLINE form's opt-in/
    )
    expect(() =>
      q.subscribe({ queue: 'orders-dead', handleBatch: (_ctx: BatchContext<Order>) => {} }, {
        batch: true,
      } as SubscribeOptions)
    ).toThrow(/batch: true is the INLINE form's opt-in/)
  })

  it('refuses a handler whose classes span queues', () => {
    class Spanning extends QueueHandler(OrderPlaced, ShipmentRequested) {
      async handle(): Promise<void> {}
    }
    expect(() => q.subscribe(new Spanning())).toThrow(
      'queue: handler spans queues "orders" and "shipments"'
    )
  })

  it('refuses a handler for a class the registry does not bind', () => {
    class NopeHandler extends QueueHandler(Elsewhere) {
      async handle(): Promise<void> {}
    }
    // @ts-expect-error — Elsewhere is not bound to any queue in this registry
    expect(() => q.subscribe(new NopeHandler())).toThrow(
      /message class "Elsewhere" is not bound to any queue/
    )
    class Mixed extends QueueBatchHandler(OrderPlaced, Elsewhere) {
      async handleBatch(): Promise<void> {}
    }
    // @ts-expect-error — one unbound class is enough to refuse the handler
    expect(() => q.subscribe(new Mixed())).toThrow(/"Elsewhere" is not bound to any queue/)
  })

  it('refuses a message handler listing no classes', () => {
    class Empty extends QueueHandler() {
      async handle(): Promise<void> {}
    }
    expect(() => q.subscribe(new Empty())).toThrow(
      'queue: a message handler must list at least one message class'
    )
  })

  it('refuses the option combinations a worker would refuse', () => {
    expect(() =>
      q.subscribe({
        queue: 'orders-dead',
        options: { flow: 'streaming' },
        handleBatch: (_ctx: BatchContext<Order>) => {},
      })
    ).toThrow(/flow "streaming" is not applicable to a batch handler/)
    expect(() =>
      q.subscribe({
        queue: 'orders-dead',
        options: { heartbeatIntervalMs: 1_000 },
        handle: (_ctx: JobContext<Order>) => {},
      })
    ).toThrow(/heartbeatIntervalMs requires consumeTimeout/)
  })

  it('requires handle on a QueueHandler subclass and a matching payload, at compile time', () => {
    // @ts-expect-error — the abstract handle must be implemented
    class Forgot extends QueueHandler(OrderPlaced) {}
    class WrongPayload extends QueueHandler(OrderPlaced) {
      // @ts-expect-error — the ctx payload must accept the bound class's payload
      async handle(_ctx: JobContext<Shipment & { extra: true }>): Promise<void> {}
    }
    const never = (): boolean => false
    if (never()) {
      // @ts-expect-error — the handler's payload does not match the queue's
      q.subscribe({ queue: 'orders-dead', handle: (_ctx: JobContext<Shipment & { x: 1 }>) => {} })
      // @ts-expect-error — undeclared queue on a handler object
      q.subscribe({ queue: 'nope', handle: () => {} })
    }
    expect([Forgot, WrongPayload]).toHaveLength(2)
  })
})
