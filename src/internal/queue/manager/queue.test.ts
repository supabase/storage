import { memory } from '../adapters/memory'
import {
  type AnyProducerQueue,
  type AnyQueue,
  type BatchContext,
  BatchFailure,
  createProducerQueue,
  createQueue,
  defineMessage,
  defineQueues,
  type JobContext,
  NotSupportedError,
  type ProducerQueue,
  type Queue,
  type QueueClasses,
  QueueHandler,
  type QueueMapOf,
  queue,
} from './index'

interface Order {
  id: string
}

class OrderPlaced extends defineMessage<Order>({ key: (o) => o.id }) {
  readonly type = 'order.placed'
}
class OrderPaid extends defineMessage<Order>() {
  readonly type = 'order.paid'
}
class Unbound extends defineMessage<{ x: number }>() {
  readonly type = 'unbound'
}

const queues = defineQueues({
  orders: queue([OrderPlaced, OrderPaid]),
  jobs: queue<{ n: number }>({ retry: { maxAttempts: 3, deadLetter: 'jobs-dead' } }),
  'jobs-dead': queue<{ n: number }>(),
})

const fast = { pollIdleIntervalMs: 5, pollBatchIntervalMs: 0 }
const until = async (pred: () => boolean, ms = 2_000): Promise<void> => {
  const deadline = Date.now() + ms
  while (!pred()) {
    if (Date.now() > deadline) throw new Error('until: timed out')
    await new Promise((r) => setTimeout(r, 5))
  }
}

describe('createQueue over the memory adapter', () => {
  it('produces to a worker by queue name and by message class', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await q.start()
    const seen: unknown[] = []
    q.subscribe('jobs', async ({ queue: name, message }) => {
      seen.push([name, message.data.n, message.attempt])
    })
    class OnOrder extends QueueHandler(OrderPlaced, OrderPaid) {
      async handle(ctx: JobContext<Order>): Promise<void> {
        seen.push([ctx.queue, ctx.message.type, ctx.message.key, ctx.message.data.id])
      }
    }
    q.subscribe(new OnOrder())

    await q.produce('jobs', { n: 1 })
    await q.produce('jobs', [{ data: { n: 2 } }, { data: { n: 3 } }])
    await q.produce(new OrderPlaced({ id: 'o1' }))
    await q.produce([new OrderPaid({ id: 'o2' })])
    await until(() => seen.length === 5)
    expect(seen).toEqual(
      expect.arrayContaining([
        ['jobs', 1, 1],
        ['jobs', 2, 1],
        ['jobs', 3, 1],
        ['orders', 'order.placed', 'o1', 'o1'],
        ['orders', 'order.paid', undefined, 'o2'],
      ])
    )
    await q.close()
    await q.close() // idempotent
  })

  it('config handlers start with start(); a failure retries under the policy and dead-letters', async () => {
    const attempts: number[] = []
    const dead: number[] = []
    const q = createQueue(memory(), {
      queues,
      ...fast,
      handlers: [
        {
          queue: 'jobs',
          handle: (ctx: JobContext<{ n: number }>) => {
            attempts.push(ctx.message.attempt)
            throw new Error('boom')
          },
        },
        {
          queue: 'jobs-dead',
          handleBatch: (ctx: BatchContext<{ n: number }>) => {
            dead.push(...ctx.messages.map((m) => m.data.n))
          },
        },
      ],
    })
    await q.start()
    await q.start() // idempotent: handlers attach once
    await q.produce('jobs', { n: 7 })
    await until(() => dead.length === 1)
    expect(attempts).toEqual([1, 2, 3])
    expect(dead).toEqual([7])
    const stats = await q.stats({ queues: ['jobs', 'jobs-dead'] })
    expect(stats.queues.map((s) => [s.queue, s.backlog?.count, s.inFlight?.count])).toEqual([
      // The dead-letter target was provisioned before the queue that names it.
      ['jobs-dead', 0, 0],
      ['jobs', 0, 0],
    ])
    await q.close()
  })

  it('a batch worker acks the batch minus a BatchFailure subset', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await q.start()
    await q.produce('jobs', [{ data: { n: 1 } }, { data: { n: 2 } }, { data: { n: 3 } }])
    const calls: number[][] = []
    let failed = false
    q.subscribe({ queue: 'jobs', batch: true, prefetch: 10 }, async ({ messages }) => {
      calls.push(messages.map((m) => m.data.n))
      if (failed) return
      failed = true
      throw new BatchFailure(messages.filter((m) => m.data.n === 2))
    })
    await until(() => calls.length === 2)
    expect(calls).toEqual([[1, 2, 3], [2]])
    await q.close()
  })

  it('rejects a delay the adapter does not declare, and stats it does not expose', async () => {
    const { delayedDelivery: _d, stats: _s, ...bare } = memory()
    const q = createProducerQueue(bare, { queues })
    await q.start()
    await expect(q.produce('jobs', { n: 1 }, { delayMs: 10 })).rejects.toThrow(NotSupportedError)
    await expect(q.stats()).rejects.toThrow(NotSupportedError)
    await q.close()
  })
})

describe('type-level guarantees', () => {
  it('holds concrete managers as erased ones without a cast, and restores them by downcast', () => {
    const full = createQueue(memory(), { queues })
    const producer = createProducerQueue(memory(), { queues })
    const anyQueue: AnyQueue = full
    const anyProducer: AnyProducerQueue = full
    const anyProducer2: AnyProducerQueue = producer
    const asProducer: ProducerQueue<typeof queues> = full
    const restored = anyQueue as Queue<typeof queues>
    expect([anyProducer, anyProducer2, asProducer, restored]).toHaveLength(4)
    expect(full.getQueue('jobs')).toBe(queues.get('jobs'))
    expect(full.getQueues().names()).toEqual(['orders', 'jobs', 'jobs-dead'])
  })

  it('refuses unbound classes, undeclared queues and mistyped payloads at compile time', () => {
    const q = createQueue(memory(), { queues })
    const never = (): boolean => false
    if (never()) {
      // @ts-expect-error — the class is not bound in the registry
      void q.produce(new Unbound({ x: 1 }))
      // @ts-expect-error — the class is not bound in the registry
      void q.invoke(new Unbound({ x: 1 }))
      // @ts-expect-error — undeclared queue
      void q.produce('nope', { n: 1 })
      // @ts-expect-error — wrong payload type
      void q.produce('jobs', { n: 'one' })
      // @ts-expect-error — the adapter declares no produce context
      void q.produce('jobs', { n: 1 }, { ctx: { tx: 1 } })
      class OnUnbound extends QueueHandler(Unbound) {
        async handle(): Promise<void> {}
      }
      // @ts-expect-error — the handler's class is not bound in the registry
      q.subscribe(new OnUnbound())
      // @ts-expect-error — a producer queue has no subscribe
      createProducerQueue(memory(), { queues }).subscribe('jobs', async () => {})
    }
    // A `static send(this: Bound…)` typed off QueueClasses compiles only for bound classes.
    type Bound = QueueClasses<QueueMapOf<typeof queues>>
    const send = <C extends Bound>(cls: C): C => cls
    send(OrderPlaced)
    // @ts-expect-error — not a bound class
    send(Unbound)
  })
})
