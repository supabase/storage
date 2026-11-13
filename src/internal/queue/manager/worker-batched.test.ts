import { memory } from '../adapters/memory'
import {
  BatchFailure,
  createQueue,
  defineQueues,
  type Message,
  type Queue,
  type QueueEvents,
  queue,
} from './index'
import {
  defer,
  fast,
  forever,
  patchSubscriptions,
  poisonCodec,
  pulled,
  reissuingBroker,
  scripted,
  sleep,
  until,
  withRetry,
} from './test-helpers'

interface Job {
  n: number
}

const queues = defineQueues({ jobs: queue<Job>(), 'jobs-dead': queue<Job>() })

const produce = (q: Queue<typeof queues>, ...ns: number[]): Promise<void> =>
  q.produce(
    'jobs',
    ns.map((n) => ({ data: { n } }))
  )

interface Handled {
  outcome: string
  size: number
  failed: number
  attempt: number | undefined
}

/** A `handled` sink recording what each invocation reported. */
const recordHandled = (): { handled: Handled[]; events: QueueEvents } => {
  const handled: Handled[] = []
  return {
    handled,
    events: {
      handled: ({ outcome, size, failed, attempt }) =>
        void handled.push({ outcome, size, failed, attempt }),
    },
  }
}

describe('batch handlers', () => {
  it('one pull is one batch; a return acks all of it', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await produce(q, 0, 1, 2, 3, 4)
    const calls: unknown[] = []

    const worker = q.subscribe({ queue: 'jobs', batch: true, prefetch: 10 }, async (ctx) => {
      calls.push([ctx.queue, ctx.messages.map((m) => m.data.n), ctx.signal.aborted])
    })

    await until(() => calls.length === 1)
    await worker.stop()
    expect(calls).toEqual([['jobs', [0, 1, 2, 3, 4], false]])
    const [stats] = (await q.stats({ queues: ['jobs'] })).queues
    expect([stats.backlog?.count, stats.inFlight?.count]).toEqual([0, 0]) // acked whole
    await q.close()
  })

  it('BatchFailure under a retry policy: the rest acks, only the subset is nacked and dies', async () => {
    const dead: unknown[] = []
    const { handled, events } = recordHandled()
    const adapter = memory({ events: { deadLettered: (ctx) => dead.push(ctx) } })
    const q = createQueue(adapter, {
      queues: withRetry({ maxAttempts: 2, deadLetter: 'jobs-dead' }),
      ...fast,
      events,
    })
    await produce(q, 1, 13, 3)
    const delivered: number[] = []

    q.subscribe(
      {
        queue: 'jobs',
        batch: true,
        prefetch: 10,
      },
      async ({ messages }) => {
        delivered.push(...messages.map((m) => m.data.n))
        const failed = messages.filter((m) => m.data.n === 13)
        if (failed.length > 0) throw new BatchFailure(failed, { cause: new Error('poison') })
      }
    )

    await until(() => dead.length === 1)
    await q.close()
    // 1 and 3 handled exactly once (never redelivered by 13's failure); 13 ran to its budget.
    expect(delivered).toEqual([1, 13, 3, 13])
    expect(dead).toEqual([{ queue: 'jobs', deadLetter: 'jobs-dead', attempt: 2 }])
    expect(handled).toEqual([
      { outcome: 'error', size: 3, failed: 1, attempt: undefined },
      { outcome: 'error', size: 1, failed: 1, attempt: 2 },
    ])
  })

  it('BatchFailure without a policy: only the failed subset redelivers, attempt intact', async () => {
    const { handled, events } = recordHandled()
    const q = createQueue(memory(), { queues, ...fast, events })
    await produce(q, 0, 1, 2)
    const calls: number[][] = []

    q.subscribe({ queue: 'jobs', batch: true, prefetch: 10 }, async ({ messages }) => {
      calls.push(messages.map((m) => m.data.n))
      if (calls.length === 1) throw new BatchFailure([messages[1]])
    })

    await until(() => calls.length === 2)
    await q.close()
    expect(calls).toEqual([[0, 1, 2], [1]])
    expect(handled).toEqual([
      { outcome: 'error', size: 3, failed: 1, attempt: undefined },
      { outcome: 'ok', size: 1, failed: 0, attempt: 1 }, // a release consumed no budget
    ])
  })

  it.each([
    ['a plain throw', (_m: Message<unknown>): Error => new Error('all bad')],
    // A copy is NOT the identity from this batch: a handler bug, failed in the safe direction.
    [
      'a BatchFailure naming a foreign message',
      (m: Message<unknown>): Error => new BatchFailure([{ ...m }]),
    ],
  ] as const)('%s fails the whole batch', async (_name, failure) => {
    const q = createQueue(memory(), { queues, ...fast })
    await produce(q, 0, 1)
    const calls: number[][] = []

    q.subscribe({ queue: 'jobs', batch: true, prefetch: 10 }, async ({ messages }) => {
      calls.push(messages.map((m) => m.data.n))
      if (calls.length === 1) throw failure(messages[0])
    })

    await until(() => calls.length === 2)
    await q.close()
    expect(calls).toEqual([
      [0, 1],
      [0, 1], // everything redelivered, nothing silently acked
    ])
  })

  it('consumeTimeout bounds the WHOLE call and fails every message of it', async () => {
    const dead: unknown[] = []
    const { handled, events } = recordHandled()
    const adapter = memory({ events: { deadLettered: (ctx) => dead.push(ctx) } })
    const q = createQueue(adapter, { queues: withRetry({ maxAttempts: 2 }), ...fast, events })
    await produce(q, 0, 1)
    const attempts: number[][] = []
    const reasons: unknown[] = []

    q.subscribe(
      { queue: 'jobs', batch: true, consumeTimeout: 20 },
      async ({ messages, signal }) => {
        attempts.push(messages.map((m) => m.attempt))
        if (attempts.length > 1) return
        signal.addEventListener('abort', () => reasons.push(signal.reason), { once: true })
        await forever() // exceed the bound once
      }
    )

    await until(() => attempts.length === 2)
    await q.close()
    // The timeout counted as a failed attempt for both messages of the call.
    expect(attempts).toEqual([
      [1, 1],
      [2, 2],
    ])
    expect(dead).toEqual([])
    expect((reasons[0] as DOMException).name).toBe('TimeoutError')
    expect(handled.map((h) => [h.outcome, h.size, h.failed])).toEqual([
      ['timeout', 2, 0],
      ['ok', 2, 0],
    ])
  })

  it('parallelism splits one pull into that many concurrent slices', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await produce(q, 0, 1, 2, 3, 4, 5, 6, 7)
    const gate = defer()
    const calls: number[][] = []

    q.subscribe({ queue: 'jobs', batch: true, parallelism: 2, prefetch: 10 }, async (ctx) => {
      calls.push(ctx.messages.map((m) => m.data.n))
      await gate.promise
    })

    await until(() => calls.length === 2) // both slices in flight at once
    gate.resolve()
    await q.close()
    expect(calls).toEqual([
      [0, 1, 2, 3],
      [4, 5, 6, 7],
    ])
  })

  it('a payload the codec rejects fails only that message; its batch-mates still run', async () => {
    const dead: number[] = []
    const adapter = memory({
      codec: poisonCodec(2),
      events: { deadLettered: ({ attempt }) => dead.push(attempt) },
    })
    const q = createQueue(adapter, { queues: withRetry({ maxAttempts: 1 }), ...fast })
    await produce(q, 1, 2, 3)
    const calls: number[][] = []

    q.subscribe({ queue: 'jobs', batch: true, prefetch: 10 }, ({ messages }) => {
      calls.push(messages.map((m) => m.data.n))
    })

    await until(() => dead.length === 1) // failed like a handler failure: budget spent
    await q.close()
    expect(calls).toEqual([[1, 3]])
  })

  it('ctx.heartbeat() beats the whole batch through the subscription', async () => {
    const s = scripted()
    s.feed([pulled(0), pulled(1)])
    const q = createQueue(s.adapter, { queues, ...fast })

    q.subscribe({ queue: 'jobs', batch: true }, async ({ heartbeat }) => {
      await heartbeat()
    })

    await until(() => s.calls.length === 2)
    expect(s.calls).toEqual(['heartbeat r0,r1', 'ack r0,r1'])
    await q.close()
  })
})

describe('batched flow for a single-message handler', () => {
  it('the slowest invocation gates its batch-mates settles and the next pull', async () => {
    const s = scripted()
    s.feed([pulled(0), pulled(1)], [pulled(2), pulled(3)])
    const q = createQueue(s.adapter, { queues, ...fast })
    const slow = defer()
    const done: number[] = []

    q.subscribe(
      { queue: 'jobs', flow: 'batched', prefetch: 2, parallelism: 2 },
      async ({ message }) => {
        if (message.data.n === 0) await slow.promise
        done.push(message.data.n)
      }
    )

    await until(() => done.length === 1)
    await sleep(20)
    expect(done).toEqual([1])
    expect(s.calls).toEqual([]) // n=1 finished, yet nothing settles before the join
    expect(s.pulls).toEqual([2]) // and nothing more is pulled
    slow.resolve()
    await until(() => done.length === 4)
    await q.close()
    expect(s.calls.slice(0, 2)).toEqual(['ack r0,r1', 'ack r2,r3'])
  })

  it('a pool runs up to `parallelism` invocations of one pull concurrently', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await produce(q, 0, 1, 2, 3, 4, 5)
    const gate = defer()
    const done: number[] = []
    let active = 0
    let maxActive = 0

    q.subscribe(
      { queue: 'jobs', flow: 'batched', parallelism: 3, prefetch: 10 },
      async ({ message }) => {
        active++
        maxActive = Math.max(maxActive, active)
        await gate.promise
        active--
        done.push(message.data.n)
      }
    )

    await until(() => active === 3)
    await sleep(10)
    expect(active).toBe(3) // bounded by the pool, not the batch
    gate.resolve()
    await until(() => done.length === 6)
    await q.close()
    expect(maxActive).toBe(3)
  })

  it('settles one batch as acks, then nacks: a throw and a timeout alike', async () => {
    const s = scripted()
    s.feed([pulled(0), pulled(1, 3), pulled(2)])
    const scheduled: unknown[] = []
    const { handled, events } = recordHandled()
    const q = createQueue(s.adapter, {
      queues: withRetry({ maxAttempts: 5, backoffMs: 9 }),
      ...fast,
      events: { ...events, retryScheduled: (ctx) => scheduled.push(ctx) },
    })

    q.subscribe(
      {
        queue: 'jobs',
        flow: 'batched',
        prefetch: 3,
        parallelism: 3,
        consumeTimeout: 20,
      },
      async ({ message }) => {
        if (message.data.n === 1) throw new Error('boom')
        if (message.data.n === 2) await forever() // times out: a failure, like the throw
      }
    )

    await until(() => s.calls.length === 2)
    expect(s.calls).toEqual(['ack r0', 'nack r1@9,r2@9'])
    expect(scheduled).toEqual([
      { queue: 'jobs', attempt: 3, delayMs: 9 },
      { queue: 'jobs', attempt: 1, delayMs: 9 },
    ])
    expect(handled).toEqual([
      { outcome: 'ok', size: 1, failed: 0, attempt: 1 },
      { outcome: 'error', size: 1, failed: 1, attempt: 3 },
      { outcome: 'timeout', size: 1, failed: 0, attempt: 1 },
    ])
    await q.close()
  })

  it('a failed ack releases the whole batch, is reported, and the worker carries on', async () => {
    let outage = true
    const s = scripted((base) => ({
      ack: async (receipts) => {
        await base.ack(receipts)
        if (outage) {
          outage = false
          throw new Error('ack outage')
        }
      },
    }))
    s.feed([pulled(0), pulled(1)])
    const errors: unknown[] = []
    const scheduled: unknown[] = []
    const q = createQueue(s.adapter, {
      queues: withRetry({ maxAttempts: 5 }),
      ...fast,
      events: { retryScheduled: (ctx) => scheduled.push(ctx) },
    })

    q.subscribe(
      {
        queue: 'jobs',
        flow: 'batched',
        onError: (e) => errors.push(e),
      },
      ({ message }) => {
        if (message.data.n === 1) throw new Error('boom')
      }
    )

    await until(() => errors.length === 1)
    // The nack was never issued: its receipt is released with the ack that may have landed.
    expect(s.calls).toEqual(['ack r0', 'release r0,r1'])
    expect(scheduled).toEqual([])
    expect(errors[0]).toMatchObject({ message: 'ack outage' })

    s.feed([pulled(2)])
    await until(() => s.calls.length === 3)
    expect(s.calls[2]).toBe('ack r2')
    await q.close()
  })

  it('stop() starts nothing new: un-started messages of the pulled batch are released', async () => {
    const s = scripted()
    s.feed([pulled(0), pulled(1), pulled(2)])
    const q = createQueue(s.adapter, { queues, ...fast })
    const gate = defer()
    const started: number[] = []

    const worker = q.subscribe({ queue: 'jobs', flow: 'batched', prefetch: 3 }, async (ctx) => {
      started.push(ctx.message.data.n)
      await gate.promise
    })

    await until(() => started.length === 1)
    const stopping = worker.stop()
    gate.resolve()
    await stopping
    expect(started).toEqual([0])
    expect(s.calls).toEqual(['ack r0', 'release r1,r2', 'close'])
    expect(worker.closed).toBe(true)
    await q.close()
  })

  it('a release that threw is never re-issued across competing workers', async () => {
    // Worker A's release lands at the broker (the lease is free) and worker B leases the
    // message under the same receipt, and only THEN does A's release call reject. A's settle
    // join must not release again: that would free B's lease while B's handler still runs.
    const released = defer()
    const secondStarted = defer()
    const proceed = defer()
    const releaseBatches: string[][] = []
    const errors: unknown[] = []
    let delivered = false
    let pullsAfterFailure = 0
    const { adapter: broker, acked } = reissuingBroker()
    const shared = patchSubscriptions(broker, (subscription) => ({
      pull: async (max) => {
        if (delivered) await released.promise // the re-issuing pull follows the landed release
        const out = await subscription.pull(max)
        if (out.length > 0) delivered = true
        if (errors.length > 0) pullsAfterFailure++
        return out
      },
      release: async (receipts) => {
        releaseBatches.push([...receipts])
        await subscription.release(receipts) // lands: the lease is free again
        released.resolve()
        await secondStarted.promise // B is running the re-issued delivery
        throw new Error('release response lost')
      },
    }))
    // Two managers over one broker: two processes consuming the same queue.
    const a = createQueue(shared, { queues, ...fast })
    const b = createQueue(shared, { queues, ...fast })
    let deliveries = 0
    const handler = async (): Promise<void> => {
      deliveries++
      if (deliveries === 1) throw new Error('boom') // no policy: so a release
      secondStarted.resolve()
      await proceed.promise
    }
    const onError = (e: unknown): void => void errors.push(e)
    a.subscribe({ queue: 'jobs', flow: 'batched', onError }, handler)
    b.subscribe({ queue: 'jobs', flow: 'batched', onError }, handler)
    await produce(a, 1)

    await secondStarted.promise
    await until(() => errors.length === 1)
    expect(errors[0]).toMatchObject({ message: 'release response lost' })
    // The failed join is reported and the idle worker keeps polling. A re-issued release would
    // have freed the running delivery's lease and a third delivery would have started.
    await until(() => pullsAfterFailure >= 3)
    expect(deliveries).toBe(2)
    expect(releaseBatches).toEqual([['id-0']])

    proceed.resolve()
    await Promise.all([a.close(), b.close()])
    expect(deliveries).toBe(2)
    expect([...acked]).toEqual(['id-0'])
  })
})
