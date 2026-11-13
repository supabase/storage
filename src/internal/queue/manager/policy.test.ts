import { memory } from '../adapters/memory'
import {
  createProducerQueue,
  createQueue,
  createSyncQueue,
  DEFAULT_LIVENESS_TIMEOUT_MS,
  defineQueues,
  type JobContext,
  livenessOf,
  type PullMessage,
  type QueueSubscription,
  queue,
  type RetryPolicy,
  retryDelayOf,
} from './index'
import { fast, pulled, scripted, until } from './test-helpers'
import { deferred, recordingAdapter, rejection, sleep } from './test-support'

interface Job {
  readonly n: number
}

describe('provisioning order: a dead-letter target before the queue that names it', () => {
  it('provisions dead-letter targets first, declared ones with their own options', async () => {
    const queues = defineQueues({
      jobs: queue<Job>({ retry: { maxAttempts: 3, deadLetter: 'jobs-dead' } }),
      plain: queue<Job>(),
      mail: queue<Job>({ retry: { maxAttempts: 2, deadLetter: 'undeclared-dead' } }),
      'jobs-dead': queue<Job>({ livenessTimeoutMs: 1_000 }),
    })
    const { adapter, provisioned } = recordingAdapter()
    await createProducerQueue(adapter, { queues }).start()

    const order = provisioned.map((p) => p.queue)
    expect([...order].sort()).toEqual(['jobs', 'jobs-dead', 'mail', 'plain', 'undeclared-dead'])
    expect(order.indexOf('jobs-dead')).toBeLessThan(order.indexOf('jobs'))
    expect(order.indexOf('undeclared-dead')).toBeLessThan(order.indexOf('mail'))
    const optionsOf = (name: string) => provisioned.find((p) => p.queue === name)?.options
    expect(optionsOf('jobs')).toBe(queues.get('jobs').options)
    expect(optionsOf('jobs-dead')).toEqual({ livenessTimeoutMs: 1_000 })
    expect(optionsOf('undeclared-dead')).toBeUndefined() // not in the registry: options-less
  })

  it('waits for the target to be provisioned, follows chains, and provisions a shared target once', async () => {
    const queues = defineQueues({
      a: queue<Job>({ retry: { maxAttempts: 1, deadLetter: 'b' } }),
      a2: queue<Job>({ retry: { maxAttempts: 1, deadLetter: 'b' } }),
      b: queue<Job>({ retry: { maxAttempts: 1, deadLetter: 'c' } }),
      c: queue<Job>(),
    })
    const gate = deferred()
    const started: string[] = []
    const finished: string[] = []
    const { adapter } = recordingAdapter({
      provision: async (name) => {
        started.push(name)
        if (name === 'c') await gate.promise
        finished.push(name)
      },
    })
    const starting = createProducerQueue(adapter, { queues }).start()
    await sleep(10)
    expect(started).toEqual(['c']) // nothing upstream starts until its target landed
    gate.resolve()
    await starting
    expect(finished).toEqual(['c', 'b', 'a', 'a2'])
  })

  it('refuses a dead-letter cycle, and the failed start can be retried', async () => {
    const queues = defineQueues({
      a: queue<Job>({ retry: { maxAttempts: 1, deadLetter: 'b' } }),
      b: queue<Job>({ retry: { maxAttempts: 1, deadLetter: 'a' } }),
    })
    const { adapter, provisioned } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues })
    expect(await rejection(q.start())).toEqual(new Error('queue: dead-letter cycle — a → b → a'))
    await expect(q.start()).rejects.toThrow('dead-letter cycle')
    expect(provisioned).toEqual([])
  })
})

describe('a handler against its queue liveness window', () => {
  const queues = defineQueues({
    short: queue<Job>({ livenessTimeoutMs: 1_000 }),
    plain: queue<Job>(),
  })
  const handle = (_ctx: JobContext<Job>): void => {}
  const tooLong =
    'queue: consumeTimeout (5000) on queue "short" exceeds the queue\'s livenessTimeoutMs (1000)'

  it('livenessOf is the declared window, or the default', () => {
    expect(livenessOf(queues.get('short').options)).toBe(1_000)
    expect(livenessOf(queues.get('plain').options)).toBe(DEFAULT_LIVENESS_TIMEOUT_MS)
    expect(livenessOf(undefined)).toBe(300_000)
  })

  it('subscribe refuses a consumeTimeout past the window without a heartbeat', async () => {
    const q = createQueue(memory(), { queues })
    expect(() => q.subscribe({ queue: 'short', consumeTimeout: 5_000 }, handle)).toThrow(tooLong)
    expect(() => q.subscribe({ queue: 'short', consumeTimeout: 5_000 }, handle)).toThrow(
      /set heartbeatIntervalMs or raise the queue's livenessTimeoutMs/
    )
    // The default window applies to a queue that declares none.
    expect(() => q.subscribe({ queue: 'plain', consumeTimeout: 300_001 }, handle)).toThrow(
      /exceeds the queue's livenessTimeoutMs \(300000\)/
    )
    // Within the window, at the window, or heartbeating through it: accepted.
    const ok = [
      q.subscribe({ queue: 'short', consumeTimeout: 1_000 }, handle),
      q.subscribe({ queue: 'short', consumeTimeout: 5_000, heartbeatIntervalMs: 300 }, handle),
      q.subscribe({ queue: 'short' }, handle),
      q.subscribe({ queue: 'plain', consumeTimeout: 300_000 }, handle),
    ]
    expect(ok).toHaveLength(4)
    await q.close()
  })

  it('subscribe refuses a heartbeat that is not shorter than the window', async () => {
    const q = createQueue(memory(), { queues })
    for (const heartbeatIntervalMs of [1_000, 2_000]) {
      expect(() =>
        q.subscribe({ queue: 'short', consumeTimeout: 5_000, heartbeatIntervalMs }, handle)
      ).toThrow(
        `queue: heartbeatIntervalMs (${heartbeatIntervalMs}) on queue "short" must be shorter than the queue's livenessTimeoutMs (1000)`
      )
    }
    await q.close()
  })

  it('config handlers are checked at start() on every manager shape', async () => {
    const handlers = [{ queue: 'short' as const, options: { consumeTimeout: 5_000 }, handle }]
    await expect(createQueue(memory(), { queues, handlers }).start()).rejects.toThrow(tooLong)
    await expect(createProducerQueue(memory(), { queues, handlers }).start()).rejects.toThrow(
      tooLong
    )
    await expect(createSyncQueue({ queues, handlers }).start()).rejects.toThrow(tooLong)
  })
})

describe('memory adapter: the policy is the queue’s', () => {
  const encode = (n: number) => [{ data: { n }, headers: {} }]
  const pullOne = async (s: QueueSubscription): Promise<PullMessage> => (await s.pull(1))[0]
  /** Fail a queue's one message until it stops redelivering; the attempts it was seen with. */
  const failUntilGone = async (s: QueueSubscription): Promise<number[]> => {
    const attempts: number[] = []
    for (let i = 0; i < 5; i++) {
      const got = await pullOne(s)
      if (got === undefined) break
      attempts.push(got.attempt)
      await s.nack([{ receipt: got.receipt, delayMs: 0 }])
    }
    return attempts
  }

  it('takes retry and dead-letter from the options given to provision', async () => {
    const adapter = memory()
    await adapter.provision('dead')
    await adapter.provision('jobs', { retry: { maxAttempts: 2, deadLetter: 'dead' } })
    await adapter.provision('jobs') // options-less: only makes sure it exists
    await adapter.send('jobs', encode(1), {})
    expect(await failUntilGone(await adapter.subscribe('jobs', {}))).toEqual([1, 2])
    const dead = await pullOne(await adapter.subscribe('dead', {}))
    expect(dead).toMatchObject({ data: { n: 1 }, attempt: 2 })
  })

  it('the registry a subscribe forwards wins over what provision was given', async () => {
    const adapter = memory()
    await adapter.provision('jobs', { retry: { maxAttempts: 1 } })
    await adapter.send('jobs', encode(1), {})
    const queues = defineQueues({ jobs: queue<Job>({ retry: { maxAttempts: 3 } }) })
    expect(await failUntilGone(await adapter.subscribe('jobs', { queues }))).toEqual([1, 2, 3])
  })

  it('a queue never provisioned is options-less: no budget, the default liveness window', async () => {
    const adapter = memory()
    await adapter.send('jobs', encode(1), {})
    const a = await adapter.subscribe('jobs', {})
    expect(await failUntilGone(a)).toEqual([1, 2, 3, 4, 5])
    const held = await pullOne(a)
    expect(held.attempt).toBe(6)
    await sleep(20)
    expect(await (await adapter.subscribe('jobs', {})).pull(1)).toEqual([]) // still leased
  })

  it('a consumer silent past the provisioned liveness window loses its deliveries', async () => {
    const adapter = memory()
    await adapter.provision('jobs', { livenessTimeoutMs: 15 })
    await adapter.send('jobs', encode(1), {})
    const a = await adapter.subscribe('jobs', {})
    const b = await adapter.subscribe('jobs', {})
    const held = await pullOne(a)
    await sleep(30)
    expect((await pullOne(b)).id).toBe(held.id)
  })
})

describe('nack backoff', () => {
  it('retryDelayOf: fixed by default, exponential on request, capped by maxBackoffMs', () => {
    const delays = (policy: RetryPolicy | undefined, attempts = [1, 2, 3, 4, 5]): number[] =>
      attempts.map((attempt) => retryDelayOf(policy, attempt))
    expect(delays(undefined)).toEqual([0, 0, 0, 0, 0])
    expect(delays({ maxAttempts: 9 })).toEqual([0, 0, 0, 0, 0])
    expect(delays({ maxAttempts: 9, backoffMs: 500 })).toEqual([500, 500, 500, 500, 500])
    expect(delays({ maxAttempts: 9, backoffMs: 500, backoff: 'fixed' })[4]).toBe(500)
    const exponential = { maxAttempts: 9, backoffMs: 500, backoff: 'exponential' } as const
    expect(delays(exponential)).toEqual([500, 1_000, 2_000, 4_000, 8_000])
    expect(delays({ ...exponential, maxBackoffMs: 3_000 })).toEqual([
      500, 1_000, 2_000, 3_000, 3_000,
    ])
    // Attempt 1 waits the base delay; anything below it is treated as attempt 1.
    expect(delays(exponential, [1, 0, -3])).toEqual([500, 500, 500])
    // No base delay means no delay, however far the curve has run; it never overflows.
    expect(delays({ maxAttempts: 9, backoff: 'exponential' }, [1, 50, 5_000])).toEqual([0, 0, 0])
    expect(retryDelayOf(exponential, 5_000)).toBe(Number.MAX_SAFE_INTEGER)
  })

  it.each(['streaming', 'batched'] as const)(
    'the %s flow hands rising delays to the adapter and the retryScheduled event',
    async (flow) => {
      const s = scripted()
      // Three deliveries that failed on attempts 1, 2 and 6.
      s.feed([pulled(0, 1), pulled(1, 2), pulled(2, 6)])
      const scheduled: unknown[] = []
      const q = createQueue(s.adapter, {
        queues: defineQueues({
          jobs: queue<Job>({
            retry: { maxAttempts: 9, backoffMs: 100, backoff: 'exponential', maxBackoffMs: 1_000 },
          }),
        }),
        ...fast,
        events: { retryScheduled: (ctx) => scheduled.push(ctx) },
      })
      q.subscribe({ queue: 'jobs', flow, prefetch: 3 }, () => {
        throw new Error('boom')
      })

      await until(() => scheduled.length === 3)
      await q.close()
      const entries = s.calls
        .filter((call) => call.startsWith('nack '))
        .flatMap((call) => call.slice('nack '.length).split(','))
      expect(entries.sort()).toEqual(['r0@100', 'r1@200', 'r2@1000'])
      expect(scheduled).toEqual(
        expect.arrayContaining([
          { queue: 'jobs', attempt: 1, delayMs: 100 },
          { queue: 'jobs', attempt: 2, delayMs: 200 },
          { queue: 'jobs', attempt: 6, delayMs: 1_000 },
        ])
      )
    }
  )

  it('a policy that cannot mean what it says is refused at start() and at subscribe', async () => {
    const refusedAtStart = async (retry: RetryPolicy): Promise<unknown> => {
      const queues = defineQueues({ jobs: queue<Job>({ retry }) })
      const errors = await Promise.all([
        rejection(createQueue(memory(), { queues }).start()),
        rejection(createProducerQueue(recordingAdapter().adapter, { queues }).start()),
        rejection(createSyncQueue({ queues }).start()),
      ])
      expect(new Set(errors.map(String)).size).toBe(1)
      expect(() => createQueue(memory(), { queues }).subscribe('jobs', () => {})).toThrow(
        (errors[0] as Error).message
      )
      return errors[0]
    }
    expect(await refusedAtStart({ maxAttempts: 3, backoffMs: -1 })).toEqual(
      new Error(
        'queue: retry.backoffMs of queue "jobs" must be a finite number of at least 0, got -1'
      )
    )
    expect(await refusedAtStart({ maxAttempts: 3, maxBackoffMs: Infinity })).toEqual(
      new Error(
        'queue: retry.maxBackoffMs of queue "jobs" must be a finite number of at least 0, got Infinity (omit it for no ceiling)'
      )
    )
    expect(await refusedAtStart({ maxAttempts: 3, backoffMs: 500, maxBackoffMs: 100 })).toEqual(
      new Error(
        'queue: retry.maxBackoffMs of queue "jobs" (100) must be at least its backoffMs (500)'
      )
    )
    expect(
      await refusedAtStart({ maxAttempts: 3, backoff: 'linear' as RetryPolicy['backoff'] })
    ).toEqual(
      new Error('queue: retry.backoff of queue "jobs" must be "fixed" or "exponential", got linear')
    )
    // Nothing was provisioned by a start that refused its registry.
    const { adapter, log } = recordingAdapter()
    const queues = defineQueues({ jobs: queue<Job>({ retry: { maxAttempts: 3, backoffMs: NaN } }) })
    await expect(createProducerQueue(adapter, { queues }).start()).rejects.toThrow(/backoffMs/)
    expect(log).toEqual([])
  })
})

describe('the pulled event', () => {
  const produceMany = async (
    app: { produce(name: 'jobs', data: Job): Promise<void> },
    n: number
  ) => {
    for (let i = 0; i < n; i++) await app.produce('jobs', { n: i })
  }

  it('reports how many messages a batch handler pulled, and no empty pulls', async () => {
    const sizes: number[] = []
    const handled: number[] = []
    const app = createQueue(memory(), {
      queues: defineQueues({ jobs: queue<Job>() }),
      events: { pulled: ({ queue: name, size }) => name === 'jobs' && sizes.push(size) },
      ...fast,
    })
    await app.start()
    await produceMany(app, 5)
    app.subscribe({ queue: 'jobs', batch: true, prefetch: 3 }, (ctx) => {
      handled.push(ctx.messages.length)
    })

    await until(() => handled.reduce((a, b) => a + b, 0) === 5)
    await sleep(30) // a few idle polls
    await app.close()

    expect(sizes).toEqual([3, 2])
    expect(handled).toEqual([3, 2])
  })

  it('reports each refill of a streaming worker', async () => {
    const sizes: number[] = []
    let handled = 0
    const app = createQueue(memory(), {
      queues: defineQueues({ jobs: queue<Job>() }),
      events: { pulled: ({ size }) => sizes.push(size) },
      ...fast,
    })
    await app.start()
    await produceMany(app, 4)
    app.subscribe({ queue: 'jobs', prefetch: 4 }, () => {
      handled++
    })

    await until(() => handled === 4)
    await app.close()

    expect(sizes.reduce((a, b) => a + b, 0)).toBe(4)
    expect(sizes.every((size) => size > 0)).toBe(true)
  })
})
