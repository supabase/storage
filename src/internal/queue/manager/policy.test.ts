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
} from './index'
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
      await s.retry([{ receipt: got.receipt, delayMs: 0 }])
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
