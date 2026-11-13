import { memory } from '../adapters/memory'
import {
  type BatchContext,
  createProducerQueue,
  createQueue,
  defineMessage,
  defineQueues,
  type JobContext,
  type QueueAdapter,
  type QueueOptions,
  queue,
  runOnce,
} from './index'
import { createMaintenance } from './maintenance'
import { deferred, recordingAdapter, rejection, sleep, until } from './test-support'

interface Ev {
  readonly n: number
}

const eventOptions = { retention: 8 } as QueueOptions
const queues = defineQueues({
  events: queue<Ev>(eventOptions), // carries provisioning options
  orders: queue<Ev>(), // type-only, default provisioning
})

const fast = { pollIdleIntervalMs: 5, pollBatchIntervalMs: 0 }

/** The memory adapter with its lifecycle calls logged, and members replaceable. */
const loggedMemory = (
  overrides?: Partial<QueueAdapter>
): { adapter: QueueAdapter; log: string[] } => {
  const log: string[] = []
  const inner = memory()
  const adapter: QueueAdapter = {
    ...inner,
    start: async () => void log.push('start'),
    provision: async (name, options) => {
      log.push(`provision:${name}`)
      await inner.provision(name, options)
    },
    subscribe: async (name, policy) => {
      log.push(`subscribe:${name}`)
      const subscription = await inner.subscribe(name, policy)
      return {
        ...subscription,
        close: async () => {
          log.push(`subscription:close:${name}`)
          await subscription.close()
        },
      }
    },
    close: async () => void log.push('close'),
    ...overrides,
  }
  return { adapter, log }
}

describe('provisioning at start()', () => {
  it('runs the adapter start hook, then provisions every declared queue with its options', async () => {
    const { adapter, log, provisioned } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues })

    await q.start()

    expect(log).toEqual(['start', 'provision:events', 'provision:orders'])
    expect(provisioned).toEqual([
      { queue: 'events', options: eventOptions },
      { queue: 'orders', options: undefined },
    ])
    expect(provisioned[0].options).toBe(eventOptions) // the declared object, verbatim
  })

  it('is memoised: repeated and concurrent starts share one pass', async () => {
    const gate = deferred()
    const { adapter, log } = recordingAdapter({
      start: async () => {
        log.push('start')
        await gate.promise
      },
    })
    const q = createProducerQueue(adapter, { queues })

    const racing = Promise.all([q.start(), q.start(), q.start()])
    gate.resolve()
    await racing
    await q.start()

    expect(log).toEqual(['start', 'provision:events', 'provision:orders'])
  })

  it('autoProvision: false skips provisioning but still runs the adapter start hook', async () => {
    const { adapter, log } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues, autoProvision: false })
    await q.start()
    expect(log).toEqual(['start'])
  })

  it('an adapter without a start hook starts fine', async () => {
    const adapter = memory() // no start member
    const provision = vi.spyOn(adapter, 'provision')
    const q = createQueue(adapter, { queues, ...fast })
    await expect(q.start()).resolves.toBeUndefined()
    expect(provision.mock.calls).toEqual([
      ['events', eventOptions],
      ['orders', undefined],
    ])
    await q.close()
  })

  it('a failed start is not remembered: the next call retries the whole pass', async () => {
    let startFailures = 1
    let provisionFailures = 1
    const { adapter, log } = recordingAdapter({
      start: async () => {
        log.push('start')
        if (startFailures-- > 0) throw new Error('boot failed')
      },
      provision: async (name) => {
        log.push(`provision:${name}`)
        if (name === 'orders' && provisionFailures-- > 0) throw new Error('ddl failed')
      },
    })
    const ran: number[] = []
    const q = createProducerQueue(adapter, {
      queues,
      handlers: [
        { queue: 'orders', handle: (ctx: JobContext<Ev>) => void ran.push(ctx.message.data.n) },
      ],
    })

    await expect(q.start()).rejects.toThrow('boot failed')
    expect(log).toEqual(['start']) // nothing provisioned behind a failed hook
    await expect(q.start()).rejects.toThrow('ddl failed')
    // Handlers attach only once provisioning has succeeded.
    await expect(q.invoke('orders', { n: 1 })).rejects.toThrow(/no local handler/)

    await q.start()
    await q.start()
    expect(log.filter((entry) => entry === 'start')).toHaveLength(3)
    await q.invoke('orders', { n: 2 })
    expect(ran).toEqual([2])
  })
})

describe('createQueue start()', () => {
  it('starts config handlers only after provisioning succeeded, and exactly once', async () => {
    const gate = deferred()
    const { adapter, log } = loggedMemory({
      provision: async (name) => {
        log.push(`provision:${name}`)
        await gate.promise
      },
    })
    const seen: number[] = []
    const q = createQueue(adapter, {
      queues,
      ...fast,
      handlers: [
        { queue: 'orders', handle: (ctx: JobContext<Ev>) => void seen.push(ctx.message.data.n) },
        {
          queue: 'events',
          handleBatch: (ctx: BatchContext<Ev>) =>
            void seen.push(...ctx.messages.map((m) => m.data.n)),
        },
      ],
    })

    const starting = Promise.all([q.start(), q.start()])
    await sleep(20)
    expect(log).toEqual(['start', 'provision:events', 'provision:orders']) // no worker yet
    gate.resolve()
    await starting
    await q.start()

    await q.produce('orders', { n: 1 })
    await q.produce('events', { n: 2 })
    await until(() => seen.length === 2)
    await sleep(20)
    expect(log.filter((entry) => entry.startsWith('subscribe:')).sort()).toEqual([
      'subscribe:events',
      'subscribe:orders',
    ])
    expect([...seen].sort()).toEqual([1, 2])
    await q.close()
  })

  it('a failed start attaches no handler; the retry does', async () => {
    let failures = 1
    const { adapter, log } = loggedMemory({
      start: async () => {
        if (failures-- > 0) throw new Error('boot failed')
      },
    })
    const q = createQueue(adapter, {
      queues,
      ...fast,
      handlers: [{ queue: 'orders', handle: (_ctx: JobContext<Ev>) => {} }],
    })

    await expect(q.start()).rejects.toThrow('boot failed')
    await expect(q.invoke('orders', { n: 1 })).rejects.toThrow(/no local handler/)
    expect(log).toEqual([])

    await q.start()
    await expect(q.invoke('orders', { n: 1 })).resolves.toBeUndefined()
    await q.close()
  })

  it('refuses manager-level timings that are not finite numbers', () => {
    expect(() => createQueue(memory(), { queues, pollIdleIntervalMs: -1 })).toThrow(
      /pollIdleIntervalMs must be a finite number of at least 0, got -1/
    )
    expect(() => createQueue(memory(), { queues, pollBatchIntervalMs: Number.NaN })).toThrow(
      /pollBatchIntervalMs must be a finite number/
    )
    expect(() => createQueue(memory(), { queues, closeTimeout: Number.POSITIVE_INFINITY })).toThrow(
      /closeTimeout must be a finite number/
    )
  })
})

describe('createQueue close()', () => {
  it('drains every worker before closing the adapter, and is idempotent', async () => {
    const { adapter, log } = loggedMemory()
    const q = createQueue(adapter, { queues, ...fast })
    await q.start()
    const gate = deferred()
    q.subscribe('orders', async () => {
      log.push('handler:start')
      await gate.promise
      log.push('handler:done')
    })
    q.subscribe('events', () => {})
    await q.produce('orders', { n: 1 })
    await until(() => log.includes('handler:start') && log.includes('subscribe:events'))

    const closing = Promise.all([q.close(), q.close()])
    await sleep(20)
    expect(log).not.toContain('close') // the in-flight handler still holds the adapter open
    gate.resolve()
    await closing
    await q.close()

    const at = (entry: string): number => log.indexOf(entry)
    expect(at('handler:done')).toBeGreaterThan(-1)
    expect(at('subscription:close:orders')).toBeGreaterThan(at('handler:done'))
    expect(at('close')).toBeGreaterThan(at('subscription:close:orders'))
    expect(at('close')).toBeGreaterThan(at('subscription:close:events'))
    expect(log.filter((entry) => entry === 'close')).toHaveLength(1)
  })

  it('stops maintenance and awaits its in-flight pass before closing the adapter', async () => {
    const gate = deferred()
    const { adapter, log } = loggedMemory({
      maintain: async () => {
        log.push('maintain:start')
        await gate.promise
        log.push('maintain:done')
      },
    })
    const q = createQueue(adapter, { queues, ...fast })
    await q.start()
    q.maintenance.start({ intervalMs: 5 })
    await until(() => log.includes('maintain:start'))

    const closing = q.close()
    await sleep(20)
    expect(log).not.toContain('close')
    gate.resolve()
    await closing
    await sleep(20) // the timer is gone: no pass after close

    expect(log.slice(log.indexOf('maintain:start'))).toEqual([
      'maintain:start',
      'maintain:done',
      'close',
    ])
  })
})

describe('createProducerQueue', () => {
  it('config handlers are dispatch-only: attached at start(), run by invoke, never polled', async () => {
    const { adapter, sends } = recordingAdapter()
    const order: string[] = []
    const q = createProducerQueue(adapter, {
      queues,
      handlers: [
        {
          queue: 'orders',
          handle: (ctx: JobContext<Ev>) => void order.push(`handler:${ctx.message.data.n}`),
        },
      ],
      // Manager-level consume stages wrap each dispatch, exactly as on a worker.
      middleware: [
        {
          consume: (next) => async (call) => {
            order.push('before')
            const result = await next(call)
            order.push(`after:${result.outcome}`)
            return result
          },
        },
      ],
    })

    await expect(q.invoke('orders', { n: 0 })).rejects.toThrow(/attach at start\(\)/)
    await q.start()
    await q.start() // handlers attach once
    await q.invoke('orders', { n: 1 })

    expect(order).toEqual(['before', 'handler:1', 'after:ok'])
    expect(sends).toEqual([]) // inline dispatch: nothing was sent
    await q.close()
  })

  it('start() attaches the config handlers all or nothing', async () => {
    class Stray extends defineMessage<{ x: number }>() {} // bound to no queue in this registry
    const { adapter, log } = recordingAdapter()
    const q = createProducerQueue(adapter, {
      queues,
      handlers: [
        { queue: 'orders', handle: () => {} },
        { messages: [Stray], handle: () => {} } as never,
      ],
    })

    await expect(q.start()).rejects.toThrow(/"Stray" is not bound to any queue/)
    // The good entry did not slip in ahead of the bad one.
    await expect(q.invoke('orders', { n: 1 })).rejects.toThrow(/no local handler/)
    // Provisioning succeeded and stays memoised across the retry.
    await expect(q.start()).rejects.toThrow(/"Stray" is not bound to any queue/)
    expect(log).toEqual(['start', 'provision:events', 'provision:orders'])
  })

  it('checks config handlers as a worker would check them', async () => {
    const starting = (handler: object): Promise<void> =>
      createProducerQueue(recordingAdapter().adapter, {
        queues,
        handlers: [handler as never],
      }).start()

    await expect(
      starting({ queue: 'orders', options: { heartbeatIntervalMs: 1_000 }, handle: () => {} })
    ).rejects.toThrow(/heartbeatIntervalMs requires consumeTimeout/)
    await expect(
      starting({
        queue: 'orders',
        options: { consumeTimeout: Number.POSITIVE_INFINITY },
        handle: () => {},
      })
    ).rejects.toThrow(/consumeTimeout must be a finite number.*omit it for no timeout/)
    await expect(
      starting({ queue: 'orders', options: { flow: 'streaming' }, handleBatch: () => {} })
    ).rejects.toThrow(/flow "streaming" is not applicable to a batch handler/)
    await expect(
      starting({ queue: 'orders', handle: () => {}, handleBatch: () => {} })
    ).rejects.toThrow(/handle OR handleBatch, never both/)
  })

  it('close() aborts an in-flight dispatch, refuses later invokes, closes the adapter once', async () => {
    const { adapter, log } = recordingAdapter()
    const entered = deferred()
    let signalled = false
    const q = createProducerQueue(adapter, {
      queues,
      handlers: [
        {
          queue: 'orders',
          // Hangs and only notes the abort: the caller's rejection comes from the manager.
          handle: ({ signal }: JobContext<Ev>) => {
            signal.addEventListener('abort', () => {
              signalled = true
            })
            entered.resolve()
            return new Promise<void>(() => {})
          },
        },
      ],
    })
    await q.start()

    const inFlight = rejection(q.invoke('orders', { n: 1 }))
    await entered.promise
    await q.close()
    await q.close()

    expect(((await inFlight) as Error).message).toBe(
      'queue: invoke — invocation on queue "orders" aborted by shutdown'
    )
    expect(signalled).toBe(true)
    await expect(q.invoke('orders', { n: 2 })).rejects.toThrow('queue: producer queue is closed')
    expect(log.filter((entry) => entry === 'close')).toHaveLength(1)
  })

  it('pins the type contract: no consumption surface, no consumption config', () => {
    const { adapter } = recordingAdapter()
    const q = createProducerQueue(adapter, { queues })
    expect(Object.keys(q).sort()).toEqual([
      'close',
      'getQueue',
      'getQueues',
      'invoke',
      'produce',
      'start',
      'stats',
    ])

    const never = (): boolean => false
    if (never()) {
      // @ts-expect-error — maintenance does not exist on a producer queue
      void q.maintenance
      // @ts-expect-error — a bare function is not a middleware entry
      createProducerQueue(adapter, { queues, middleware: [(next) => next] })
      // @ts-expect-error — an entry has produce and consume stages only
      createProducerQueue(adapter, { queues, middleware: [{ handle: (next) => next }] })
      // A consume-only entry is legal: its stage runs around invoke dispatches.
      createProducerQueue(adapter, { queues, middleware: [{ consume: (next) => next }] })
      // @ts-expect-error — poll intervals configure consumption loops that cannot exist
      createProducerQueue(adapter, { queues, pollIdleIntervalMs: 50 })
      // @ts-expect-error — closeTimeout configures a drain that cannot exist
      createProducerQueue(adapter, { queues, closeTimeout: 1_000 })
      // @ts-expect-error — only the produce hook can ever fire here
      createProducerQueue(adapter, { queues, events: { handled: () => {} } })
      // @ts-expect-error — a handler for a queue the registry does not declare
      createProducerQueue(adapter, { queues, handlers: [{ queue: 'nope', handle: () => {} }] })
    }
  })
})

describe('maintenance runner', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it('is opt-in: nothing runs until start(), then one pass per interval until stop()', async () => {
    const maintain = vi.fn(async () => {})
    const maintenance = createMaintenance({ maintain })

    await vi.advanceTimersByTimeAsync(120_000)
    expect(maintain).not.toHaveBeenCalled()

    maintenance.start({ intervalMs: 10 })
    maintenance.start({ intervalMs: 1 }) // idempotent: the running schedule stands
    await vi.advanceTimersByTimeAsync(35)
    expect(maintain).toHaveBeenCalledTimes(3)

    await maintenance.stop()
    await vi.advanceTimersByTimeAsync(100)
    expect(maintain).toHaveBeenCalledTimes(3)

    maintenance.start({ intervalMs: 10 }) // restartable after a stop
    await vi.advanceTimersByTimeAsync(10)
    expect(maintain).toHaveBeenCalledTimes(4)
    await maintenance.stop()
  })

  it('defaults to one pass a minute', async () => {
    const maintain = vi.fn(async () => {})
    const maintenance = createMaintenance({ maintain })
    maintenance.start()
    await vi.advanceTimersByTimeAsync(59_999)
    expect(maintain).not.toHaveBeenCalled()
    await vi.advanceTimersByTimeAsync(1)
    expect(maintain).toHaveBeenCalledTimes(1)
    await maintenance.stop()
  })

  it('never overlaps a slow pass with the next tick', async () => {
    let gate = deferred()
    const maintain = vi.fn(() => gate.promise)
    const maintenance = createMaintenance({ maintain })

    maintenance.start({ intervalMs: 5 })
    await vi.advanceTimersByTimeAsync(200) // forty ticks into one slow pass
    expect(maintain).toHaveBeenCalledTimes(1)

    const previous = gate
    gate = deferred()
    previous.resolve()
    await vi.advanceTimersByTimeAsync(5)
    expect(maintain).toHaveBeenCalledTimes(2) // the next tick after it finished runs again
    gate.resolve()
    await maintenance.stop()
  })

  it('contains a failing pass: onError receives it and the loop keeps running', async () => {
    let calls = 0
    const errors: unknown[] = []
    const maintenance = createMaintenance({
      maintain: async () => {
        calls += 1
        throw new Error(`boom ${calls}`)
      },
    })

    maintenance.start({
      intervalMs: 10,
      onError: (error) => {
        errors.push(error)
        throw new Error('the error handler itself failed') // must not break the loop either
      },
    })
    await vi.advanceTimersByTimeAsync(30)
    await maintenance.stop()

    expect(calls).toBe(3)
    expect(errors.map((e) => (e as Error).message)).toEqual(['boom 1', 'boom 2', 'boom 3'])
  })

  it('swallows a failing pass when no onError is given', async () => {
    const maintain = vi.fn(async () => {
      throw new Error('boom')
    })
    const maintenance = createMaintenance({ maintain })
    maintenance.start({ intervalMs: 10 })
    await vi.advanceTimersByTimeAsync(20)
    await expect(maintenance.stop()).resolves.toBeUndefined()
    expect(maintain).toHaveBeenCalledTimes(2)
  })

  it('runOnce() runs exactly one pass now and throws on failure', async () => {
    const boom = new Error('boom')
    let fail = false
    const maintain = vi.fn(async () => {
      if (fail) throw boom
    })
    const maintenance = createMaintenance({ maintain })

    await maintenance.runOnce()
    expect(maintain).toHaveBeenCalledTimes(1)
    expect(vi.getTimerCount()).toBe(0) // a one-off pass schedules nothing

    fail = true
    expect(await rejection(maintenance.runOnce())).toBe(boom)
  })

  it('stop() awaits the in-flight pass', async () => {
    const gate = deferred()
    const maintenance = createMaintenance({ maintain: () => gate.promise })
    maintenance.start({ intervalMs: 10 })
    await vi.advanceTimersByTimeAsync(10)

    let stopped = false
    const stopping = maintenance.stop().then(() => {
      stopped = true
    })
    await vi.advanceTimersByTimeAsync(50)
    expect(stopped).toBe(false)

    gate.resolve()
    await stopping
    expect(stopped).toBe(true)
  })

  it('is a no-op when the adapter exposes no maintain()', async () => {
    const q = createQueue(memory(), { queues }) // memory has no maintain
    q.maintenance.start({ intervalMs: 5 })
    expect(vi.getTimerCount()).toBe(0)
    await expect(q.maintenance.runOnce()).resolves.toBeUndefined()
    await expect(q.maintenance.stop()).resolves.toBeUndefined()
  })

  it("a manager's maintenance drives the adapter's maintain()", async () => {
    const maintain = vi.fn(async () => {})
    const q = createQueue({ ...memory(), maintain }, { queues })
    await q.maintenance.runOnce()
    q.maintenance.start({ intervalMs: 10 })
    await vi.advanceTimersByTimeAsync(20)
    await q.maintenance.stop()
    expect(maintain).toHaveBeenCalledTimes(3)
  })
})

/**
 * `runOnce` memoises the OUTCOME of an async run, not the attempt: a success is remembered,
 * concurrent callers share one in-flight run, and a failure is forgotten so the next call
 * retries. That distinction is what keeps a one-time startup retryable.
 */
describe('runOnce', () => {
  it('runs once on success: later calls return the settled value without re-running', async () => {
    let runs = 0
    const once = runOnce(async () => ++runs)

    expect(await once()).toBe(1)
    expect(await once()).toBe(1)
    expect(runs).toBe(1)
  })

  it('concurrent callers share the one in-flight run', async () => {
    let runs = 0
    const gate = deferred()
    const once = runOnce(async () => {
      runs += 1
      await gate.promise
      return runs
    })

    const a = once()
    const b = once() // overlaps a: must not start a second run
    expect(b).toBe(a) // literally the same promise
    gate.resolve()
    expect(await Promise.all([a, b])).toEqual([1, 1])
    expect(runs).toBe(1)
  })

  it('a rejection is not remembered: the next call runs again', async () => {
    let runs = 0
    const once = runOnce(async () => {
      runs += 1
      if (runs === 1) throw new Error('boot failed')
      return runs
    })

    await expect(once()).rejects.toThrow('boot failed')
    expect(await once()).toBe(2) // a fresh run, not the cached failure
    expect(await once()).toBe(2) // and THAT success is remembered
    expect(runs).toBe(2)
  })

  it('a synchronous throw inside fn rejects the call and is not remembered either', async () => {
    let runs = 0
    const once = runOnce((): Promise<number> => {
      runs += 1
      if (runs === 1) throw new Error('sync boom')
      return Promise.resolve(runs)
    })

    await expect(once()).rejects.toThrow('sync boom')
    expect(await once()).toBe(2)
  })

  it('reset() forgets a success, so the next call runs again', async () => {
    let runs = 0
    const once = runOnce(async () => ++runs)

    await once()
    once.reset()
    expect(await once()).toBe(2)
    expect(await once()).toBe(2)
  })

  it("a stale attempt's failure never forgets a run started after reset()", async () => {
    const stale = deferred<number>()
    let runs = 0
    const once = runOnce(() => {
      runs += 1
      return runs === 1 ? stale.promise : Promise.resolve(runs)
    })

    const first = once() // in flight
    once.reset() // teardown mid-flight
    const fresh = once() // a new run begins, and must stay memoised
    stale.reject(new Error('late failure')) // the stale attempt fails AFTER the fresh one began
    await expect(first).rejects.toThrow('late failure')

    expect(await once()).toBe(2) // still the fresh run: the stale failure did not clear it
    expect(await fresh).toBe(2)
    expect(runs).toBe(2)
  })
})
