import { memory } from '../adapters/memory'
import { createQueue, defineQueues, type PullMessage, type QueueEvents, queue } from './index'
import {
  defer,
  fast,
  forever,
  patchSubscriptions,
  pulled,
  scripted,
  sleep,
  until,
  withRetry,
} from './test-helpers'

interface Job {
  n: number
}

const queues = defineQueues({ jobs: queue<Job>(), 'jobs-dead': queue<Job>() })
const flows = ['streaming', 'batched'] as const
const noop = (): void => {}

/** A handler that only returns once its signal aborts, the moment it does. */
const untilAborted = (signal: AbortSignal): Promise<void> =>
  new Promise((resolve) => signal.addEventListener('abort', () => resolve(), { once: true }))

describe.each(flows)('stop() under the %s flow', (flow) => {
  it('closeTimeout aborts a stuck handler, releases its message and closes the subscription', async () => {
    const s = scripted()
    s.feed([pulled(0)])
    const outcomes: string[] = []
    const q = createQueue(s.adapter, {
      queues,
      ...fast,
      events: { handled: ({ outcome }) => outcomes.push(outcome) },
    })
    const reasons: unknown[] = []
    let started = false

    const worker = q.subscribe({ queue: 'jobs', flow, closeTimeout: 20 }, async ({ signal }) => {
      started = true
      signal.addEventListener('abort', () => reasons.push(signal.reason), { once: true })
      await forever() // ignores the signal: only closeTimeout can free the worker
    })

    await until(() => started)
    expect(worker.closed).toBe(false)
    await worker.stop()
    expect((reasons[0] as DOMException).name).toBe('AbortError')
    expect(s.calls).toEqual(['release r0', 'close'])
    expect(outcomes).toEqual(['shutdown'])
    expect(worker.closed).toBe(true)
    await q.close()
  })

  it('without a closeTimeout it drains to natural completion and acks', async () => {
    const s = scripted()
    s.feed([pulled(0)])
    const q = createQueue(s.adapter, { queues, ...fast })
    const gate = defer()
    let started = false
    let stopped = false
    let abortedAtReturn: boolean | undefined

    const worker = q.subscribe({ queue: 'jobs', flow }, async ({ signal }) => {
      started = true
      await gate.promise
      abortedAtReturn = signal.aborted
    })

    await until(() => started)
    const stopping = worker.stop().then(() => {
      stopped = true
    })
    await sleep(30)
    expect(stopped).toBe(false)
    expect(s.calls).toEqual([])
    gate.resolve()
    await stopping
    expect(abortedAtReturn).toBe(false)
    expect(s.calls).toEqual(['ack r0', 'close'])
    await worker.stop() // idempotent: the subscription is already closed
    expect(s.calls).toEqual(['ack r0', 'close'])
    await q.close()
  })

  it('messages a pull returns after stop() began are released, never handed out', async () => {
    const pull = defer<PullMessage[]>()
    const pulling = defer()
    const s = scripted(() => ({
      pull: () => {
        pulling.resolve()
        return pull.promise
      },
    }))
    const q = createQueue(s.adapter, { queues, ...fast })
    let calls = 0

    const worker = q.subscribe({ queue: 'jobs', flow }, () => void calls++)
    await pulling.promise // the loop is inside its pull

    const stopping = worker.stop()
    pull.resolve([pulled(1), pulled(2)]) // the pull straddles the stop
    await stopping
    expect(calls).toBe(0)
    expect(s.calls).toEqual(['release r1,r2', 'close']) // released through the still-open subscription
    await q.close()
  })

  it('stop() during a pending subscribe closes the subscription that subscribe opens', async () => {
    const s = scripted()
    const subscribing = defer()
    const opened = defer()
    const q = createQueue(
      {
        ...s.adapter,
        subscribe: async (name, policy) => {
          subscribing.resolve()
          await opened.promise
          return s.adapter.subscribe(name, policy)
        },
      },
      { queues, ...fast }
    )

    const worker = q.subscribe({ queue: 'jobs', flow }, noop)
    await subscribing.promise
    const stopping = worker.stop()
    opened.resolve() // the subscribe lands after stop() began
    await stopping
    expect(s.calls).toEqual(['close'])
    await q.close()
  })
})

describe('stop(): grace period and teardown', () => {
  it('closeTimeout defaults to the manager setting, and a worker setting wins over it', async () => {
    const inherited = scripted()
    inherited.feed([pulled(0)])
    const a = createQueue(inherited.adapter, { queues, ...fast, closeTimeout: 20 })
    let aborted = false
    const first = a.subscribe('jobs', async ({ signal }) => {
      await untilAborted(signal)
      aborted = true
      await forever()
    })
    await until(() => inherited.pulls.length > 0)
    await first.stop()
    expect(aborted).toBe(true)

    // A long manager default: only the worker's own 20ms lets this stop() return in time.
    const overridden = scripted()
    overridden.feed([pulled(1)])
    const b = createQueue(overridden.adapter, { queues, ...fast, closeTimeout: 60_000 })
    let started = false
    const second = b.subscribe({ queue: 'jobs', closeTimeout: 20 }, async () => {
      started = true
      await forever()
    })
    await until(() => started)
    await second.stop()
    expect(overridden.calls).toEqual(['release r1', 'close'])
    await Promise.all([a.close(), b.close()])
  })

  it.each(['consumeTimeout', 'closeTimeout'] as const)(
    'a handler that settles in the same tick as its %s abort is honoured',
    async (bound) => {
      const s = scripted()
      s.feed([pulled(0)])
      const outcomes: string[] = []
      const q = createQueue(s.adapter, {
        queues,
        ...fast,
        events: { handled: ({ outcome }) => outcomes.push(outcome) },
      })
      let started = false

      const limit = bound === 'consumeTimeout' ? { consumeTimeout: 20 } : { closeTimeout: 20 }
      const worker = q.subscribe({ queue: 'jobs', ...limit }, async ({ signal }) => {
        started = true
        await untilAborted(signal) // returns the instant the bound fires
      })

      await until(() => started)
      if (bound === 'closeTimeout') await worker.stop()
      else await until(() => s.calls.length === 1)
      // It actually settled, so it acks: the abort that raced it does not release it.
      expect(s.calls[0]).toBe('ack r0')
      expect(outcomes).toEqual(['ok'])
      await q.close()
    }
  )

  it('a subscription close that threw is retried by the next stop()', async () => {
    let closes = 0
    const s = scripted(() => ({
      close: async () => {
        if (++closes === 1) throw new Error('close failed')
      },
    }))
    const q = createQueue(s.adapter, { queues, ...fast })
    const worker = q.subscribe('jobs', noop)
    await until(() => s.pulls.length > 0) // the subscription is open

    await expect(worker.stop()).rejects.toThrow('close failed')
    await worker.stop() // the worker kept the subscription, so this closes it
    expect(closes).toBe(2)
    await worker.stop() // closed for good: nothing more to do
    expect(closes).toBe(2)
    await q.close()
  })

  it('queue.close() drains every worker, then closes the adapter last, once', async () => {
    const s = scripted()
    s.feed([pulled(0)])
    const q = createQueue(s.adapter, { queues, ...fast })
    const gate = defer()
    let started = false
    const worker = q.subscribe('jobs', async () => {
      started = true
      await gate.promise
    })
    const stoppedEarly = q.subscribe('jobs', noop)
    await until(() => started)
    await stoppedEarly.stop() // an individual stop untracks it; the subscription is shared here
    expect(s.calls).toEqual(['close'])

    const closing = q.close()
    gate.resolve()
    await closing
    expect(worker.closed).toBe(true)
    expect(s.calls).toEqual(['close', 'ack r0', 'close', 'adapter.close'])
    await q.close()
    expect(s.calls.filter((c) => c === 'adapter.close')).toHaveLength(1)
  })
})

describe('consumeTimeout', () => {
  it('aborts with a TimeoutError and counts the delivery as a failed attempt', async () => {
    const dead: unknown[] = []
    const scheduled: unknown[] = []
    const outcomes: string[] = []
    const adapter = memory({ events: { deadLettered: (ctx) => dead.push(ctx) } })
    const q = createQueue(adapter, {
      queues: withRetry({ maxAttempts: 3, deadLetter: 'jobs-dead', backoffMs: 5 }),
      ...fast,
      events: {
        handled: ({ outcome }) => outcomes.push(outcome),
        retryScheduled: (ctx) => scheduled.push(ctx),
      },
    })
    await q.produce('jobs', { n: 7 })
    const attempts: number[] = []
    const reasons: string[] = []

    // Time out the first two deliveries, then succeed on the last budgeted one.
    q.subscribe(
      {
        queue: 'jobs',
        consumeTimeout: 15,
      },
      async ({ message, signal }) => {
        attempts.push(message.attempt)
        if (attempts.length === 3) return
        signal.addEventListener('abort', () => reasons.push(signal.reason.name), { once: true })
        await forever()
      }
    )

    await until(() => outcomes.length === 3)
    await q.close()
    // A timeout is a failure: each one uses an attempt and waits out the backoff.
    expect(attempts).toEqual([1, 2, 3])
    expect(reasons).toEqual(['TimeoutError', 'TimeoutError'])
    expect(outcomes).toEqual(['timeout', 'timeout', 'ok'])
    expect(scheduled).toEqual([
      { queue: 'jobs', attempt: 1, delayMs: 5 },
      { queue: 'jobs', attempt: 2, delayMs: 5 },
    ])
    expect(dead).toEqual([])
  })

  it('dead-letters a message whose handler times out on every attempt', async () => {
    const dead: { queue: string; attempt: number }[] = []
    const outcomes: string[] = []
    const adapter = memory({
      events: { deadLettered: (ctx) => dead.push(ctx as { queue: string; attempt: number }) },
    })
    const q = createQueue(adapter, {
      queues: withRetry({ maxAttempts: 2, deadLetter: 'jobs-dead' }),
      ...fast,
      events: { handled: ({ outcome }) => outcomes.push(outcome) },
    })
    await q.produce('jobs', { n: 7 })

    q.subscribe({ queue: 'jobs', consumeTimeout: 15 }, () => forever())

    await until(() => dead.length === 1)
    await q.close()
    expect(outcomes).toEqual(['timeout', 'timeout'])
    expect(dead[0]).toMatchObject({ queue: 'jobs', attempt: 2 })
  })

  it('releases a timed-out delivery at once on a queue with no retry policy', async () => {
    const outcomes: string[] = []
    const q = createQueue(memory(), {
      queues: defineQueues({ jobs: queue<{ n: number }>() }),
      ...fast,
      events: { handled: ({ outcome }) => outcomes.push(outcome) },
    })
    await q.produce('jobs', { n: 7 })
    const attempts: number[] = []

    q.subscribe({ queue: 'jobs', consumeTimeout: 15 }, async ({ message }) => {
      attempts.push(message.attempt)
      if (attempts.length === 3) return
      await forever()
    })

    await until(() => outcomes.length === 3)
    await q.close()
    // Nothing counts attempts without a policy: the same delivery comes round again.
    expect(attempts).toEqual([1, 1, 1])
    expect(outcomes).toEqual(['timeout', 'timeout', 'ok'])
  })
})

describe('the subscribe policy a worker declares', () => {
  it('carries a consumer id and the registry; the queue policy stays on the queue', async () => {
    const s = scripted()
    const q = createQueue(s.adapter, {
      queues: withRetry({ maxAttempts: 3, deadLetter: 'jobs-dead' }),
      ...fast,
    })

    q.subscribe('jobs', noop)
    q.subscribe({ queue: 'jobs', consumeTimeout: 10_000, prefetch: 8 }, noop)

    await until(() => s.policies.length === 2)
    expect(s.policies.map((p) => Object.keys(p).sort())).toEqual([
      ['consumerId', 'queues'],
      ['consumerId', 'queues'],
    ])
    expect(new Set(s.policies.map((p) => p.consumerId)).size).toBe(2)
    expect(s.policies.every((p) => p.queues === q.getQueues())).toBe(true)
    await q.close()
  })
})

describe.each(flows)('heartbeat under the %s flow', (flow) => {
  it('ctx.heartbeat() is best-effort: a failed beat is reported, the handler still succeeds', async () => {
    const s = scripted((base) => ({
      heartbeat: async (receipts) => {
        await base.heartbeat?.(receipts)
        throw new Error('heartbeat outage')
      },
    }))
    s.feed([pulled(0)])
    const errors: unknown[] = []
    const reported: unknown[] = []
    const q = createQueue(s.adapter, {
      queues,
      ...fast,
      events: { workerError: ({ error }) => reported.push(error) },
    })

    q.subscribe({ queue: 'jobs', flow, onError: (e) => errors.push(e) }, async ({ heartbeat }) => {
      await heartbeat() // must not throw into the handler
    })

    await until(() => s.calls.length === 2)
    expect(s.calls).toEqual(['heartbeat r0', 'ack r0'])
    expect(errors).toEqual([expect.objectContaining({ message: 'heartbeat outage' })])
    expect(reported).toEqual(errors) // the event fires alongside onError
    await q.close()
  })

  it('ctx.heartbeat() is an advisory no-op on a subscription without the capability', async () => {
    const s = scripted(() => ({ heartbeat: undefined }))
    s.feed([pulled(0)])
    const q = createQueue(s.adapter, { queues, ...fast })

    q.subscribe({ queue: 'jobs', flow }, async ({ heartbeat }) => {
      await heartbeat()
    })

    await until(() => s.calls.length === 1)
    expect(s.calls).toEqual(['ack r0'])
    await q.close()
  })

  it('heartbeatIntervalMs beats on its own cadence only while work is in flight', async () => {
    const s = scripted()
    s.feed([pulled(0)])
    const q = createQueue(s.adapter, { queues, ...fast })
    const gate = defer()

    q.subscribe(
      { queue: 'jobs', flow, heartbeatIntervalMs: 5, consumeTimeout: 5_000 },
      async () => {
        await gate.promise // slow but legitimate: never calls heartbeat() itself
      }
    )

    await until(() => s.receipts('heartbeat').length >= 3)
    gate.resolve()
    await until(() => s.receipts('ack').length === 1)
    const beats = s.receipts('heartbeat')
    expect(new Set(beats)).toEqual(new Set(['r0']))
    await sleep(25)
    expect(s.receipts('heartbeat')).toHaveLength(beats.length) // idle: nothing to renew
    await q.close()
  })
})

describe.each(flows)('infrastructure errors under the %s flow', (flow) => {
  it('survives pull failures: each is reported, even by a throwing onError, then it resumes', async () => {
    let failuresLeft = 3
    const flaky = patchSubscriptions(memory(), (subscription) => ({
      pull: async (max) => {
        if (failuresLeft > 0) {
          failuresLeft--
          throw new Error('connection reset')
        }
        return subscription.pull(max)
      },
    }))
    const reported: unknown[] = []
    const q = createQueue(flaky, {
      queues,
      ...fast,
      events: { workerError: (ctx) => reported.push(ctx) },
    })
    const seen: number[] = []
    const errors: unknown[] = []

    q.subscribe(
      {
        queue: 'jobs',
        flow,
        onError: (e) => {
          errors.push(e)
          throw new Error('reporter bug') // must not take the loop down
        },
      },
      ({ message }) => {
        seen.push(message.data.n)
      }
    )
    await q.produce('jobs', { n: 1 })

    await until(() => seen.length === 1)
    await q.close()
    expect(errors).toHaveLength(3) // every failure surfaced, nothing swallowed
    expect(reported).toEqual(errors.map((error) => ({ queue: 'jobs', error })))
  })

  it('a failed ack releases the message: it is redelivered, never lost or skipped', async () => {
    let failAcks = 1
    const flaky = patchSubscriptions(memory(), (subscription) => ({
      ack: async (receipts) => {
        if (failAcks > 0) {
          failAcks--
          throw new Error('connection reset during ack')
        }
        return subscription.ack(receipts)
      },
    }))
    const q = createQueue(flaky, { queues, ...fast })
    const seen: number[] = []
    const errors: unknown[] = []

    q.subscribe({ queue: 'jobs', flow, onError: (e) => errors.push(e) }, ({ message }) => {
      seen.push(message.data.n)
    })
    await q.produce('jobs', { n: 7 })

    // Handled, ack failed, released, redelivered, handled again, acked for real.
    await until(() => seen.length === 2)
    await q.close()
    expect(seen).toEqual([7, 7])
    expect(errors).toHaveLength(1)
    const [stats] = (await q.stats({ queues: ['jobs'] })).queues
    expect([stats.backlog?.count, stats.inFlight?.count]).toEqual([0, 0])
  })

  it('a failed subscribe is retried, then the worker consumes', async () => {
    const inner = memory()
    let failSubs = 2
    const q = createQueue(
      {
        ...inner,
        subscribe: async (name, policy) => {
          if (failSubs > 0) {
            failSubs--
            throw new Error('connection reset during subscribe')
          }
          return inner.subscribe(name, policy)
        },
      },
      { queues, ...fast }
    )
    const seen: number[] = []
    const errors: unknown[] = []

    q.subscribe({ queue: 'jobs', flow, onError: (e) => errors.push(e) }, ({ message }) => {
      seen.push(message.data.n)
    })
    await q.produce('jobs', { n: 3 })

    await until(() => seen.length === 1)
    await q.close()
    expect(errors).toHaveLength(2)
  })
})

describe('infrastructure errors: the default reporter', () => {
  it('logs to console.error when the worker declares no onError', async () => {
    const logged = vi.spyOn(console, 'error').mockImplementation(noop)
    let failed = false
    const s = scripted((base) => ({
      pull: async (max) => {
        if (!failed) {
          failed = true
          throw new Error('blip')
        }
        return base.pull(max)
      },
    }))
    const q = createQueue(s.adapter, { queues, ...fast })
    q.subscribe('jobs', noop)

    await until(() => logged.mock.calls.length === 1)
    expect(logged.mock.calls[0]).toEqual([
      'queue: worker on "jobs" — retrying after',
      expect.objectContaining({ message: 'blip' }),
    ])
    await q.close()
  })
})

describe('worker events', () => {
  it('reports handled outcomes, scheduled retries and the dead-letter with attempt counts', async () => {
    const handled: unknown[] = []
    const retries: unknown[] = []
    const dead: unknown[] = []
    const events: QueueEvents = {
      handled: ({ durationMs, ...rest }) => {
        expect(durationMs).toBeGreaterThanOrEqual(0)
        handled.push(rest)
      },
      retryScheduled: (ctx) => retries.push(ctx),
    }
    // The dead-letter verdict is the ADAPTER's, so the fact surfaces on its sink.
    const adapter = memory({ events: { deadLettered: (ctx) => dead.push(ctx) } })
    const q = createQueue(adapter, {
      queues: withRetry({ maxAttempts: 2, deadLetter: 'jobs-dead', backoffMs: 5 }),
      ...fast,
      events,
    })

    q.subscribe({ queue: 'jobs' }, () => {
      throw new Error('always')
    })
    await q.produce('jobs', { n: 1 })

    await until(() => dead.length === 1)
    await q.close()
    expect(handled).toEqual([
      { queue: 'jobs', outcome: 'error', size: 1, failed: 1, attempt: 1 },
      { queue: 'jobs', outcome: 'error', size: 1, failed: 1, attempt: 2 },
    ])
    // Both failures are nacked to the adapter; the second is the exhausted
    // hand-off the adapter turned into a dead-letter (the manager cannot tell them apart).
    expect(retries).toEqual([
      { queue: 'jobs', attempt: 1, delayMs: 5 },
      { queue: 'jobs', attempt: 2, delayMs: 5 },
    ])
    expect(dead).toEqual([{ queue: 'jobs', deadLetter: 'jobs-dead', attempt: 2 }])
  })
})

describe('competing workers', () => {
  it('each message is handled by exactly one of the workers on a queue', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    const handledCount = new Map<number, number>()
    for (let w = 0; w < 3; w++) {
      q.subscribe({ queue: 'jobs', prefetch: 4 }, ({ message }) => {
        handledCount.set(message.data.n, (handledCount.get(message.data.n) ?? 0) + 1)
      })
    }

    for (let n = 0; n < 20; n++) await q.produce('jobs', { n })

    await until(() => handledCount.size === 20)
    await q.close()
    expect([...handledCount.values()].every((c) => c === 1)).toBe(true)
  })
})

describe('numeric options must be finite', () => {
  // Node turns an Infinity, NaN or negative timer delay into 1 ms with only a warning, so a
  // timeout that reads as "no limit" would fire at once.
  it('subscribe refuses Infinity, NaN and out-of-range values at the call site', async () => {
    const q = createQueue(memory(), { queues })
    const refused = (options: object): (() => unknown) => {
      return () => q.subscribe({ queue: 'jobs', ...options }, noop)
    }
    expect(refused({ consumeTimeout: Infinity })).toThrow(
      'queue: consumeTimeout must be a finite number of at least 1, got Infinity (omit it for no timeout)'
    )
    expect(refused({ consumeTimeout: 1_000, heartbeatIntervalMs: NaN })).toThrow(
      /heartbeatIntervalMs must be a finite number/
    )
    expect(refused({ closeTimeout: Infinity })).toThrow(/closeTimeout must be a finite number/)
    expect(refused({ closeTimeout: -5 })).toThrow(/closeTimeout must be a finite number/)
    expect(refused({ parallelism: Infinity })).toThrow(/parallelism must be a finite number/)
    expect(refused({ parallelism: NaN })).toThrow(/parallelism must be a finite number/)
    expect(refused({ prefetch: 0 })).toThrow(
      /prefetch must be a finite number of at least 1, got 0/
    )
    expect(refused({ pollIdleIntervalMs: -1 })).toThrow(
      /pollIdleIntervalMs must be a finite number of at least 0/
    )
    expect(refused({ pollBatchIntervalMs: Infinity })).toThrow(
      /pollBatchIntervalMs must be a finite number/
    )
    await q.close()
  })

  it('zero is a valid poll interval and close grace', async () => {
    const q = createQueue(memory(), { queues })
    const worker = q.subscribe(
      { queue: 'jobs', pollIdleIntervalMs: 0, pollBatchIntervalMs: 0, closeTimeout: 0 },
      noop
    )
    await worker.stop()
    await q.close()
  })

  it('manager-level worker defaults are checked once, at createQueue', () => {
    expect(() => createQueue(memory(), { queues, closeTimeout: Infinity })).toThrow(
      /closeTimeout must be a finite number/
    )
    expect(() => createQueue(memory(), { queues, pollIdleIntervalMs: NaN })).toThrow(
      /pollIdleIntervalMs must be a finite number/
    )
    expect(() => createQueue(memory(), { queues, pollBatchIntervalMs: -1 })).toThrow(
      /pollBatchIntervalMs must be a finite number/
    )
  })
})

describe('option combinations', () => {
  it('refuses the streaming flow on a batch handler: one pull IS its batch', async () => {
    const q = createQueue(memory(), { queues })
    expect(() => q.subscribe({ queue: 'jobs', batch: true, flow: 'streaming' }, noop)).toThrow(
      'queue: flow "streaming" is not applicable to a batch handler'
    )
    // The batched flow is a batch handler's default, and selectable for a single-message one.
    await q.subscribe({ queue: 'jobs', batch: true }, noop).stop()
    await q.subscribe({ queue: 'jobs', flow: 'batched' }, noop).stop()
    await q.close()
  })

  it('refuses an auto-heartbeat without consumeTimeout: it would keep a wedged handler alive', async () => {
    const q = createQueue(memory(), { queues })
    expect(() => q.subscribe({ queue: 'jobs', heartbeatIntervalMs: 50 }, noop)).toThrow(
      'queue: heartbeatIntervalMs requires consumeTimeout'
    )
    await q.close()
  })
})
