import { memory } from '../adapters/memory'
import { createQueue, defineQueues, type Queue, queue } from './index'
import {
  defer,
  fast,
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

const queues = defineQueues({ 'jobs-dead': queue<Job>(), jobs: queue<Job>() })

const produce = (q: Queue<typeof queues>, ...ns: number[]): Promise<void> =>
  q.produce(
    'jobs',
    ns.map((n) => ({ data: { n } }))
  )

describe('streaming flow: the sliding window', () => {
  it('is the default for a single-message handler: fast messages flow around a slow one', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await produce(q, 0, 1, 2, 3, 4)
    const slow = defer()
    const fastDone: number[] = []
    let slowDone = false

    // No `flow` declared. Under the batched flow (prefetch 2) n=2..4 could not deliver while
    // n=0 still runs, so seeing them finish first IS seeing the streaming default.
    q.subscribe({ queue: 'jobs', prefetch: 2, parallelism: 2 }, async ({ message }) => {
      if (message.data.n === 0) {
        await slow.promise
        slowDone = true
      } else fastDone.push(message.data.n)
    })

    await until(() => fastDone.length === 4)
    expect(slowDone).toBe(false)
    slow.resolve()
    await q.close() // no closeTimeout: the slow one drains naturally
    expect(slowDone).toBe(true)
  })

  it('prefetch caps concurrency even when parallelism exceeds it', async () => {
    const q = createQueue(memory(), { queues, ...fast })
    await produce(q, ...Array.from({ length: 12 }, (_, n) => n))
    let active = 0
    let maxActive = 0
    const done: number[] = []

    q.subscribe({ queue: 'jobs', prefetch: 3, parallelism: 8 }, async ({ message }) => {
      active++
      maxActive = Math.max(maxActive, active)
      await sleep(5)
      active--
      done.push(message.data.n)
    })

    await until(() => done.length === 12)
    await q.close()
    expect(maxActive).toBeGreaterThanOrEqual(2) // genuinely concurrent
    expect(maxActive).toBeLessThanOrEqual(3) // but never past the window
  })

  it('every message acks exactly once and acks coalesce into fewer round trips', async () => {
    let ackCalls = 0
    const acked: string[] = []
    const adapter = patchSubscriptions(memory(), (subscription) => ({
      ack: async (receipts) => {
        ackCalls++
        acked.push(...receipts)
        // A settle round trip that takes time: completions landing during it must coalesce
        // into the NEXT flush.
        await sleep(5)
        return subscription.ack(receipts)
      },
    }))
    const q = createQueue(adapter, { queues, ...fast })
    await produce(q, ...Array.from({ length: 24 }, (_, n) => n))
    const done: number[] = []

    const worker = q.subscribe({ queue: 'jobs', prefetch: 8, parallelism: 4 }, async (ctx) => {
      await sleep(1)
      done.push(ctx.message.data.n)
    })

    await until(() => done.length === 24)
    await worker.stop() // the final flush lands before stop() returns
    expect(new Set(done).size).toBe(24)
    expect(new Set(acked).size).toBe(24)
    expect(acked).toHaveLength(24)
    expect(ackCalls).toBeLessThan(24)
    const [stats] = (await q.stats({ queues: ['jobs'] })).queues
    expect([stats.backlog?.count, stats.inFlight?.count]).toEqual([0, 0])
    await q.close()
  })

  it('a slow settle never lets pulled-but-unsettled messages grow past prefetch', async () => {
    let held = 0
    let maxHeld = 0
    const adapter = patchSubscriptions(memory(), (subscription) => ({
      pull: async (max) => {
        const messages = await subscription.pull(max)
        held += messages.length
        maxHeld = Math.max(maxHeld, held)
        return messages
      },
      ack: async (receipts) => {
        // Completed-but-unflushed receipts count against the window, so the puller waits
        // for this to land instead of refilling around it.
        await sleep(5)
        await subscription.ack(receipts)
        held -= receipts.length
      },
    }))
    const q = createQueue(adapter, { queues, ...fast })
    await produce(q, ...Array.from({ length: 20 }, (_, n) => n))
    const done: number[] = []

    q.subscribe({ queue: 'jobs', prefetch: 4, parallelism: 2 }, ({ message }) => {
      done.push(message.data.n)
    })

    await until(() => done.length === 20)
    await q.close()
    expect(maxHeld).toBeLessThanOrEqual(4)
    expect(new Set(done).size).toBe(20)
  })

  it('parallelism 1 still pipelines: the pull-ahead keeps one sequential lane fed', async () => {
    const s = scripted()
    s.feed([pulled(0), pulled(1), pulled(2), pulled(3)], [pulled(4)])
    const q = createQueue(s.adapter, { queues, ...fast })
    const gate = defer()
    const done: number[] = []

    q.subscribe({ queue: 'jobs', prefetch: 4 }, async ({ message }) => {
      if (message.data.n === 0) await gate.promise
      done.push(message.data.n)
    })

    // The window is full while the first message runs: the puller parks without pulling.
    await until(() => s.pulls.length === 1)
    await sleep(20)
    expect(s.pulls).toEqual([4])
    gate.resolve()
    await until(() => done.length === 5)
    expect(done).toEqual([0, 1, 2, 3, 4]) // one lane: strictly in delivery order
    await q.close()
  })

  it('a completion nudges the puller out of its idle sleep', async () => {
    let emptyPulls = 0
    const adapter = patchSubscriptions(memory(), (subscription) => ({
      pull: async (max) => {
        const out = await subscription.pull(max)
        if (out.length === 0) emptyPulls++
        return out
      },
    }))
    // A deliberately absurd idle interval: if the puller waits it out, the test times out.
    const q = createQueue(adapter, { queues, pollIdleIntervalMs: 60_000, pollBatchIntervalMs: 0 })
    await produce(q, 0)
    const gate = defer()
    const done: number[] = []

    q.subscribe({ queue: 'jobs', prefetch: 4, parallelism: 2 }, async ({ message }) => {
      if (message.data.n === 0) await gate.promise
      done.push(message.data.n)
    })

    await until(() => emptyPulls === 1) // caught up: parked in the 60s idle sleep
    await produce(q, 1)
    gate.resolve() // n=0's flush lands and must nudge an early re-pull
    await until(() => done.length === 2)
    expect(done).toEqual([0, 1])
    await q.close()
  })
})

describe('streaming flow: per-message settle', () => {
  it('a failure is nacked individually under a policy while its neighbours ack', async () => {
    const s = scripted()
    s.feed([pulled(0), pulled(1), pulled(2, 4), pulled(3)])
    const scheduled: unknown[] = []
    const q = createQueue(s.adapter, {
      queues: withRetry({ maxAttempts: 5, backoffMs: 7 }),
      ...fast,
      events: { retryScheduled: (ctx) => scheduled.push(ctx) },
    })

    q.subscribe({ queue: 'jobs', prefetch: 4, parallelism: 2 }, ({ message }) => {
      if (message.data.n === 2) throw new Error('boom')
    })

    await until(() => s.receipts('ack').length === 3 && s.receipts('nack').length === 1)
    expect(s.receipts('ack').sort()).toEqual(['r0', 'r1', 'r3'])
    expect(s.calls).toContain('nack r2@7') // the policy's backoff is the retry's delay
    expect(s.receipts('release')).toEqual([])
    expect(scheduled).toEqual([{ queue: 'jobs', attempt: 4, delayMs: 7 }])
    await q.close()
  })

  it('a failure without a policy is released, never nacked', async () => {
    const s = scripted()
    s.feed([pulled(0), pulled(1)])
    const scheduled: unknown[] = []
    const q = createQueue(s.adapter, {
      queues,
      ...fast,
      events: { retryScheduled: (ctx) => scheduled.push(ctx) },
    })

    q.subscribe('jobs', ({ message }) => {
      if (message.data.n === 1) throw new Error('boom')
    })

    await until(() => s.receipts('release').length === 1)
    expect(s.receipts('ack')).toEqual(['r0'])
    expect(s.receipts('release')).toEqual(['r1'])
    expect(s.receipts('nack')).toEqual([])
    expect(scheduled).toEqual([])
    await q.close()
  })

  it('a payload the codec rejects fails only that message, against its retry budget', async () => {
    const dead: number[] = []
    const adapter = memory({
      codec: poisonCodec(2),
      events: { deadLettered: ({ attempt }) => dead.push(attempt) },
    })
    const q = createQueue(adapter, { queues: withRetry({ maxAttempts: 2, backoffMs: 1 }), ...fast })
    await produce(q, 1, 2, 3)
    const handled: number[] = []

    q.subscribe({ queue: 'jobs' }, ({ message }) => {
      handled.push(message.data.n)
    })

    // The neighbours run; the poison message counts its budget down and dies instead of
    // being released to redeliver forever with attempt 1.
    await until(() => dead.length === 1)
    expect(dead).toEqual([2])
    expect([...handled].sort()).toEqual([1, 3])
    await q.close()
  })
})

describe('streaming flow: stop', () => {
  it('releases the un-started remainder promptly, and a successor consumes it at once', async () => {
    const handedBack: string[] = []
    const adapter = patchSubscriptions(memory(), (subscription) => ({
      release: async (receipts) => {
        handedBack.push(...receipts)
        return subscription.release(receipts)
      },
    }))
    const q = createQueue(adapter, { queues, ...fast })
    await produce(q, ...Array.from({ length: 10 }, (_, n) => n))
    const gate = defer()
    const first: number[] = []
    const second: number[] = []
    let active = 0

    const worker = q.subscribe({ queue: 'jobs', prefetch: 10, parallelism: 2 }, async (ctx) => {
      active++
      await gate.promise
      first.push(ctx.message.data.n)
    })

    // Both lanes busy, eight more fetched-but-unstarted in the window.
    await until(() => active === 2)
    const stopping = worker.stop()
    await until(() => handedBack.length === 8) // released while the two in flight still run
    expect(first).toEqual([])
    gate.resolve()
    await stopping
    expect(first).toHaveLength(2)
    expect(active).toBe(2) // stop() started nothing new

    q.subscribe({ queue: 'jobs', prefetch: 10 }, ({ message }) => {
      second.push(message.data.n)
    })
    await until(() => second.length === 8)
    expect([...first, ...second].sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    await q.close()
  })
})

describe('streaming flow: a receipt re-issued while its predecessor settles', () => {
  it('stays held: the window and the heartbeat count the new delivery', async () => {
    // The puller overlaps the flusher, so a pull can return a receipt again after its release
    // took effect at the adapter but before the release's promise resolves and the flush does
    // its bookkeeping. Script exactly that ordering.
    const released = defer() // the real release has taken effect at the adapter
    const releaseReturn = defer() // the worker may now observe the release as returned
    const secondStarted = defer()
    const proceed = defer()
    const pulls: number[] = []
    const beats: string[][] = []
    let delivered = false
    const adapter = patchSubscriptions(reissuingBroker().adapter, (subscription) => ({
      pull: async (max) => {
        // Once the message is out, the next pull completes only after its release took effect:
        // that pull is the one that re-issues the receipt.
        if (delivered) await released.promise
        const messages = await subscription.pull(max)
        pulls.push(max)
        if (messages.length > 0) delivered = true
        return messages
      },
      release: async (receipts) => {
        await subscription.release(receipts) // takes effect: the message is deliverable again
        released.resolve()
        await releaseReturn.promise // but the worker sees the release return only when told
      },
      heartbeat: async (receipts) => {
        beats.push([...receipts])
      },
    }))
    const q = createQueue(adapter, { queues, ...fast })
    await produce(q, 0)
    let deliveries = 0

    q.subscribe({ queue: 'jobs', prefetch: 2 }, async ({ heartbeat }) => {
      deliveries++
      if (deliveries === 1) throw new Error('boom') // no policy: so a release
      secondStarted.resolve()
      await proceed.promise
      await heartbeat()
    })

    // The re-issued delivery is in the lane while its predecessor's release has not returned.
    await secondStarted.promise
    releaseReturn.resolve() // now it returns and the flush's bookkeeping runs
    // The puller keeps polling. Every pull from here on must leave one credit for the
    // delivery still in the lane; a set-based window would have dropped it when the release
    // landed and asked for the full window.
    const settledAt = pulls.length
    await until(() => pulls.length >= settledAt + 3)
    expect(pulls.slice(settledAt + 1).every((max) => max === 1)).toBe(true)

    // And that delivery is still on the heartbeat surface.
    proceed.resolve()
    await until(() => beats.length >= 1)
    expect(beats[0]).toEqual(['id-0'])
    await q.close()
    expect(deliveries).toBe(2)
  })

  it('a release that threw is never re-issued, so it cannot release the new delivery', async () => {
    // The release lands at the adapter, a pull hands the receipt to a lane, and only THEN does
    // the release call reject, as a connection dropped after the statement committed would. A
    // compensating second release would free the delivery in the lane and a third delivery
    // would start while the second still runs.
    const released = defer()
    const secondStarted = defer()
    const proceed = defer()
    const releaseBatches: string[][] = []
    const errors: unknown[] = []
    let delivered = false
    let pullsAfterFailure = 0
    const { adapter: broker, acked } = reissuingBroker()
    const adapter = patchSubscriptions(broker, (subscription) => ({
      pull: async (max) => {
        if (delivered) await released.promise
        const messages = await subscription.pull(max)
        if (messages.length > 0) delivered = true
        if (errors.length > 0) pullsAfterFailure++
        return messages
      },
      release: async (receipts) => {
        releaseBatches.push([...receipts])
        await subscription.release(receipts) // lands: the message is deliverable again
        released.resolve()
        await secondStarted.promise // the re-issued delivery is running in a lane
        throw new Error('release response lost')
      },
    }))
    const q = createQueue(adapter, { queues, ...fast })
    await produce(q, 0)
    let deliveries = 0

    q.subscribe(
      { queue: 'jobs', prefetch: 2, parallelism: 2, onError: (e) => errors.push(e) },
      async () => {
        deliveries++
        if (deliveries === 1) throw new Error('boom')
        secondStarted.resolve()
        await proceed.promise
      }
    )

    await secondStarted.promise
    await until(() => errors.length === 1)
    expect(errors[0]).toMatchObject({ message: 'release response lost' })
    // The puller keeps polling with a lane free: a re-issued release would have released the
    // running delivery and a third one would have started.
    await until(() => pullsAfterFailure >= 3)
    expect(deliveries).toBe(2)
    expect(releaseBatches).toEqual([['id-0']])

    proceed.resolve()
    await q.close()
    expect(deliveries).toBe(2)
    expect([...acked]).toEqual(['id-0'])
  })
})

describe('streaming flow: a failed settle', () => {
  it('a throwing onError never wedges the flusher: later settles still land', async () => {
    let outage = true
    let acked = 0
    const adapter = patchSubscriptions(memory(), (subscription) => ({
      ack: async (receipts) => {
        if (outage) {
          outage = false
          throw new Error('ack outage')
        }
        await subscription.ack(receipts)
        acked += receipts.length
      },
    }))
    const reported: unknown[] = []
    const q = createQueue(adapter, {
      queues,
      ...fast,
      events: { workerError: ({ queue: name, error }) => reported.push([name, error]) },
    })
    await produce(q, 0, 1, 2, 3, 4, 5)

    q.subscribe(
      {
        queue: 'jobs',
        prefetch: 2,
        onError: () => {
          throw new Error('reporter bug')
        },
      },
      () => {}
    )

    // The first flush fails and its reporter throws. The compensation releases those
    // receipts for redelivery and every later flush lands; a wedged flush chain would leave
    // every settle after the first queued forever.
    await until(() => acked === 6)
    expect(reported).toEqual([['jobs', expect.objectContaining({ message: 'ack outage' })]])
    await q.close()
  })
})
