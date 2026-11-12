import { memory } from '../adapters/memory'
import {
  type BatchResult,
  type ConsumeCall,
  createQueue,
  defineQueues,
  type QueueEvents,
  type QueueMiddleware,
  queue,
} from './index'
import { defer, fast, forever, pulled, scripted, sleep, until, withRetry } from './test-helpers'

interface Job {
  n: number
}

const queues = defineQueues({ jobs: queue<Job>() })
const noop = (): void => {}
const nOf = (call: ConsumeCall): number[] => call.messages.map((m) => (m.data as Job).n)

/** A stage that records what `next` resolved with, or what it threw. */
const observer = (): { results: BatchResult[]; thrown: unknown[]; middleware: QueueMiddleware } => {
  const results: BatchResult[] = []
  const thrown: unknown[] = []
  return {
    results,
    thrown,
    middleware: {
      consume: (next) => async (call) => {
        try {
          const result = await next(call)
          results.push(result)
          return result
        } catch (error) {
          thrown.push(error)
          throw error
        }
      },
    },
  }
}

interface Handled {
  outcome: string
  size: number
  failed: number
}

const recordHandled = (): { handled: Handled[]; events: QueueEvents } => {
  const handled: Handled[] = []
  return {
    handled,
    events: {
      handled: ({ outcome, size, failed }) => void handled.push({ outcome, size, failed }),
    },
  }
}

describe('consume middleware: composition', () => {
  it('composes manager-level outside worker-level and observes the result as data', async () => {
    const order: string[] = []
    const results: BatchResult[] = []
    const stage = (name: string): QueueMiddleware => ({
      consume: (next) => async (call) => {
        order.push(`${name}:before:${call.queue}:${call.signal.aborted}`)
        const result = await next(call)
        order.push(`${name}:after`)
        if (name === 'manager') results.push(result)
        return result
      },
    })
    const q = createQueue(memory(), { queues, ...fast, middleware: [stage('manager')] })
    await q.produce('jobs', { n: 1 })

    q.subscribe({ queue: 'jobs', middleware: [stage('worker')] }, () => void order.push('handler'))

    await until(() => order.includes('manager:after'))
    await q.close()
    expect(order).toEqual([
      'manager:before:jobs:false',
      'worker:before:jobs:false',
      'handler',
      'worker:after',
      'manager:after',
    ])
    expect(results).toEqual([{ outcome: 'ok', size: 1, ok: 1, released: 0, failures: [] }])
  })

  it('runs once per handler invocation: per message, or per batch slice', async () => {
    const sizes: Record<string, number[][]> = { single: [], batch: [] }
    const stage = (name: string): QueueMiddleware => ({
      consume: (next) => (call) => {
        sizes[name].push(nOf(call))
        return next(call)
      },
    })
    const single = scripted()
    single.feed([pulled(0), pulled(1), pulled(2)])
    const batch = scripted()
    batch.feed([pulled(0), pulled(1), pulled(2)])
    const a = createQueue(single.adapter, { queues, ...fast, middleware: [stage('single')] })
    const b = createQueue(batch.adapter, { queues, ...fast, middleware: [stage('batch')] })

    a.subscribe('jobs', noop)
    b.subscribe({ queue: 'jobs', batch: true }, noop)

    await until(() => single.receipts('ack').length === 3 && batch.receipts('ack').length === 3)
    await Promise.all([a.close(), b.close()])
    expect(sizes).toEqual({ single: [[0], [1], [2]], batch: [[0, 1, 2]] })
  })

  it('reports handler outcomes as data: ok, error and a timeout release', async () => {
    const { results, thrown, middleware } = observer()
    const s = scripted()
    s.feed([pulled(0), pulled(1), pulled(2)])
    const q = createQueue(s.adapter, { queues, ...fast, middleware: [middleware] })
    const boom = new Error('boom')

    q.subscribe(
      { queue: 'jobs', prefetch: 3, parallelism: 3, consumeTimeout: 20 },
      async ({ message }) => {
        if (message.data.n === 1) throw boom
        if (message.data.n === 2) await forever()
      }
    )

    await until(() => results.length === 3)
    await q.close()
    expect(thrown).toEqual([]) // a handler failure never arrives as a throw
    expect(results).toEqual([
      { outcome: 'ok', size: 1, ok: 1, released: 0, failures: [] },
      {
        outcome: 'error',
        size: 1,
        ok: 0,
        released: 0,
        failures: [{ message: expect.objectContaining({ data: { n: 1 } }), error: boom }],
      },
      { outcome: 'timeout', size: 1, ok: 0, released: 1, failures: [] },
    ])
  })
})

describe('consume middleware: transforms and rejection', () => {
  it('a before-next 1:1 transform is what the handler sees', async () => {
    const transform: QueueMiddleware = {
      consume: (next) => async (call) => {
        call.messages = call.messages.map((m) => ({
          ...m,
          headers: { ...m.headers, decrypted: 'yes' },
        }))
        return next(call)
      },
    }
    const s = scripted()
    s.feed([pulled(0), pulled(1)])
    const q = createQueue(s.adapter, { queues, ...fast, middleware: [transform] })
    const seen: unknown[] = []

    q.subscribe({ queue: 'jobs', batch: true }, ({ messages }) => {
      seen.push(...messages.map((m) => [m.data.n, m.headers.decrypted]))
    })

    await until(() => s.calls.length === 1)
    await q.close()
    expect(seen).toEqual([
      [0, 'yes'],
      [1, 'yes'],
    ])
    expect(s.calls[0]).toBe('ack r0,r1') // settles ride the ORIGINAL deliveries
  })

  it('an assignment that adds or drops messages throws and fails the call loudly', async () => {
    const { results, thrown, middleware } = observer()
    const dropper: QueueMiddleware = {
      consume: (next) => async (call) => {
        call.messages = call.messages.slice(1) // contract violation
        return next(call)
      },
    }
    const s = scripted()
    s.feed([pulled(0), pulled(1)])
    const { handled, events } = recordHandled()
    const q = createQueue(s.adapter, {
      queues: withRetry({ maxAttempts: 3 }),
      ...fast,
      middleware: [middleware],
      events,
    })
    const errors: unknown[] = []
    let calls = 0

    q.subscribe(
      {
        queue: 'jobs',
        batch: true,
        middleware: [dropper],
        onError: (e) => errors.push(e),
      },
      () => void calls++
    )

    await until(() => s.calls.length === 1)
    await q.close()
    // An INNER stage's throw unwinds through outer stages as an exception (only HANDLER
    // failures arrive as data) and fails the call's messages. It is not an infrastructure
    // error, so onError stays silent.
    expect(String(thrown[0])).toMatch(/must not add or remove messages/)
    expect(results).toEqual([])
    expect(s.calls[0]).toBe('nack r0@0,r1@0')
    expect(handled).toEqual([{ outcome: 'error', size: 2, failed: 2 }])
    expect(calls).toBe(0)
    expect(errors).toEqual([])
  })

  it.each([
    ['retries it under a policy', { maxAttempts: 3, backoffMs: 4 }, 'nack r1@4'],
    ['releases it without one', undefined, 'release r1'],
  ] as const)(
    'reject drops a message from the batch and %s; the rest runs and acks',
    async (_name, retry, settled) => {
      const { results, middleware } = observer()
      const bad = new Error('bad signature')
      const guard: QueueMiddleware = {
        consume: (next) => async (call) => {
          for (const m of [...call.messages]) if ((m.data as Job).n === 1) call.reject(m, bad)
          return next(call)
        },
      }
      const s = scripted()
      s.feed([pulled(0), pulled(1), pulled(2)])
      const { handled, events } = recordHandled()
      const q = createQueue(s.adapter, {
        queues: withRetry(retry),
        ...fast,
        middleware: [middleware, guard],
        events,
      })
      const seen: number[][] = []

      q.subscribe({ queue: 'jobs', batch: true }, ({ messages }) => {
        seen.push(messages.map((m) => m.data.n))
      })

      await until(() => s.calls.length === 2)
      await q.close()
      expect(seen).toEqual([[0, 2]]) // the rejected message never reached the handler
      expect(s.calls.slice(0, 2)).toEqual(['ack r0,r2', settled])
      expect(results).toEqual([
        {
          outcome: 'error',
          size: 3,
          ok: 2,
          released: 0,
          failures: [{ message: expect.objectContaining({ data: { n: 1 } }), error: bad }],
        },
      ])
      expect(handled).toEqual([{ outcome: 'error', size: 3, failed: 1 }])
    }
  )

  it('rejecting the only message of a call never invokes the handler', async () => {
    const guard: QueueMiddleware = {
      consume: (next) => async (call) => {
        if ((call.messages[0].data as Job).n === 1) call.reject(call.messages[0])
        return next(call)
      },
    }
    const s = scripted()
    s.feed([pulled(0), pulled(1)])
    const q = createQueue(s.adapter, { queues, ...fast, middleware: [guard] })
    const seen: number[] = []

    q.subscribe('jobs', ({ message }) => void seen.push(message.data.n))

    await until(() => s.receipts('release').length === 1)
    await q.close()
    expect(seen).toEqual([0])
    expect(s.receipts('ack')).toEqual(['r0'])
    expect(s.receipts('release')).toEqual(['r1'])
  })

  it('reject throws for a message outside the batch, and after next', async () => {
    const thrown: string[] = []
    const misuse: QueueMiddleware = {
      consume: (next) => async (call) => {
        const attempt = (message = call.messages[0]): void => {
          try {
            call.reject(message)
          } catch (error) {
            thrown.push((error as Error).message)
          }
        }
        attempt({ ...call.messages[0] }) // a copy is not the batch's own message
        const result = await next(call)
        attempt()
        return result
      },
    }
    const s = scripted()
    s.feed([pulled(0)])
    const q = createQueue(s.adapter, { queues, ...fast, middleware: [misuse] })
    let calls = 0

    q.subscribe('jobs', () => void calls++)

    await until(() => s.calls.length === 1)
    await q.close()
    expect(thrown).toEqual([
      'queue: reject() called with a message that is not in this batch',
      'queue: reject() after next() — rejection is a before-next operation',
    ])
    // Neither refused rejection took effect: the message ran and acked.
    expect(calls).toBe(1)
    expect(s.calls[0]).toBe('ack r0')
  })
})

describe('consume middleware: it cannot change settle semantics', () => {
  it('cannot swallow or hide a handler failure: the nack still lands', async () => {
    let caught = 0
    const seen: BatchResult[] = []
    const hider: QueueMiddleware = {
      consume: (next) => async (call) => {
        try {
          const result = await next(call)
          seen.push(result)
          return { ...result, outcome: 'ok', failures: [] }
        } catch {
          caught++
          throw new Error('unreachable')
        }
      },
    }
    const s = scripted()
    s.feed([pulled(0)])
    const { handled, events } = recordHandled()
    const q = createQueue(s.adapter, {
      queues: withRetry({ maxAttempts: 3 }),
      ...fast,
      middleware: [hider],
      events,
    })

    q.subscribe({ queue: 'jobs' }, () => {
      throw new Error('boom')
    })

    await until(() => s.calls.length === 1)
    await q.close()
    expect(caught).toBe(0) // the failure arrived as data, not a throw
    expect((seen[0].failures[0].error as Error).message).toBe('boom')
    expect(s.calls[0]).toBe('nack r0@0')
    expect(handled).toEqual([{ outcome: 'error', size: 1, failed: 1 }])
  })

  it.each([false, true])(
    'a throw after next fails every message of the call, completed or not (batch: %s)',
    async (batch) => {
      const afterThrow: QueueMiddleware = {
        consume: (next) => async (call) => {
          await next(call)
          throw new Error('after-half')
        },
      }
      const s = scripted()
      s.feed([pulled(0), pulled(1)])
      const { handled, events } = recordHandled()
      const q = createQueue(s.adapter, {
        queues: withRetry({ maxAttempts: 3, backoffMs: 1 }),
        ...fast,
        middleware: [afterThrow],
        events,
      })
      let calls = 0
      const options = {
        queue: 'jobs',
        prefetch: 2,
      } as const

      if (batch) q.subscribe({ ...options, batch: true }, () => void calls++)
      else q.subscribe(options, () => void calls++)

      await until(() => s.receipts('nack').length === 2)
      await q.close()
      expect(s.receipts('nack').sort()).toEqual(['r0', 'r1'])
      expect(s.receipts('ack')).toEqual([])
      expect(calls).toBe(batch ? 1 : 2) // the handler did complete
      expect(handled).toEqual(
        batch
          ? [{ outcome: 'error', size: 2, failed: 2 }]
          : [
              { outcome: 'error', size: 1, failed: 1 },
              { outcome: 'error', size: 1, failed: 1 },
            ]
      )
    }
  )

  it.each([
    [
      'a stage that throws before next',
      (): QueueMiddleware => ({
        consume: () => async () => {
          throw new Error('before')
        },
      }),
    ],
    [
      'a stage whose wrap throws synchronously',
      (): QueueMiddleware => ({
        consume: () => {
          throw new Error('before')
        },
      }),
    ],
  ] as const)(
    '%s fails the call, still reports handled, never runs the handler',
    async (_name, bomb) => {
      const s = scripted()
      s.feed([pulled(0)])
      const { handled, events } = recordHandled()
      const q = createQueue(s.adapter, { queues, ...fast, events })
      const errors: unknown[] = []
      let calls = 0

      q.subscribe(
        { queue: 'jobs', batch: true, middleware: [bomb()], onError: (e) => errors.push(e) },
        () => void calls++
      )

      await until(() => s.calls.length === 1)
      await q.close()
      expect(s.calls[0]).toBe('release r0') // failed without a policy: released
      expect(handled).toEqual([{ outcome: 'error', size: 1, failed: 1 }])
      expect(calls).toBe(0)
      expect(errors).toEqual([])
    }
  )
})

describe('consume middleware: the next contract', () => {
  it('a stage that never calls next fails the message loudly instead of acking it', async () => {
    const { thrown, results, middleware } = observer()
    const skip: QueueMiddleware = {
      consume: () => async () => ({ outcome: 'ok', size: 1, ok: 1, released: 0, failures: [] }),
    }
    const s = scripted()
    s.feed([pulled(0)])
    const { handled, events } = recordHandled()
    const scheduled: number[] = []
    const q = createQueue(s.adapter, {
      queues: withRetry({ maxAttempts: 3, backoffMs: 1 }),
      ...fast,
      middleware: [middleware, skip],
      events: { ...events, retryScheduled: ({ attempt }) => scheduled.push(attempt) },
    })
    let calls = 0

    q.subscribe({ queue: 'jobs' }, () => void calls++)

    await until(() => s.calls.length === 1)
    await q.close()
    expect(calls).toBe(0)
    expect(s.calls[0]).toBe('nack r0@1')
    expect(scheduled).toEqual([1])
    expect(handled).toEqual([{ outcome: 'error', size: 1, failed: 1 }])
    // The skip resolved: the verdict is the worker's, not a throw through the chain.
    expect(thrown).toEqual([])
    expect(results).toHaveLength(1)
  })

  it('a second next throws and fails the call; the handler ran once', async () => {
    const { thrown, middleware } = observer()
    const twice: QueueMiddleware = {
      consume: (next) => async (call) => {
        await next(call)
        return next(call)
      },
    }
    const s = scripted()
    s.feed([pulled(0)])
    const q = createQueue(s.adapter, { queues, ...fast, middleware: [middleware, twice] })
    let calls = 0

    q.subscribe('jobs', () => void calls++)

    await until(() => s.calls.length === 1)
    await q.close()
    expect((thrown[0] as Error).message).toMatch(/called next more than once/)
    expect(calls).toBe(1) // the second next never re-ran the handler
    expect(s.calls[0]).toBe('release r0')
  })

  it('a stage that returns without awaiting next leaves the handler to conclude the call', async () => {
    const eager: QueueMiddleware = {
      consume: (next) => async (call) => {
        void next(call)
        return { outcome: 'ok', size: 1, ok: 1, released: 0, failures: [] }
      },
    }
    const s = scripted()
    s.feed([pulled(0)])
    const q = createQueue(s.adapter, { queues, ...fast, middleware: [eager] })
    const gate = defer()
    let started = false

    q.subscribe('jobs', async () => {
      started = true
      await gate.promise
      throw new Error('late failure')
    })

    await until(() => started)
    await sleep(20)
    expect(s.calls).toEqual([]) // the chain resolved, yet nothing settles before the handler
    gate.resolve()
    await until(() => s.calls.length === 1)
    expect(s.calls[0]).toBe('release r0') // and the handler's own verdict stands
    await q.close()
  })
})

describe('consume middleware: the attempt bound covers the stages', () => {
  it.each([
    ['consumeTimeout', 'timeout'],
    ['closeTimeout', 'shutdown'],
  ] as const)(
    '%s fires in a before-half: released, the handler never starts, a late next sees the release',
    async (bound, outcome) => {
      const gate = defer()
      const late: BatchResult[] = []
      const slow: QueueMiddleware = {
        consume: (next) => async (call) => {
          await gate.promise
          const result = await next(call)
          late.push(result)
          return result
        },
      }
      const s = scripted()
      s.feed([pulled(0)])
      const { handled, events } = recordHandled()
      const q = createQueue(s.adapter, { queues, ...fast, middleware: [slow], events })
      let calls = 0

      const worker = q.subscribe(
        {
          queue: 'jobs',
          ...(bound === 'consumeTimeout' ? { consumeTimeout: 20 } : { closeTimeout: 20 }),
        },
        () => void calls++
      )

      await until(() => s.pulls.length > 0)
      if (bound === 'closeTimeout') await worker.stop()
      else await until(() => s.calls.length === 1)
      expect(s.calls[0]).toBe('release r0')
      expect(handled).toEqual([{ outcome, size: 1, failed: 0 }])

      gate.resolve() // the abandoned stage resumes and calls next
      await until(() => late.length === 1)
      expect(late).toEqual([{ outcome, size: 1, ok: 0, released: 1, failures: [] }])
      expect(calls).toBe(0)
      await worker.stop()
      await q.close()
    }
  )

  it.each([
    ['consumeTimeout', 'TimeoutError'],
    ['closeTimeout', 'AbortError'],
  ] as const)(
    'a stage hanging after next is freed by %s: the handler settle is honoured and acks',
    async (bound, reason) => {
      const reasons: string[] = []
      const hangAfterNext: QueueMiddleware = {
        consume: (next) => async (call) => {
          const result = await next(call)
          await new Promise<void>((resolve) => {
            call.signal.addEventListener('abort', () => resolve(), { once: true })
          })
          reasons.push((call.signal.reason as DOMException).name)
          return result
        },
      }
      const s = scripted()
      s.feed([pulled(0)])
      const { handled, events } = recordHandled()
      const q = createQueue(s.adapter, { queues, ...fast, middleware: [hangAfterNext], events })
      let calls = 0

      const worker = q.subscribe(
        {
          queue: 'jobs',
          ...(bound === 'consumeTimeout' ? { consumeTimeout: 20 } : { closeTimeout: 20 }),
        },
        () => void calls++
      )

      await until(() => calls === 1)
      if (bound === 'closeTimeout') {
        await sleep(30)
        expect(s.calls).toEqual([]) // no bound of its own: only stop() can end the wait
        await worker.stop()
      } else await until(() => s.calls.length === 1)
      expect(s.calls[0]).toBe('ack r0') // acked on the handler's own return
      expect(handled).toEqual([{ outcome: 'ok', size: 1, failed: 0 }])
      await until(() => reasons.length === 1)
      expect(reasons).toEqual([reason])
      await q.close()
    }
  )
})
