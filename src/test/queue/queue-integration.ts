/**
 * Queue-logic integration suite: the manager driven end to end over a real engine, through
 * its public surface only, with test-only queues, messages and handlers. A helper module, not
 * a test file: call `describeQueueIntegration` from an engine's own `*.test.ts`.
 *
 * Queue names are fixed (`qi_*`), so the target must isolate each run itself: a throwaway
 * schema, a per-run name prefix. `createAdapter()` is called once per manager, several times
 * in the tests that hand work from one manager to another; every adapter it returns must
 * reach the same backend.
 */
import {
  type BatchContext,
  BatchFailure,
  type BatchResult,
  createQueue,
  defineMessage,
  defineQueues,
  type JobContext,
  type Message,
  type QueueAdapter,
  type QueueConfig,
  QueueHandler,
  type QueueMap,
  type QueueMiddleware,
  type QueueOptions,
  queue,
} from '@internal/queue/manager'

export interface QueueIntegrationTarget {
  readonly name: string
  // biome-ignore lint/suspicious/noExplicitAny: the suite is generic over the wire and ctx types
  createAdapter(): QueueAdapter<any, any>
  /** Producer dedup: the options of the queue that test runs on. Omit to skip it. */
  readonly dedup?: { readonly queueOptions?: QueueOptions }
  /** A release (shutdown, `consumeTimeout`) does not consume retry budget. Omit where
   * the engine counts every delivery. */
  readonly budgetFreeRelease?: boolean
  /** The shortest liveness contract the engine honours. The heartbeat test holds one message
   * for 1.6x this long; omit to skip it. */
  readonly livenessMs?: number
  /** Ceiling of every eventually-consistent wait. Default 10000. */
  readonly waitMs?: number
}

interface Payload {
  n: number
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))
const aborted = (signal: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    if (signal.aborted) resolve()
    else signal.addEventListener('abort', () => resolve(), { once: true })
  })

export function describeQueueIntegration(target: QueueIntegrationTarget): void {
  const wait = target.waitMs ?? 10_000
  const timeout = 3 * wait

  /** Re-run `fn` until it stops throwing, or rethrow its last failure at the deadline. */
  const until = async <T>(fn: () => T | Promise<T>, timeoutMs = wait): Promise<T> => {
    const deadline = Date.now() + timeoutMs
    for (;;) {
      try {
        return await fn()
      } catch (error) {
        if (Date.now() >= deadline) throw error
      }
      await sleep(25)
    }
  }

  const open: { close(): Promise<void> }[] = []
  /** A started manager over `adapter` (default: a fresh one), closed after the test. */
  const start = async <M extends QueueMap>(
    config: QueueConfig<M>,
    adapter = target.createAdapter()
  ) => {
    const manager = createQueue(adapter, {
      pollIdleIntervalMs: 50,
      pollBatchIntervalMs: 10,
      closeTimeout: 500,
      statsTtlMs: 0,
      ...config,
    })
    open.push(manager)
    await manager.start()
    return manager
  }
  afterEach(async () => {
    await Promise.all(open.splice(0).map((manager) => manager.close()))
  })

  /** Everything the engine still holds for a queue: waiting, in flight or parked. */
  const workLeft = async (
    manager: { stats(query: { queues: string[] }): Promise<{ queues: readonly QueueRow[] }> },
    name: string
  ): Promise<number> => {
    const row = (await manager.stats({ queues: [name] })).queues.find((r) => r.queue === name)
    expect(row).toBeDefined()
    return (
      (row?.backlog?.count ?? 0) +
      (row?.inFlight?.count ?? 0) +
      (row?.retryParked?.total.count ?? 0)
    )
  }
  interface QueueRow {
    readonly queue: string
    readonly backlog?: { readonly count: number }
    readonly inFlight?: { readonly count: number }
    readonly retryParked?: { readonly total: { readonly count: number } }
  }

  describe(`queue integration · ${target.name}`, () => {
    it(
      'a produced message reaches a streaming handler once and is acked',
      async () => {
        class Greeting extends defineMessage<Payload>({ key: (data) => `k${data.n}` }) {
          readonly type = 'greeting'
        }
        const seen: Message<Payload>[] = []
        class Greeter extends QueueHandler(Greeting) {
          handle(ctx: JobContext<Payload>): void {
            seen.push(ctx.message)
          }
        }
        const manager = await start({
          queues: defineQueues({ qi_basic: queue(Greeting) }),
          handlers: [new Greeter()],
        })

        await manager.produce(new Greeting({ n: 1 }, { headers: { h: 'v' } }))
        await manager.produce('qi_basic', { n: 2 })

        await until(() => expect(seen.map((m) => m.data.n).sort()).toEqual([1, 2]))
        expect(seen.find((m) => m.data.n === 1)).toMatchObject({
          type: 'greeting',
          key: 'k1',
          attempt: 1,
          headers: { h: 'v' },
        })
        await until(async () => expect(await workLeft(manager, 'qi_basic')).toBe(0))
        expect(seen).toHaveLength(2)
      },
      timeout
    )

    it(
      'a batch handler acks its batch and BatchFailure fails only the named subset',
      async () => {
        const attempts = new Map<number, number[]>()
        const manager = await start({
          queues: defineQueues({ qi_batch: queue<Payload>({ retry: { maxAttempts: 3 } }) }),
          handlers: [
            {
              queue: 'qi_batch',
              options: { prefetch: 10 },
              handleBatch(ctx: BatchContext<Payload>) {
                for (const m of ctx.messages) {
                  attempts.set(m.data.n, [...(attempts.get(m.data.n) ?? []), m.attempt])
                }
                // Odd payloads fail their first delivery; their batch-mates still ack.
                const failed = ctx.messages.filter((m) => m.data.n % 2 === 1 && m.attempt === 1)
                if (failed.length > 0) throw new BatchFailure(failed)
              },
            },
          ],
        })

        await manager.produce(
          'qi_batch',
          [0, 1, 2, 3].map((n) => ({ data: { n } }))
        )

        await until(async () => {
          expect(await workLeft(manager, 'qi_batch')).toBe(0)
          expect(Object.fromEntries(attempts)).toEqual({ 0: [1], 1: [1, 2], 2: [1], 3: [1, 2] })
        })
      },
      timeout
    )

    it(
      'a failing handler is retried with a rising attempt, then the message is dead-lettered',
      async () => {
        const attempts: number[] = []
        const dead: Message<Payload>[] = []
        const manager = await start({
          // Declared before its dead-letter queue: provisioning orders them itself.
          queues: defineQueues({
            qi_retry: queue<Payload>({ retry: { maxAttempts: 3, deadLetter: 'qi_retry_dead' } }),
            qi_retry_dead: queue<Payload>(),
          }),
          handlers: [
            {
              queue: 'qi_retry',
              handle(ctx: JobContext<Payload>) {
                attempts.push(ctx.message.attempt)
                throw new Error('always fails')
              },
            },
            {
              queue: 'qi_retry_dead',
              handle: (ctx: JobContext<Payload>) => void dead.push(ctx.message),
            },
          ],
        })

        await manager.produce('qi_retry', { n: 7 }, { headers: { h: 'v' } })

        await until(() => expect(dead).toHaveLength(1))
        expect(attempts).toEqual([1, 2, 3])
        expect(dead[0]).toMatchObject({ data: { n: 7 }, headers: { h: 'v' } })
        await until(async () => expect(await workLeft(manager, 'qi_retry')).toBe(0))
        expect(attempts).toHaveLength(3)
      },
      timeout
    )

    it(
      'a message produced before any worker pulled is governed by the queue policy',
      async () => {
        const attempts: number[] = []
        const dead: Message<Payload>[] = []
        const manager = await start({
          queues: defineQueues({
            qi_early: queue<Payload>({ retry: { maxAttempts: 2, deadLetter: 'qi_early_dead' } }),
            qi_early_dead: queue<Payload>(),
          }),
        })
        // The policy is on the queue from start(): no consumer has attached yet.
        await manager.produce('qi_early', { n: 9 })

        manager.subscribe('qi_early', (ctx) => {
          attempts.push(ctx.message.attempt)
          throw new Error('always fails')
        })
        manager.subscribe('qi_early_dead', (ctx) => void dead.push(ctx.message))

        await until(() => expect(dead).toHaveLength(1))
        expect(attempts).toEqual([1, 2])
        expect(dead[0]?.data).toEqual({ n: 9 })
        await until(async () => expect(await workLeft(manager, 'qi_early')).toBe(0))
        expect(attempts).toHaveLength(2)
      },
      timeout
    )

    it(
      'consumeTimeout aborts the invocation and the message is redelivered',
      async () => {
        const deliveries: { id: string; reason?: string }[] = []
        const manager = await start({ queues: defineQueues({ qi_timeout: queue<Payload>() }) })
        manager.subscribe({ queue: 'qi_timeout', consumeTimeout: 300 }, async (ctx) => {
          if (deliveries.length > 0) {
            deliveries.push({ id: ctx.message.id })
            return
          }
          const delivery: { id: string; reason?: string } = { id: ctx.message.id }
          deliveries.push(delivery)
          // A handler that never finishes: one that returns as the abort fires is acked.
          await aborted(ctx.signal)
          delivery.reason = (ctx.signal.reason as Error).name
          await new Promise(() => {})
        })

        await manager.produce('qi_timeout', { n: 1 })

        await until(() => expect(deliveries).toHaveLength(2))
        expect(deliveries[0]?.reason).toBe('TimeoutError')
        expect(deliveries[1]?.id).toBe(deliveries[0]?.id)
        await until(async () => expect(await workLeft(manager, 'qi_timeout')).toBe(0))
      },
      timeout
    )

    const livenessMs = target.livenessMs ?? 0
    it.skipIf(target.livenessMs === undefined)(
      'heartbeats keep a handler running past the liveness timeout from being redelivered',
      async () => {
        const queues = defineQueues({
          qi_beat: queue<Payload>({ livenessTimeoutMs: livenessMs }),
        })
        let invocations = 0
        let finished = false
        const holder = await start({ queues })
        holder.subscribe(
          {
            queue: 'qi_beat',
            prefetch: 1,
            consumeTimeout: 4 * livenessMs,
            heartbeatIntervalMs: livenessMs / 4,
          },
          async (ctx) => {
            invocations++
            await ctx.heartbeat()
            await Promise.race([sleep(1.6 * livenessMs), aborted(ctx.signal)])
            finished = !ctx.signal.aborted
          }
        )
        await holder.produce('qi_beat', { n: 1 })
        await until(() => expect(invocations).toBe(1))

        // A competing consumer, attached once the message is held: it gets the message the
        // moment the engine decides the holder is gone.
        let stolen = 0
        const peer = await start({ queues })
        peer.subscribe('qi_beat', () => void stolen++)

        await until(() => expect(finished).toBe(true), 1.6 * livenessMs + wait)
        await until(async () => expect(await workLeft(holder, 'qi_beat')).toBe(0))
        expect({ invocations, stolen }).toEqual({ invocations: 1, stolen: 0 })
      },
      3 * livenessMs + timeout
    )

    it(
      'parallelism runs handlers concurrently',
      async () => {
        let active = 0
        let peak = 0
        let handled = 0
        const manager = await start({ queues: defineQueues({ qi_parallel: queue<Payload>() }) })
        manager.subscribe({ queue: 'qi_parallel', parallelism: 3, prefetch: 3 }, async () => {
          peak = Math.max(peak, ++active)
          // Hold until all three overlap (or give up, and let the assertion below fail).
          const deadline = Date.now() + wait / 2
          while (peak < 3 && Date.now() < deadline) await sleep(10)
          active--
          handled++
        })

        await manager.produce(
          'qi_parallel',
          [1, 2, 3].map((n) => ({ data: { n } }))
        )

        await until(() => expect(handled).toBe(3))
        expect(peak).toBe(3)
      },
      timeout
    )

    it(
      'close() past its closeTimeout aborts the in-flight handler; a new manager gets the message',
      async () => {
        const queues = defineQueues({ qi_close: queue<Payload>() })
        const started: string[] = []
        const reasons: string[] = []
        const first = await start({ queues, closeTimeout: 200 })
        first.subscribe('qi_close', async (ctx) => {
          started.push(ctx.message.id)
          await aborted(ctx.signal)
          reasons.push((ctx.signal.reason as Error).name)
          await new Promise(() => {}) // never finishes, so it is released, not acked
        })
        await first.produce('qi_close', { n: 1 })
        await until(() => expect(started).toHaveLength(1))

        await first.close()
        expect(reasons).toEqual(['AbortError'])

        const redelivered: Message<Payload>[] = []
        await start({
          queues,
          handlers: [
            {
              queue: 'qi_close',
              handle: (ctx: JobContext<Payload>) => void redelivered.push(ctx.message),
            },
          ],
        })
        await until(() => expect(redelivered.map((m) => m.id)).toEqual(started))
      },
      timeout
    )

    it.skipIf(target.budgetFreeRelease !== true)(
      'messages released by shutdown never consume budget, however often it happens',
      async () => {
        const queues = defineQueues({
          qi_release: queue<Payload>({ retry: { maxAttempts: 2, deadLetter: 'qi_release_dead' } }),
          qi_release_dead: queue<Payload>(),
        })
        const producer = await start({ queues })
        await producer.produce(
          'qi_release',
          [1, 2, 3].map((n) => ({ data: { n } }))
        )
        const inFlight = async () =>
          (await producer.stats({ queues: ['qi_release'] })).queues[0]?.inFlight?.count

        // More shutdowns than the budget allows failures. Each manager holds all three: one in
        // a handler that never finishes, two prefetched behind it and never started.
        for (let round = 0; round < 3; round++) {
          const manager = await start({ queues, closeTimeout: 100 })
          manager.subscribe({ queue: 'qi_release', prefetch: 3 }, () => new Promise(() => {}))
          await until(async () => expect(await inFlight()).toBe(3))
          await manager.close()
          await until(async () => expect(await inFlight()).toBe(0))
        }

        const seen: Message<Payload>[] = []
        const dead: Message<Payload>[] = []
        const last = await start({ queues })
        last.subscribe('qi_release', (ctx) => void seen.push(ctx.message))
        last.subscribe('qi_release_dead', (ctx) => void dead.push(ctx.message))
        await until(() => expect(seen.map((m) => m.data.n).sort()).toEqual([1, 2, 3]))
        expect(seen.map((m) => m.attempt)).toEqual([1, 1, 1])
        await until(async () => expect(await workLeft(last, 'qi_release')).toBe(0))
        expect(dead).toHaveLength(0)
        expect(await workLeft(last, 'qi_release_dead')).toBe(0)
      },
      timeout
    )

    it(
      'produce and consume middleware both wrap the message',
      async () => {
        const produced: string[] = []
        const results: BatchResult[] = []
        const stamp = (header: string) => (m: { headers?: Readonly<Record<string, string>> }) => ({
          headers: { ...m.headers, [header]: 'mw' },
        })
        const middleware: QueueMiddleware = {
          produce: (next) => (call) => {
            produced.push(call.queue)
            const sent = stamp('x-produced')
            return next({ ...call, messages: call.messages.map((m) => ({ ...m, ...sent(m) })) })
          },
          consume: (next) => async (call) => {
            const received = stamp('x-consumed')
            call.messages = call.messages.map((m) => ({ ...m, ...received(m) }))
            const result = await next(call)
            results.push(result)
            return result
          },
        }
        const seen: Message<Payload>[] = []
        const manager = await start({
          queues: defineQueues({ qi_middleware: queue<Payload>() }),
          middleware: [middleware],
          handlers: [
            {
              queue: 'qi_middleware',
              handle: (ctx: JobContext<Payload>) => void seen.push(ctx.message),
            },
          ],
        })

        await manager.produce('qi_middleware', { n: 1 })

        await until(() => expect(results).toHaveLength(1))
        expect(produced).toEqual(['qi_middleware'])
        expect(seen[0]?.headers).toMatchObject({ 'x-produced': 'mw', 'x-consumed': 'mw' })
        expect(results[0]).toMatchObject({ outcome: 'ok', size: 1, ok: 1 })
      },
      timeout
    )

    it(
      'invoke runs the handler inline and sends nothing to the engine',
      async () => {
        const adapter = target.createAdapter()
        let sends = 0
        const seen: Message<Payload>[] = []
        const manager = await start(
          {
            queues: defineQueues({ qi_invoke: queue<Payload>() }),
            handlers: [
              {
                queue: 'qi_invoke',
                handle: (ctx: JobContext<Payload>) => void seen.push(ctx.message),
              },
            ],
          },
          {
            ...adapter,
            send: (...args: Parameters<typeof adapter.send>) => {
              sends++
              return adapter.send(...args)
            },
          }
        )

        await manager.invoke('qi_invoke', { n: 5 })

        // Already handled when invoke resolves: nothing was queued for the worker to find.
        expect(seen).toMatchObject([{ data: { n: 5 }, attempt: 1 }])
        expect(sends).toBe(0)
        expect(await workLeft(manager, 'qi_invoke')).toBe(0)
      },
      timeout
    )

    it.skipIf(target.dedup === undefined)(
      'a re-produced idempotencyKey is delivered once',
      async () => {
        const seen: number[] = []
        const manager = await start({
          queues: defineQueues({ qi_dedup: queue<Payload>(target.dedup?.queueOptions) }),
          handlers: [
            {
              queue: 'qi_dedup',
              handle: (ctx: JobContext<Payload>) => void seen.push(ctx.message.data.n),
            },
          ],
        })
        const keyed = (idempotencyKey: string) => ({ idempotencyKey, idempotencyTtlMs: 60_000 })

        await manager.produce('qi_dedup', { n: 1 }, keyed('a'))
        await manager.produce('qi_dedup', { n: 2 }, keyed('a')) // the duplicate
        await manager.produce('qi_dedup', { n: 3 }, keyed('b'))

        await until(() => expect([...seen].sort()).toEqual([1, 3]))
        await until(async () => expect(await workLeft(manager, 'qi_dedup')).toBe(0))
        expect(seen).toHaveLength(2)
      },
      timeout
    )
  })
}
