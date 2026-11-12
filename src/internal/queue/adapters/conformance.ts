/**
 * Adapter conformance suite: everything a `QueueAdapter` must honour, exercised through the
 * public adapter interface only. A helper module, not a test file. Call it from the adapter's
 * own `*.test.ts` (vitest globals must be enabled):
 *
 * ```ts
 * describeAdapterConformance({
 *   name: 'memory',
 *   create: () => memory(),
 * })
 * ```
 *
 * `create()` is called once per test and the suite `start()`s and `close()`s what it returns.
 * Every test uses its own queue names (`target.queueName`, default `conf_<random>`), so the
 * factory may return handles onto one shared backend.
 *
 * Everything in `capabilities` defaults to the FULL contract; an adapter opts out of what
 * its engine cannot do, and the matching tests are skipped. What is always tested: provision
 * idempotency, message fidelity, competing delivery, and the settle contract (idempotent
 * ack/nack/retry, unknown receipts, the closed-session rule, retry attempt counting).
 */
import type {
  PullMessage,
  QueueAdapter,
  QueueSession,
  QueueStats,
  SubscribePolicy,
} from '../manager'

/** One gauge of `QueueStats`. */
export type StatsGauge = 'backlog' | 'inFlight' | 'retryParked' | 'oldestAge'

export interface ConformanceCapabilities {
  /** Delayed delivery (`ProduceMessage.delayMs`; the adapter declares `delayedDelivery`).
   * Default true. */
  readonly delay?: boolean
  /** Producer dedup (`idempotencyKey` + `idempotencyTtlMs`). Default true. */
  readonly dedup?: boolean
  /** Sessions implement `heartbeat`, and it keeps a silent consumer's deliveries from
   * redelivering. Only tested together with `livenessRedelivery`. Default true. */
  readonly heartbeat?: boolean
  /** A consumer silent past its `livenessTimeoutMs` (= `timing.livenessMs`) has its unsettled
   * deliveries redelivered to a peer within `timing.waitMs`. Default true. */
  readonly livenessRedelivery?: boolean
  /** A `retry` past the declared budget lands the message on the policy's `deadLetter` queue.
   * Default true. */
  readonly deadLetter?: boolean
  /**
   * How the engine counts toward `RetryPolicy.maxAttempts`. `'explicit'` (default): only
   * `retry` advances `attempt`; a nack redelivers with the same attempt. `'deliveries'`:
   * every delivery advances it.
   */
  readonly retryAccounting?: 'explicit' | 'deliveries'
  /** The gauges `stats()` must produce; `false` when the adapter has no `stats()`. Default:
   * all four. A gauge left out is not asserted. */
  readonly stats?: readonly StatsGauge[] | false
  /** Timing knobs, for slow or coarse-grained engines. */
  readonly timing?: {
    /** `livenessTimeoutMs` every suite consumer subscribes with. Default 1000. */
    readonly livenessMs?: number
    /** Ceiling for every eventually-consistent wait. Default 5000. */
    readonly waitMs?: number
    /** How long "nothing arrives" is observed before it counts as nothing. Default 100. */
    readonly quietMs?: number
    /** The delay the delayed-delivery test produces with. Default 500. */
    readonly delayMs?: number
  }
}

export interface ConformanceTarget<W = unknown> {
  /** Display name of the suite. */
  readonly name: string
  /** A fresh adapter per test. */
  create(): QueueAdapter<W> | Promise<QueueAdapter<W>>
  /** A fresh, unique queue name per call, for engines with naming rules. */
  queueName?(): string
  readonly capabilities?: ConformanceCapabilities
}

const ALL_GAUGES: readonly StatsGauge[] = ['backlog', 'inFlight', 'retryParked', 'oldestAge']

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms))

/** Re-run `fn` until it stops throwing or `timeoutMs` elapses (then rethrow the last failure). */
const waitFor = async <T>(fn: () => T | Promise<T>, timeoutMs: number): Promise<T> => {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    try {
      return await fn()
    } catch (error) {
      if (Date.now() >= deadline) throw error
    }
    await sleep(20)
  }
}

export function describeAdapterConformance<W>(target: ConformanceTarget<W>): void {
  const caps = target.capabilities ?? {}
  const livenessMs = caps.timing?.livenessMs ?? 1_000
  const wait = caps.timing?.waitMs ?? 5_000
  const quiet = caps.timing?.quietMs ?? 100
  const delayMs = caps.timing?.delayMs ?? 500
  const accounting = caps.retryAccounting ?? 'explicit'
  const gauges = caps.stats === false ? [] : (caps.stats ?? ALL_GAUGES)
  const has = (capability: 'delay' | 'dedup' | 'heartbeat' | 'livenessRedelivery' | 'deadLetter') =>
    caps[capability] !== false

  const queueName = (): string =>
    target.queueName?.() ?? `conf_${crypto.randomUUID().slice(0, 13).replace(/-/g, '')}`
  const policy = (extra?: Partial<SubscribePolicy>): SubscribePolicy => ({
    consumerId: `c_${crypto.randomUUID().slice(0, 8)}`,
    livenessTimeoutMs: livenessMs,
    ...extra,
  })

  /** Run `fn` against a fresh, started adapter and close it afterwards. */
  const run = async (fn: (adapter: QueueAdapter<W>) => Promise<void>): Promise<void> => {
    const adapter = await target.create()
    try {
      await adapter.start?.()
      await fn(adapter)
    } finally {
      await adapter.close()
    }
  }
  /** Send `{ n }` payloads, one message per number, in one call. */
  const send = (adapter: QueueAdapter<W>, queue: string, ...ns: number[]): Promise<void> =>
    adapter.send(
      queue,
      ns.map((n) => ({ data: adapter.codec.encode({ n }), headers: {} })),
      {}
    )
  const nOf = (adapter: QueueAdapter<W>, m: PullMessage<W>): number =>
    (adapter.codec.decode(m.data) as { n: number }).n
  /** Pull until exactly `count` deliveries were collected. */
  const pullN = (session: QueueSession<W>, count: number): Promise<PullMessage<W>[]> => {
    const got: PullMessage<W>[] = []
    return waitFor(async () => {
      got.push(...(await session.pull(count - got.length)))
      expect(got).toHaveLength(count)
      return got
    }, wait)
  }
  const pullOne = async (session: QueueSession<W>): Promise<PullMessage<W>> =>
    (await pullN(session, 1))[0]
  /** Assert nothing is deliverable to `session` for the quiet period. */
  const expectQuiet = async (session: QueueSession<W>): Promise<void> => {
    await sleep(quiet)
    expect(await session.pull(10)).toHaveLength(0)
  }

  describe(`queue adapter conformance · ${target.name}`, () => {
    describe('lifecycle', () => {
      it('provision is idempotent and start may be called twice', async () => {
        await run(async (adapter) => {
          const queue = queueName()
          await adapter.start?.()
          await adapter.provision(queue)
          await adapter.provision(queue)
          await send(adapter, queue, 1)
        })
      })

      it('subscribe provisions the queue; a message sent before a consumer attached is kept', async () => {
        await run(async (adapter) => {
          const early = queueName()
          await adapter.provision(early)
          await send(adapter, early, 1)
          const late = await adapter.subscribe(early, policy())
          expect(nOf(adapter, await pullOne(late))).toBe(1)
          await late.close()

          const unprovisioned = queueName()
          const me = await adapter.subscribe(unprovisioned, policy())
          await send(adapter, unprovisioned, 2)
          const got = await pullOne(me)
          expect(nOf(adapter, got)).toBe(2)
          await me.ack([got.receipt])
          await me.close()
        })
      })

      it('a delivery carries what was sent plus what the backend stamps', async () => {
        await run(async (adapter) => {
          const queue = queueName()
          const me = await adapter.subscribe(queue, policy())
          const before = Date.now()
          await adapter.send(
            queue,
            [
              {
                data: adapter.codec.encode({ n: 1, nested: { a: [1, 'x'] } }),
                headers: { h: 'v' },
              },
              { data: adapter.codec.encode({ n: 2 }), headers: {}, key: 'k1' },
            ],
            {}
          )
          const got = await pullN(me, 2)
          const first = got.find((m) => nOf(adapter, m) === 1)
          const second = got.find((m) => nOf(adapter, m) === 2)
          expect(adapter.codec.decode(first?.data as W)).toEqual({ n: 1, nested: { a: [1, 'x'] } })
          expect(first?.headers).toMatchObject({ h: 'v' })
          expect(second?.key).toBe('k1')
          for (const m of got) {
            expect(typeof m.id).toBe('string')
            expect(typeof m.receipt).toBe('string')
            expect(m.attempt).toBe(1)
            // Backend clocks may differ slightly from this process's.
            expect(Math.abs(m.timestamp - before)).toBeLessThan(60_000)
          }
          expect(first?.id).not.toBe(second?.id)
          await me.ack(got.map((m) => m.receipt))
          await me.close()
        })
      })
    })

    describe('delivery (competing consumers)', () => {
      it('pull never returns more than max, and every message is delivered exactly once across consumers', async () => {
        await run(async (adapter) => {
          const queue = queueName()
          const total = 10
          const a = await adapter.subscribe(queue, policy())
          const b = await adapter.subscribe(queue, policy())
          for (let i = 0; i < total; i++) await send(adapter, queue, i)

          const seen: number[] = []
          await waitFor(async () => {
            for (const member of [a, b]) {
              const got = await member.pull(3)
              expect(got.length).toBeLessThanOrEqual(3)
              for (const m of got) seen.push(nOf(adapter, m))
              await member.ack(got.map((m) => m.receipt))
            }
            expect(seen.length).toBeGreaterThanOrEqual(total)
          }, wait)
          expect([...seen].sort((x, y) => x - y)).toEqual(
            Array.from({ length: total }, (_, i) => i)
          )
          await expectQuiet(a)
          await a.close()
          await b.close()
        })
      })

      it.skipIf(!has('livenessRedelivery'))(
        'a consumer silent past its liveness contract has its unsettled work redelivered to a peer',
        async () => {
          await run(async (adapter) => {
            const queue = queueName()
            const a = await adapter.subscribe(queue, policy())
            const b = await adapter.subscribe(queue, policy())
            await send(adapter, queue, 1, 2)
            // `a` receives and crashes: never settles, never pulls again. HOW the adapter
            // recovers is its own business; THAT it does within the contract is the fact.
            const missing = new Set((await pullN(a, 2)).map((m) => m.id))
            await waitFor(async () => {
              const got = await b.pull(10)
              for (const m of got) missing.delete(m.id)
              await b.ack(got.map((m) => m.receipt))
              expect(missing.size).toBe(0)
            }, wait + livenessMs)
            await b.close()
          })
        }
      )

      it.skipIf(!has('heartbeat') || !has('livenessRedelivery'))(
        'heartbeat keeps a consumer that is not pulling alive past its liveness contract',
        async () => {
          await run(async (adapter) => {
            const queue = queueName()
            const a = await adapter.subscribe(queue, policy())
            const b = await adapter.subscribe(queue, policy())
            await send(adapter, queue, 1)
            const held = await pullOne(a)
            expect(a.heartbeat).toBeTypeOf('function')
            const until = Date.now() + 2.5 * livenessMs
            while (Date.now() < until) {
              await a.heartbeat?.([held.receipt])
              expect(await b.pull(10)).toHaveLength(0)
              await sleep(livenessMs / 5)
            }
            // Idempotent, and harmless on receipts it does not own.
            await a.heartbeat?.([held.receipt, held.receipt, 'conformance:not-a-receipt'])
            await a.ack([held.receipt])
            await expectQuiet(b)
            await a.close()
            await b.close()
          })
        }
      )

      it('the adapter declares delayedDelivery exactly when the target claims delay', async () => {
        await run(async (adapter) => {
          expect(adapter.delayedDelivery === true).toBe(has('delay'))
        })
      })

      it.skipIf(!has('delay'))(
        'a delayed message is not deliverable before its delay',
        async () => {
          await run(async (adapter) => {
            const queue = queueName()
            const me = await adapter.subscribe(queue, policy())
            const sentAt = Date.now()
            await adapter.send(
              queue,
              [{ data: adapter.codec.encode({ n: 1 }), headers: {}, delayMs }],
              {}
            )
            expect(await me.pull(10)).toHaveLength(0)
            const got = await waitFor(async () => {
              const pulled = await me.pull(10)
              expect(pulled).toHaveLength(1)
              return pulled[0]
            }, wait + delayMs)
            expect(Date.now() - sentAt).toBeGreaterThanOrEqual(delayMs * 0.9)
            await me.ack([got.receipt])
            await me.close()
          })
        }
      )

      it.skipIf(!has('dedup'))(
        'producer dedup: a re-send with the same idempotencyKey admits exactly one message',
        async () => {
          await run(async (adapter) => {
            const queue = queueName()
            const me = await adapter.subscribe(queue, policy())
            const keyed = (n: number, idempotencyKey: string) => ({
              data: adapter.codec.encode({ n }),
              headers: {},
              idempotencyKey,
              idempotencyTtlMs: 60_000,
            })
            await adapter.send(queue, [keyed(1, 'idem-1')], {})
            // A retried publish (same key) and an interleaved distinct key, batch-shaped.
            await adapter.send(
              queue,
              [
                keyed(1, 'idem-1'),
                keyed(2, 'idem-2'),
                { data: adapter.codec.encode({ n: 3 }), headers: {} },
              ],
              {}
            )
            const got = await pullN(me, 3)
            await me.ack(got.map((m) => m.receipt))
            expect(got.map((m) => nOf(adapter, m)).sort()).toEqual([1, 2, 3])
            await expectQuiet(me)
            await me.close()
          })
        }
      )
    })

    describe('settle contract (batch-shaped, idempotent)', () => {
      it('double ack, nack-after-ack and unknown receipts are all no-ops', async () => {
        await run(async (adapter) => {
          const queue = queueName()
          const me = await adapter.subscribe(queue, policy())
          await send(adapter, queue, 1)
          const { receipt } = await pullOne(me)

          await me.ack([receipt])
          await me.ack([receipt, receipt])
          await me.nack([receipt]) // a late release must not un-settle: ack wins
          await me.retry([{ receipt, delayMs: 0 }])
          await me.ack(['conformance:not-a-receipt'])
          await me.nack(['conformance:not-a-receipt'])
          await me.ack([])
          await me.nack([])
          await me.retry([])

          await expectQuiet(me)
          await me.close()
        })
      })

      it('a closed session is quiet: it pulls nothing, its settles never land, and what it held redelivers', async () => {
        await run(async (adapter) => {
          const queue = queueName()
          const me = await adapter.subscribe(queue, policy())
          await send(adapter, queue, 1)
          const held = await pullOne(me)

          await me.close()
          await me.close() // closing twice is harmless
          await me.ack([held.receipt])
          await me.nack([held.receipt])
          await me.retry([{ receipt: held.receipt, delayMs: 0 }])
          await me.heartbeat?.([held.receipt])
          expect(await me.pull(10)).toHaveLength(0)

          // HOW it redelivers (released on close, or its lease running out) is the adapter's.
          const next = await adapter.subscribe(queue, policy())
          await waitFor(async () => {
            const got = await next.pull(10)
            expect(got.map((m) => m.id)).toContain(held.id)
            await next.ack(got.map((m) => m.receipt))
          }, wait + livenessMs)
          await next.close()
        })
      })

      it('nack redelivers; the attempt tracks the declared accounting unit', async () => {
        await run(async (adapter) => {
          const queue = queueName()
          const me = await adapter.subscribe(queue, policy())
          await send(adapter, queue, 1)
          const first = await pullOne(me)
          expect(first.attempt).toBe(1)
          await me.nack([first.receipt])
          const again = await pullOne(me)
          expect(again.id).toBe(first.id)
          expect(again.attempt).toBe(accounting === 'deliveries' ? 2 : 1)
          await me.ack([again.receipt])
          await me.close()
        })
      })

      it('retry honours delayMs; a double retry, in one call or two, counts once', async () => {
        await run(async (adapter) => {
          const queue = queueName()
          const me = await adapter.subscribe(queue, policy())
          await send(adapter, queue, 1)
          const { receipt } = await pullOne(me)
          // A far-future retry is settled now and invisible until due.
          await me.retry([
            { receipt, delayMs: 60_000 },
            { receipt, delayMs: 60_000 },
          ])
          await me.retry([{ receipt, delayMs: 0 }]) // a retried settle must not re-park
          await expectQuiet(me)
          await me.close()
        })
      })

      it('a due retry redelivers exactly once, with attempt + 1', async () => {
        await run(async (adapter) => {
          const queue = queueName()
          const me = await adapter.subscribe(queue, policy())
          await send(adapter, queue, 1)
          const first = await pullOne(me)
          await me.retry([
            { receipt: first.receipt, delayMs: 0 },
            { receipt: first.receipt, delayMs: 0 },
          ])
          await me.retry([{ receipt: first.receipt, delayMs: 0 }])
          const redelivered = await pullOne(me)
          expect(redelivered.id).toBe(first.id)
          expect(redelivered.attempt).toBe(2)
          await me.ack([redelivered.receipt])
          await expectQuiet(me)
          await me.close()
        })
      })

      it('one batched retry gives every entry its own delay; unknown receipts are skipped', async () => {
        await run(async (adapter) => {
          const queue = queueName()
          const me = await adapter.subscribe(queue, policy())
          await send(adapter, queue, 1, 2)
          const got = await pullN(me, 2)
          const receiptOf = (n: number): string =>
            got.find((m) => nOf(adapter, m) === n)?.receipt ?? ''

          await me.retry([
            { receipt: receiptOf(1), delayMs: 0 },
            { receipt: receiptOf(2), delayMs: 60_000 },
            { receipt: 'conformance:not-a-receipt', delayMs: 0 },
          ])
          const redelivered = await pullOne(me)
          expect(nOf(adapter, redelivered)).toBe(1)
          expect(redelivered.attempt).toBe(2)
          await me.ack([redelivered.receipt])
          await expectQuiet(me)
          await me.close()
        })
      })

      it.skipIf(!has('deadLetter'))(
        'retry past the declared budget dead-letters once; the original stops redelivering',
        async () => {
          await run(async (adapter) => {
            const queue = queueName()
            const deadLetter = queueName()
            const me = await adapter.subscribe(
              queue,
              policy({ retry: { maxAttempts: 2, deadLetter } })
            )
            // Subscribing with the policy provisioned the dead-letter queue.
            const collector = await adapter.subscribe(deadLetter, policy())
            await send(adapter, queue, 42)

            const first = await pullOne(me)
            expect(first.attempt).toBe(1)
            await me.retry([{ receipt: first.receipt, delayMs: 0 }]) // within budget
            const second = await pullOne(me)
            expect(second.attempt).toBe(2)
            // Budget spent. Named twice in one call, it dead-letters once.
            await me.retry([
              { receipt: second.receipt, delayMs: 0 },
              { receipt: second.receipt, delayMs: 0 },
            ])

            const dead = await pullOne(collector)
            expect(nOf(adapter, dead)).toBe(42)
            await collector.ack([dead.receipt])
            await expectQuiet(collector)
            await expectQuiet(me)
            await me.close()
            await collector.close()
          })
        }
      )
    })

    describe.skipIf(caps.stats === false)('stats', () => {
      const gauge = (g: StatsGauge): boolean => !gauges.includes(g)
      const rowOf = async (
        adapter: QueueAdapter<W>,
        queue: string,
        countCap?: number
      ): Promise<QueueStats | undefined> => {
        const report = await adapter.stats?.({
          queues: [queue],
          ...(countCap !== undefined ? { countCap } : {}),
        })
        return report?.queues.find((row) => row.queue === queue)
      }
      const exact = (count: number) => ({ count, saturated: false })

      it('the report is scoped by the queues filter', async () => {
        await run(async (adapter) => {
          expect(adapter.stats).toBeTypeOf('function')
          const a = queueName()
          const b = queueName()
          await adapter.provision(a)
          await adapter.provision(b)
          const scoped = await adapter.stats?.({ queues: [a] })
          expect(scoped?.queues.every((row) => row.queue === a)).toBe(true)
        })
      })

      it.skipIf(gauge('backlog'))('backlog rises with sends and drains to zero', async () => {
        await run(async (adapter) => {
          const queue = queueName()
          const me = await adapter.subscribe(queue, policy())
          await send(adapter, queue, 0, 1, 2)
          await waitFor(async () => {
            const row = await rowOf(adapter, queue)
            // Backlog may be engine-approximate: rising is the portable fact.
            expect(row?.backlog?.saturated).toBe(false)
            expect(row?.backlog?.count).toBeGreaterThanOrEqual(2)
            expect(row?.backlog?.count).toBeLessThanOrEqual(3)
          }, wait)
          const got = await pullN(me, 3)
          await me.ack(got.map((m) => m.receipt))
          await waitFor(async () => {
            expect((await rowOf(adapter, queue))?.backlog).toEqual(exact(0))
          }, wait)
          await me.close()
        })
      })

      it.skipIf(gauge('backlog'))(
        'a delivered, unsettled batch still counts as work left',
        async () => {
          await run(async (adapter) => {
            const queue = queueName()
            const me = await adapter.subscribe(queue, policy())
            await send(adapter, queue, 0, 1, 2)
            const held = (await pullN(me, 3)).map((m) => m.receipt)
            // In `inFlight` on an engine that can see leases, still in `backlog` on one that
            // cannot: their sum must not drop while the messages are held.
            const workLeft = async (): Promise<number> => {
              const row = await rowOf(adapter, queue)
              return (row?.backlog?.count ?? 0) + (row?.inFlight?.count ?? 0)
            }
            expect(await workLeft()).toBeGreaterThanOrEqual(2)
            await me.ack(held)
            await waitFor(async () => expect(await workLeft()).toBe(0), wait)
            await me.close()
          })
        }
      )

      it.skipIf(gauge('backlog'))('counts saturate at the query cap', async () => {
        await run(async (adapter) => {
          const queue = queueName()
          await adapter.provision(queue)
          await send(adapter, queue, 0, 1, 2, 3, 4)
          await waitFor(async () => {
            expect((await rowOf(adapter, queue, 2))?.backlog).toEqual({ count: 2, saturated: true })
          }, wait)
        })
      })

      it.skipIf(gauge('inFlight'))(
        'in-flight tracks delivered-but-unsettled deliveries',
        async () => {
          await run(async (adapter) => {
            const queue = queueName()
            const me = await adapter.subscribe(queue, policy())
            await send(adapter, queue, 1)
            const { receipt } = await pullOne(me)
            await waitFor(async () => {
              expect((await rowOf(adapter, queue))?.inFlight).toEqual(exact(1))
            }, wait)
            await me.ack([receipt])
            await waitFor(async () => {
              expect((await rowOf(adapter, queue))?.inFlight).toEqual(exact(0))
            }, wait)
            await me.close()
          })
        }
      )

      it.skipIf(gauge('retryParked'))('retry-parked splits total from due', async () => {
        await run(async (adapter) => {
          const queue = queueName()
          const me = await adapter.subscribe(queue, policy())
          await send(adapter, queue, 1)
          const { receipt } = await pullOne(me)
          // A far-future park is stable: counted in total, never due.
          await me.retry([{ receipt, delayMs: 60_000 }])
          await waitFor(async () => {
            expect((await rowOf(adapter, queue))?.retryParked).toEqual({
              total: exact(1),
              due: exact(0),
            })
          }, wait)
          await me.close()
        })
      })

      it.skipIf(gauge('retryParked') || gauge('inFlight'))(
        'a redelivered retry is in flight, not parked',
        async () => {
          await run(async (adapter) => {
            const queue = queueName()
            const me = await adapter.subscribe(queue, policy())
            await send(adapter, queue, 1)
            const first = await pullOne(me)
            await me.retry([{ receipt: first.receipt, delayMs: 0 }])
            const second = await pullOne(me)
            await waitFor(async () => {
              const row = await rowOf(adapter, queue)
              expect(row?.inFlight).toEqual(exact(1))
              expect(row?.retryParked).toEqual({ total: exact(0), due: exact(0) })
            }, wait)
            await me.ack([second.receipt])
            await me.close()
          })
        }
      )

      it.skipIf(gauge('oldestAge'))(
        'oldest-backlog age follows the head of the backlog and zeroes when drained',
        async () => {
          await run(async (adapter) => {
            const queue = queueName()
            const me = await adapter.subscribe(queue, policy())
            await send(adapter, queue, 0, 1)
            await waitFor(async () => {
              expect((await rowOf(adapter, queue))?.oldestBacklogAgeMs).toBeGreaterThan(0)
            }, wait)
            const got = await pullN(me, 2)
            await me.ack(got.map((m) => m.receipt))
            await waitFor(async () => {
              expect((await rowOf(adapter, queue))?.oldestBacklogAgeMs).toBe(0)
            }, wait)
            await me.close()
          })
        }
      )
    })
  })
}
