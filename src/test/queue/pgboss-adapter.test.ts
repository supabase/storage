import { randomUUID } from 'node:crypto'
import { Pool } from 'pg'
import { PgBoss } from 'pg-boss'
import { getConfig } from '../../config'
import { type PgBossAdapter, pgboss } from '../../internal/queue/adapters/pgboss'
import type { QueueSession, SubscribePolicy } from '../../internal/queue/manager/adapter'
import type { ProduceMessage, PullMessage } from '../../internal/queue/manager/message'
import { defineQueues, queue } from '../../internal/queue/manager/registry'

// A throwaway pg-boss schema, installed by pg-boss's own migration and dropped at the end, so
// this file never touches the app's queue schema.
const SCHEMA = 'queue_pgboss_adapter_test'
const url =
  process.env.QUEUE_TEST_DATABASE_URL ?? getConfig().pgQueueConnectionURL ?? getConfig().databaseURL

const uniq = (): string => `q_${randomUUID().slice(0, 12).replace(/-/g, '')}`
const message = (
  n: number,
  extra?: Omit<ProduceMessage<unknown>, 'data'>
): ProduceMessage<unknown> => ({ data: { n }, headers: {}, ...extra })
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

// Type-level: the `pgboss` namespace is merged into the manager's QueueOptions.
queue<{ n: number }>({ pgboss: { policy: 'exclusive', expireInSeconds: 30 } })
// @ts-expect-error a typo inside the namespace is rejected
queue<{ n: number }>({ pgboss: { polcy: 'exclusive' } })

describe('pgboss queue adapter', () => {
  let pool: Pool
  let adapter: PgBossAdapter
  const sessions: QueueSession[] = []

  interface JobRow {
    id: string
    state: string
    data: unknown
    singleton_key: string | null
    singleton_on: Date | null
    start_after: Date
    created_on: Date
    heartbeat_on: Date | null
    retry_count: number
    retry_limit: number
    dead_letter: string | null
  }
  const jobs = async (name: string): Promise<JobRow[]> => {
    const result = await pool.query(
      `select * from ${SCHEMA}.job where name = $1 order by created_on, id`,
      [name]
    )
    return result.rows
  }
  const queueRow = async (name: string) => {
    const result = await pool.query(`select * from ${SCHEMA}.queue where name = $1`, [name])
    return result.rows[0]
  }
  const subscribe = async (name: string, policy: SubscribePolicy = {}) => {
    const session = await adapter.subscribe(name, policy)
    sessions.push(session)
    return session
  }
  /** Pull until `count` messages arrived (a retried job may take a moment to become due). */
  const pullN = async (session: QueueSession, count: number, timeoutMs = 8_000) => {
    const pulled: PullMessage[] = []
    const deadline = Date.now() + timeoutMs
    while (pulled.length < count && Date.now() < deadline) {
      const batch = await session.pull(count - pulled.length)
      pulled.push(...batch)
      if (batch.length === 0) await sleep(50)
    }
    expect(pulled).toHaveLength(count)
    return pulled
  }

  beforeAll(async () => {
    pool = new Pool({ connectionString: url, max: 4 })
    await pool.query(`drop schema if exists ${SCHEMA} cascade`)
    adapter = pgboss({
      connectionString: url,
      // 1s cadence so an expiry sweep observes a 1s expireInSeconds inside the test budget.
      bossOptions: { schema: SCHEMA, monitorIntervalSeconds: 1, superviseIntervalSeconds: 1 },
    })
    await adapter.start()
  })

  afterEach(async () => {
    await Promise.all(sessions.splice(0).map((session) => session.close()))
  })

  afterAll(async () => {
    await adapter.close()
    await pool.query(`drop schema if exists ${SCHEMA} cascade`)
    await pool.end()
  })

  describe('construction', () => {
    it('requires exactly one of boss / connectionString', () => {
      expect(() => pgboss({})).toThrow(/exactly one of/)
      expect(() => pgboss({ connectionString: url, boss: new PgBoss(url) })).toThrow(
        /exactly one of/
      )
    })

    it('rejects a schema that is not a plain identifier', () => {
      expect(() => pgboss({ connectionString: url, schema: 'a"; drop' })).toThrow(/invalid schema/)
    })

    it('leaves a caller-owned instance running on close()', async () => {
      const boss = new PgBoss({ connectionString: url, schema: SCHEMA, supervise: false })
      boss.on('error', () => undefined)
      const shared = pgboss({ boss, schema: SCHEMA })
      try {
        await shared.start()
        const name = uniq()
        await shared.provision(name)
        await shared.close()
        // Still usable: a plain pg-boss job on the same queue delivers as a bare payload.
        await boss.send(name, { plain: true })
        const session = await shared.subscribe(name, {})
        const [pulled] = await session.pull(1)
        expect(pulled).toMatchObject({ data: { plain: true }, headers: {}, attempt: 1 })
        expect(pulled?.key).toBeUndefined()
      } finally {
        await boss.stop({ graceful: false, close: true })
      }
    })
  })

  describe('provision', () => {
    it('creates the queue with the retry defaults and is idempotent', async () => {
      const name = uniq()
      await adapter.provision(name)
      await adapter.provision(name)
      const row = await queueRow(name)
      expect(row).toMatchObject({
        policy: 'standard',
        retry_limit: 10,
        retry_delay: 0,
        retry_backoff: false,
      })
    })

    it('applies adapter defaults under the queue-level pgboss options', async () => {
      const boss = new PgBoss({ connectionString: url, schema: SCHEMA, supervise: false })
      boss.on('error', () => undefined)
      const withDefaults = pgboss({
        boss,
        schema: SCHEMA,
        queue: { expireInSeconds: 120, retentionSeconds: 600 },
      })
      try {
        await withDefaults.start()
        const [plain, overridden] = [uniq(), uniq()]
        await withDefaults.provision(plain)
        await withDefaults.provision(overridden, {
          pgboss: { policy: 'singleton', expireInSeconds: 30 },
        })
        expect(await queueRow(plain)).toMatchObject({
          expire_seconds: 120,
          retention_seconds: 600,
        })
        expect(await queueRow(overridden)).toMatchObject({
          policy: 'singleton',
          expire_seconds: 30,
          retention_seconds: 600,
        })
      } finally {
        await boss.stop({ graceful: false, close: true })
      }
    })

    it('rejects key_strict_fifo', async () => {
      await expect(
        adapter.provision(uniq(), { pgboss: { policy: 'key_strict_fifo' } })
      ).rejects.toThrow(/queue pgboss: policy "key_strict_fifo" is not supported/)
    })

    it('throws when the declared policy drifted from the stored one', async () => {
      const name = uniq()
      await adapter.provision(name, { pgboss: { policy: 'exclusive' } })
      await adapter.provision(name) // undeclared: only makes sure it exists
      await expect(adapter.provision(name, { pgboss: { policy: 'singleton' } })).rejects.toThrow(
        /exists with policy "exclusive"/
      )
    })
  })

  describe('send', () => {
    it('round-trips data, key, headers, timestamp and attempt', async () => {
      const name = uniq()
      await adapter.provision(name)
      const before = Date.now()
      await adapter.send(name, [message(1, { key: 'k1', headers: { 'message-type': 'T' } })], {})

      const [row] = await jobs(name)
      expect(row?.data).toEqual({ __q: 1, p: { n: 1 }, h: { 'message-type': 'T' }, k: 'k1' })

      const session = await subscribe(name)
      const [pulled] = await session.pull(10)
      expect(pulled).toMatchObject({
        id: row?.id,
        data: { n: 1 },
        key: 'k1',
        headers: { 'message-type': 'T' },
        attempt: 1,
        receipt: `${row?.id}:1`,
      })
      expect(Math.abs((pulled?.timestamp ?? 0) - before)).toBeLessThan(5_000)
    })

    it('fails loudly on a queue that was never provisioned', async () => {
      await expect(adapter.send(uniq(), [message(1)], {})).rejects.toThrow(/does not exist/)
    })

    it('does nothing for an empty batch and honours an aborted signal', async () => {
      const name = uniq()
      await adapter.provision(name)
      await adapter.send(name, [], {})
      await expect(adapter.send(name, [message(1)], {}, AbortSignal.abort())).rejects.toThrow()
      expect(await jobs(name)).toHaveLength(0)
    })

    it('maps idempotencyKey + idempotencyTtlMs to the singleton key and window', async () => {
      const name = uniq()
      await adapter.provision(name) // "standard": the window dedup has no policy gate
      const dedup = { idempotencyKey: 'same', idempotencyTtlMs: 3_600_000 }
      await adapter.send(name, [message(1, dedup)], {})
      await adapter.send(name, [message(2, dedup)], {}) // dropped
      await adapter.send(name, [message(3, { ...dedup, idempotencyKey: 'other' })], {})

      const rows = await jobs(name)
      expect(rows.map((row) => row.singleton_key).sort()).toEqual(['other', 'same'])
      expect(rows.every((row) => row.singleton_on !== null)).toBe(true)
    })

    it('an idempotencyKey without a window gets no engine dedup', async () => {
      const name = uniq()
      await adapter.provision(name)
      await adapter.send(name, [message(1, { idempotencyKey: 'same' })], {})
      await adapter.send(name, [message(2, { idempotencyKey: 'same' })], {})

      const rows = await jobs(name)
      expect(rows).toHaveLength(2)
      expect(rows.every((row) => row.singleton_key === 'same' && row.singleton_on === null)).toBe(
        true
      )
    })

    it('an exclusive queue dedupes queued jobs per key and rejects keyless sends', async () => {
      const name = uniq()
      const options = { pgboss: { policy: 'exclusive' } } as const
      await adapter.provision(name, options)
      await adapter.send(name, [message(1, { idempotencyKey: 'a' })], {})
      await adapter.send(name, [message(2, { idempotencyKey: 'a' })], {}) // dropped: queued dup
      await adapter.send(name, [message(3, { idempotencyKey: 'b' })], {})
      expect((await jobs(name)).map((row) => row.singleton_key).sort()).toEqual(['a', 'b'])

      // The guard reads the DECLARED policy from the registry the manager forwards.
      const queues = defineQueues({ [name]: queue<unknown>(options) })
      await expect(adapter.send(name, [message(4)], { queues })).rejects.toThrow(
        /requires an idempotencyKey/
      )
    })

    it('maps delayMs to start_after and holds the message back', async () => {
      const name = uniq()
      await adapter.provision(name)
      const before = Date.now()
      await adapter.send(name, [message(1, { delayMs: 90_000 }), message(2)], {})

      const delays = (await jobs(name))
        .map((row) => row.start_after.getTime() - before)
        .sort((a, b) => a - b)
      expect(delays[0]).toBeLessThan(5_000)
      expect(delays[1]).toBeGreaterThan(80_000)

      const session = await subscribe(name)
      expect((await session.pull(10)).map((pulled) => pulled.data)).toEqual([{ n: 2 }])
    })

    it('joins the caller transaction: rollback discards, commit lands', async () => {
      const name = uniq()
      await adapter.provision(name)
      const client = await pool.connect()
      try {
        await client.query('begin')
        await adapter.send(name, [message(1)], { ctx: client }) // a plain pg client
        expect(await jobs(name)).toHaveLength(0) // not visible outside the transaction
        await client.query('rollback')
        expect(await jobs(name)).toHaveLength(0)

        await client.query('begin')
        const db = {
          executeSql: async (text: string, values?: unknown[]) => client.query(text, values),
        }
        await adapter.send(name, [message(2)], { ctx: db }) // a pg-boss Db
        await client.query('commit')
        expect((await jobs(name)).map((row) => row.data)).toMatchObject([{ p: { n: 2 } }])
      } finally {
        client.release()
      }
    })
  })

  describe('session', () => {
    it('competing sessions never receive the same message', async () => {
      const name = uniq()
      await adapter.provision(name)
      await adapter.send(
        name,
        Array.from({ length: 20 }, (_, n) => message(n)),
        {}
      )
      const [a, b] = [await subscribe(name), await subscribe(name)]
      const [fromA, fromB] = await Promise.all([a.pull(15), b.pull(15)])
      const ids = [...fromA, ...fromB].map((pulled) => pulled.id)
      expect(ids).toHaveLength(20)
      expect(new Set(ids).size).toBe(20)
    })

    it('ack completes, is idempotent and ignores junk receipts', async () => {
      const name = uniq()
      await adapter.provision(name)
      await adapter.send(name, [message(1), message(2)], {})
      const session = await subscribe(name)
      const pulled = await session.pull(10)
      const receipts = pulled.map((m) => m.receipt)

      await session.ack([...receipts, 'junk', 'not-a-uuid:1', `${randomUUID()}:1`])
      await session.ack(receipts)
      await session.ack([])
      expect((await jobs(name)).map((row) => row.state)).toEqual(['completed', 'completed'])
      expect(await session.pull(10)).toEqual([])
    })

    it('nack redelivers with the next attempt and never un-acks', async () => {
      const name = uniq()
      await adapter.provision(name)
      await adapter.send(name, [message(1), message(2)], {})
      const session = await subscribe(name)
      const [first, second] = await pullN(session, 2)

      await session.ack([first!.receipt])
      await session.nack([first!.receipt, second!.receipt, 'junk'])

      const [redelivered] = await pullN(session, 1)
      expect(redelivered).toMatchObject({ id: second!.id, attempt: 2, data: second!.data })
      expect(redelivered!.receipt).toBe(`${second!.id}:2`)
      const states = Object.fromEntries((await jobs(name)).map((row) => [row.id, row.state]))
      expect(states[first!.id]).toBe('completed')

      // ack wins in the other order too: a nacked (queued again) job is still completed.
      await session.nack([redelivered!.receipt])
      await session.ack([redelivered!.receipt])
      expect((await jobs(name)).map((row) => row.state)).toEqual(['completed', 'completed'])
    })

    it('retry redelivers until the budget is spent, then dead-letters natively', async () => {
      const [name, deadLetter] = [uniq(), uniq()]
      const queues = defineQueues({
        [name]: queue<unknown>({ pgboss: { expireInSeconds: 45 } }),
        [deadLetter]: queue<unknown>({ pgboss: { retentionSeconds: 7_200 } }),
      })
      // The subscribe provisions both queues, from their declared options.
      const session = await subscribe(name, { retry: { maxAttempts: 2, deadLetter }, queues })
      expect(await queueRow(name)).toMatchObject({
        retry_limit: 1,
        retry_delay: 0,
        dead_letter: deadLetter,
        expire_seconds: 45,
      })
      expect(await queueRow(deadLetter)).toMatchObject({ retention_seconds: 7_200 })

      await adapter.send(name, [message(7, { key: 'k', headers: { h: 'v' } })], { queues })
      const [first] = await pullN(session, 1)
      // A receipt named twice counts once; unknown ones are skipped.
      await session.retry([
        { receipt: first!.receipt, delayMs: 0 },
        { receipt: first!.receipt, delayMs: 0 },
        { receipt: `${randomUUID()}:1`, delayMs: 0 },
        { receipt: 'junk', delayMs: 0 },
      ])
      const [second] = await pullN(session, 1)
      expect(second).toMatchObject({ id: first!.id, attempt: 2 })

      await session.retry([{ receipt: second!.receipt, delayMs: 0 }])
      expect(await session.pull(1)).toEqual([])
      expect((await jobs(name)).map((row) => row.state)).toEqual(['failed'])

      // The dead letter is an ordinary message on an ordinary queue, envelope intact.
      const dlq = await subscribe(deadLetter)
      const [dead] = await pullN(dlq, 1)
      expect(dead).toMatchObject({ data: { n: 7 }, key: 'k', headers: { h: 'v' }, attempt: 1 })
    })

    it('a delayed retry parks the job until its delay has passed', async () => {
      const name = uniq()
      const session = await subscribe(name, { retry: { maxAttempts: 3 } })
      await adapter.send(name, [message(1), message(2), message(3)], {})
      const [a, b, c] = await pullN(session, 3)
      await session.ack([c!.receipt])

      const before = Date.now()
      await session.retry([
        { receipt: a!.receipt, delayMs: 60_000 },
        { receipt: b!.receipt, delayMs: 0 },
        { receipt: c!.receipt, delayMs: 60_000 }, // already acked: stays completed
      ])
      const rows = Object.fromEntries((await jobs(name)).map((row) => [row.id, row]))
      expect(rows[a!.id]).toMatchObject({ state: 'retry' })
      expect(rows[a!.id]!.start_after.getTime() - before).toBeGreaterThan(50_000)
      expect(rows[c!.id]).toMatchObject({ state: 'completed' })

      const again = await pullN(session, 1)
      expect(again.map((m) => [m.id, m.attempt])).toEqual([[b!.id, 2]])
      expect(await session.pull(5)).toEqual([])
    })

    it('without a dead-letter queue an exhausted message is dropped (failed)', async () => {
      const name = uniq()
      const session = await subscribe(name, { retry: { maxAttempts: 1, backoffMs: 1_500 } })
      expect(await queueRow(name)).toMatchObject({
        retry_limit: 0,
        retry_delay: 2,
        dead_letter: null,
      })
      await adapter.send(name, [message(1)], {})
      const [only] = await pullN(session, 1)
      await session.retry([{ receipt: only!.receipt, delayMs: 1_500 }])
      expect((await jobs(name)).map((row) => row.state)).toEqual(['failed'])
    })

    it('maps livenessTimeoutMs to heartbeatSeconds and lifts the attempt cap', async () => {
      const [lifted, capped, short] = [uniq(), uniq(), uniq()]
      const queues = defineQueues({ [capped]: queue<unknown>({ pgboss: { expireInSeconds: 30 } }) })
      await subscribe(lifted, { livenessTimeoutMs: 45_500 })
      await subscribe(capped, { livenessTimeoutMs: 45_500, queues })
      await subscribe(short, { livenessTimeoutMs: 1_000 })
      expect(await queueRow(lifted)).toMatchObject({
        heartbeat_seconds: 46,
        expire_seconds: 86_399,
      })
      expect(await queueRow(capped)).toMatchObject({ heartbeat_seconds: 46, expire_seconds: 30 })
      expect(await queueRow(short)).toMatchObject({ heartbeat_seconds: 10 })
    })

    it('heartbeat re-stamps only the active jobs it is given', async () => {
      const name = uniq()
      const session = await subscribe(name, { livenessTimeoutMs: 30_000 })
      await adapter.send(name, [message(1), message(2)], {})
      const [held, settled] = await pullN(session, 2)
      await session.ack([settled!.receipt])
      const stampOf = async (id: string) =>
        (await jobs(name)).find((row) => row.id === id)?.heartbeat_on?.getTime()
      const [heldBefore, settledBefore] = [await stampOf(held!.id), await stampOf(settled!.id)]
      expect(heldBefore).toBeDefined()

      await sleep(50)
      await session.heartbeat!([held!.receipt, settled!.receipt, 'junk'])
      expect(await stampOf(held!.id)).toBeGreaterThan(heldBefore!)
      expect(await stampOf(settled!.id)).toBe(settledBefore)
      expect((await jobs(name)).map((row) => row.state).sort()).toEqual(['active', 'completed'])
    })

    it('an unsettled delivery is redelivered once its attempt cap lapses', async () => {
      const name = uniq()
      const queues = defineQueues({ [name]: queue<unknown>({ pgboss: { expireInSeconds: 1 } }) })
      const session = await subscribe(name, { queues })
      await adapter.send(name, [message(1)], { queues })
      const [first] = await pullN(session, 1)

      const crashed = await subscribe(name, { queues })
      let redelivered: PullMessage[] = []
      const deadline = Date.now() + 20_000
      while (redelivered.length === 0 && Date.now() < deadline) {
        await adapter.maintain()
        redelivered = await crashed.pull(1)
        if (redelivered.length === 0) await sleep(250)
      }
      expect(redelivered[0]).toMatchObject({ id: first!.id, attempt: 2 })
    }, 30_000)

    it('is silent after close(), and close() may be repeated', async () => {
      const name = uniq()
      const session = await subscribe(name, { retry: { maxAttempts: 5 } })
      await adapter.send(name, [message(1), message(2)], {})
      const [held] = await pullN(session, 1)

      await session.close()
      await session.close()
      expect(await session.pull(10)).toEqual([])
      await session.ack([held!.receipt])
      await session.nack([held!.receipt])
      await session.retry([{ receipt: held!.receipt, delayMs: 0 }])
      await session.heartbeat!([held!.receipt])
      expect((await jobs(name)).map((row) => row.state).sort()).toEqual(['active', 'created'])
    })
  })

  describe('stats', () => {
    it('reports backlog, in-flight, parked retries and the oldest due age per queue', async () => {
      const [name, other] = [uniq(), uniq()]
      await adapter.provision(other)
      const session = await subscribe(name, { retry: { maxAttempts: 5 } })
      await adapter.send(
        name,
        [message(1), message(2), message(3), message(4), message(5, { delayMs: 60_000 })],
        {}
      )
      const [a, b, c] = await pullN(session, 3)
      await session.ack([a!.receipt])
      await session.retry([
        { receipt: b!.receipt, delayMs: 0 },
        { receipt: c!.receipt, delayMs: 60_000 },
      ])
      const [held] = await pullN(session, 1) // the due retry, or the remaining created one
      expect(held).toBeDefined()
      await sleep(20)

      const report = await adapter.stats({ queues: [name, other] })
      expect(report.queues.map((row) => row.queue).sort()).toEqual([name, other].sort())
      const row = report.queues.find((stats) => stats.queue === name)!
      const exact = (count: number) => ({ count, saturated: false })
      expect(row.inFlight).toEqual(exact(1))
      // 5 sent: 1 acked, 1 in flight, 1 parked for a minute, and of {due retry, created} one
      // was just pulled; the delayed message is backlog either way.
      const dueRetry = row.retryParked!.due.count
      expect(row.retryParked!.total).toEqual(exact(1 + dueRetry))
      expect(row.backlog).toEqual(exact(2 - dueRetry))
      if (dueRetry === 1) expect(row.oldestBacklogAgeMs).toBe(0)
      else expect(row.oldestBacklogAgeMs).toBeGreaterThan(0)

      expect(report.queues.find((stats) => stats.queue === other)).toEqual({
        queue: other,
        backlog: exact(0),
        inFlight: exact(0),
        retryParked: { total: exact(0), due: exact(0) },
        oldestBacklogAgeMs: 0,
      })
      // An unfiltered report covers every queue in the schema.
      const all = await adapter.stats()
      expect(all.queues.length).toBeGreaterThan(2)
    })

    it('a backlog past the cap saturates the retry counts with it', async () => {
      const name = uniq()
      await adapter.provision(name)
      await adapter.send(name, [message(1), message(2), message(3)], {})
      const rowOf = async (countCap: number) =>
        (await adapter.stats({ queues: [name], countCap })).queues[0]

      // One bounded scan feeds backlog and both retry figures, so past the cap all three are
      // floors; active jobs are a read of their own and stay exact.
      expect(await rowOf(2)).toMatchObject({
        backlog: { count: 2, saturated: true },
        retryParked: { total: { count: 0, saturated: true }, due: { count: 0, saturated: true } },
        inFlight: { count: 0, saturated: false },
      })
      expect(await rowOf(3)).toMatchObject({
        backlog: { count: 3, saturated: false },
        retryParked: { total: { count: 0, saturated: false }, due: { count: 0, saturated: false } },
      })
    })
  })

  describe('maintain', () => {
    it('runs one supervision pass without throwing', async () => {
      await expect(adapter.maintain()).resolves.toBeUndefined()
    })
  })
})
