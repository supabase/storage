import { Pool } from 'pg'
import { getConfig } from '../../config'
import type { MessageInit, MessageInstance } from '../../internal/queue/manager'
import { PartialSendError } from '../../internal/queue/manager'
import { QUEUES } from '../../storage/events/registry'
import { type MoveTarget, moveRound } from '../../storage/events/upgrades/move-jobs-sqs'

// A throwaway schema shaped like the v1 fleet's pgboss_v10 job table (the columns, the state
// order and the fetch index the mover relies on), so this file never touches a real backlog.
const SCHEMA = 'queue_move_sqs_test'
const url =
  process.env.QUEUE_TEST_DATABASE_URL || getConfig().pgQueueConnectionURL || getConfig().databaseURL

/** What the fake engine received: one entry per send, in order. */
interface Sent {
  readonly data: Record<string, unknown>
  readonly delayMs: number | undefined
}

class Moved {
  constructor(
    readonly data: Record<string, unknown>,
    readonly init?: MessageInit
  ) {}
}

/** A move target that records what it is sent. `refuse` makes it fail a send. */
function fakeTarget(refuse?: (messages: readonly Sent[]) => Error | undefined) {
  const sent: Sent[] = []
  const target: MoveTarget = {
    classOf: () => Moved as never,
    isFifo: (queue) => queue === QUEUES.runMigrations,
    send: async (messages: readonly MessageInstance[]) => {
      const batch = (messages as unknown as Moved[]).map((m) => ({
        data: m.data,
        delayMs: m.init?.delayMs,
      }))
      const error = refuse?.(batch)
      if (error instanceof PartialSendError) {
        sent.push(...batch.filter((_, position) => !error.unsent.has(position)))
      }
      if (error !== undefined) throw error
      sent.push(...batch)
    },
  }
  return { target, sent }
}

describe.skipIf(getConfig().databaseEngine === 'multigres')('move pgboss_v10 jobs to sqs', () => {
  let pool: Pool
  const signal = new AbortController().signal
  const round = (batchSize: number, target: MoveTarget, n = 0) =>
    moveRound(pool, { batchSize, round: n, schema: SCHEMA }, target, signal)

  const insert = async (
    name: string,
    data: Record<string, unknown>,
    extra: { startAfter?: string; state?: string; startedOn?: string; expireIn?: string } = {}
  ) => {
    await pool.query(
      `insert into ${SCHEMA}.job (name, data, state, start_after, started_on, expire_in)
       values ($1, $2, $3::${SCHEMA}.job_state, now() + $4::interval, now() + $5::interval, $6::interval)`,
      [
        name,
        JSON.stringify(data),
        extra.state ?? 'created',
        extra.startAfter ?? '0 seconds',
        extra.startedOn ?? null,
        extra.expireIn ?? '15 minutes',
      ]
    )
  }
  const left = async (name: string): Promise<number[]> => {
    const result = await pool.query(
      `select (data->>'n')::int as n from ${SCHEMA}.job where name = $1 order by 1`,
      [name]
    )
    return result.rows.map((row) => row.n)
  }

  beforeAll(async () => {
    pool = new Pool({ connectionString: url, max: 6 })
    await pool.query(`drop schema if exists ${SCHEMA} cascade`)
    await pool.query(`create schema ${SCHEMA}`)
    await pool.query(
      `create type ${SCHEMA}.job_state as enum ('created', 'retry', 'active', 'completed', 'cancelled', 'failed')`
    )
    await pool.query(`
      create table ${SCHEMA}.job (
        id uuid not null default gen_random_uuid(),
        name text not null,
        data jsonb,
        state ${SCHEMA}.job_state not null default 'created',
        start_after timestamptz not null default now(),
        started_on timestamptz,
        expire_in interval not null default interval '15 minutes',
        completed_on timestamptz,
        output jsonb,
        primary key (name, id)
      )`)
    await pool.query(
      `create index job_i5 on ${SCHEMA}.job (name, start_after) include (id) where state < 'active'`
    )
  })

  beforeEach(async () => {
    await pool.query(`truncate ${SCHEMA}.job`)
  })

  afterAll(async () => {
    await pool.query(`drop schema if exists ${SCHEMA} cascade`)
    await pool.end()
  })

  it('sends due jobs as v2 messages and deletes them from v10', async () => {
    await insert('webhooks', { n: 1, scheduleAt: '2026-01-01' })
    await insert('webhooks', { n: 2 }, { state: 'retry' })
    const { target, sent } = fakeTarget()

    const result = await round(500, target)

    expect(result).toEqual({ moved: 2, unsent: 0, cancelled: 0, remaining: false })
    // v1's transport-only delay field does not travel in the payload.
    expect(sent.map((m) => m.data).sort((a, b) => Number(a.n) - Number(b.n))).toEqual([
      { n: 1 },
      { n: 2 },
    ])
    expect(await left('webhooks')).toEqual([])
  })

  it('moves a job due later at once: it keeps its delay, up to the 14 minutes SQS allows', async () => {
    await insert('webhooks', { n: 1 }, { startAfter: '5 minutes' })
    // Deep into a retry backoff: it cannot wait that long in SQS, so it runs early.
    await insert('webhooks', { n: 2 }, { startAfter: '9 hours' })
    const { target, sent } = fakeTarget()

    const result = await round(500, target)

    // Nothing waits in v10 for its due time: the move is done.
    expect(result).toEqual({ moved: 2, unsent: 0, cancelled: 0, remaining: false })
    const delayOf = (n: number) => sent.find((m) => m.data.n === n)?.delayMs
    expect(delayOf(1)).toBeGreaterThan(4 * 60_000)
    expect(delayOf(1)).toBeLessThanOrEqual(5 * 60_000)
    expect(delayOf(2)).toBe(14 * 60_000)
    expect(await left('webhooks')).toEqual([])
  })

  it('moves a job due later to a FIFO queue with no delay', async () => {
    await insert('tenants-migrations-v2', { n: 1 })
    await insert('tenants-migrations-v2', { n: 2 }, { startAfter: '5 minutes' })
    const { target, sent } = fakeTarget()

    const result = await round(500, target)

    expect(result).toEqual({ moved: 2, unsent: 0, cancelled: 0, remaining: false })
    // A FIFO queue takes no per-message delay.
    expect(sent).toEqual([
      { data: { n: 1 }, delayMs: undefined },
      { data: { n: 2 }, delayMs: undefined },
    ])
  })

  it('leaves every job in v10 when the send fails: the next round sends them again', async () => {
    await insert('webhooks', { n: 1 })
    await insert('webhooks', { n: 2 })
    const failing = fakeTarget(() => new Error('sqs unreachable'))

    await expect(round(500, failing.target)).rejects.toThrow('sqs unreachable')
    expect(await left('webhooks')).toEqual([1, 2])

    const { target, sent } = fakeTarget()
    expect(await round(500, target)).toMatchObject({ moved: 2 })
    expect(sent).toHaveLength(2)
  })

  it('deletes only what a partial send confirmed and pushes the rest back', async () => {
    for (const n of [1, 2, 3]) await insert('webhooks', { n })
    // The jobs go out in start_after order: refuse the second of them.
    const partial = fakeTarget(() => new PartialSendError([1]))

    const result = await round(500, partial.target)

    expect(result).toEqual({ moved: 2, unsent: 1, cancelled: 0, remaining: true })
    expect(partial.sent).toHaveLength(2)
    const kept = await left('webhooks')
    expect(kept).toHaveLength(1)
    expect(partial.sent.map((m) => m.data.n)).not.toContain(kept[0])
    // Pushed back behind the jobs that are due: it does not head the next round's batch.
    await insert('webhooks', { n: 4 })
    const again = fakeTarget()
    expect(await round(1, again.target)).toMatchObject({ moved: 1, remaining: true })
    expect(again.sent.map((m) => m.data.n)).toEqual([4])
    expect(await left('webhooks')).toEqual(kept)
  })

  it('cancels a job too large for an SQS message, so the queue drains', async () => {
    await insert('webhooks', { n: 1, blob: 'x'.repeat(250_000) })
    await insert('webhooks', { n: 2 })
    const { target, sent } = fakeTarget()

    const result = await round(500, target)

    // Nothing is left queued: the lane can finish.
    expect(result).toEqual({ moved: 1, unsent: 0, cancelled: 1, remaining: false })
    expect(sent.map((m) => m.data.n)).toEqual([2])
    // The job is still there to read, payload included, with why it did not move.
    const kept = await pool.query(
      `select state, completed_on, output, (data->>'n')::int as n from ${SCHEMA}.job where name = 'webhooks'`
    )
    expect(kept.rows).toEqual([
      {
        state: 'cancelled',
        completed_on: expect.any(Date),
        output: { reason: 'too large to move to sqs', bytes: expect.any(Number) },
        n: 1,
      },
    ])
    expect(kept.rows[0].output.bytes).toBeGreaterThan(250_000)
  })

  it('moves a job a v1 worker left active past its expiry, and waits for one still running', async () => {
    await insert('webhooks', { n: 1 }, { state: 'active', startedOn: '-1 hour' })
    await insert('webhooks', { n: 2 }, { state: 'active', startedOn: '-1 minute' })
    await insert('webhooks', { n: 3 }, { state: 'completed' })
    const { target, sent } = fakeTarget()

    const result = await round(500, target)

    expect(result).toEqual({ moved: 1, unsent: 0, cancelled: 0, remaining: true })
    expect(sent).toEqual([{ data: { n: 1 }, delayMs: undefined }])
    expect(await left('webhooks')).toEqual([2, 3])
  })

  it('takes no more than its batch size, starting with a different queue each round', async () => {
    for (const n of [1, 2, 3]) await insert('webhooks', { n })
    for (const n of [11, 12, 13]) await insert('object:admin:delete', { n })
    const first = fakeTarget()
    const second = fakeTarget()

    // Round 0 starts with webhooks, round 1 with the queue after it.
    expect(await round(2, first.target, 0)).toEqual({
      moved: 2,
      unsent: 0,
      cancelled: 0,
      remaining: true,
    })
    expect(await round(2, second.target, 1)).toEqual({
      moved: 2,
      unsent: 0,
      cancelled: 0,
      remaining: true,
    })

    expect(first.sent.every((m) => Number(m.data.n) < 10)).toBe(true)
    expect(second.sent.every((m) => Number(m.data.n) > 10)).toBe(true)
    expect(await left('webhooks')).toHaveLength(1)
    expect(await left('object:admin:delete')).toHaveLength(1)
  })

  it('lanes running at once never move the same job', async () => {
    for (let n = 0; n < 60; n++) await insert('webhooks', { n })
    const all: number[] = []
    const lane = (): MoveTarget => ({
      classOf: () => Moved as never,
      isFifo: () => false,
      send: async (messages) => {
        // Hold the rows locked long enough for the other lanes to run into them.
        await new Promise((resolve) => setTimeout(resolve, 50))
        all.push(...(messages as unknown as Moved[]).map((m) => Number(m.data.n)))
      },
    })

    const results = await Promise.all([round(20, lane()), round(20, lane()), round(20, lane())])

    expect(results.reduce((total, result) => total + result.moved, 0)).toBe(60)
    expect(new Set(all).size).toBe(60)
    expect(await left('webhooks')).toEqual([])
  })
})
