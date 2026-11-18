import { logger, logSchema } from '@internal/monitoring'
import type { BasePayload, StorageJob } from '@internal/queue'
import { getDirectProducer, getQueue } from '@internal/queue'
import type {
  AnyMessageClass,
  MessageInit,
  MessageInstance,
  SubscribeOptions,
} from '@internal/queue/manager'
import { PartialSendError, QueueHandler } from '@internal/queue/manager'
import type { Pool } from 'pg'
import { getConfig } from '../../../config'
import { storageEvent } from '../base'
import type { StorageQueues } from '../registry'
import {
  assertSqlIdentifier,
  PGBOSS_V1_SCHEMA,
  reportV1Leftovers,
  type V1QueueMapping,
  v1QueueMap,
  v1SchemaExists,
  withQueueDb,
} from './move-jobs'

const { queueAdapter } = getConfig()

/**
 * Drains the v1 fleet's `pgboss_v10` backlog into SQS, for the cutover to the SQS engine.
 *
 * Postgres and SQS share no transaction, so a move cannot be atomic the way the
 * schema-to-schema moves are. A lane works in rounds instead, and one round is small:
 *
 * 1. In one transaction, lock up to `batchSize` queued v1 jobs (`SKIP LOCKED`).
 * 2. Send them to SQS as the messages the v2 producers would have sent.
 * 3. Delete the jobs SQS confirmed, and commit.
 *
 * A crash before the commit rolls the transaction back: the jobs are still in v10 and the
 * next round sends them again. So a crash can duplicate at most the round in flight, per
 * lane, and can lose nothing. `SKIP LOCKED` keeps lanes, and v1 workers still draining, off
 * each other's rows, so any number of lanes may run at once.
 *
 * A lane is a chain of jobs: each runs one round, then sends the next with a delay, which is
 * what paces the move. The chain ends when nothing movable is left in v10.
 *
 * What does not carry over, or only partly:
 * - A job is moved only when it is due within the 14 minutes SQS can delay a message, and on
 *   a FIFO queue (no per-message delay) only once it is due. Later ones wait in v10 for a
 *   later round.
 * - A job too large for an SQS message stays in v10 and is named in the leftovers report.
 * - Retry budgets reset: a moved job arrives as a fresh message.
 * - A job a v1 worker is running stays put. Once it is past its own expiry it is moved, since
 *   nothing is left to expire it after the v1 fleet is gone.
 */
export interface MoveJobsToSqsPayload extends BasePayload {
  /** Which of the parallel lanes this is, for the logs. */
  lane: number
  /** The most v1 jobs one round moves, and so the most a crash can duplicate. */
  batchSize: number
  /** The pause (ms) between two rounds of this lane. */
  delayMs: number
  /** Rounds this lane has run. Rotates which queue a round starts with. */
  round: number
  /** Epoch ms after which the lane stops, whatever is left. */
  expiresAt: number
}

export class MoveJobsV10ToSqs extends storageEvent<MoveJobsToSqsPayload>({
  type: 'MoveJobsV10ToSqs',
  // Never run a move inline on a produce failure: it needs the queue database.
  allowSync: false,
  // A system job: no tenant gates it.
  disableKeys: () => [],
}) {}

/** The most lanes one worker runs at once, and the most the admin endpoint starts. */
export const MOVE_TO_SQS_MAX_LANES = 8
/** How long a lane lives when the caller sets no bound: v1's longest retry backoff is about
 * nine hours, so a day covers every job that will ever come due. */
export const MOVE_TO_SQS_LIFETIME_MS = 24 * 3_600_000

/** SQS delays a message 15 minutes at most; a job due later than this waits in v10. */
const DELAY_HORIZON_SECONDS = 14 * 60
/** The pause before the next round when a round moved nothing but jobs remain (not due yet,
 * held by another lane, or running on a v1 worker). */
const IDLE_DELAY_MS = 60_000
/** The longest pause a lane's next job can be sent with (the SQS delay limit). */
const MAX_DELAY_MS = 15 * 60_000
/** How far a job SQS did not take is pushed back: behind the jobs that are due, so one that
 * SQS keeps refusing cannot hold them up. It goes out again, with that delay, when a round has
 * room for it. */
const UNSENT_BACKOFF_SECONDS = 5 * 60
/**
 * The largest v1 job moved, measured as the job's JSON text. An SQS message holds 262,144
 * bytes with its attributes; the margin covers them and the fields v2 adds to the payload.
 */
const MAX_JOB_BYTES = 240_000

/** Where a round sends what it takes out of v10. A seam, so the round can be tested without
 * a queue engine. */
export interface MoveTarget {
  /** The message class a v1 queue's jobs become, or `undefined` when v2 declares no such
   * queue. */
  classOf(mapping: V1QueueMapping): AnyMessageClass | undefined
  /** Whether the destination queue is FIFO: it takes no per-message delay. */
  isFifo(queue: string): boolean
  /** Send messages of one queue to the engine. Rejects as the adapter does: a
   * `PartialSendError` names the messages that were not taken. */
  send(messages: readonly MessageInstance[], signal: AbortSignal): Promise<void>
}

export interface MoveRoundResult {
  /** Jobs sent to SQS and deleted from v10. */
  readonly moved: number
  /** Jobs SQS did not take: still in v10, pushed back. */
  readonly unsent: number
  /** Whether v10 still holds jobs a later round could move. */
  readonly remaining: boolean
}

interface V1JobRow {
  id: string
  data: Record<string, unknown> | null
  delay_ms: number
}

/** The storage queue's engine as a move target: its registry names the classes and the FIFO
 * queues, and its middleware-free producer does the sending. */
const engineTarget = (): MoveTarget => {
  const producer = getDirectProducer<StorageQueues>()
  const queues = producer.getQueues()
  return {
    classOf(mapping) {
      const classes = queues.get(mapping.queue)?.classes ?? []
      return (
        classes.find((cls) => (cls as { eventType?: string }).eventType === mapping.type) ??
        (classes.length === 1 ? classes[0] : undefined)
      )
    },
    isFifo(queue) {
      const options = queues.get(queue)?.options as { sqs?: { fifo?: boolean } } | undefined
      return options?.sqs?.fifo === true
    },
    send: (messages, signal) =>
      // The instances were built from registry classes, so they are bound by construction.
      producer.produce(messages as never, { signal }),
  }
}

/**
 * One round: up to `batchSize` jobs, taken from the mapped v1 queues in turn. The queue a
 * round starts with rotates, so a large backlog on one queue does not hold the others back.
 * Each queue's share is its own transaction, so a failure on one queue cannot duplicate what
 * an earlier one already moved.
 */
export async function moveRound(
  db: Pool,
  options: { readonly batchSize: number; readonly round: number; readonly schema?: string },
  target: MoveTarget,
  signal: AbortSignal
): Promise<MoveRoundResult> {
  const job = `${assertSqlIdentifier(options.schema ?? PGBOSS_V1_SCHEMA)}.job`
  const mapped = v1QueueMap().flatMap((mapping) => {
    const cls = target.classOf(mapping)
    if (cls === undefined) {
      logSchema.warning(
        logger,
        `[MoveJobs][v10→sqs] no v2 message class for ${mapping.v1Queue}; skipping`,
        { type: 'queue' }
      )
      return []
    }
    return [{ mapping, cls }]
  })
  const start = mapped.length === 0 ? 0 : options.round % mapped.length
  const inTurn = [...mapped.slice(start), ...mapped.slice(0, start)]

  let budget = options.batchSize
  let moved = 0
  let unsent = 0
  for (const { mapping, cls } of inTurn) {
    if (budget <= 0 || signal.aborted) break
    const chunk = await moveChunk(db, job, mapping, cls, target, budget, signal)
    moved += chunk.moved
    unsent += chunk.unsent
    // What SQS refused counts too: a round never takes more than its budget out of v10.
    budget -= chunk.moved + chunk.unsent
  }

  const left = await db.query<{ found: boolean }>(
    `SELECT EXISTS (
       SELECT 1 FROM ${job}
       WHERE name = ANY($1::text[]) AND state <= 'active' AND octet_length(data::text) <= $2
     ) AS found`,
    [mapped.map(({ mapping }) => mapping.v1Queue), MAX_JOB_BYTES]
  )
  return { moved, unsent, remaining: left.rows[0]?.found === true }
}

/** Move up to `limit` jobs of one v1 queue, in one transaction. */
async function moveChunk(
  db: Pool,
  job: string,
  mapping: V1QueueMapping,
  cls: AnyMessageClass,
  target: MoveTarget,
  limit: number,
  signal: AbortSignal
): Promise<{ moved: number; unsent: number }> {
  const horizon = target.isFifo(mapping.queue) ? 0 : DELAY_HORIZON_SECONDS
  const client = await db.connect()
  try {
    await client.query('BEGIN')
    // The predicate and order match v10's fetch index (name, start_after WHERE state < 'active').
    const queued = await client.query<V1JobRow>(
      `SELECT id, data,
              GREATEST(0, ceil(extract(epoch FROM start_after - now()) * 1000))::int AS delay_ms
       FROM ${job}
       WHERE name = $1 AND state < 'active'
         AND start_after <= now() + $2 * interval '1 second'
         AND octet_length(data::text) <= $3
       ORDER BY start_after
       LIMIT $4
       FOR UPDATE SKIP LOCKED`,
      [mapping.v1Queue, horizon, MAX_JOB_BYTES, limit]
    )
    const rows = queued.rows
    if (rows.length < limit) {
      // The queue is nearly drained, so this scan is over few rows. A job a v1 worker left
      // active past its own expiry: v10's maintenance would have re-queued it, and nothing
      // runs that once the v1 fleet is gone.
      const expired = await client.query<V1JobRow>(
        `SELECT id, data, 0 AS delay_ms
         FROM ${job}
         WHERE name = $1 AND state = 'active' AND started_on + expire_in < now()
           AND octet_length(data::text) <= $2
         LIMIT $3
         FOR UPDATE SKIP LOCKED`,
        [mapping.v1Queue, MAX_JOB_BYTES, limit - rows.length]
      )
      rows.push(...expired.rows)
    }
    if (rows.length === 0) {
      await client.query('COMMIT')
      return { moved: 0, unsent: 0 }
    }

    // The registry holds its classes as abstract constructors; these are concrete ones.
    const Message = cls as unknown as new (data: unknown, init?: MessageInit) => MessageInstance
    const messages = rows.map((row) => {
      // `scheduleAt` was v1's transport-only delay field; the delay travels on the message.
      const { scheduleAt: _scheduleAt, ...payload } = row.data ?? {}
      return new Message(payload, row.delay_ms > 0 ? { delayMs: row.delay_ms } : undefined)
    })
    let notTaken: ReadonlySet<number> = new Set()
    try {
      await target.send(messages, signal)
    } catch (error) {
      // Any other failure says nothing about what landed: roll back and send it all again.
      if (!(error instanceof PartialSendError)) throw error
      notTaken = error.unsent
      logSchema.warning(
        logger,
        `[MoveJobs][v10→sqs] ${notTaken.size} of ${rows.length} ${mapping.v1Queue} jobs were not accepted; left in v10`,
        { type: 'queue', error }
      )
    }

    const sentIds = rows.filter((_, position) => !notTaken.has(position)).map((row) => row.id)
    const unsentIds = rows.filter((_, position) => notTaken.has(position)).map((row) => row.id)
    if (sentIds.length > 0) {
      await client.query(`DELETE FROM ${job} WHERE name = $1 AND id = ANY($2::uuid[])`, [
        mapping.v1Queue,
        sentIds,
      ])
    }
    if (unsentIds.length > 0) {
      // To the back of the line, so a job SQS keeps refusing does not head every round's batch.
      await client.query(
        `UPDATE ${job} SET start_after = now() + $3 * interval '1 second'
         WHERE name = $1 AND id = ANY($2::uuid[])`,
        [mapping.v1Queue, unsentIds, UNSENT_BACKOFF_SECONDS]
      )
    }
    await client.query('COMMIT')
    return { moved: sentIds.length, unsent: unsentIds.length }
  } catch (error) {
    await client.query('ROLLBACK').catch(() => undefined)
    throw error
  } finally {
    client.release()
  }
}

/** The pause before a lane's next round, or `undefined` when the lane is done. */
export function nextRoundDelay(
  payload: Pick<MoveJobsToSqsPayload, 'delayMs' | 'expiresAt'>,
  result: MoveRoundResult,
  now: number
): number | undefined {
  if (!result.remaining || now >= payload.expiresAt) return undefined
  // A round that moved nothing found only jobs it cannot take yet: come back less often.
  const delay = result.moved > 0 ? payload.delayMs : Math.max(payload.delayMs, IDLE_DELAY_MS)
  return Math.min(MAX_DELAY_MS, Math.max(0, delay))
}

export class MoveJobsV10ToSqsHandler extends QueueHandler(MoveJobsV10ToSqs) {
  override readonly options: SubscribeOptions = {
    prefetch: MOVE_TO_SQS_MAX_LANES,
    parallelism: MOVE_TO_SQS_MAX_LANES,
    // One round is a few seconds; well inside the queue's liveness window.
    consumeTimeout: 120_000,
  }

  async handle(ctx: StorageJob<MoveJobsToSqsPayload>): Promise<void> {
    const { data } = ctx.message
    if (queueAdapter !== 'sqs') {
      logSchema.warning(
        logger,
        `[MoveJobs][v10→sqs] the running queue adapter is '${queueAdapter}'; nothing moved`,
        { type: 'queue' }
      )
      return
    }

    let result: MoveRoundResult | undefined
    await withQueueDb(async (db) => {
      if (!(await v1SchemaExists(db))) return
      result = await moveRound(db, data, engineTarget(), ctx.signal)
      if (!result.remaining) await reportV1Leftovers(db)
    })
    if (result === undefined) return
    const round: MoveRoundResult = result

    const lane = `lane ${data.lane}, round ${data.round}`
    const delayMs = nextRoundDelay(data, round, Date.now())
    logSchema.info(
      logger,
      `[MoveJobs][v10→sqs] ${lane}: moved ${round.moved}, not accepted ${round.unsent}` +
        (delayMs === undefined ? '; lane finished' : ''),
      { type: 'queue', sbReqId: data.sbReqId }
    )
    if (delayMs === undefined) {
      if (round.remaining) {
        logSchema.warning(
          logger,
          `[MoveJobs][v10→sqs] ${lane}: stopped at its time limit with jobs left in v10`,
          { type: 'queue', sbReqId: data.sbReqId }
        )
      }
      return
    }

    // Sent before this job is acked: if the worker dies first, this job is delivered again
    // and the lane carries on from there.
    const next = new MoveJobsV10ToSqs(
      { ...data, round: data.round + 1 },
      delayMs > 0 ? { delayMs } : undefined
    )
    await getQueue<StorageQueues>().produce(next as never)
  }
}
