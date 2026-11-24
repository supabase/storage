import { checkoutClient } from '@internal/database/postgres/pool-errors'
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
 * what paces the move. The chain ends when v10 is drained, or at the lane's time limit, so a
 * move is over in a known time whatever is in v10.
 *
 * What does not carry over, or only partly:
 * - Every queued job moves at once, whenever it was due: none waits in v10. One due later
 *   keeps its delay up to the 14 minutes SQS can hold a message back, so a job scheduled
 *   further out than that (a retry deep into its backoff) runs early. A FIFO queue takes no
 *   per-message delay, so there every job is deliverable as soon as it lands.
 * - A job too large for an SQS message cannot move at all. It is cancelled in v10, where it
 *   stays readable, so that the queue drains and the lanes can finish.
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
/**
 * How long a lane lives when the caller sets no limit. Queued jobs drain in minutes; the rest
 * of the time is for jobs v1 workers were still running to finish or expire, which most do
 * within seconds to ten minutes. One still active at the limit is named in the leftovers log.
 */
export const MOVE_TO_SQS_DEFAULT_MINUTES = 30

/** The longest delay a moved job keeps. SQS delays a message 15 minutes at most; a job due
 * later than this runs early. */
const MAX_JOB_DELAY_MS = 14 * 60_000
/** The pause before the next round when a round moved nothing but jobs remain (held by
 * another lane, or still running on a v1 worker). */
const IDLE_DELAY_MS = 60_000
/** The longest pause a lane's next job can be sent with (the SQS delay limit). */
const MAX_DELAY_MS = 15 * 60_000
/** How far a job SQS did not take is pushed back: behind the jobs that are due, so one that
 * SQS keeps refusing cannot hold them up. It goes out again, with that delay, when a round has
 * room for it. */
const UNSENT_BACKOFF_SECONDS = 5 * 60
/**
 * The largest v1 job moved. An SQS message holds 1,048,576 bytes (1 MiB) with its attributes,
 * and the storage queues are provisioned to allow that much (`maxMessageSize` in the
 * infrastructure repo). They have to be: a queue left at the older 262,144 limit refuses a
 * larger job, and the send that carries it fails.
 *
 * The size is measured in Postgres, as the job's JSON text, so that a job too large is never
 * loaded. That text is a little longer than the JSON that is sent, and the margin covers the
 * message attributes and the fields v2 adds to the payload. A larger job is cancelled in v10.
 */
const MAX_JOB_BYTES = 1_000_000

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
  /** Jobs too large for an SQS message: cancelled in v10. */
  readonly cancelled: number
  /** Whether v10 still holds jobs a later round could move. */
  readonly remaining: boolean
}

interface V1JobRow {
  id: string
  /** Not loaded for a job that is too large to move. */
  data: Record<string, unknown> | null
  bytes: number
  delay_ms: number
}

/** The columns a round reads. The payload of a job too large to move is left on the server. */
const JOB_COLUMNS = `id, octet_length(data::text) AS bytes,
              CASE WHEN octet_length(data::text) <= ${MAX_JOB_BYTES} THEN data END AS data`

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
  let cancelled = 0
  for (const { mapping, cls } of inTurn) {
    if (budget <= 0 || signal.aborted) break
    const chunk = await moveChunk(db, job, mapping, cls, target, budget, signal)
    moved += chunk.moved
    unsent += chunk.unsent
    cancelled += chunk.cancelled
    // Every job a round touches counts, whatever became of it: a round never handles more
    // than its budget.
    budget -= chunk.moved + chunk.unsent + chunk.cancelled
  }

  const left = await db.query<{ found: boolean }>(
    `SELECT EXISTS (
       SELECT 1 FROM ${job} WHERE name = ANY($1::text[]) AND state <= 'active'
     ) AS found`,
    [mapped.map(({ mapping }) => mapping.v1Queue)]
  )
  return { moved, unsent, cancelled, remaining: left.rows[0]?.found === true }
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
): Promise<{ moved: number; unsent: number; cancelled: number }> {
  // A FIFO queue takes no per-message delay.
  const maxDelayMs = target.isFifo(mapping.queue) ? 0 : MAX_JOB_DELAY_MS
  const lost = new AbortController()
  const checkout = await checkoutClient(db, (error) => {
    logSchema.warning(logger, '[MoveJobs][v10→sqs] database client error', {
      type: 'queue',
      error,
    })
    lost.abort(error)
  })
  const { client } = checkout
  let releaseError: Error | undefined
  try {
    checkout.throwIfErrored()
    await client.query('BEGIN')
    // The predicate and order match v10's fetch index (name, start_after WHERE state < 'active').
    const queued = await client.query<V1JobRow>(
      `SELECT ${JOB_COLUMNS},
              LEAST($2::float8, GREATEST(0, ceil(extract(epoch FROM start_after - now()) * 1000)))::int
                AS delay_ms
       FROM ${job}
       WHERE name = $1 AND state < 'active'
       ORDER BY start_after
       LIMIT $3
       FOR UPDATE SKIP LOCKED`,
      [mapping.v1Queue, maxDelayMs, limit]
    )
    const claimed = queued.rows
    if (claimed.length < limit) {
      // The queue is nearly drained, so this scan is over few rows. A job a v1 worker left
      // active past its own expiry: v10's maintenance would have re-queued it, and nothing
      // runs that once the v1 fleet is gone.
      const expired = await client.query<V1JobRow>(
        `SELECT ${JOB_COLUMNS}, 0 AS delay_ms
         FROM ${job}
         WHERE name = $1 AND state = 'active' AND started_on + expire_in < now()
         LIMIT $2
         FOR UPDATE SKIP LOCKED`,
        [mapping.v1Queue, limit - claimed.length]
      )
      claimed.push(...expired.rows)
    }
    if (claimed.length === 0) {
      await client.query('COMMIT')
      return { moved: 0, unsent: 0, cancelled: 0 }
    }

    // A job no SQS message can hold will never move. Cancelled, it stays readable in v10
    // (payload included) but no longer counts as backlog, so the lanes can finish.
    const tooLarge = claimed.filter((row) => row.bytes > MAX_JOB_BYTES)
    if (tooLarge.length > 0) {
      await client.query(
        `UPDATE ${job}
         SET state = 'cancelled', completed_on = now(),
             output = jsonb_build_object('reason', 'too large to move to sqs', 'bytes', octet_length(data::text))
         WHERE name = $1 AND id = ANY($2::uuid[])`,
        [mapping.v1Queue, tooLarge.map((row) => row.id)]
      )
      logSchema.warning(
        logger,
        `[MoveJobs][v10→sqs] cancelled ${tooLarge.length} ${mapping.v1Queue} jobs too large for an SQS message`,
        {
          type: 'queue',
          metadata: JSON.stringify(tooLarge.map((row) => ({ id: row.id, bytes: row.bytes }))),
        }
      )
    }
    const rows = claimed.filter((row) => row.bytes <= MAX_JOB_BYTES)
    if (rows.length === 0) {
      await client.query('COMMIT')
      return { moved: 0, unsent: 0, cancelled: tooLarge.length }
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
      await target.send(messages, AbortSignal.any([signal, lost.signal]))
    } catch (error) {
      checkout.throwIfErrored()
      // Any other failure says nothing about what landed: roll back and send it all again.
      if (!(error instanceof PartialSendError)) throw error
      notTaken = error.unsent
      logSchema.warning(
        logger,
        `[MoveJobs][v10→sqs] ${notTaken.size} of ${rows.length} ${mapping.v1Queue} jobs were not accepted; left in v10`,
        { type: 'queue', error }
      )
    }
    checkout.throwIfErrored()

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
    return { moved: sentIds.length, unsent: unsentIds.length, cancelled: tooLarge.length }
  } catch (error) {
    const failure = checkout.error ?? error
    if (!checkout.error) {
      await client.query('ROLLBACK').catch((rollbackError) => {
        releaseError ??=
          rollbackError instanceof Error ? rollbackError : new Error(String(rollbackError))
      })
    }
    throw failure
  } finally {
    checkout.release(releaseError)
  }
}

/** The pause before a lane's next round, or `undefined` when the lane is done. */
export function nextRoundDelay(
  payload: Pick<MoveJobsToSqsPayload, 'delayMs' | 'expiresAt'>,
  result: MoveRoundResult,
  now: number
): number | undefined {
  if (!result.remaining || now >= payload.expiresAt) return undefined
  // A round that handled nothing found only jobs it cannot take yet: come back less often.
  const busy = result.moved + result.cancelled > 0
  const delay = busy ? payload.delayMs : Math.max(payload.delayMs, IDLE_DELAY_MS)
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
      `[MoveJobs][v10→sqs] ${lane}: moved ${round.moved}, not accepted ${round.unsent}, cancelled ${round.cancelled}` +
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
