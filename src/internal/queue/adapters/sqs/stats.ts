import type { SQSClient } from '@aws-sdk/client-sqs'
import {
  type BoundedCount,
  DEFAULT_STATS_COUNT_CAP,
  type QueueStats,
  type StatsQuery,
  type StatsReport,
} from '../../manager/stats'
import { mapWithConcurrency } from './client/batching'
import { readQueueMessageCounts } from './client/queues'
import type { Queues } from './queues'

/** Queue attribute reads in flight at once during one sample. */
const READ_CONCURRENCY = 8

/**
 * The sampled gauges, read from each queue's own counters: visible and delayed messages are
 * the backlog, messages inside a visibility timeout are in flight. The numbers are the
 * approximations SQS publishes, so they can lag a burst by some seconds.
 *
 * Without `query.queues` the report covers the queues this instance has resolved (every
 * provisioned, sent-to or subscribed queue and their dead-letter queues): an account's queue
 * list says nothing about which queues are this deployment's. A named queue that does not
 * exist is left out, and nothing is ever created.
 *
 * `retryParked` stays absent: a parked retry is a message whose visibility timeout was
 * extended, which SQS counts as in flight like one a worker is holding. `oldestBacklogAgeMs`
 * stays absent too: it is a CloudWatch metric, not a queue attribute.
 */
export async function statsReport(
  sqs: SQSClient,
  queues: Queues,
  query: StatsQuery | undefined,
  signal: AbortSignal | undefined
): Promise<StatsReport> {
  const cap = query?.countCap ?? DEFAULT_STATS_COUNT_CAP
  const bounded = (count: number): BoundedCount =>
    count > cap ? { count: cap, saturated: true } : { count, saturated: false }

  const names = [...new Set(query?.queues ?? queues.resolvedNames())].sort()
  const rows = await mapWithConcurrency(
    names,
    READ_CONCURRENCY,
    async (queue): Promise<QueueStats | undefined> => {
      const url = await queues.findUrl(queue, signal)
      // `undefined` counts: the queue was deleted between the lookup and the read.
      const counts = url !== undefined ? await readQueueMessageCounts(sqs, url, signal) : undefined
      if (counts === undefined) return undefined
      return {
        queue,
        backlog: bounded(counts.visible + counts.delayed),
        inFlight: bounded(counts.inFlight),
      }
    }
  )
  return { queues: rows.filter((row) => row !== undefined) }
}
