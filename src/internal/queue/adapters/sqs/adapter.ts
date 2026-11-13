import { SQSClient } from '@aws-sdk/client-sqs'
import type { QueueAdapter, QueueSubscription } from '../../manager/adapter'
import { jsonText } from '../../manager/codec'
import type { StatsQuery, StatsReport } from '../../manager/stats'
import { createSubscribe } from './consumer'
import type { SqsOptions } from './options'
import { createSend } from './producer'
import { createQueues } from './queues'
import { statsReport } from './stats'

/** The SQS adapter: a {@link QueueAdapter} that also delays messages and reports stats. */
export interface SqsAdapter extends QueueAdapter<string> {
  /** Honoured on standard queues; a send with a delay to a FIFO queue throws. */
  readonly delayedDelivery: true
  stats(query?: StatsQuery, signal?: AbortSignal): Promise<StatsReport>
}

/**
 * The SQS adapter. One declared queue is one SQS queue; SQS decides which consumer gets which
 * message. Assembled from its roles: `queues.ts` (which SQS queue realizes a declared queue),
 * `producer.ts` (send), `consumer.ts` and `settle.ts` (subscriptions) and `stats.ts`. The calls to
 * SQS themselves live under `client/`.
 *
 * There is no maintenance pass (SQS runs its own redrive and retention) and no produce
 * context, because nothing here can join a database transaction.
 *
 * Two limits of SQS show through:
 * - A delivery's attempt is SQS's receive count, and a release does not lower it. Handing a
 *   message back (a worker shutting down) therefore uses one of the queue's attempts.
 * - A delivery can be held for twelve hours from its receive at most. Past that SQS refuses the
 *   heartbeat and redelivers the message, so a handler must finish or give up before then.
 */
export function sqs(options: SqsOptions = {}): SqsAdapter {
  const waitTimeSeconds = options.waitTimeSeconds ?? 20
  if (!Number.isInteger(waitTimeSeconds) || waitTimeSeconds < 0 || waitTimeSeconds > 20) {
    throw new Error(
      `queue sqs: waitTimeSeconds must be a whole number from 0 to 20, got ${waitTimeSeconds}`
    )
  }
  const prefix = options.prefix ?? ''
  if (!/^[A-Za-z0-9_-]*$/.test(prefix)) {
    throw new Error(`queue sqs: invalid prefix "${prefix}": use letters, digits, "-" and "_"`)
  }
  const provisioning = options.provisioning ?? 'manage'
  if (provisioning !== 'manage' && provisioning !== 'verify') {
    throw new Error(
      `queue sqs: provisioning must be "manage" or "verify", got "${String(provisioning)}"`
    )
  }

  // Ownership follows construction: a client the adapter built is destroyed at close, a
  // client the caller passed is theirs.
  const client = options.sqs ?? new SQSClient(options.client ?? {})

  const queues = createQueues({
    sqs: client,
    prefix,
    queueName: options.queueName,
    manage: provisioning === 'manage',
    queueAttributes: options.queueAttributes ?? {},
  })
  const openSubscriptions = new Set<QueueSubscription<string>>()

  return {
    name: 'sqs',
    codec: options.codec ?? jsonText(),
    delayedDelivery: true,

    async close() {
      // Closing a subscription cancels its waiting receive, so nothing outlives the adapter.
      for (const subscription of [...openSubscriptions]) await subscription.close()
      if (options.sqs === undefined) client.destroy()
    },

    provision: queues.provision,
    send: createSend({ sqs: client, queues }),
    subscribe: createSubscribe({
      sqs: client,
      queues,
      waitTimeSeconds,
      events: options.events,
      openSubscriptions,
    }),
    stats: (query, signal) => statsReport(client, queues, query, signal),
  }
}
