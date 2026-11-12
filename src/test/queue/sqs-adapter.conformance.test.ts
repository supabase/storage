import { describeAdapterConformance } from '../../internal/queue/adapters/conformance'
import { type SqsAdapter, sqs } from '../../internal/queue/adapters/sqs'
import type { QueueAdapter } from '../../internal/queue/manager/adapter'
import type { QueuesView } from '../../internal/queue/manager/registry'
import { sqsClientConfig, sqsEndpoint } from './sqs-helpers'

// The portable adapter contract against an SQS endpoint, skipped entirely without one (see
// sqs-adapter.test.ts for the command). Run twice: every queue standard, then every queue
// FIFO, which trade delay for dedup.

const create = (): SqsAdapter =>
  sqs({ client: sqsClientConfig(), prefix: 'qc-', waitTimeSeconds: 0 })

// SQS budgets by deliveries, and reads only the counters it publishes per queue. A visibility
// timeout and a message delay are whole seconds, hence the one-second floors.
const shared = {
  retryAccounting: 'deliveries',
  stats: ['backlog', 'inFlight'],
  timing: { livenessMs: 1_000, waitMs: 8_000, quietMs: 250, delayMs: 1_000 },
} as const

/** A registry in which every queue is declared FIFO, handed to each send and subscribe. */
const everyQueueFifo: QueuesView = {
  get: () => ({ options: { sqs: { fifo: true } } }) as ReturnType<QueuesView['get']>,
  names: () => [],
  classIndex: new Map(),
}

const createFifo = (): QueueAdapter<string> => {
  const { delayedDelivery: _perMessageDelayIsStandardOnly, ...adapter } = create()
  return {
    ...adapter,
    provision: (queue, options) => adapter.provision(queue, { sqs: { fifo: true }, ...options }),
    send: (queue, messages, ctx, signal) =>
      adapter.send(queue, messages, { ...ctx, queues: everyQueueFifo }, signal),
    subscribe: (queue, policy) => adapter.subscribe(queue, { ...policy, queues: everyQueueFifo }),
  }
}

describe.skipIf(sqsEndpoint === undefined)('sqs queue adapter', () => {
  describeAdapterConformance({
    name: 'sqs (standard queues)',
    create,
    capabilities: { ...shared, dedup: false },
  })

  describeAdapterConformance({
    name: 'sqs (FIFO queues)',
    create: createFifo,
    capabilities: { ...shared, delay: false },
  })
})
