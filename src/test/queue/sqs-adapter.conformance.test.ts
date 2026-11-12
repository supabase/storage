import { describeAdapterConformance } from '../../internal/queue/adapters/conformance'
import { type SqsAdapter, sqs } from '../../internal/queue/adapters/sqs'
import type { QueueAdapter } from '../../internal/queue/manager/adapter'
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

/**
 * The same adapter with every queue FIFO: `provision` adds the kind to what the suite
 * declares, and a queue the suite sends or subscribes to without provisioning is declared
 * FIFO first (a registry would do it too, but would also replace the suite's policy).
 */
const createFifo = (): QueueAdapter<string> => {
  const { delayedDelivery: _perMessageDelayIsStandardOnly, ...adapter } = create()
  const declared = new Set<string>()
  const provision: QueueAdapter<string>['provision'] = (queue, options) => {
    declared.add(queue)
    return adapter.provision(queue, { ...options, sqs: { fifo: true } })
  }
  const declareOnce = async (queue: string): Promise<void> => {
    if (!declared.has(queue)) await provision(queue)
  }
  return {
    ...adapter,
    provision,
    async send(queue, messages, ctx, signal) {
      await declareOnce(queue)
      return adapter.send(queue, messages, ctx, signal)
    },
    async subscribe(queue, policy) {
      await declareOnce(queue)
      return adapter.subscribe(queue, policy)
    },
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
