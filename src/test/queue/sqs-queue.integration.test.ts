import { randomUUID } from 'node:crypto'
import { sqs } from '../../internal/queue/adapters/sqs'
import { describeQueueIntegration } from './queue-integration'
import { sqsClientConfig, sqsEndpoint } from './sqs-helpers'

// The manager end to end over an SQS endpoint, skipped entirely without one (see
// sqs-adapter.test.ts for the command). Standard queues under a per-run prefix; the dedup
// test alone runs on a FIFO queue, which is what gives SQS producer deduplication.
const prefix = `qi-${randomUUID().slice(0, 8)}-`

describe.skipIf(sqsEndpoint === undefined)('sqs queue', () => {
  describeQueueIntegration({
    name: 'sqs',
    createAdapter: () => sqs({ client: sqsClientConfig(), prefix, waitTimeSeconds: 0 }),
    dedup: { queueOptions: { sqs: { fifo: true } } },
    // The liveness contract is the visibility timeout, in whole seconds.
    livenessMs: 2_000,
  })
})
