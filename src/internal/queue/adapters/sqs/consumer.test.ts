import type { SQSClient } from '@aws-sdk/client-sqs'
import { vi } from 'vitest'
import { sqs } from './index'

type Receive = () => Promise<{ Messages?: unknown[] }>

const message = (n: number) => ({
  MessageId: `id-${n}`,
  ReceiptHandle: `handle-${n}`,
  Body: JSON.stringify({ n }),
  Attributes: { ApproximateReceiveCount: '1', SentTimestamp: '0' },
})
const batch = (from: number, count = 10) => ({
  Messages: Array.from({ length: count }, (_, i) => message(from + i)),
})

/** An SQS client whose receives are scripted, one entry per ReceiveMessage call in order. */
function fakeClient(receives: Receive[]) {
  const released: string[] = []
  let call = 0
  const send = vi.fn(async (command: { constructor: { name: string }; input: never }) => {
    const input = command.input as Record<string, never>
    switch (command.constructor.name) {
      case 'GetQueueUrlCommand':
        return { QueueUrl: 'https://sqs.test/q' }
      case 'GetQueueAttributesCommand':
        return { Attributes: { QueueArn: 'arn:aws:sqs:test:0:q', VisibilityTimeout: '300' } }
      case 'ReceiveMessageCommand':
        return (receives[call++] ?? (async () => ({})))()
      case 'ChangeMessageVisibilityBatchCommand': {
        const entries = input.Entries as { Id: string; ReceiptHandle: string }[]
        released.push(...entries.map((entry) => entry.ReceiptHandle))
        return { Successful: entries.map(({ Id }) => ({ Id })) }
      }
      default:
        return {}
    }
  })
  return { client: { send, destroy: vi.fn() } as unknown as SQSClient, released }
}

async function pull(receives: Receive[], max: number) {
  const fake = fakeClient(receives)
  const adapter = sqs({ sqs: fake.client })
  const subscription = await adapter.subscribe('q', {})
  const pulled = subscription.pull(max)
  return { ...fake, pulled, ids: async () => (await pulled).map((m) => m.id) }
}

describe('sqs pull beyond one receive', () => {
  afterEach(() => vi.useRealTimers())

  it('returns a short first receive at once, without filling', async () => {
    const { ids, client } = await pull([async () => batch(0, 3)], 30)

    expect(await ids()).toEqual(['id-0', 'id-1', 'id-2'])
    expect(
      vi
        .mocked(client.send)
        .mock.calls.filter(([command]) => command.constructor.name === 'ReceiveMessageCommand')
    ).toHaveLength(1)
  })

  it('fills the batch with parallel receives and skips the ones that fail', async () => {
    const { ids } = await pull(
      [
        async () => batch(0),
        async () => batch(10),
        async () => {
          throw new Error('throttled')
        },
      ],
      30
    )

    expect(await ids()).toEqual(
      [...batch(0).Messages, ...batch(10).Messages].map((m) => m.MessageId)
    )
  })

  it('hands the batch out after one second and gives back what answers later', async () => {
    vi.useFakeTimers()
    const late = Promise.withResolvers<{ Messages?: unknown[] }>()
    const { pulled, released } = await pull(
      [async () => batch(0), async () => batch(10), () => late.promise],
      30
    )
    let done = false
    void pulled.then(() => {
      done = true
    })

    await vi.advanceTimersByTimeAsync(999)
    expect(done).toBe(false)
    await vi.advanceTimersByTimeAsync(1)
    expect((await pulled).map((m) => m.id)).toHaveLength(20)

    // The slow receive answers after the batch was handed out: its messages are released.
    late.resolve(batch(20, 2))
    await vi.advanceTimersByTimeAsync(0)
    expect(released).toEqual(['handle-20', 'handle-21'])
  })
})
