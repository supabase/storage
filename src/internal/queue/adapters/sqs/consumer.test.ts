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
  const renewed: string[] = []
  const refuseRenewalOf = new Set<string>()
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
        const entries = input.Entries as {
          Id: string
          ReceiptHandle: string
          VisibilityTimeout: number
        }[]
        const accepted = entries.filter((entry) => !refuseRenewalOf.has(entry.ReceiptHandle))
        for (const entry of accepted) {
          const list = entry.VisibilityTimeout === 0 ? released : renewed
          list.push(entry.ReceiptHandle)
        }
        return {
          Successful: accepted.map(({ Id }) => ({ Id })),
          // What SQS answers for a handle that is no longer the message's current one.
          Failed: entries
            .filter((entry) => refuseRenewalOf.has(entry.ReceiptHandle))
            .map(({ Id }) => ({ Id, Code: 'ReceiptHandleIsInvalid', SenderFault: true })),
        }
      }
      default:
        return {}
    }
  })
  return {
    client: { send, destroy: vi.fn() } as unknown as SQSClient,
    released,
    renewed,
    refuseRenewalOf,
  }
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

  async function cutOffAfterOneSecond(extraReceives: Receive[] = []) {
    vi.useFakeTimers()
    const late = Promise.withResolvers<{ Messages?: unknown[] }>()
    const fake = fakeClient([
      async () => batch(0),
      async () => batch(10),
      () => late.promise,
      ...extraReceives,
    ])
    const subscription = await sqs({ sqs: fake.client }).subscribe('q', {})
    const pulled = subscription.pull(30)
    let done = false
    void pulled.then(() => {
      done = true
    })

    await vi.advanceTimersByTimeAsync(999)
    expect(done).toBe(false)
    await vi.advanceTimersByTimeAsync(1)
    expect(await pulled).toHaveLength(20)

    // The slow receive answers after the batch was handed out.
    late.resolve(batch(20, 2))
    await vi.advanceTimersByTimeAsync(0)
    return { ...fake, subscription }
  }

  it('hands the batch out after one second and carries a late answer into the next pull', async () => {
    const { subscription, released, renewed, client } = await cutOffAfterOneSecond([
      async () => batch(30, 1),
    ])

    const next = await subscription.pull(30)

    // Carried first, with their lease restarted, then a short poll for the rest: nothing was
    // released or fetched twice.
    expect(next.map((m) => m.id)).toEqual(['id-20', 'id-21', 'id-30'])
    expect(renewed).toEqual(['handle-20', 'handle-21'])
    expect(released).toEqual([])
    const receives = vi
      .mocked(client.send)
      .mock.calls.filter(([command]) => command.constructor.name === 'ReceiveMessageCommand')
    const lastReceive = receives[receives.length - 1][0].input as { WaitTimeSeconds: number }
    expect(lastReceive.WaitTimeSeconds).toBe(0)
  })

  it('drops a carried message whose lease SQS does not renew', async () => {
    const { subscription, refuseRenewalOf } = await cutOffAfterOneSecond([async () => ({})])
    refuseRenewalOf.add('handle-20')

    expect((await subscription.pull(30)).map((m) => m.id)).toEqual(['id-21'])
  })

  it('does not hand out a carried message whose lease is over', async () => {
    const { subscription } = await cutOffAfterOneSecond([async () => ({})])

    // The default liveness window is 300s: by then the carried messages are someone else's.
    await vi.advanceTimersByTimeAsync(300_000)

    expect(await subscription.pull(30)).toEqual([])
  })

  it('gives carried messages back when the subscription closes', async () => {
    const { subscription, released } = await cutOffAfterOneSecond()

    await subscription.close()

    expect(released).toEqual(['handle-20', 'handle-21'])
  })
})
