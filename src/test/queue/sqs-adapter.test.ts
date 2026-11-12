import {
  ReceiveMessageCommand,
  SendMessageBatchCommand,
  type SendMessageBatchCommandOutput,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs'
import { type SqsAdapter, type SqsOptions, sqs } from '../../internal/queue/adapters/sqs'
import { NotSupportedError, type SubscribePolicy } from '../../internal/queue/manager/adapter'
import type { AdapterEvents } from '../../internal/queue/manager/events'
import type { ProduceMessage, PullMessage } from '../../internal/queue/manager/message'
import { defineQueues, queue } from '../../internal/queue/manager/registry'
import {
  queueAttributesOf,
  queueUrlOf,
  rawSqsClient,
  sleep,
  sqsClientConfig,
  sqsEndpoint,
  uniqueName,
  until,
} from './sqs-helpers'

// Type-level: the `sqs` namespace is merged into the manager's QueueOptions.
queue<{ n: number }>({ sqs: { fifo: true, attributes: { VisibilityTimeout: '30' } } })
// @ts-expect-error a typo inside the namespace is rejected
queue<{ n: number }>({ sqs: { fiffo: true } })

type DeadLettered = Parameters<NonNullable<AdapterEvents['deadLettered']>>[0]

// Runs against an SQS endpoint, skipped entirely without one:
//
//   QUEUE_TEST_SQS_ENDPOINT=http://127.0.0.1:9324 npx vitest run \
//     --config vitest.integration.config.ts src/test/queue/sqs-adapter.test.ts
describe.skipIf(sqsEndpoint === undefined)('sqs queue adapter', () => {
  const PREFIX = 'qt-'
  const adapters: SqsAdapter[] = []
  let raw: SQSClient

  // Short polls by default: most tests assert "nothing arrives" by pulling.
  const make = (options: SqsOptions = {}): SqsAdapter => {
    const adapter = sqs({
      client: sqsClientConfig(),
      prefix: PREFIX,
      waitTimeSeconds: 0,
      ...options,
    })
    adapters.push(adapter)
    return adapter
  }

  beforeAll(() => {
    raw = rawSqsClient()
  })
  afterEach(async () => {
    for (const adapter of adapters.splice(0)) await adapter.close()
  })
  afterAll(() => {
    raw.destroy()
  })

  const policy = (extra: Partial<SubscribePolicy> = {}): SubscribePolicy => ({
    consumerId: uniqueName('c'),
    livenessTimeoutMs: 2_000,
    ...extra,
  })
  const text = (
    n: number,
    extra?: Omit<ProduceMessage<string>, 'data'>
  ): ProduceMessage<string> => ({
    data: JSON.stringify({ n }),
    headers: {},
    ...extra,
  })
  const n = (message: { data: string }): number => (JSON.parse(message.data) as { n: number }).n
  const pullOne = (subscription: { pull(max: number): Promise<PullMessage<string>[]> }) =>
    until(async () => {
      const pulled = await subscription.pull(10)
      expect(pulled).toHaveLength(1)
      return pulled[0] as PullMessage<string>
    })
  const receiveRaw = async (name: string) => {
    const out = await raw.send(
      new ReceiveMessageCommand({
        QueueUrl: await queueUrlOf(raw, name),
        MaxNumberOfMessages: 10,
        MessageAttributeNames: ['All'],
        MessageSystemAttributeNames: ['All'],
        VisibilityTimeout: 0,
      })
    )
    return out.Messages ?? []
  }

  describe('options', () => {
    it('rejects configuration it cannot honor, before touching SQS', () => {
      expect(() => sqs({ waitTimeSeconds: 21 })).toThrow(/queue sqs: waitTimeSeconds/)
      expect(() => sqs({ prefix: 'has space' })).toThrow(/invalid prefix/)
      expect(() => sqs({ provisioning: 'auto' as never })).toThrow(/provisioning/)
    })

    it('leaves a caller-owned client open at close and destroys one it built', async () => {
      const owned = rawSqsClient()
      const destroy = vi.spyOn(owned, 'destroy')
      const adapter = sqs({ sqs: owned, prefix: PREFIX })
      await adapter.provision(uniqueName('own'))
      await adapter.close()
      expect(destroy).not.toHaveBeenCalled()
      owned.destroy()
    })
  })

  describe('provisioning', () => {
    it('creates one standard queue per declared queue, idempotently, with its attributes', async () => {
      const adapter = make({ queueAttributes: { VisibilityTimeout: '45', DelaySeconds: '0' } })
      const name = uniqueName('p')
      await adapter.provision(name, { sqs: { attributes: { VisibilityTimeout: '77' } } })
      await adapter.provision(name)
      await make().provision(name)

      const attributes = await queueAttributesOf(raw, `${PREFIX}${name}`, ['All'])
      // The queue's own attributes win over the adapter's.
      expect(attributes.VisibilityTimeout).toBe('77')
      expect(attributes.FifoQueue ?? 'false').toBe('false')
      await expect(queueUrlOf(raw, `${PREFIX}${name}.fifo`)).rejects.toThrow()
    })

    it('creates a FIFO queue for sqs: { fifo: true } and refuses the other kind afterwards', async () => {
      const adapter = make()
      const name = uniqueName('p')
      await adapter.provision(name, { sqs: { fifo: true } })
      expect((await queueAttributesOf(raw, `${PREFIX}${name}.fifo`, ['All'])).FifoQueue).toBe(
        'true'
      )

      await adapter.provision(name) // nothing declared: only makes sure it exists
      await expect(adapter.provision(name, { sqs: { fifo: false } })).rejects.toThrow(
        /in use as a FIFO queue.*asked\s+for as a standard one/
      )
    })

    it('reads a queue declared only in the registry, without a provision call', async () => {
      const name = uniqueName('p')
      const queues = defineQueues({ [name]: queue<{ n: number }>({ sqs: { fifo: true } }) })
      const adapter = make()
      await adapter.send(name, [text(1)], { queues })
      const subscription = await adapter.subscribe(name, policy({ queues }))
      expect(n(await pullOne(subscription))).toBe(1)
      await queueUrlOf(raw, `${PREFIX}${name}.fifo`)
    })

    it('maps names through queueName and rejects one SQS could not carry', async () => {
      const name = uniqueName('p')
      const adapter = make({
        queueName: (declared, fifo) => `adopted-${declared}${fifo ? '.fifo' : ''}`,
      })
      await adapter.provision(name)
      await queueUrlOf(raw, `adopted-${name}`)

      await expect(make().provision('has:colon')).rejects.toThrow(
        /queue "has:colon" maps to the SQS name "qt-has:colon", which is not valid/
      )
      await expect(make().provision('x'.repeat(80))).rejects.toThrow(/80 characters at most/)
      await expect(
        make({ queueName: (declared) => declared }).provision(name, { sqs: { fifo: true } })
      ).rejects.toThrow(/must end in "\.fifo"/)
    })

    it('verify uses queues that exist and creates or changes nothing', async () => {
      const name = uniqueName('v')
      const dlq = uniqueName('vdlq')
      const manager = make()
      await manager.provision(name)
      await manager.provision(dlq)

      const verifier = make({ provisioning: 'verify' })
      await verifier.provision(name)
      await verifier.send(name, [text(1)], {})
      const subscription = await verifier.subscribe(
        name,
        policy({ retry: { maxAttempts: 3, deadLetter: dlq } })
      )
      const got = await pullOne(subscription)
      await subscription.ack([got.receipt])
      // The redrive policy is a change, so verify leaves it alone.
      expect((await queueAttributesOf(raw, `${PREFIX}${name}`, ['All'])).RedrivePolicy ?? '').toBe(
        ''
      )

      const ghost = uniqueName('v')
      await expect(verifier.provision(ghost)).rejects.toThrow(
        /does not exist, and provisioning is "verify"/
      )
      await expect(verifier.send(ghost, [text(1)], {})).rejects.toThrow(/does not exist/)
      await expect(verifier.subscribe(ghost, policy())).rejects.toThrow(/does not exist/)
      await expect(queueUrlOf(raw, `${PREFIX}${ghost}`)).rejects.toThrow()
    })
  })

  describe('wire format', () => {
    it('writes the payload as the body and packs headers and key into attributes', async () => {
      const adapter = make()
      const name = uniqueName('w')
      await adapter.send(
        name,
        [{ data: '{"hello":"world"}', headers: { trace: 't1', tenant: 'acme' }, key: 'k1' }],
        {}
      )
      const [received] = await until(async () => {
        const messages = await receiveRaw(`${PREFIX}${name}`)
        expect(messages).toHaveLength(1)
        return messages
      })
      expect(received?.Body).toBe('{"hello":"world"}')
      expect(Object.keys(received?.MessageAttributes ?? {}).sort()).toEqual([
        'queue.headers',
        'queue.key',
      ])
      expect(
        JSON.parse(received?.MessageAttributes?.['queue.headers']?.StringValue as string)
      ).toEqual({ trace: 't1', tenant: 'acme' })

      const subscription = await adapter.subscribe(name, policy())
      const got = await pullOne(subscription)
      expect(got).toMatchObject({
        id: received?.MessageId,
        data: '{"hello":"world"}',
        headers: { trace: 't1', tenant: 'acme' },
        key: 'k1',
      })
      expect(Math.abs(got.timestamp - Date.now())).toBeLessThan(60_000)
    })

    it('sends a message without headers or key with no attributes at all', async () => {
      const adapter = make()
      const name = uniqueName('w')
      await adapter.send(name, [{ data: '1', headers: {} }], {})
      const [received] = await until(async () => {
        const messages = await receiveRaw(`${PREFIX}${name}`)
        expect(messages).toHaveLength(1)
        return messages
      })
      expect(received?.MessageAttributes ?? {}).toEqual({})
    })

    it("consumes another producer's message, its string attributes as headers", async () => {
      const adapter = make()
      const name = uniqueName('w')
      await adapter.provision(name)
      await raw.send(
        new SendMessageCommand({
          QueueUrl: await queueUrlOf(raw, `${PREFIX}${name}`),
          MessageBody: '{"n":7}',
          MessageAttributes: { source: { DataType: 'String', StringValue: 'legacy' } },
        })
      )
      const subscription = await adapter.subscribe(name, policy())
      const got = await pullOne(subscription)
      expect(n(got)).toBe(7)
      expect(got.headers).toEqual({ source: 'legacy' })
      expect(got.key).toBeUndefined()
      expect(got.attempt).toBe(1)
    })

    it('rejects a payload the codec did not encode to text, with nothing sent', async () => {
      const adapter = make()
      const name = uniqueName('w')
      await expect(
        adapter.send(name, [text(1), { data: { n: 2 } as never, headers: {} }], {})
      ).rejects.toThrow(/non-string payload/)
      const subscription = await adapter.subscribe(name, policy())
      await sleep(200)
      expect(await subscription.pull(10)).toHaveLength(0)
    })
  })

  describe('send', () => {
    it('splits a send larger than one batch call and a pull larger than one receive', async () => {
      const adapter = make()
      const name = uniqueName('b')
      const subscription = await adapter.subscribe(name, policy({ livenessTimeoutMs: 30_000 }))
      await adapter.send(
        name,
        Array.from({ length: 35 }, (_, i) => text(i)),
        {}
      )
      const seen = new Set<number>()
      let largest = 0
      await until(async () => {
        const pulled = await subscription.pull(30)
        largest = Math.max(largest, pulled.length)
        for (const message of pulled) seen.add(n(message))
        await subscription.ack(pulled.map((message) => message.receipt))
        expect(seen.size).toBe(35)
      })
      expect(largest).toBeGreaterThan(10) // one pull spanned several receives
      expect(largest).toBeLessThanOrEqual(30)
    })

    it('splits batch calls by size as well as by count', async () => {
      const client = rawSqsClient()
      const send = vi.spyOn(client, 'send')
      const adapter = make({ sqs: client })
      const name = uniqueName('b')
      await adapter.provision(name)
      send.mockClear()
      // Three 100 KiB bodies: two fit one 256 KiB call, the third needs another.
      const body = 'x'.repeat(100 * 1024)
      await adapter.send(
        name,
        [1, 2, 3].map(() => ({ data: body, headers: {} })),
        {}
      )
      const batchSizes = send.mock.calls
        .map(([command]) => command)
        .filter((command) => command instanceof SendMessageBatchCommand)
        .map((command) => command.input.Entries?.length)
        .sort()
      expect(batchSizes).toEqual([1, 2])
      client.destroy()
    })

    // The real client, with the first SendMessageBatch call reporting every second entry as
    // failed (and not sending it), the way SQS reports a partial failure.
    const partiallyFailingClient = (senderFault: boolean) => {
      const client = rawSqsClient()
      const original = client.send.bind(client) as (command: unknown) => Promise<unknown>
      let batchCalls = 0
      vi.spyOn(client, 'send').mockImplementation((async (command: unknown) => {
        if (!(command instanceof SendMessageBatchCommand) || batchCalls++ > 0) {
          return original(command)
        }
        const entries = command.input.Entries ?? []
        const kept = entries.filter((_, index) => index % 2 === 0)
        const failed = entries.filter((_, index) => index % 2 === 1)
        const output = (await original(
          new SendMessageBatchCommand({ ...command.input, Entries: kept })
        )) as SendMessageBatchCommandOutput
        return {
          ...output,
          Failed: failed.map((entry) => ({
            Id: entry.Id,
            Code: senderFault ? 'InvalidMessageContents' : 'InternalError',
            Message: 'injected',
            SenderFault: senderFault,
          })),
        }
      }) as never)
      return { client, batchCalls: () => batchCalls }
    }

    it('sends the entries SQS failed on its own side again, and only those', async () => {
      const { client, batchCalls } = partiallyFailingClient(false)
      const adapter = make({ sqs: client })
      const name = uniqueName('f')
      await adapter.send(
        name,
        Array.from({ length: 6 }, (_, i) => text(i)),
        {}
      )
      expect(batchCalls()).toBe(2)

      const subscription = await adapter.subscribe(name, policy({ livenessTimeoutMs: 30_000 }))
      const seen: number[] = []
      await until(async () => {
        for (const message of await subscription.pull(10)) seen.push(n(message))
        expect(seen.length).toBeGreaterThanOrEqual(6)
      })
      await sleep(300)
      for (const message of await subscription.pull(10)) seen.push(n(message))
      expect(seen.sort()).toEqual([0, 1, 2, 3, 4, 5]) // each exactly once
      client.destroy()
    })

    it("fails the send at once on an entry SQS rejects as the sender's fault", async () => {
      const { client, batchCalls } = partiallyFailingClient(true)
      const adapter = make({ sqs: client })
      const name = uniqueName('f')
      await expect(
        adapter.send(
          name,
          Array.from({ length: 4 }, (_, i) => text(i)),
          {}
        )
      ).rejects.toThrow(/queue sqs: 2 of 4 messages were not accepted.*InvalidMessageContents/)
      expect(batchCalls()).toBe(1)
      client.destroy()
    })

    it('does nothing for an empty send, not even resolving the queue', async () => {
      const adapter = make({ provisioning: 'verify' })
      await adapter.send(uniqueName('nope'), [], {})
    })
  })

  describe('delay', () => {
    it('delays a message on a standard queue, up to 15 minutes', async () => {
      const adapter = make()
      expect(adapter.delayedDelivery).toBe(true)
      const name = uniqueName('d')
      const subscription = await adapter.subscribe(name, policy())
      await adapter.send(name, [text(1, { delayMs: 2_000 }), text(2, { delayMs: 0 })], {})
      const first = await pullOne(subscription)
      expect(n(first)).toBe(2)
      await subscription.ack([first.receipt])
      expect(await subscription.pull(10)).toHaveLength(0)
      expect(n(await pullOne(subscription))).toBe(1)

      await expect(adapter.send(name, [text(1, { delayMs: 16 * 60_000 })], {})).rejects.toThrow(
        /15 minutes/
      )
    })

    it('refuses a per-message delay on a FIFO queue, with nothing sent', async () => {
      const adapter = make()
      const name = uniqueName('d')
      await adapter.provision(name, { sqs: { fifo: true } })
      await expect(
        adapter.send(name, [text(1), text(2, { delayMs: 1_000 })], {})
      ).rejects.toBeInstanceOf(NotSupportedError)
      const subscription = await adapter.subscribe(name, policy())
      await sleep(200)
      expect(await subscription.pull(10)).toHaveLength(0)
    })
  })

  describe('dedup', () => {
    it('ignores an idempotency key on a standard queue: both sends are delivered', async () => {
      const adapter = make()
      const name = uniqueName('i')
      const subscription = await adapter.subscribe(name, policy({ livenessTimeoutMs: 30_000 }))
      const once = { idempotencyKey: 'same', idempotencyTtlMs: 60_000 }
      await adapter.send(name, [text(1, once)], {})
      await adapter.send(name, [text(1, once)], {})
      const seen: number[] = []
      await until(async () => {
        for (const message of await subscription.pull(10)) seen.push(n(message))
        expect(seen).toEqual([1, 1])
      })
    })

    it('deduplicates on a FIFO queue by idempotency key, keyed and keyless alike', async () => {
      const adapter = make()
      const name = uniqueName('i')
      await adapter.provision(name, { sqs: { fifo: true } })
      const subscription = await adapter.subscribe(name, policy({ livenessTimeoutMs: 30_000 }))
      // A window longer than SQS's five minutes is accepted and clamped, not rejected.
      const once = { idempotencyTtlMs: 60 * 60_000 }
      await adapter.send(name, [text(1, { idempotencyKey: 'keyless-1', ...once })], {})
      await adapter.send(name, [text(1, { idempotencyKey: 'keyless-1', ...once })], {})
      await adapter.send(name, [text(2, { key: 'a', idempotencyKey: 'keyed-1', ...once })], {})
      await adapter.send(name, [text(2, { key: 'a', idempotencyKey: 'keyed-1', ...once })], {})
      // No idempotency key: never deduplicated, even with identical bodies.
      await adapter.send(name, [text(3), text(3)], {})

      const seen: number[] = []
      await until(async () => {
        const pulled = await subscription.pull(10)
        for (const message of pulled) seen.push(n(message))
        await subscription.ack(pulled.map((message) => message.receipt))
        expect(seen.length).toBeGreaterThanOrEqual(4)
      })
      await sleep(500)
      expect(await subscription.pull(10)).toHaveLength(0)
      expect(seen.sort()).toEqual([1, 2, 3, 3])
    })

    it('groups a FIFO message by its key, and gives a keyless one a group of its own', async () => {
      const adapter = make()
      const name = uniqueName('i')
      await adapter.provision(name, { sqs: { fifo: true } })
      await adapter.send(
        name,
        [
          text(1, { key: 'tenant-a' }),
          text(2, { key: 'tenant-a' }),
          text(3, { key: 'has space', idempotencyKey: 'has space too' }),
          text(4),
          text(5),
          text(6, { idempotencyKey: 'idem-6' }),
        ],
        {}
      )
      const ids = new Map<number, { group?: string; dedup?: string }>()
      await until(async () => {
        for (const message of await receiveRaw(`${PREFIX}${name}.fifo`)) {
          ids.set(n({ data: message.Body as string }), {
            group: message.Attributes?.MessageGroupId,
            dedup: message.Attributes?.MessageDeduplicationId,
          })
        }
        expect(ids.size).toBe(6)
      })
      expect(ids.get(1)?.group).toBe('tenant-a')
      expect(ids.get(2)?.group).toBe('tenant-a')
      expect(ids.get(1)?.dedup).not.toBe(ids.get(2)?.dedup)
      // Not valid as FIFO identifiers: hashed, deterministically.
      expect(ids.get(3)?.group).toMatch(/^sha256:[0-9a-f]{64}$/)
      expect(ids.get(3)?.dedup).toMatch(/^sha256:[0-9a-f]{64}$/)
      expect(ids.get(3)?.group).not.toBe(ids.get(3)?.dedup)
      // Keyless: a group per message, so competing consumers are never serialised behind one.
      expect(ids.get(4)?.group).toBeDefined()
      expect(ids.get(4)?.group).not.toBe(ids.get(5)?.group)
      expect(ids.get(6)).toEqual({ group: 'idem-6', dedup: 'idem-6' })
    })
  })

  describe('settling', () => {
    it('acks for good, and a nack after the ack does not bring the message back', async () => {
      const adapter = make()
      const name = uniqueName('s')
      const subscription = await adapter.subscribe(name, policy())
      await adapter.send(name, [text(1)], {})
      const got = await pullOne(subscription)
      expect(got.attempt).toBe(1)
      await subscription.ack([got.receipt, got.receipt])
      await subscription.ack([got.receipt])
      await subscription.nack([got.receipt])
      await subscription.retry([{ receipt: got.receipt, delayMs: 0 }])
      await subscription.heartbeat?.([got.receipt])
      await sleep(300)
      expect(await subscription.pull(10)).toHaveLength(0)
    })

    it('redelivers a nacked message at once, as the next attempt', async () => {
      const adapter = make()
      const name = uniqueName('s')
      const subscription = await adapter.subscribe(name, policy({ livenessTimeoutMs: 30_000 }))
      await adapter.send(name, [text(1)], {})
      const first = await pullOne(subscription)
      await subscription.nack([first.receipt])
      const second = await pullOne(subscription)
      expect(second.id).toBe(first.id)
      expect(second.attempt).toBe(2)
    })

    it('parks a retry for its delay, then redelivers it as the next attempt', async () => {
      const adapter = make()
      const name = uniqueName('s')
      const subscription = await adapter.subscribe(name, policy({ livenessTimeoutMs: 30_000 }))
      await adapter.send(name, [text(1)], {})
      const first = await pullOne(subscription)
      const parkedAt = Date.now()
      await subscription.retry([
        { receipt: first.receipt, delayMs: 2_000 },
        { receipt: first.receipt, delayMs: 2_000 },
      ])
      // The delivery is settled: a repeated settle neither releases nor renews the park.
      await subscription.retry([{ receipt: first.receipt, delayMs: 0 }])
      await subscription.nack([first.receipt])
      await subscription.heartbeat?.([first.receipt])
      expect(await subscription.pull(10)).toHaveLength(0)
      const second = await pullOne(subscription)
      expect(Date.now() - parkedAt).toBeGreaterThanOrEqual(1_500)
      expect(Date.now() - parkedAt).toBeLessThan(5_000)
      expect(second.attempt).toBe(2)
    })

    it('treats receipts it did not issue as no-ops', async () => {
      const adapter = make()
      const subscription = await adapter.subscribe(uniqueName('s'), policy())
      const strangers = ['unknown-receipt', '1:0:not-a-receipt-handle', '']
      await subscription.ack(strangers)
      await subscription.nack(strangers)
      await subscription.retry(strangers.map((receipt) => ({ receipt, delayMs: 0 })))
      await subscription.heartbeat?.(strangers)
      await subscription.ack([])
    })

    it('settles more deliveries than one batch call takes', async () => {
      const adapter = make()
      const name = uniqueName('s')
      const subscription = await adapter.subscribe(name, policy({ livenessTimeoutMs: 30_000 }))
      await adapter.send(
        name,
        Array.from({ length: 25 }, (_, i) => text(i)),
        {}
      )
      const held: PullMessage<string>[] = []
      await until(async () => {
        held.push(...(await subscription.pull(30)))
        expect(held).toHaveLength(25)
      })
      await subscription.nack(held.slice(0, 12).map((message) => message.receipt))
      await subscription.ack(held.slice(12).map((message) => message.receipt))
      const again: PullMessage<string>[] = []
      await until(async () => {
        again.push(...(await subscription.pull(30)))
        expect(again).toHaveLength(12)
      })
      expect(again.map(n).sort()).toEqual(held.slice(0, 12).map(n).sort())
    })
  })

  describe('retry budget', () => {
    it('drops a message whose budget nacks spent, at the receive, and says so', async () => {
      const dead: DeadLettered[] = []
      const adapter = make({ events: { deadLettered: (ctx) => dead.push(ctx) } })
      const name = uniqueName('r')
      // No dead-letter queue: nothing native can enforce the budget, so the adapter must.
      const subscription = await adapter.subscribe(name, policy({ retry: { maxAttempts: 2 } }))
      await adapter.send(name, [text(1)], {})
      const attempts: number[] = []
      await until(async () => {
        const pulled = await subscription.pull(10)
        for (const message of pulled) attempts.push(message.attempt)
        await subscription.nack(pulled.map((message) => message.receipt))
        expect(dead).toHaveLength(1)
      })
      expect(attempts).toEqual([1, 2]) // the third receive was never handed out
      expect(dead[0]).toEqual({ queue: name, deadLetter: undefined, attempt: 3 })
      await sleep(300)
      expect(await subscription.pull(10)).toHaveLength(0)
    })

    it('drops an exhausted retry at once when there is no dead-letter queue', async () => {
      const dead: DeadLettered[] = []
      const adapter = make({ events: { deadLettered: (ctx) => dead.push(ctx) } })
      const name = uniqueName('r')
      const subscription = await adapter.subscribe(name, policy({ retry: { maxAttempts: 1 } }))
      await adapter.send(name, [text(1)], {})
      const got = await pullOne(subscription)
      await subscription.retry([{ receipt: got.receipt, delayMs: 5_000 }])
      expect(dead).toEqual([{ queue: name, deadLetter: undefined, attempt: 1 }])
      const stats = await adapter.stats({ queues: [name] })
      expect(stats.queues[0]).toMatchObject({ backlog: { count: 0 }, inFlight: { count: 0 } })
    })

    it('dead-letters an exhausted retry once, with payload, headers and key intact', async () => {
      const dead: DeadLettered[] = []
      const adapter = make({ events: { deadLettered: (ctx) => dead.push(ctx) } })
      const name = uniqueName('r')
      const dlq = uniqueName('rdlq')
      const subscription = await adapter.subscribe(
        name,
        policy({ retry: { maxAttempts: 2, deadLetter: dlq } })
      )
      // Subscribing provisioned the dead-letter queue and pointed the redrive policy at it.
      const dlqArn = (await queueAttributesOf(raw, `${PREFIX}${dlq}`, ['QueueArn'])).QueueArn
      const redrive = JSON.parse(
        (await queueAttributesOf(raw, `${PREFIX}${name}`, ['RedrivePolicy']))
          .RedrivePolicy as string
      ) as { deadLetterTargetArn: string; maxReceiveCount: unknown }
      expect(redrive.deadLetterTargetArn).toBe(dlqArn)
      expect(Number(redrive.maxReceiveCount)).toBe(2)

      await adapter.send(name, [{ data: '{"n":9}', headers: { trace: 't9' }, key: 'k9' }], {})
      const first = await pullOne(subscription)
      await subscription.retry([{ receipt: first.receipt, delayMs: 0 }]) // budget left: parked
      expect(dead).toEqual([])
      const second = await pullOne(subscription)
      expect(second.attempt).toBe(2)
      // Named twice in one call, then settled again: one copy only.
      await subscription.retry([
        { receipt: second.receipt, delayMs: 0 },
        { receipt: second.receipt, delayMs: 0 },
      ])
      await subscription.retry([{ receipt: second.receipt, delayMs: 0 }])
      expect(dead).toEqual([{ queue: name, deadLetter: dlq, attempt: 2 }])

      const collector = await adapter.subscribe(dlq, policy())
      const copies: Partial<PullMessage<string>>[] = []
      await until(async () => {
        const pulled = await collector.pull(10)
        for (const { data, headers, key, attempt } of pulled) {
          copies.push({ data, headers, key, attempt })
        }
        await collector.ack(pulled.map((message) => message.receipt))
        expect(copies.length).toBeGreaterThanOrEqual(1)
      })
      await sleep(500)
      expect(await collector.pull(10)).toHaveLength(0)
      // A dead letter is a fresh message in its queue: its attempts start over.
      expect(copies).toEqual([{ data: '{"n":9}', headers: { trace: 't9' }, key: 'k9', attempt: 1 }])
      expect(await subscription.pull(10)).toHaveLength(0)
    })

    it('lets the redrive policy dead-letter a message whose budget nacks spent', async () => {
      const dead: DeadLettered[] = []
      const adapter = make({ events: { deadLettered: (ctx) => dead.push(ctx) } })
      const name = uniqueName('r')
      const dlq = uniqueName('rdlq')
      const subscription = await adapter.subscribe(
        name,
        policy({ retry: { maxAttempts: 2, deadLetter: dlq } })
      )
      const collector = await adapter.subscribe(dlq, policy())
      await adapter.send(name, [text(5, { headers: { trace: 't5' }, key: 'k5' })], {})

      const attempts: number[] = []
      let landed: PullMessage<string> | undefined
      await until(async () => {
        const pulled = await subscription.pull(10)
        for (const message of pulled) attempts.push(message.attempt)
        await subscription.nack(pulled.map((message) => message.receipt))
        landed ??= (await collector.pull(10))[0]
        expect(landed).toBeDefined()
      })
      expect(attempts).toEqual([1, 2]) // the budget counts deliveries
      expect(landed).toMatchObject({ data: '{"n":5}', headers: { trace: 't5' }, key: 'k5' })
      // SQS moved it: the adapter saw no verdict to report.
      expect(dead).toEqual([])
    })

    it('an undeclared dead-letter queue takes after its source; a declared mismatch throws', async () => {
      const adapter = make()
      const name = uniqueName('r')
      const dlq = uniqueName('rdlq')
      await adapter.provision(name, { sqs: { fifo: true } })
      const subscription = await adapter.subscribe(
        name,
        policy({ retry: { maxAttempts: 1, deadLetter: dlq } })
      )
      await queueUrlOf(raw, `${PREFIX}${dlq}.fifo`) // FIFO like its source

      await adapter.send(name, [text(1, { key: 'k' })], {})
      const got = await pullOne(subscription)
      await subscription.retry([{ receipt: got.receipt, delayMs: 0 }])
      const collector = await adapter.subscribe(dlq, policy())
      expect(await pullOne(collector)).toMatchObject({ data: '{"n":1}', key: 'k' })

      const standardDlq = uniqueName('rdlq')
      await adapter.provision(standardDlq, { sqs: { fifo: false } })
      await expect(
        adapter.subscribe(name, policy({ retry: { maxAttempts: 3, deadLetter: standardDlq } }))
      ).rejects.toThrow(/dead-letter between two FIFO or two standard/)
    })
  })

  describe('liveness', () => {
    it('a heartbeat keeps a delivery past its liveness window; without one it redelivers', async () => {
      const adapter = make()
      const name = uniqueName('h')
      const subscription = await adapter.subscribe(name, policy())
      const peer = await adapter.subscribe(name, policy())
      await adapter.send(name, [text(1)], {})
      const held = await pullOne(subscription)
      // Beat across more than one 2 second window: the peer never sees the message.
      for (let i = 0; i < 4; i++) {
        await sleep(1_000)
        await subscription.heartbeat?.([held.receipt])
        expect(await peer.pull(10)).toHaveLength(0)
      }
      // Stop beating: the window lapses and the peer takes over.
      const taken = await pullOne(peer)
      expect(taken.attempt).toBe(2)

      // The first consumer lost its lease: it no longer releases or renews what the peer holds.
      await subscription.nack([held.receipt])
      await subscription.heartbeat?.([held.receipt])
      await subscription.retry([{ receipt: held.receipt, delayMs: 0 }])
      expect(await subscription.pull(10)).toHaveLength(0)
      await peer.ack([taken.receipt])
    }, 20_000)
  })

  describe('long polling', () => {
    it('cancels a waiting receive when the subscription or the adapter closes', async () => {
      const adapter = make({ waitTimeSeconds: 20 })
      const name = uniqueName('l')
      const subscription = await adapter.subscribe(name, policy())
      let startedAt = Date.now()
      const waiting = subscription.pull(10)
      setTimeout(() => void subscription.close(), 300)
      expect(await waiting).toEqual([])
      expect(Date.now() - startedAt).toBeLessThan(5_000)

      const other = await adapter.subscribe(name, policy())
      startedAt = Date.now()
      const second = other.pull(10)
      setTimeout(() => void adapter.close(), 300)
      expect(await second).toEqual([])
      expect(Date.now() - startedAt).toBeLessThan(5_000)
      expect(await other.pull(10)).toEqual([]) // closing the adapter closed the subscription
    })

    it('returns as soon as a message arrives, without waiting out the poll', async () => {
      const adapter = make({ waitTimeSeconds: 20 })
      const name = uniqueName('l')
      const subscription = await adapter.subscribe(name, policy())
      const startedAt = Date.now()
      const waiting = subscription.pull(10)
      await sleep(300)
      await adapter.send(name, [text(1)], {})
      const pulled = await waiting
      expect(pulled.map(n)).toEqual([1])
      expect(Date.now() - startedAt).toBeLessThan(8_000)
    })
  })

  describe('closed subscription', () => {
    it('makes every method a no-op after close, and close can be called again', async () => {
      const client = rawSqsClient()
      const adapter = make({ sqs: client })
      const name = uniqueName('c')
      const subscription = await adapter.subscribe(name, policy({ livenessTimeoutMs: 1_000 }))
      await adapter.send(name, [text(1)], {})
      const held = await pullOne(subscription)

      await subscription.close()
      await subscription.close()
      const send = vi.spyOn(client, 'send')
      expect(await subscription.pull(10)).toEqual([])
      await subscription.ack([held.receipt])
      await subscription.nack([held.receipt])
      await subscription.retry([{ receipt: held.receipt, delayMs: 0 }])
      await subscription.heartbeat?.([held.receipt])
      expect(send).not.toHaveBeenCalled()

      // What the closed subscription held redelivers once its liveness window lapses.
      const peer = await adapter.subscribe(name, policy())
      const taken = await pullOne(peer)
      expect(taken.id).toBe(held.id)
      expect(taken.attempt).toBe(2)
      client.destroy()
    })
  })

  describe('stats', () => {
    it("reports each queue's backlog and in-flight counts", async () => {
      const adapter = make()
      const name = uniqueName('st')
      const dlq = uniqueName('stdlq')
      const other = uniqueName('st')
      const subscription = await adapter.subscribe(
        name,
        policy({ livenessTimeoutMs: 30_000, retry: { maxAttempts: 5, deadLetter: dlq } })
      )
      await adapter.send(name, [text(1), text(2), text(3, { delayMs: 60_000 })], {})
      await adapter.send(other, [text(1)], {})
      await until(async () => expect(await subscription.pull(1)).toHaveLength(1))

      const exact = (count: number) => ({ count, saturated: false })
      await until(async () => {
        const report = await adapter.stats({ queues: [name, uniqueName('missing'), name] })
        // A delayed message is backlog; gauges SQS cannot compute stay absent.
        expect(report).toEqual({
          queues: [{ queue: name, backlog: exact(2), inFlight: exact(1) }],
        })
      })

      // Without a filter: every queue this instance resolved, dead-letter queues included.
      const all = await adapter.stats()
      expect(all.queues.map((row) => row.queue)).toEqual([name, dlq, other].sort())
      expect(all.queues.find((row) => row.queue === dlq)).toEqual({
        queue: dlq,
        backlog: exact(0),
        inFlight: exact(0),
      })

      const capped = await adapter.stats({ queues: [name], countCap: 1 })
      expect(capped.queues[0]?.backlog).toEqual({ count: 1, saturated: true })
      expect(capped.queues[0]?.inFlight).toEqual(exact(1))

      // A fresh instance has resolved nothing, and reading stats creates nothing.
      const fresh = make()
      expect(await fresh.stats()).toEqual({ queues: [] })
      const byName = await fresh.stats({ queues: [other] })
      expect(byName.queues).toEqual([{ queue: other, backlog: exact(1), inFlight: exact(0) }])
    })
  })
})
