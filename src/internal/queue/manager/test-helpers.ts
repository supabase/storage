import {
  type Codec,
  defineQueues,
  json,
  type PullMessage,
  type QueueAdapter,
  type QueueSubscription,
  queue,
  type RetryPolicy,
  type SubscribePolicy,
} from './index'

/** Worker timing for tests: poll at once, re-poll an empty queue every 5 ms. */
export const fast = { pollIdleIntervalMs: 5, pollBatchIntervalMs: 0 } as const

/** A `jobs` queue declaring `retry` (plus its `jobs-dead` dead-letter queue). */
export const withRetry = (retry?: RetryPolicy) =>
  defineQueues({
    'jobs-dead': queue<{ n: number }>(),
    jobs: queue<{ n: number }>(retry !== undefined ? { retry } : {}),
  })

/** Resolve after `ms` milliseconds. */
export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))

/** A promise with its `resolve` exposed, for scripting an exact interleaving in a test. */
export const defer = <T = void>(): { promise: Promise<T>; resolve: (value: T) => void } => {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((r) => {
    resolve = r
  })
  return { promise, resolve }
}

/** A promise that never settles: a handler or stage that only its abort can free. */
export const forever = (): Promise<never> => new Promise<never>(() => {})

/** Poll until `pred` holds; fail loudly when it never does. */
export const until = async (pred: () => boolean, ms = 2_000): Promise<void> => {
  const deadline = Date.now() + ms
  while (!pred()) {
    if (Date.now() > deadline) throw new Error('until: timed out')
    await sleep(2)
  }
}

/** Wrap every subscription `adapter` opens: the methods `patch` returns replace the subscription's own. */
export function patchSubscriptions<W>(
  adapter: QueueAdapter<W>,
  patch: (
    subscription: QueueSubscription<W>,
    at: { queue: string; policy: SubscribePolicy }
  ) => Partial<QueueSubscription<W>>
): QueueAdapter<W> {
  return {
    ...adapter,
    subscribe: async (queue, policy) => {
      const subscription = await adapter.subscribe(queue, policy)
      return { ...subscription, ...patch(subscription, { queue, policy }) }
    },
  }
}

/** A leased delivery of payload `{ n }` under receipt `r<n>`. */
export const pulled = (n: number, attempt = 1): PullMessage => ({
  id: `id-${n}`,
  data: { n },
  headers: {},
  timestamp: 0,
  attempt,
  receipt: `r${n}`,
})

/** A codec that refuses to decode the payload whose `n` is `poison`. */
export const poisonCodec = (poison: number): Codec => ({
  ...json(),
  decode: (wire) => {
    if ((wire as { n?: number }).n === poison) throw new Error('undecodable')
    return wire
  },
})

export interface Scripted {
  readonly adapter: QueueAdapter
  /**
   * Every settle, heartbeat and close the worker issued, in order, as text: `ack r0,r1`,
   * `nack r2@5` (receipt@delayMs), `release r3`, `heartbeat r0`, `close`.
   */
  readonly calls: string[]
  /** Every pull's `max`, in order. */
  readonly pulls: number[]
  /** The policy of every subscribe, in order. */
  readonly policies: SubscribePolicy[]
  /** Queue deliveries: each argument is what ONE later pull returns. */
  feed(...batches: PullMessage[][]): void
  /** The receipts of every recorded call of `op`, flattened (nack entries without delay). */
  receipts(op: 'ack' | 'nack' | 'release' | 'heartbeat'): string[]
}

/**
 * An adapter with one scripted subscription: pulls return what the test fed, every other call is
 * recorded. `override` replaces subscription methods; call through to `base` to keep the record.
 */
export function scripted(
  override?: (base: QueueSubscription) => Partial<QueueSubscription>
): Scripted {
  const calls: string[] = []
  const pulls: number[] = []
  const policies: SubscribePolicy[] = []
  const batches: PullMessage[][] = []
  const base: QueueSubscription = {
    async pull(max) {
      pulls.push(max)
      return batches.shift() ?? []
    },
    async ack(receipts) {
      calls.push(`ack ${receipts.join(',')}`)
    },
    async release(receipts) {
      calls.push(`release ${receipts.join(',')}`)
    },
    async nack(entries) {
      calls.push(`nack ${entries.map((e) => `${e.receipt}@${e.delayMs}`).join(',')}`)
    },
    async heartbeat(receipts) {
      calls.push(`heartbeat ${receipts.join(',')}`)
    },
    async close() {
      calls.push('close')
    },
  }
  const subscription: QueueSubscription = { ...base, ...override?.(base) }
  return {
    calls,
    pulls,
    policies,
    feed: (...more) => void batches.push(...more),
    receipts: (op) =>
      calls
        .filter((c) => c.startsWith(`${op} `))
        .flatMap((c) => c.slice(op.length + 1).split(','))
        .map((r) => r.split('@')[0]),
    adapter: {
      name: 'scripted',
      codec: json(),
      async close() {
        calls.push('adapter.close')
      },
      async provision() {},
      async send() {},
      async subscribe(_queue, policy) {
        policies.push(policy)
        return subscription
      },
    },
  }
}

/**
 * A competing-fetch broker whose receipt IS the message id, so a redelivery re-issues the
 * receipt of the delivery before it (the memory adapter mints a fresh one per delivery). A
 * release returns the lease; ack wins over a later release.
 */
export function reissuingBroker(): { adapter: QueueAdapter; acked: Set<string> } {
  const leased = new Set<string>()
  const acked = new Set<string>()
  const messages: PullMessage[] = []
  const adapter: QueueAdapter = {
    name: 'reissuing',
    codec: json(),
    async close() {},
    async provision() {},
    async send(_queue, batch) {
      for (const m of batch) {
        const id = `id-${messages.length}`
        messages.push({
          id,
          data: m.data,
          headers: m.headers ?? {},
          timestamp: 0,
          attempt: 1,
          receipt: id,
        })
      }
    },
    async subscribe() {
      return {
        async pull(max) {
          const out: PullMessage[] = []
          for (const m of messages) {
            if (out.length >= max) break
            if (acked.has(m.id) || leased.has(m.id)) continue
            leased.add(m.id)
            out.push(m)
          }
          return out
        },
        async ack(receipts) {
          for (const r of receipts) {
            leased.delete(r)
            acked.add(r)
          }
        },
        async release(receipts) {
          for (const r of receipts) if (!acked.has(r)) leased.delete(r)
        },
        async nack() {},
        async close() {},
      }
    },
  }
  return { adapter, acked }
}
