import type {
  AdapterEvents,
  BoundedCount,
  Codec,
  ProduceMessage,
  PullMessage,
  QueueAdapter,
  QueueOptions,
  QueueStats,
  QueueSubscription,
  StatsReport,
} from '../../manager'
import { DEFAULT_STATS_COUNT_CAP, json, livenessOf } from '../../manager'

export interface MemoryOptions<W = unknown> {
  /** Storage codec. Defaults to structured JSON. */
  readonly codec?: Codec<W>
  /** Sink for the adapter-side facts (`deadLettered`). */
  readonly events?: AdapterEvents
}

/** One unsettled message. It leaves the queue when acked, dead-lettered or dropped. */
interface Entry<W> {
  readonly id: string
  readonly data: W
  readonly headers: Readonly<Record<string, string>>
  readonly key?: string
  readonly timestamp: number
  /** The attempt its next (or current) delivery carries. Only `nack` advances it. */
  attempt: number
  /** Not deliverable before this time (a produce delay or a retry backoff). */
  visibleAt: number
  /** True once a `nack` parked it. */
  retried: boolean
  /** Deliveries so far: makes every delivery's receipt unique. */
  deliveries: number
  /** The delivery currently holding it. */
  lease?: { readonly receipt: string; readonly owner: Consumer }
}

/** One attached consumer's liveness: silent past the queue's window, its leases expire. */
interface Consumer {
  lastSeen: number
  closed: boolean
}

interface QueueState<W> {
  /** Unsettled messages, in arrival order. */
  readonly entries: Set<Entry<W>>
  /** Producer-dedup claims: idempotencyKey → claim expiry (ms). */
  readonly dedup: Map<string, number>
  /** The queue's policy: what the manager's registry declares, else the options last given
   * to `provision`, else none. */
  options?: QueueOptions
}

/**
 * The in-memory adapter: the reference implementation of the adapter contract and the test
 * substrate. Competing consumers, no order.
 *
 * - Delay, producer dedup (`idempotencyKey` requires `idempotencyTtlMs`), heartbeat and stats
 *   are all supported and exact.
 * - Retry budget counts `nack` calls only: a release or an expired lease redelivers
 *   with the same `attempt`. An exhausted message goes to the policy's `deadLetter` queue
 *   (keeping the attempt it died with) or is dropped.
 * - The policy (retry, dead-letter, liveness) is the queue's: declared in the registry the
 *   manager forwards, or given to `provision`. A queue with neither has no retry budget and
 *   the default liveness window.
 * - A consumer silent (no pull, no heartbeat) past the queue's liveness window has its in-flight
 *   deliveries released for redelivery; so does one that closes.
 */
export function memory<W = unknown>(options?: MemoryOptions<W>): QueueAdapter<W> {
  const queues = new Map<string, QueueState<W>>()

  const queueOf = (name: string): QueueState<W> => {
    let state = queues.get(name)
    if (state === undefined) {
      state = { entries: new Set(), dedup: new Map() }
      queues.set(name, state)
    }
    return state
  }

  const enqueue = (name: string, messages: readonly ProduceMessage<W>[], attempt = 1): void => {
    const state = queueOf(name)
    const now = Date.now()
    for (const message of messages) {
      const { idempotencyKey, idempotencyTtlMs } = message
      if (idempotencyKey !== undefined) {
        if (idempotencyTtlMs === undefined) {
          throw new Error(
            `queue memory: sending to "${name}" with an idempotencyKey requires idempotencyTtlMs — the adapter has no default dedup window`
          )
        }
        if ((state.dedup.get(idempotencyKey) ?? 0) > now) continue // a live claim: dropped
        if (state.dedup.size >= 4096) {
          for (const [key, expiry] of state.dedup) if (expiry <= now) state.dedup.delete(key)
        }
        state.dedup.set(idempotencyKey, now + idempotencyTtlMs)
      }
      state.entries.add({
        id: crypto.randomUUID(),
        data: message.data,
        headers: message.headers ?? {},
        ...(message.key !== undefined ? { key: message.key } : {}),
        timestamp: now,
        attempt,
        visibleAt: now + (message.delayMs ?? 0),
        retried: false,
        deliveries: 0,
      })
    }
  }

  /** Release the leases of consumers that closed or went silent past their contract. */
  const expireLeases = (state: QueueState<W>, now: number): void => {
    const livenessMs = livenessOf(state.options)
    for (const entry of state.entries) {
      const owner = entry.lease?.owner
      if (owner === undefined) continue
      if (owner.closed || now - owner.lastSeen > livenessMs) entry.lease = undefined
    }
  }

  return {
    name: 'memory',
    codec: options?.codec ?? json<W>(),
    delayedDelivery: true,

    async provision(queue, queueOptions) {
      const state = queueOf(queue)
      if (queueOptions !== undefined) state.options = queueOptions
    },

    async send(queue, messages) {
      enqueue(queue, messages)
    },

    async subscribe(queue, policy) {
      const state = queueOf(queue)
      const declared = policy.queues?.get(queue)
      if (declared !== undefined) state.options = declared.options
      const me: Consumer = {
        lastSeen: Date.now(),
        closed: false,
      }
      // receipt → the entry it was issued for. A receipt settles only while its delivery
      // still holds the entry, which makes every settle idempotent and stale-safe.
      const issued = new Map<string, Entry<W>>()
      const take = (receipt: string): Entry<W> | undefined => {
        const entry = issued.get(receipt)
        issued.delete(receipt)
        return !me.closed && entry?.lease?.receipt === receipt ? entry : undefined
      }

      const subscription: QueueSubscription<W> = {
        async pull(max) {
          if (me.closed) return []
          const now = Date.now()
          me.lastSeen = now
          expireLeases(state, now)
          const out: PullMessage<W>[] = []
          for (const entry of state.entries) {
            if (out.length >= max) break
            if (entry.lease !== undefined || entry.visibleAt > now) continue
            const receipt = `${entry.id}:${++entry.deliveries}`
            entry.lease = { receipt, owner: me }
            issued.set(receipt, entry)
            out.push({
              id: entry.id,
              data: entry.data,
              headers: entry.headers,
              ...(entry.key !== undefined ? { key: entry.key } : {}),
              timestamp: entry.timestamp,
              attempt: entry.attempt,
              receipt,
            })
          }
          return out
        },

        async ack(receipts) {
          for (const receipt of receipts) {
            const entry = take(receipt)
            if (entry !== undefined) state.entries.delete(entry)
          }
        },

        async release(receipts) {
          for (const receipt of receipts) {
            const entry = take(receipt)
            if (entry !== undefined) entry.lease = undefined
          }
        },

        async nack(entries) {
          for (const { receipt, delayMs } of entries) {
            const entry = take(receipt) // a receipt named twice was taken the first time
            if (entry === undefined) continue
            const retryPolicy = state.options?.retry
            entry.lease = undefined
            if (retryPolicy !== undefined && entry.attempt >= retryPolicy.maxAttempts) {
              state.entries.delete(entry)
              if (retryPolicy.deadLetter !== undefined) {
                const { data, headers, key } = entry
                enqueue(
                  retryPolicy.deadLetter,
                  [{ data, headers, ...(key !== undefined ? { key } : {}) }],
                  entry.attempt
                )
              }
              options?.events?.deadLettered?.({
                queue,
                deadLetter: retryPolicy.deadLetter,
                attempt: entry.attempt,
              })
              continue
            }
            entry.attempt += 1
            entry.visibleAt = Date.now() + delayMs
            entry.retried = true
          }
        },

        async heartbeat() {
          if (!me.closed) me.lastSeen = Date.now()
        },

        async close() {
          me.closed = true
          issued.clear()
          expireLeases(state, Date.now())
        },
      }
      return subscription
    },

    async stats(query): Promise<StatsReport> {
      const cap = query?.countCap ?? DEFAULT_STATS_COUNT_CAP
      const bounded = (n: number): BoundedCount => ({ count: Math.min(n, cap), saturated: n > cap })
      const now = Date.now()
      const report: QueueStats[] = []
      for (const [name, state] of queues) {
        if (query?.queues !== undefined && !query.queues.includes(name)) continue
        expireLeases(state, now)
        let backlog = 0
        let inFlight = 0
        let parked = 0
        let due = 0
        let oldest: number | undefined
        for (const entry of state.entries) {
          if (entry.lease !== undefined) inFlight++
          else if (entry.retried) {
            parked++
            if (entry.visibleAt <= now) due++
          } else {
            backlog++
            if (entry.visibleAt <= now) oldest = Math.min(oldest ?? now, entry.timestamp)
          }
        }
        report.push({
          queue: name,
          backlog: bounded(backlog),
          inFlight: bounded(inFlight),
          retryParked: { total: bounded(parked), due: bounded(due) },
          oldestBacklogAgeMs: oldest === undefined ? 0 : now - oldest,
        })
      }
      return { queues: report }
    },

    async close() {
      // Nothing to release.
    },
  }
}
