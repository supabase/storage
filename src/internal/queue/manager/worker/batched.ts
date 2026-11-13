import type { QueueSubscription } from '../adapter'
import { type Message, type PullMessage, toMessage } from '../message'
import { sleep } from '../signals'
import { deliver, type Slot } from './invocation'
import { type Settles, settle, sortSettle } from './settle'
import type { WorkerRuntime } from './types'

/** Run the thunks with at most `limit` in flight; every thunk runs. */
const pool = async (thunks: readonly (() => Promise<void>)[], limit: number): Promise<void> => {
  let next = 0
  const lanes = Array.from({ length: Math.min(limit, thunks.length) }, async () => {
    while (next < thunks.length) await thunks[next++]()
  })
  await Promise.all(lanes)
}

/**
 * The batched flow: pull → handle the whole batch → one settle join → linger → repeat, until
 * `stop()`. Every iteration is fault-isolated: an infrastructure error (failed subscribe,
 * pull or settle; handler failures are captured by the invocation) is reported, backs off
 * one idle interval and resumes. A worker never dies from a transient outage.
 */
export async function runBatchedLoop(rt: WorkerRuntime): Promise<void> {
  while (rt.running) {
    try {
      const subscription = await rt.subscription()
      const pulled = await subscription.pull(rt.prefetch)
      if (pulled.length === 0) {
        await sleep(rt.pollIdleIntervalMs, rt.wake)
        continue
      }
      rt.events?.pulled?.({ queue: rt.queue, size: pulled.length })
      // The batch's delivery heartbeat (settled receipts are no-ops by the adapter contract).
      // Best-effort: a failed beat is reported, never thrown into the handler.
      const heartbeat = async (): Promise<void> => {
        try {
          await subscription.heartbeat?.(pulled.map((r) => r.receipt))
        } catch (error) {
          rt.report(error)
        }
      }
      const hbTimer =
        rt.heartbeatIntervalMs !== undefined
          ? setInterval(() => void heartbeat(), rt.heartbeatIntervalMs)
          : undefined
      try {
        await processBatch(rt, subscription, pulled, heartbeat)
      } finally {
        clearInterval(hbTimer)
      }
      await sleep(rt.pollBatchIntervalMs, rt.wake)
    } catch (error) {
      if (!rt.running) break // shutting down: don't spin on teardown errors
      rt.report(error)
      await sleep(rt.pollIdleIntervalMs, rt.wake)
    }
  }
}

async function processBatch(
  rt: WorkerRuntime,
  subscription: QueueSubscription,
  pulled: PullMessage[],
  heartbeat: () => Promise<void>
): Promise<void> {
  // One slot per message. A payload the codec rejects fails THAT message (retried or
  // dead-lettered like any handler failure) instead of throwing out of the pull: one poison
  // payload must not block its neighbours.
  const slots: Slot[] = new Array<Slot>(pulled.length)
  const messages: (Message<unknown> | undefined)[] = pulled.map((r, i) => {
    try {
      return toMessage(r, rt.codec)
    } catch (error) {
      slots[i] = { status: 'failed', error }
      return undefined
    }
  })
  const decoded = pulled.map((_, i) => i).filter((i) => messages[i] !== undefined)
  // A single-message handler gets one invocation per message; a batch handler one per
  // contiguous slice, `parallelism` slices per batch.
  const size = rt.batch ? Math.max(1, Math.ceil(decoded.length / rt.parallelism)) : 1
  const invocations: (() => Promise<void>)[] = []
  for (let start = 0; start < decoded.length; start += size) {
    const positions = decoded.slice(start, start + size)
    invocations.push(async () => {
      if (!rt.running) return // never started: released at the join
      const verdicts = await deliver(
        rt,
        positions.map((i) => pulled[i]),
        positions.map((i) => messages[i] as Message<unknown>),
        heartbeat
      )
      positions.forEach((p, k) => {
        slots[p] = verdicts[k]
      })
    })
  }
  await pool(invocations, rt.parallelism)

  const settles: Settles = { acks: [], nacks: [], releases: [] }
  pulled.forEach((leased, i) => sortSettle(settles, leased, slots[i], rt.retry !== undefined))
  await settle(rt, subscription, settles)
}
