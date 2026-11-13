import type { QueueSubscription } from '../adapter'
import { type Message, type PullMessage, toMessage } from '../message'
import { sleep } from '../signals'
import { deliver, type Slot } from './invocation'
import { type Settles, settle, sortSettle } from './settle'
import type { WorkerRuntime } from './types'

/**
 * The streaming flow: `prefetch` is a sliding window of pulled-but-unsettled messages. One
 * puller refills the window in chunks (at least a quarter window so fetches stay amortized,
 * or with any credit when the lanes would otherwise starve), `min(parallelism, prefetch)`
 * lanes consume the ready queue, and every invocation settles as IT completes through a
 * serialized, self-coalescing flusher: each settle round trip carries everything that
 * completed during the previous one. There is no batch join, so a slow invocation occupies
 * one lane while the window keeps refilling around it. On stop, the un-started remainder
 * releases promptly, in-flight lanes drain under `closeTimeout`, and the final flush lands
 * before teardown.
 */
export async function runStreamingLoop(rt: WorkerRuntime): Promise<void> {
  // The subscription, once opened: the flusher and the heartbeat only run after a pull delivered
  // something, so it exists whenever they need it.
  let subscription: QueueSubscription | undefined
  const chunkMin = Math.max(1, Math.ceil(rt.prefetch / 4))
  const ready: PullMessage[] = []

  // Every delivery this worker holds: pulled and not yet durably settled. `held` is the
  // window; the map's keys are the heartbeat surface. Deliveries are COUNTED per receipt
  // rather than kept as a set: an adapter may re-issue a receipt on redelivery, and the
  // puller overlaps the flusher, so a pull can return a receipt again after its release took
  // effect but before the flush's bookkeeping runs. A set would then drop the NEW delivery
  // when the old one's flush lands.
  const outstanding = new Map<string, number>()
  let held = 0
  const hold = (receipt: string): void => {
    outstanding.set(receipt, (outstanding.get(receipt) ?? 0) + 1)
    held += 1
  }
  const release = (receipt: string): void => {
    const count = outstanding.get(receipt)
    if (count === undefined) return
    if (count === 1) outstanding.delete(receipt)
    else outstanding.set(receipt, count - 1)
    held -= 1
  }

  const heartbeat = async (): Promise<void> => {
    if (outstanding.size === 0) return
    try {
      await subscription?.heartbeat?.([...outstanding.keys()])
    } catch (error) {
      rt.report(error)
    }
  }

  // Wakes: the puller parks when the window is too full to justify a fetch and every landed
  // flush nudges it; lanes park on an arrival latch when the ready queue is empty. A nudge
  // with nobody parked is remembered until the next pull consumes it: a flush landing WHILE
  // a pull is in flight, when that pull comes back empty, must not be lost to a full idle
  // sleep.
  let wakePuller: (() => void) | undefined
  let nudged = false
  const nudgePuller = (): void => {
    const wake = wakePuller
    wakePuller = undefined
    if (wake !== undefined) wake()
    else nudged = true
  }
  /** Park the puller until nudged, stopped, or (when given) `ms` elapsed. */
  const parkPuller = (ms?: number): Promise<void> =>
    new Promise<void>((resolve) => {
      if (!rt.running) return resolve()
      const timer = ms === undefined ? undefined : setTimeout(resolve, ms)
      wakePuller = () => {
        clearTimeout(timer)
        resolve()
      }
    }).then(() => {
      wakePuller = undefined
    })
  let signalArrival = (): void => {}
  let arrival = new Promise<void>((resolve) => {
    signalArrival = resolve
  })
  const announceArrival = (): void => {
    signalArrival()
    arrival = new Promise<void>((resolve) => {
      signalArrival = resolve
    })
  }
  const onStop = (): void => {
    nudgePuller()
    signalArrival()
  }
  rt.wake.addEventListener('abort', onStop, { once: true })

  // The settle flusher: one flush in flight at a time; completions landing DURING a flush
  // coalesce into the next.
  let pending: Settles = { acks: [], nacks: [], releases: [] }
  let flushChain = Promise.resolve()
  let flushQueued = false
  // Never rejects (adapter failures are compensated inside `settle` and reported here): a
  // rejected link would leave `flushQueued` set and every later settle queued forever.
  const flushNow = async (): Promise<void> => {
    const live = subscription
    if (live === undefined) return
    const batch = pending
    pending = { acks: [], nacks: [], releases: [] }
    const receipts = [...batch.acks, ...batch.nacks.map((r) => r.receipt), ...batch.releases]
    if (receipts.length === 0) return
    try {
      await settle(rt, live, batch)
    } catch (error) {
      rt.report(error)
    }
    // Settled or compensated, these deliveries are no longer held: one count each, so a
    // receipt a concurrent pull has meanwhile re-issued stays held for its new delivery.
    for (const receipt of receipts) release(receipt)
    nudgePuller() // the window has credits again
  }
  const scheduleFlush = (): void => {
    if (flushQueued) return
    flushQueued = true
    flushChain = flushChain.then(() => {
      flushQueued = false // cleared BEFORE draining, so mid-flush completions queue the next
      return flushNow()
    })
  }

  const lane = async (): Promise<void> => {
    for (;;) {
      if (!rt.running) return // stop() starts no new work; the remainder releases below
      const leased = ready.shift()
      if (leased === undefined) {
        await arrival
        continue
      }
      // A payload the codec rejects fails THIS message instead of releasing it to redeliver
      // forever.
      let slot: Slot
      let message: Message<unknown> | undefined
      try {
        message = toMessage(leased, rt.codec)
      } catch (error) {
        slot = { status: 'failed', error }
      }
      if (message !== undefined) [slot] = await deliver(rt, [leased], [message], heartbeat)
      sortSettle(pending, leased, slot, rt.retry !== undefined)
      // The receipt stays held (and in the window) until its flush lands.
      scheduleFlush()
    }
  }

  const hbTimer =
    rt.heartbeatIntervalMs !== undefined
      ? setInterval(() => void heartbeat(), rt.heartbeatIntervalMs)
      : undefined
  // The window holds at most `prefetch` messages, so lanes past it could never find work.
  const lanes = Array.from({ length: Math.min(rt.parallelism, rt.prefetch) }, () => lane())

  try {
    // The puller: fault-isolated exactly like the batched loop.
    while (rt.running) {
      try {
        const live = await rt.subscription()
        subscription = live
        const credits = rt.prefetch - held
        if (credits < chunkMin && !(ready.length === 0 && credits > 0)) {
          await parkPuller()
          continue
        }
        nudged = false
        const pulled = await live.pull(credits)
        if (pulled.length === 0) {
          if (nudged) continue // a flush landed during the pull: re-pull before parking
          // Caught up. Park interruptibly: a landed flush nudges an early re-pull.
          await parkPuller(rt.pollIdleIntervalMs)
          continue
        }
        rt.events?.pulled?.({ queue: rt.queue, size: pulled.length })
        for (const r of pulled) {
          hold(r.receipt)
          ready.push(r)
        }
        announceArrival()
        await sleep(rt.pollBatchIntervalMs, rt.wake)
      } catch (error) {
        if (!rt.running) break // shutting down: don't spin on teardown errors
        rt.report(error)
        await sleep(rt.pollIdleIntervalMs, rt.wake)
      }
    }

    // Stopping: release the un-started remainder promptly, so fetched-but-unhandled messages
    // redeliver instead of waiting out their leases.
    const unstarted = ready.splice(0)
    for (const r of unstarted) pending.releases.push(r.receipt)
    if (unstarted.length > 0) scheduleFlush()
    signalArrival() // release parked lanes so they observe the stop
    await Promise.all(lanes) // in-flight drains under stop()'s closeTimeout
    await flushChain // the final settles land before teardown
  } finally {
    clearInterval(hbTimer)
    rt.wake.removeEventListener('abort', onStop)
  }
}
