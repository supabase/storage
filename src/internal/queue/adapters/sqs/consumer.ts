import type { SQSClient } from '@aws-sdk/client-sqs'
import type { QueueSubscription, SubscribePolicy } from '../../manager/adapter'
import type { AdapterEvents } from '../../manager/events'
import type { PullMessage } from '../../manager/message'
import { MAX_BATCH_ENTRIES } from './client/batching'
import { receiveMessages } from './client/messages'
import { toPullMessage } from './message'
import { maxAttemptsOf, type QueueInfo, type Queues, visibilityTimeoutSecondsOf } from './queues'
import { createSettler } from './settle'

export interface ConsumerDeps {
  readonly sqs: SQSClient
  readonly queues: Queues
  /** How long one receive waits for a message before returning empty. */
  readonly waitTimeSeconds: number
  readonly events: AdapterEvents | undefined
  /** Every open subscription, so the adapter's `close()` can close them. */
  readonly openSubscriptions: Set<QueueSubscription<string>>
}

export type Subscribe = (
  queue: string,
  policy: SubscribePolicy
) => Promise<QueueSubscription<string>>

/**
 * `subscribe`: resolve the queue and open a subscription governed by the queue's declared
 * policy. Nothing is written here: the visibility timeout and redrive policy are `provision`'s.
 * Every subscription on a queue is the same competing receive.
 */
export function createSubscribe(deps: ConsumerDeps): Subscribe {
  const { queues } = deps

  return async (queue, policy) => {
    const info = await queues.resolve(queue, policy.queues)
    const options = queues.declared(queue, policy.queues)
    const maxAttempts = maxAttemptsOf(options)
    return openSubscription(deps, {
      queue: info,
      maxAttempts,
      deadLetter:
        maxAttempts !== undefined ? await queues.deadLetterOf(info, policy.queues) : undefined,
      visibilityTimeoutSeconds: visibilityTimeoutSecondsOf(options),
    })
  }
}

/**
 * Keep one delivery of each message, in place. A standard queue is at-least-once, and receives
 * issued at the same moment can each return the same message. The delivery kept is the one
 * with the highest attempt: it is the most recent receive, whose receipt handle is the valid
 * one.
 */
const dropRepeatedMessages = (received: PullMessage<string>[]): void => {
  const latestById = new Map<string, PullMessage<string>>()
  for (const message of received) {
    const kept = latestById.get(message.id)
    if (kept === undefined || message.attempt > kept.attempt) latestById.set(message.id, message)
  }
  if (latestById.size === received.length) return
  const unique = received.filter((message) => latestById.get(message.id) === message)
  received.length = 0
  received.push(...unique)
}

/** How long a pull waits for the short polls that fill a batch beyond the first receive. */
const FILL_TIMEOUT_MS = 1_000

interface SubscriptionConfig {
  readonly queue: QueueInfo
  /** Deliveries allowed per message before it is dead-lettered. Absent means no limit. */
  readonly maxAttempts: number | undefined
  /** Where exhausted messages go. Absent means they are dropped. */
  readonly deadLetter: QueueInfo | undefined
  /**
   * The queue's liveness window as the visibility timeout every receive asks for: a consumer
   * silent for that long has its deliveries handed to someone else. Asked for per receive, so
   * it holds on a queue whose own attribute this instance could not set (`verify`).
   */
  readonly visibilityTimeoutSeconds: number
}

/**
 * One consumer's subscription on a queue. `pull` is ReceiveMessage: a received message is
 * invisible to other consumers for its visibility timeout, which is the subscription's lease on
 * it. The settle verbs live in `settle.ts`.
 */
function openSubscription(
  deps: ConsumerDeps,
  config: SubscriptionConfig
): QueueSubscription<string> {
  const { sqs } = deps
  const { queue, maxAttempts, visibilityTimeoutSeconds } = config

  const settler = createSettler({
    sqs,
    events: deps.events,
    queue,
    visibilityTimeoutSeconds,
    maxAttempts,
    deadLetter: config.deadLetter,
  })

  let closed = false
  // Cancels a receive that is still waiting when the subscription closes.
  const closing = new AbortController()

  /** One receive of up to ten messages, stamped with the lease deadline they came under. */
  const receiveOnce = async (
    max: number,
    waitTimeSeconds: number
  ): Promise<PullMessage<string>[]> => {
    const messages = await receiveMessages(sqs, {
      queueUrl: queue.url,
      max,
      waitTimeSeconds,
      visibilityTimeoutSeconds,
      signal: closing.signal,
    })
    // The visibility timeout started when SQS answered, which is now at the latest.
    const leaseDeadline = Date.now() + visibilityTimeoutSeconds * 1000
    return messages.map((message) => toPullMessage(message, leaseDeadline))
  }

  // Messages a fill call returned after its batch was handed out. They are already received,
  // so the next pull hands them out first instead of asking SQS for them again.
  let carried: PullMessage<string>[] = []

  /**
   * Up to `max` carried messages, each with its lease restarted so it reaches the handler
   * with a full visibility timeout. One whose renewal SQS does not accept is dropped: it
   * redelivers on its own.
   */
  const takeCarried = async (max: number): Promise<PullMessage<string>[]> => {
    if (carried.length === 0) return []
    const taken = carried.slice(0, max)
    carried = carried.slice(max)
    return settler.renew(taken)
  }

  const receive = async (max: number): Promise<PullMessage<string>[]> => {
    const received = await takeCarried(max)
    if (received.length >= max) return received

    // One receive of up to ten: a long poll, unless carried messages are already waiting to
    // be handed out. Only when it comes back full is there reason to think more is waiting,
    // and then short polls fill the rest of what was asked for.
    const wanted = max - received.length
    const first = await receiveOnce(wanted, received.length > 0 ? 0 : deps.waitTimeSeconds)
    received.push(...first)
    if (first.length < MAX_BATCH_ENTRIES || wanted <= first.length) {
      dropRepeatedMessages(received)
      return received
    }

    // The fill is best effort and bounded: the batch is handed out after FILL_TIMEOUT_MS with
    // whatever answered by then. A call that fails is skipped. A call that answers late has
    // done its work already: its messages are carried into the next pull.
    const callsNeeded = Math.ceil((wanted - first.length) / MAX_BATCH_ENTRIES)
    let handedOut = false
    const fills = Array.from({ length: callsNeeded }, (_, call) =>
      receiveOnce(wanted - MAX_BATCH_ENTRIES * (call + 1), 0).then(
        (batch) => {
          if (!handedOut) received.push(...batch)
          else if (!closed) carried.push(...batch)
        },
        () => undefined
      )
    )
    let deadline: NodeJS.Timeout | undefined
    await Promise.race([
      Promise.all(fills),
      new Promise<void>((resolve) => {
        deadline = setTimeout(resolve, FILL_TIMEOUT_MS)
      }),
    ])
    clearTimeout(deadline)
    handedOut = true
    dropRepeatedMessages(received)
    return received
  }

  const subscription: QueueSubscription<string> = {
    async pull(max) {
      if (closed || max <= 0) return []
      const received = await receive(max)
      // A receive that was answered as the subscription closed is not handed out: its messages
      // redeliver when their visibility timeout runs out.
      if (closed) return []
      if (maxAttempts === undefined) return received

      // Releases and crashes consume attempts too, so a message can arrive already past its
      // limit where no redrive policy caught it. It is never handed out: it is dead-lettered
      // here, with the message in hand.
      const withinLimit = received.filter((message) => message.attempt <= maxAttempts)
      if (withinLimit.length < received.length) {
        await settler.deadLetterExhausted(
          received.filter((message) => message.attempt > maxAttempts)
        )
      }
      settler.trackDelivered(withinLimit)
      return withinLimit
    },

    ack: async (receipts) => (closed ? undefined : settler.ack(receipts)),
    release: async (receipts) => (closed ? undefined : settler.release(receipts)),
    nack: async (entries) => (closed ? undefined : settler.nack(entries)),
    heartbeat: async (receipts) => (closed ? undefined : settler.heartbeat(receipts)),

    async close() {
      if (closed) return
      closed = true
      // What this subscription handed out redelivers when its visibility timeout runs out,
      // which is the queue's liveness window. Messages carried for a next pull were never
      // handed out, so they are given back now.
      const unhanded = carried
      carried = []
      if (unhanded.length > 0) {
        await settler.release(unhanded.map((message) => message.receipt)).catch(() => undefined)
      }
      closing.abort()
      deps.openSubscriptions.delete(subscription)
      settler.clear()
    },
  }
  deps.openSubscriptions.add(subscription)
  return subscription
}
