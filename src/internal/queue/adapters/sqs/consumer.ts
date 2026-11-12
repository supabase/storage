import type { SQSClient } from '@aws-sdk/client-sqs'
import type { QueueSession, SubscribePolicy } from '../../manager/adapter'
import type { AdapterEvents } from '../../manager/events'
import type { PullMessage } from '../../manager/message'
import { MAX_BATCH_ENTRIES } from './aws/batching'
import { receiveMessages } from './aws/messages'
import { toPullMessage } from './message'
import type { QueueInfo, Queues } from './queues'
import { createSettler } from './settle'

export interface ConsumerDeps {
  readonly sqs: SQSClient
  readonly queues: Queues
  /** How long one receive waits for a message before returning empty. */
  readonly waitTimeSeconds: number
  readonly events: AdapterEvents | undefined
  /** Every open session, so the adapter's `close()` can close them. */
  readonly openSessions: Set<QueueSession<string>>
}

export type Subscribe = (queue: string, policy: SubscribePolicy) => Promise<QueueSession<string>>

/**
 * `subscribe`: make sure the queue and its dead-letter queue exist, wire the redrive policy
 * between them, and open a session. Every session on a queue is the same competing receive.
 */
export function createSubscribe(deps: ConsumerDeps): Subscribe {
  const { queues } = deps

  return async (queue, policy) => {
    const info = await queues.resolve(queue, policy.queues)
    const maxAttempts =
      policy.retry !== undefined ? Math.max(1, Math.floor(policy.retry.maxAttempts)) : undefined

    let deadLetter: QueueInfo | undefined
    if (maxAttempts !== undefined && policy.retry?.deadLetter !== undefined) {
      // An undeclared dead-letter queue takes after its source.
      deadLetter = await queues.resolve(policy.retry.deadLetter, policy.queues, info.fifo)
      if (deadLetter.fifo !== info.fifo) {
        throw new Error(
          `queue sqs: dead-letter queue "${deadLetter.queue}" is ` +
            `${deadLetter.fifo ? 'FIFO' : 'standard'} but "${queue}" is ` +
            `${info.fifo ? 'FIFO' : 'standard'}. SQS can only dead-letter between two FIFO or ` +
            `two standard queues`
        )
      }
      // SQS moves a message here itself once it was received `maxAttempts` times, which is
      // what enforces the budget for deliveries lost to a nack or a crash.
      await queues.setRedrivePolicy(info, deadLetter, maxAttempts)
    }

    return openSession(deps, {
      queue: info,
      maxAttempts,
      deadLetter,
      livenessTimeoutMs: policy.livenessTimeoutMs,
    })
  }
}

/** A visibility timeout is whole seconds, at most twelve hours. */
const MAX_VISIBILITY_TIMEOUT_SECONDS = 43_200

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

interface SessionConfig {
  readonly queue: QueueInfo
  /** Deliveries allowed per message before it is dead-lettered. Absent means no limit. */
  readonly maxAttempts: number | undefined
  /** Where exhausted messages go. Absent means they are dropped. */
  readonly deadLetter: QueueInfo | undefined
  /** How long a silent consumer keeps its deliveries before they go to someone else. */
  readonly livenessTimeoutMs: number | undefined
}

/**
 * One consumer's session on a queue. `pull` is ReceiveMessage: a received message is
 * invisible to other consumers for its visibility timeout, which is the session's lease on
 * it. The settle verbs live in `settle.ts`.
 */
function openSession(deps: ConsumerDeps, config: SessionConfig): QueueSession<string> {
  const { sqs } = deps
  const { queue, maxAttempts } = config

  // The liveness contract is the visibility timeout: a consumer silent for that long has its
  // deliveries handed to someone else. Without one the queue's own timeout applies.
  const visibilityTimeoutSeconds =
    config.livenessTimeoutMs !== undefined
      ? Math.min(
          MAX_VISIBILITY_TIMEOUT_SECONDS,
          Math.max(1, Math.ceil(config.livenessTimeoutMs / 1000))
        )
      : undefined

  const settler = createSettler({
    sqs,
    events: deps.events,
    queue,
    visibilityTimeoutSeconds,
    maxAttempts,
    deadLetter: config.deadLetter,
  })

  let closed = false
  // Cancels a receive that is still waiting when the session closes.
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
    const leaseDeadline =
      visibilityTimeoutSeconds !== undefined ? Date.now() + visibilityTimeoutSeconds * 1000 : 0
    return messages.map((message) => toPullMessage(message, leaseDeadline))
  }

  const receive = async (max: number): Promise<PullMessage<string>[]> => {
    // One long-polled receive of up to ten. Only when it comes back full is there reason to
    // think more is waiting, and then short polls fill the rest of what was asked for.
    const received = await receiveOnce(max, deps.waitTimeSeconds)
    if (received.length < MAX_BATCH_ENTRIES || max <= received.length) return received
    const callsNeeded = Math.ceil((max - received.length) / MAX_BATCH_ENTRIES)
    const batches = await Promise.all(
      Array.from({ length: callsNeeded }, (_, call) =>
        receiveOnce(max - MAX_BATCH_ENTRIES * (call + 1), 0)
      )
    )
    for (const batch of batches) received.push(...batch)
    dropRepeatedMessages(received)
    return received
  }

  const session: QueueSession<string> = {
    async pull(max) {
      if (closed || max <= 0) return []
      const received = await receive(max)
      // A receive that was answered as the session closed is not handed out: its messages
      // redeliver when their visibility timeout runs out.
      if (closed) return []
      if (maxAttempts === undefined) return received

      // Nacks and crashes consume attempts too, so a message can arrive already past its
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
    nack: async (receipts) => (closed ? undefined : settler.nack(receipts)),
    retry: async (entries) => (closed ? undefined : settler.retry(entries)),
    heartbeat: async (receipts) => (closed ? undefined : settler.heartbeat(receipts)),

    async close() {
      if (closed) return
      closed = true
      // Nothing to release: what this session still held redelivers when its visibility
      // timeout runs out, which is the liveness window it declared.
      closing.abort()
      deps.openSessions.delete(session)
      settler.clear()
    },
  }
  deps.openSessions.add(session)
  return session
}
