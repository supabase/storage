import type { SQSClient } from '@aws-sdk/client-sqs'
import { livenessOf, type QueueOptions, type QueuesView } from '../../manager/registry'
import {
  createQueue,
  ensurePolicyAttributes,
  findQueueUrl,
  readQueueAttributes,
} from './client/queues'
import type { SqsQueueExtension } from './options'

// Which SQS queue realizes a declared queue: one queue each, named `<prefix><queue>` (plus
// `.fifo`). In `manage` mode resolving a queue creates it when missing; in `verify` mode
// nothing is ever created or changed, and a missing or mismatched queue throws.
//
// A queue's declared policy lands on two SQS attributes when it is provisioned: the liveness
// window is its `VisibilityTimeout`, and a retry budget with a dead-letter queue is its
// `RedrivePolicy`. Subscriptions read the same declaration for their own behaviour.

/** A visibility timeout is whole seconds, at most twelve hours. */
const MAX_VISIBILITY_TIMEOUT_SECONDS = 43_200

/** The visibility timeout that realizes a queue's liveness window, declared or default. */
export const visibilityTimeoutSecondsOf = (options: QueueOptions | undefined): number =>
  Math.min(MAX_VISIBILITY_TIMEOUT_SECONDS, Math.max(1, Math.ceil(livenessOf(options) / 1000)))

/** Deliveries a queue allows per message, or `undefined` when it declares no budget. */
export const maxAttemptsOf = (options: QueueOptions | undefined): number | undefined =>
  options?.retry !== undefined ? Math.max(1, Math.floor(options.retry.maxAttempts)) : undefined

/** A declared queue as resolved against SQS. */
export interface QueueInfo {
  /** The declared queue name. */
  readonly queue: string
  readonly url: string
  readonly arn: string
  readonly fifo: boolean
}

export interface Queues {
  /**
   * `provision`: make sure the queue exists as declared and, in `manage` mode, that its
   * visibility timeout and redrive policy match the declared policy (an existing queue that
   * differs is updated: that is how a changed policy rolls out). An options-less call only
   * makes sure the queue exists.
   */
  provision(queue: string, options?: QueueOptions): Promise<void>
  /**
   * A queue's declared options: the manager's registry when the queue is declared there,
   * else what `provision` was last given on this instance, else `undefined`.
   */
  declared(queue: string, registry?: QueuesView): QueueOptions | undefined
  /**
   * Resolve a queue. Where its declared options do not say which kind it is, `inheritedFifo`
   * does, and failing that the queue stays what it was first resolved as, standard by default.
   */
  resolve(queue: string, registry?: QueuesView, inheritedFifo?: boolean): Promise<QueueInfo>
  /** Resolve the dead-letter queue `source` declares, or `undefined` when it declares none. */
  deadLetterOf(source: QueueInfo, registry?: QueuesView): Promise<QueueInfo | undefined>
  /** The declared names of the queues this instance has resolved. */
  resolvedNames(): string[]
  /** The URL of a queue that exists, without ever creating it. */
  findUrl(queue: string, signal?: AbortSignal): Promise<string | undefined>
}

export interface QueuesDeps {
  readonly sqs: SQSClient
  readonly prefix: string
  readonly queueName: ((queue: string, fifo: boolean) => string) | undefined
  readonly manage: boolean
  readonly queueAttributes: Readonly<Record<string, string>>
}

const FIFO_SUFFIX = '.fifo'
const MAX_QUEUE_NAME = 80
const QUEUE_NAME = /^[A-Za-z0-9_-]+$/

const kind = (fifo: boolean): string => (fifo ? 'FIFO' : 'standard')

export function createQueues(deps: QueuesDeps): Queues {
  const { sqs, manage } = deps

  // Per-instance state, bounded by the queues this instance touches.
  const declaredByProvision = new Map<string, QueueOptions>()
  const resolved = new Map<string, Promise<QueueInfo>>()

  const declared: Queues['declared'] = (queue, registry) => {
    const descriptor = registry?.get(queue)
    return descriptor !== undefined ? (descriptor.options ?? {}) : declaredByProvision.get(queue)
  }

  /** The SQS name of a declared queue. Checked here so a bad name fails with the declared
   * name in the message instead of an SQS error in the middle of a send. */
  const sqsNameOf = (queue: string, fifo: boolean): string => {
    const name = deps.queueName?.(queue, fifo) ?? `${deps.prefix}${queue}${fifo ? FIFO_SUFFIX : ''}`
    if (fifo !== name.endsWith(FIFO_SUFFIX)) {
      throw new Error(
        `queue sqs: the SQS name "${name}" for ${kind(fifo)} queue "${queue}" ` +
          `${fifo ? 'must' : 'must not'} end in "${FIFO_SUFFIX}"`
      )
    }
    const stem = fifo ? name.slice(0, -FIFO_SUFFIX.length) : name
    if (!QUEUE_NAME.test(stem) || name.length > MAX_QUEUE_NAME) {
      throw new Error(
        `queue sqs: queue "${queue}" maps to the SQS name "${name}", which is not valid: use ` +
          `letters, digits, "-" and "_", ${MAX_QUEUE_NAME} characters at most. Rename the ` +
          `queue or map it with the queueName option`
      )
    }
    return name
  }

  const find = async (
    queue: string,
    extension: SqsQueueExtension,
    fifo: boolean
  ): Promise<QueueInfo> => {
    const name = sqsNameOf(queue, fifo)
    let url = await findQueueUrl(sqs, name)
    if (url === undefined) {
      if (!manage) {
        throw new Error(
          `queue sqs: the SQS queue "${name}" for queue "${queue}" does not exist, and ` +
            `provisioning is "verify" so the adapter will not create it`
        )
      }
      url = await createQueue(sqs, name, {
        ...deps.queueAttributes,
        ...extension.attributes,
        ...(fifo ? { FifoQueue: 'true' } : {}),
      })
    }
    // Only the ARN is read. `FifoQueue` is a FIFO-only attribute: asking a standard queue for
    // it is refused by SQS (InvalidAttributeName). The kind needs no read anyway: SQS only
    // lets a FIFO queue's name end in `.fifo`, and `sqsNameOf` already holds the name to that.
    const attributes = await readQueueAttributes(sqs, url, ['QueueArn'])
    if (attributes.QueueArn === undefined) {
      throw new Error(`queue sqs: the SQS queue "${name}" reported no ARN`)
    }
    return { queue, url, arn: attributes.QueueArn, fifo }
  }

  const resolve: Queues['resolve'] = async (queue, registry, inheritedFifo) => {
    const extension = declared(queue, registry)?.sqs ?? {}
    // `undefined`: nothing says which kind, so whatever the queue was resolved as stands.
    const fifo = extension.fifo ?? inheritedFifo
    let pending = resolved.get(queue)
    if (pending === undefined) {
      // A rejection is evicted, so the next call tries again instead of replaying the failure.
      pending = find(queue, extension, fifo ?? false).catch((error: unknown) => {
        resolved.delete(queue)
        throw error
      })
      resolved.set(queue, pending)
    }
    const info = await pending
    // The cache is keyed by queue alone: a queue resolved as one kind cannot be asked for as
    // the other.
    if (fifo !== undefined && info.fifo !== fifo) {
      throw new Error(
        `queue sqs: queue "${queue}" is in use as a ${kind(info.fifo)} queue, but is now asked ` +
          `for as a ${kind(fifo)} one. Declare it with sqs: { fifo: ${String(fifo)} } wherever ` +
          `it is declared (a queue's kind is fixed when it is created)`
      )
    }
    return info
  }

  const deadLetterOf: Queues['deadLetterOf'] = async (source, registry) => {
    const name = declared(source.queue, registry)?.retry?.deadLetter
    if (name === undefined) return undefined
    // A dead-letter queue nobody resolved yet and that declares no kind takes after its source.
    const target = await resolve(name, registry, resolved.has(name) ? undefined : source.fifo)
    if (target.fifo !== source.fifo) {
      throw new Error(
        `queue sqs: dead-letter queue "${name}" is ${kind(target.fifo)} but "${source.queue}" ` +
          `is ${kind(source.fifo)}, and SQS only dead-letters between two queues of one kind. ` +
          `Declare "${name}" with sqs: { fifo: ${String(source.fifo)} }`
      )
    }
    return target
  }

  return {
    declared,
    resolve,
    deadLetterOf,

    async provision(queue, options) {
      // An options-less call only makes sure the queue exists: it declares nothing, so it
      // neither conflicts with nor replaces what an earlier call declared.
      if (options === undefined) {
        await resolve(queue)
        return
      }
      const previous = declaredByProvision.get(queue)
      declaredByProvision.set(queue, options)
      try {
        const info = await resolve(queue)
        const deadLetter = await deadLetterOf(info)
        if (!manage) return
        const maxAttempts = maxAttemptsOf(options)
        // SQS moves a message to the dead-letter queue itself once it was received
        // `maxAttempts` times, which is what enforces the budget for deliveries lost to a
        // release or a crash. A budget without a dead-letter queue is the subscription's to
        // enforce: SQS has no way to drop.
        await ensurePolicyAttributes(
          sqs,
          info.url,
          visibilityTimeoutSecondsOf(options),
          deadLetter !== undefined && maxAttempts !== undefined
            ? { deadLetterArn: deadLetter.arn, maxReceiveCount: maxAttempts }
            : undefined
        )
      } catch (error) {
        // A refused declaration must not replace the one the queue is in use under.
        if (previous !== undefined) declaredByProvision.set(queue, previous)
        else declaredByProvision.delete(queue)
        throw error
      }
    },

    resolvedNames: () => [...resolved.keys()],

    async findUrl(queue, signal) {
      const known = await resolved.get(queue)?.catch(() => undefined)
      if (known !== undefined) return known.url
      const declaredFifo = declaredByProvision.get(queue)?.sqs?.fifo ?? false
      return (
        (await findQueueUrl(sqs, sqsNameOf(queue, declaredFifo), signal)) ??
        (await findQueueUrl(sqs, sqsNameOf(queue, !declaredFifo), signal))
      )
    },
  }
}
