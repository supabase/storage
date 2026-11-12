import type { SQSClient } from '@aws-sdk/client-sqs'
import type { QueueOptions, QueuesView } from '../../manager/registry'
import { createQueue, ensureRedrivePolicy, findQueueUrl, readQueueAttributes } from './aws/queues'
import type { SqsQueueExtension } from './options'

// Which SQS queue realizes a declared queue: one queue each, named `<prefix><queue>` (plus
// `.fifo`). In `manage` mode resolving a queue creates it when missing; in `verify` mode
// nothing is ever created or changed, and a missing or mismatched queue throws.

/** A declared queue as resolved against SQS. */
export interface QueueInfo {
  /** The declared queue name. */
  readonly queue: string
  readonly url: string
  readonly arn: string
  readonly fifo: boolean
}

export interface Queues {
  /** `provision`: make sure the queue exists as declared. */
  provision(queue: string, options?: QueueOptions): Promise<void>
  /**
   * Resolve a queue. Its `sqs` options come from the manager's registry when the queue is
   * declared there with any, else from an earlier `provision` on this instance. Where neither
   * says which kind it is, `inheritedFifo` does (a dead-letter queue takes after its source),
   * and failing that the queue stays what it was first resolved as, standard by default.
   */
  resolve(queue: string, registry?: QueuesView, inheritedFifo?: boolean): Promise<QueueInfo>
  /** Point a queue's redrive policy at its dead-letter queue (`manage` only). */
  setRedrivePolicy(source: QueueInfo, deadLetter: QueueInfo, maxReceiveCount: number): Promise<void>
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
  const declaredByProvision = new Map<string, SqsQueueExtension>()
  const resolved = new Map<string, Promise<QueueInfo>>()
  const confirmedRedrivePolicies = new Set<string>()

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
    declared: SqsQueueExtension,
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
        ...declared.attributes,
        ...(fifo ? { FifoQueue: 'true' } : {}),
      })
    }
    const attributes = await readQueueAttributes(sqs, url, ['QueueArn', 'FifoQueue'])
    if (attributes.QueueArn === undefined) {
      throw new Error(`queue sqs: the SQS queue "${name}" reported no ARN`)
    }
    // Only a `queueName` mapping can get here: derived names differ by the `.fifo` suffix.
    if ((attributes.FifoQueue === 'true') !== fifo) {
      throw new Error(
        `queue sqs: the SQS queue "${name}" is ${kind(!fifo)}, but queue "${queue}" is ` +
          `declared ${kind(fifo)}`
      )
    }
    return { queue, url, arn: attributes.QueueArn, fifo }
  }

  const resolve: Queues['resolve'] = async (queue, registry, inheritedFifo) => {
    const declared = registry?.get(queue)?.options?.sqs ?? declaredByProvision.get(queue) ?? {}
    // `undefined`: nothing says which kind, so whatever the queue was resolved as stands.
    const fifo = declared.fifo ?? inheritedFifo
    let pending = resolved.get(queue)
    if (pending === undefined) {
      // A rejection is evicted, so the next call tries again instead of replaying the failure.
      pending = find(queue, declared, fifo ?? false).catch((error: unknown) => {
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

  return {
    resolve,

    async provision(queue, options) {
      // An options-less call only makes sure the queue exists: it declares nothing, so it
      // never conflicts with what an earlier call declared.
      if (options?.sqs === undefined) {
        await resolve(queue)
        return
      }
      const previous = declaredByProvision.get(queue)
      declaredByProvision.set(queue, options.sqs)
      try {
        await resolve(queue)
      } catch (error) {
        // A refused declaration must not replace the one the queue is in use under.
        if (previous !== undefined) declaredByProvision.set(queue, previous)
        else declaredByProvision.delete(queue)
        throw error
      }
    },

    async setRedrivePolicy(source, deadLetter, maxReceiveCount) {
      if (!manage) return
      // Confirmed once per instance, so a subscribe does not re-read the attributes each time.
      const cacheKey = `${source.url}\0${deadLetter.arn}\0${maxReceiveCount}`
      if (confirmedRedrivePolicies.has(cacheKey)) return
      await ensureRedrivePolicy(sqs, source.url, deadLetter.arn, maxReceiveCount)
      confirmedRedrivePolicies.add(cacheKey)
    },

    resolvedNames: () => [...resolved.keys()],

    async findUrl(queue, signal) {
      const known = await resolved.get(queue)?.catch(() => undefined)
      if (known !== undefined) return known.url
      const declaredFifo = declaredByProvision.get(queue)?.fifo ?? false
      return (
        (await findQueueUrl(sqs, sqsNameOf(queue, declaredFifo), signal)) ??
        (await findQueueUrl(sqs, sqsNameOf(queue, !declaredFifo), signal))
      )
    },
  }
}
