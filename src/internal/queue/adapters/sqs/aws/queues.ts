import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  type QueueAttributeName,
  SetQueueAttributesCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs'
import { isQueueAlreadyExists, isQueueMissing } from './errors'

const requestOptions = (signal: AbortSignal | undefined): { abortSignal?: AbortSignal } =>
  signal !== undefined ? { abortSignal: signal } : {}

/** The URL of the queue with this name, or `undefined` when there is none. */
export async function findQueueUrl(
  sqs: SQSClient,
  name: string,
  signal?: AbortSignal
): Promise<string | undefined> {
  try {
    const found = await sqs.send(
      new GetQueueUrlCommand({ QueueName: name }),
      requestOptions(signal)
    )
    return found.QueueUrl
  } catch (error) {
    if (isQueueMissing(error)) return undefined
    throw error
  }
}

export async function readQueueAttributes(
  sqs: SQSClient,
  url: string,
  attributeNames: QueueAttributeName[],
  signal?: AbortSignal
): Promise<Partial<Record<string, string>>> {
  const response = await sqs.send(
    new GetQueueAttributesCommand({ QueueUrl: url, AttributeNames: attributeNames }),
    requestOptions(signal)
  )
  return response.Attributes ?? {}
}

/** Create a queue and resolve with its URL. */
export async function createQueue(
  sqs: SQSClient,
  name: string,
  attributes: Record<string, string>
): Promise<string> {
  let url: string | undefined
  try {
    url = (await sqs.send(new CreateQueueCommand({ QueueName: name, Attributes: attributes })))
      .QueueUrl
  } catch (error) {
    // Creating a queue that exists with the same attributes succeeds; this is one that exists
    // with other attributes (another deploy created it). It is adopted as it is.
    if (!isQueueAlreadyExists(error)) throw error
    url = await findQueueUrl(sqs, name)
  }
  if (url === undefined) throw new Error(`queue sqs: could not create the queue "${name}"`)
  return url
}

const redrivePolicyMatches = (
  current: string | undefined,
  deadLetterArn: string,
  maxReceiveCount: number
): boolean => {
  if (current === undefined || current === '') return false
  try {
    const parsed = JSON.parse(current) as {
      deadLetterTargetArn?: string
      maxReceiveCount?: number | string
    }
    return (
      parsed.deadLetterTargetArn === deadLetterArn &&
      Number(parsed.maxReceiveCount) === maxReceiveCount
    )
  } catch {
    return false
  }
}

/** The redrive policy a queue should have; `undefined` means none. */
export interface RedriveTarget {
  readonly deadLetterArn: string
  readonly maxReceiveCount: number
}

/**
 * Bring the two attributes a queue's declared policy owns in line with it: the default
 * visibility timeout and the redrive policy. Writes only what differs, so re-provisioning an
 * unchanged queue is a read.
 */
export async function ensurePolicyAttributes(
  sqs: SQSClient,
  url: string,
  visibilityTimeoutSeconds: number,
  redrive: RedriveTarget | undefined
): Promise<void> {
  const current = await readQueueAttributes(sqs, url, ['VisibilityTimeout', 'RedrivePolicy'])
  const changes: Record<string, string> = {}
  if (Number(current.VisibilityTimeout) !== visibilityTimeoutSeconds) {
    changes.VisibilityTimeout = String(visibilityTimeoutSeconds)
  }
  if (redrive === undefined) {
    // An empty value removes the policy.
    if (current.RedrivePolicy !== undefined && current.RedrivePolicy !== '') {
      changes.RedrivePolicy = ''
    }
  } else if (
    !redrivePolicyMatches(current.RedrivePolicy, redrive.deadLetterArn, redrive.maxReceiveCount)
  ) {
    changes.RedrivePolicy = JSON.stringify({
      deadLetterTargetArn: redrive.deadLetterArn,
      maxReceiveCount: redrive.maxReceiveCount,
    })
  }
  if (Object.keys(changes).length === 0) return
  await sqs.send(new SetQueueAttributesCommand({ QueueUrl: url, Attributes: changes }))
}

/** The approximate message counts SQS publishes for a queue. They can lag a burst by seconds. */
export interface QueueMessageCounts {
  /** Deliverable now. */
  readonly visible: number
  /** Sent with a delay that has not passed yet. */
  readonly delayed: number
  /** Received and inside their visibility timeout. */
  readonly inFlight: number
}

/** Read a queue's message counts, or `undefined` when the queue no longer exists. */
export async function readQueueMessageCounts(
  sqs: SQSClient,
  url: string,
  signal?: AbortSignal
): Promise<QueueMessageCounts | undefined> {
  let attributes: Partial<Record<string, string>>
  try {
    attributes = await readQueueAttributes(
      sqs,
      url,
      [
        'ApproximateNumberOfMessages',
        'ApproximateNumberOfMessagesNotVisible',
        'ApproximateNumberOfMessagesDelayed',
      ],
      signal
    )
  } catch (error) {
    if (isQueueMissing(error)) return undefined
    throw error
  }
  const count = (name: string): number => Number(attributes[name] ?? 0) || 0
  return {
    visible: count('ApproximateNumberOfMessages'),
    delayed: count('ApproximateNumberOfMessagesDelayed'),
    inFlight: count('ApproximateNumberOfMessagesNotVisible'),
  }
}
