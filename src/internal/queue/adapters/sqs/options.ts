import type { SQSClient, SQSClientConfig } from '@aws-sdk/client-sqs'
import type { Codec } from '../../manager/codec'
import type { AdapterEvents } from '../../manager/events'

/**
 * The `sqs` namespace of a queue's options. `fifo` is fixed once the queue exists (a FIFO
 * queue is a different SQS queue, named `<name>.fifo`); re-declaring it differently throws.
 */
export interface SqsQueueExtension {
  /**
   * Make this a FIFO queue, which is what gives it producer deduplication. Default `false`.
   *
   * - Standard queue (default): a message's `idempotencyKey` / `idempotencyTtlMs` do NOT
   *   deduplicate, so message classes that rely on them on another engine still send here.
   *   Every send is delivered at least once. The message `key` (else its idempotency key) is
   *   sent as the `MessageGroupId`, which on a standard queue is the tenant id of SQS fair
   *   queues: a group with a backlog does not delay the others. It orders nothing and holds
   *   nothing back.
   * - FIFO queue: `idempotencyKey` becomes the `MessageDeduplicationId` (a message without one
   *   gets a random id, so it is never deduplicated). The window is SQS's fixed five minutes:
   *   a longer `idempotencyTtlMs` is accepted and effectively clamped to it, a shorter one is
   *   widened to it. The `MessageGroupId` is the idempotency key too, else the message `key`,
   *   else a group of its own: SQS holds a group back while one of its messages is in
   *   flight, so the group is the narrowest identity the message has.
   *
   * A FIFO queue cannot delay a single message (`delayMs` throws `NotSupportedError`), a
   * parked retry holds back the other messages of its key until it is redelivered, and its
   * dead-letter queue must be FIFO too (one that declares no kind is created FIFO).
   */
  readonly fifo?: boolean
  /**
   * SQS attributes for this queue, over the adapter's `queueAttributes`: a longer
   * `MessageRetentionPeriod` on a dead-letter queue, or high-throughput FIFO
   * (`DeduplicationScope: 'messageGroup'` with `FifoThroughputLimit: 'perMessageGroupId'`,
   * which narrows deduplication to messages sharing a `key`). Applied when the adapter
   * creates the queue, never to one that already exists. `VisibilityTimeout` and
   * `RedrivePolicy` are not for here: they follow the queue's `livenessTimeoutMs` and `retry`,
   * which every provision re-applies.
   */
  readonly attributes?: Readonly<Record<string, string>>
}

declare module '../../manager/registry' {
  interface QueueOptions {
    /** Read by the SQS adapter only; other engines ignore it. */
    readonly sqs?: SqsQueueExtension
  }
}

export interface SqsOptions {
  /**
   * A caller-owned SQS client. `close()` does not destroy it. When absent the adapter builds
   * one from {@link client} and destroys it at `close()`.
   */
  readonly sqs?: SQSClient
  /**
   * Configuration of the client the adapter builds itself (region, endpoint, credentials).
   * Omitted fields fall back to the AWS SDK's default provider chain.
   */
  readonly client?: SQSClientConfig
  /**
   * Prefix of every SQS queue name the adapter derives (`<prefix><queue>`, plus `.fifo`).
   * Default: none, so a declared queue name is the SQS queue name. Set one to keep several
   * deployments apart in one account and region.
   */
  readonly prefix?: string
  /**
   * Replace the derived SQS queue name, to adopt queues that already exist under other names.
   * Gets the declared queue name and returns the full SQS name ({@link prefix} is not
   * applied); a FIFO queue's must end in `.fifo`.
   */
  readonly queueName?: (queue: string, fifo: boolean) => string
  /**
   * `manage` (default): create missing queues and set redrive policies, idempotently.
   * `verify`: never create or change anything; resolve queues by name and throw when one is
   * missing or of the wrong kind.
   */
  readonly provisioning?: 'manage' | 'verify'
  /** Wire codec. The SQS body is text, so the wire type is `string`. Default: JSON text. */
  readonly codec?: Codec<string>
  /**
   * How long one receive waits for a message before returning empty (SQS long polling),
   * 0 to 20. Default 20. Closing the subscription or the adapter cancels the wait.
   */
  readonly waitTimeSeconds?: number
  /**
   * Attributes of every queue the adapter creates (`MessageRetentionPeriod`,
   * `KmsMasterKeyId`, …). Applied at creation only, in `manage` mode.
   */
  readonly queueAttributes?: Readonly<Record<string, string>>
  /**
   * Sink for the adapter's own facts. Only `deadLettered` is emitted, and only for verdicts
   * the adapter issues itself: a message SQS moves through the redrive policy is not seen.
   */
  readonly events?: AdapterEvents
}
