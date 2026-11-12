/** The name AWS gave an error. The SDK puts it on `name`; some endpoints only set `Code`. */
export const errorCode = (error: unknown): string => {
  if (typeof error !== 'object' || error === null) return ''
  const fields = error as { name?: unknown; Code?: unknown; code?: unknown }
  for (const candidate of [fields.name, fields.Code, fields.code]) {
    if (typeof candidate === 'string' && candidate !== '' && candidate !== 'Error') return candidate
  }
  return ''
}

const QUEUE_MISSING_CODES = new Set([
  'QueueDoesNotExist',
  'AWS.SimpleQueueService.NonExistentQueue',
  'NonExistentQueue',
])
const QUEUE_ALREADY_EXISTS_CODES = new Set(['QueueNameExists', 'QueueAlreadyExists'])
const ABORT_CODES = new Set(['AbortError', 'RequestAbortedError'])

export const isQueueMissing = (error: unknown): boolean => QUEUE_MISSING_CODES.has(errorCode(error))
export const isQueueAlreadyExists = (error: unknown): boolean =>
  QUEUE_ALREADY_EXISTS_CODES.has(errorCode(error))
export const isAbortError = (error: unknown): boolean => ABORT_CODES.has(errorCode(error))

// Settling a delivery this consumer no longer holds is a no-op by contract: the message was
// already deleted, its receipt handle expired, or it is no longer in flight. SQS reports each
// of those as an error under these codes. `InvalidParameterValue` is the broad one: it is also
// what a visibility timeout past the twelve-hour ceiling gets, which the adapter avoids by
// clamping what it asks for.
const STALE_RECEIPT_HANDLE_CODES = new Set([
  'ReceiptHandleIsInvalid',
  'MessageNotInflight',
  'InvalidParameterValue',
])

/**
 * Whether an error code says the receipt handle no longer refers to a delivery in flight. The
 * same code arrives plain, namespaced (`AWS.SimpleQueueService.…`) or with an `Exception`
 * suffix depending on the protocol and the endpoint.
 */
export const isStaleReceiptHandleCode = (code: string | undefined): boolean =>
  code !== undefined &&
  STALE_RECEIPT_HANDLE_CODES.has(
    code.replace(/^AWS\.SimpleQueueService\./, '').replace(/Exception$/, '')
  )

/** A failed entry of a batch call. */
export interface FailedBatchEntry {
  readonly Id?: string | undefined
  readonly Code?: string | undefined
  readonly Message?: string | undefined
  readonly SenderFault?: boolean | undefined
}

/** The failed entries of a batch call as one line for an error message. */
export const describeFailedEntries = (failed: readonly FailedBatchEntry[]): string =>
  failed
    .map(
      (entry) =>
        `${entry.Code ?? 'Unknown'}${entry.Message !== undefined ? `: ${entry.Message}` : ''}`
    )
    .join('; ')
