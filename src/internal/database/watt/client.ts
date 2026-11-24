import { getMessaging } from '@platformatic/globals'
import hyperid from 'hyperid'
import {
  type AcquireConnectionRequest,
  type AcquireConnectionResponse,
  type BatchRequest,
  type BeginTransactionRequest,
  type CommitTransactionRequest,
  DATABASE_APPLICATION_ID,
  DATABASE_MESSAGES,
  type DatabaseErrorResponse,
  type DatabaseMessageName,
  type DatabaseMessageRequest,
  type LockedQueryRequest,
  type QueryField,
  type QueryRequest,
  type QueryResponse,
  type RawQueryResponse,
  type ReleaseConnectionRequest,
  type RollbackTransactionRequest,
} from '../../../applications/database/protocol'
import { assertValidSignal } from '../postgres/asserts'
import { createAbortError } from '../postgres/pool-errors'

// Requests are plain data, so Platformatic posts them without walking them first. A value
// that can't be cloned still goes through: Platformatic then sanitizes the request and retries.
const PLAIN_DATA = { sanitize: false }

export interface DatabaseWattRequestOptions {
  signal?: AbortSignal
}

/**
 * A query result as the Database application returns it: raw rows plus their fields, or
 * (from a transport that already parsed them) row objects without fields.
 */
export type DatabaseWattQueryResponse<Row = unknown> =
  | QueryResponse<Row>
  | (RawQueryResponse & { fields: QueryField[] })

export type DatabaseWattBatchResponse = DatabaseWattQueryResponse & {
  lockId?: string
  /** The results of the statements flagged with `result`, in order. */
  results?: DatabaseWattQueryResponse[]
}

// Request IDs key in-flight operations (for cancellation) in the Database application,
// across every storage worker. hyperid gives each thread a random UUID and counts from it,
// so the IDs stay unique without generating random bytes for every message.
const nextRequestId = hyperid()

interface DatabaseWattSendOptions extends DatabaseWattRequestOptions {
  lockId?: string
  onLateResponse?: (response: unknown) => void
}

export interface DatabaseWattTransport {
  query<Row = unknown>(
    request: QueryRequest,
    options?: DatabaseWattRequestOptions
  ): Promise<DatabaseWattQueryResponse<Row>>
  acquire(request: AcquireConnectionRequest): Promise<AcquireConnectionResponse>
  batch(
    request: BatchRequest,
    options?: DatabaseWattRequestOptions
  ): Promise<DatabaseWattBatchResponse>
  lockedQuery<Row = unknown>(
    request: LockedQueryRequest,
    options?: DatabaseWattRequestOptions
  ): Promise<DatabaseWattQueryResponse<Row>>
  release(request: ReleaseConnectionRequest): Promise<void>
  beginTransaction(request: BeginTransactionRequest): Promise<AcquireConnectionResponse>
  commitTransaction(request: CommitTransactionRequest): Promise<void>
  rollbackTransaction(request: RollbackTransactionRequest): Promise<void>
}

export class DatabaseWattClient implements DatabaseWattTransport {
  query<Row = unknown>(
    request: QueryRequest,
    options?: DatabaseWattRequestOptions
  ): Promise<DatabaseWattQueryResponse<Row>> {
    return this.send(DATABASE_MESSAGES.query, request, parseQueryResponse<Row>, options)
  }

  acquire(request: AcquireConnectionRequest): Promise<AcquireConnectionResponse> {
    return this.send(DATABASE_MESSAGES.acquire, request, parseAcquireResponse)
  }

  batch(
    request: BatchRequest,
    options?: DatabaseWattRequestOptions
  ): Promise<DatabaseWattBatchResponse> {
    return this.send(DATABASE_MESSAGES.batch, request, parseBatchResponse, {
      ...options,
      // If the caller gave up but the Database application still opened the
      // transaction, roll it back now instead of leaving it to the lock idle timeout.
      onLateResponse: (response) => {
        const lockId = (response as { lockId?: unknown }).lockId
        if (typeof lockId === 'string') {
          this.rollbackTransaction({ lockId }).catch(() => undefined)
        }
      },
    })
  }

  lockedQuery<Row = unknown>(
    request: LockedQueryRequest,
    options?: DatabaseWattRequestOptions
  ): Promise<DatabaseWattQueryResponse<Row>> {
    return this.send(DATABASE_MESSAGES.lockedQuery, request, parseQueryResponse<Row>, {
      ...options,
      lockId: request.lockId,
    })
  }

  async release(request: ReleaseConnectionRequest): Promise<void> {
    await this.send(DATABASE_MESSAGES.release, request, parseReleasedResponse)
  }

  beginTransaction(request: BeginTransactionRequest): Promise<AcquireConnectionResponse> {
    return this.send(DATABASE_MESSAGES.beginTransaction, request, parseAcquireResponse)
  }

  async commitTransaction(request: CommitTransactionRequest): Promise<void> {
    await this.send(DATABASE_MESSAGES.commitTransaction, request, parseCommittedResponse)
  }

  async rollbackTransaction(request: RollbackTransactionRequest): Promise<void> {
    await this.send(DATABASE_MESSAGES.rollbackTransaction, request, parseRolledBackResponse)
  }

  private async send<Message extends DatabaseMessageName, Response>(
    message: Message,
    data: DatabaseMessageRequest<Message>,
    parseResponse: (response: unknown) => Response,
    options: DatabaseWattSendOptions = {}
  ): Promise<Response> {
    const { signal, lockId, onLateResponse } = options
    assertValidSignal(signal)

    let requestId = data.requestId
    if (typeof requestId !== 'string') {
      requestId = nextRequestId()
      data.requestId = requestId
    }
    const payload = data
    const request = getMessaging()
      .send(DATABASE_APPLICATION_ID, message, payload, PLAIN_DATA)
      .catch((error: unknown) => {
        throw toTransportError(error, requestId, lockId)
      })

    if (!signal) {
      return parseResponseOrThrow(await request, parseResponse)
    }

    let abortListener: (() => void) | undefined
    let settled = false

    const abortPromise = new Promise<never>((_, reject) => {
      abortListener = () => {
        if (settled) {
          return
        }

        sendCancel(requestId, lockId)
        if (onLateResponse) {
          request.then((response) => {
            if (!isDatabaseErrorResponse(response)) {
              onLateResponse(response)
            }
          }, noop)
        }
        reject(createAbortError())
      }

      signal.addEventListener('abort', abortListener, { once: true })
    })

    try {
      const response = await Promise.race([request, abortPromise])
      return parseResponseOrThrow(response, parseResponse)
    } finally {
      settled = true
      if (abortListener) {
        signal.removeEventListener('abort', abortListener)
      }
    }
  }
}

export class DatabaseWattResponseError extends Error {
  readonly code: string

  constructor(readonly response: DatabaseErrorResponse) {
    super(response.message)
    this.name = 'DatabaseWattResponseError'
    this.code = response.code
  }
}

export class DatabaseWattProtocolError extends Error {
  readonly code = 'PROTOCOL_ERROR'

  constructor(message: string) {
    super(message)
    this.name = 'DatabaseWattProtocolError'
  }
}

export const databaseWattClient: DatabaseWattTransport = new DatabaseWattClient()

function sendCancel(requestId: string, lockId: string | undefined): void {
  void getMessaging()
    .send(DATABASE_APPLICATION_ID, DATABASE_MESSAGES.cancel, { requestId, lockId }, PLAIN_DATA)
    .catch(() => undefined)
}

const MESSAGING_ERROR_CODE = 'PLT_RUNTIME_MESSAGING_ERROR'
const MESSAGING_TIMEOUT_MESSAGE = 'Timeout while waiting for a response'

// Platformatic messaging failures (no database worker, closed channel, or the
// runtime messagingTimeout) become Database Watt error responses so callers see
// one error contract. On a timeout the Database application may still be running
// the statement, so ask it to cancel.
function toTransportError(error: unknown, requestId: string, lockId: string | undefined): unknown {
  if (
    !(error instanceof Error) ||
    (error as Error & { code?: unknown }).code !== MESSAGING_ERROR_CODE
  ) {
    return error
  }

  const timedOut = error.message.includes(MESSAGING_TIMEOUT_MESSAGE)
  if (timedOut) {
    sendCancel(requestId, lockId)
  }

  return new DatabaseWattResponseError({
    code: timedOut ? 'MESSAGING_TIMEOUT' : 'MESSAGING_ERROR',
    message: error.message,
    requestId,
    lockId,
  })
}

function parseResponseOrThrow<Response>(
  response: unknown,
  parseResponse: (response: unknown) => Response
): Response {
  if (isDatabaseErrorResponse(response)) {
    throw new DatabaseWattResponseError(response)
  }

  return parseResponse(response)
}

function parseQueryResponse<Row>(response: unknown): DatabaseWattQueryResponse<Row> {
  if (
    !isRecord(response) ||
    typeof response.rowCount !== 'number' ||
    !Array.isArray(response.rows) ||
    (response.fields !== undefined && !Array.isArray(response.fields))
  ) {
    throw new DatabaseWattProtocolError('Invalid Database Watt query response')
  }

  return response as DatabaseWattQueryResponse<Row>
}

function parseBatchResponse(response: unknown): DatabaseWattBatchResponse {
  const parsed = parseQueryResponse(response) as DatabaseWattBatchResponse
  const { lockId, results } = response as { lockId?: unknown; results?: unknown }
  if (lockId !== undefined && typeof lockId !== 'string') {
    throw new DatabaseWattProtocolError('Invalid Database Watt batch response')
  }
  if (results !== undefined) {
    if (!Array.isArray(results)) {
      throw new DatabaseWattProtocolError('Invalid Database Watt batch response')
    }
    for (const result of results) {
      parseQueryResponse(result)
    }
  }
  return parsed
}

function parseAcquireResponse(response: unknown): AcquireConnectionResponse {
  if (!isRecord(response) || typeof response.lockId !== 'string') {
    throw new DatabaseWattProtocolError('Invalid Database Watt acquire response')
  }

  return { lockId: response.lockId }
}

function parseReleasedResponse(response: unknown): void {
  assertBooleanResponse(response, 'released')
}

function parseCommittedResponse(response: unknown): void {
  assertBooleanResponse(response, 'committed')
}

function parseRolledBackResponse(response: unknown): void {
  assertBooleanResponse(response, 'rolledBack')
}

function assertBooleanResponse(response: unknown, property: string): void {
  if (!isRecord(response) || response[property] !== true) {
    throw new DatabaseWattProtocolError(
      `Invalid Database Watt response: expected ${property} to be true`
    )
  }
}

function isDatabaseErrorResponse(response: unknown): response is DatabaseErrorResponse {
  return (
    isRecord(response) && typeof response.code === 'string' && typeof response.message === 'string'
  )
}

function noop(): void {}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}
