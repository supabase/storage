import { randomUUID } from 'node:crypto'
import { getMessaging } from '@platformatic/globals'
import {
  type AcquireConnectionRequest,
  type AcquireConnectionResponse,
  type BeginTransactionRequest,
  type CommitTransactionRequest,
  DATABASE_APPLICATION_ID,
  DATABASE_MESSAGES,
  type DatabaseErrorResponse,
  type DatabaseMessageName,
  type DatabaseMessageRequest,
  type LockedQueryRequest,
  type QueryRequest,
  type QueryResponse,
  type ReleaseConnectionRequest,
  type RollbackTransactionRequest,
} from '../../../applications/database/protocol'
import { assertValidSignal } from '../postgres/asserts'
import { createAbortError } from '../postgres/pool-errors'

export interface DatabaseWattRequestOptions {
  signal?: AbortSignal
}

interface DatabaseWattSendOptions extends DatabaseWattRequestOptions {
  lockId?: string
}

export interface DatabaseWattTransport {
  query<Row = unknown>(
    request: QueryRequest,
    options?: DatabaseWattRequestOptions
  ): Promise<QueryResponse<Row>>
  acquire(request: AcquireConnectionRequest): Promise<AcquireConnectionResponse>
  lockedQuery<Row = unknown>(
    request: LockedQueryRequest,
    options?: DatabaseWattRequestOptions
  ): Promise<QueryResponse<Row>>
  release(request: ReleaseConnectionRequest): Promise<void>
  beginTransaction(request: BeginTransactionRequest): Promise<AcquireConnectionResponse>
  commitTransaction(request: CommitTransactionRequest): Promise<void>
  rollbackTransaction(request: RollbackTransactionRequest): Promise<void>
}

export class DatabaseWattClient implements DatabaseWattTransport {
  query<Row = unknown>(
    request: QueryRequest,
    options?: DatabaseWattRequestOptions
  ): Promise<QueryResponse<Row>> {
    return this.send(DATABASE_MESSAGES.query, request, parseQueryResponse<Row>, options)
  }

  acquire(request: AcquireConnectionRequest): Promise<AcquireConnectionResponse> {
    return this.send(DATABASE_MESSAGES.acquire, request, parseAcquireResponse)
  }

  lockedQuery<Row = unknown>(
    request: LockedQueryRequest,
    options?: DatabaseWattRequestOptions
  ): Promise<QueryResponse<Row>> {
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
    const { signal, lockId } = options
    assertValidSignal(signal)

    const requestId = typeof data.requestId === 'string' ? data.requestId : randomUUID()
    const payload = { ...data, requestId }
    const request = getMessaging()
      .send(DATABASE_APPLICATION_ID, message, payload)
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
    .send(DATABASE_APPLICATION_ID, DATABASE_MESSAGES.cancel, { requestId, lockId })
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

function parseQueryResponse<Row>(response: unknown): QueryResponse<Row> {
  if (
    !isRecord(response) ||
    typeof response.rowCount !== 'number' ||
    !Array.isArray(response.rows)
  ) {
    throw new DatabaseWattProtocolError('Invalid Database Watt query response')
  }

  return {
    rowCount: response.rowCount,
    rows: response.rows,
  }
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}
