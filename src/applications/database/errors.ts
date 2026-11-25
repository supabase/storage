import { isConnectionTimeoutError } from '@internal/database/postgres/pool-errors'
import { DatabaseError } from 'pg'
import { countBusyRejection } from './metrics.js'
import {
  type DatabaseErrorCode,
  type DatabaseErrorResponse,
  POSTGRES_ERROR_FIELDS,
  type PostgresErrorFields,
} from './protocol.js'

export type { DatabaseErrorCode, DatabaseErrorResponse } from './protocol.js'

export type ErrorContext = {
  requestId?: string
  operationName?: string
  destination?: string
  lockId?: string
}

export class DatabaseWattError extends Error {
  readonly code: DatabaseErrorCode
  readonly connectionDiscarded?: boolean
  readonly sqlState?: string

  constructor(
    code: DatabaseErrorCode,
    message: string,
    options: { connectionDiscarded?: boolean; cause?: unknown; sqlState?: string } = {}
  ) {
    super(message, { cause: options.cause })
    this.name = 'DatabaseWattError'
    this.code = code
    this.connectionDiscarded = options.connectionDiscarded
    this.sqlState = options.sqlState
  }
}

/** The limit that made the Database application refuse a request with BUSY. */
export type BusyReason =
  | 'max_active_pools'
  | 'global_acquire_queue'
  | 'destination_acquire_queue'
  | 'global_connection_budget'
  | 'destination_not_owned'

/**
 * A BUSY refusal: the request was turned away before running anything, so the caller can
 * retry it. Each refusal is counted by reason (database_watt_rejected_requests).
 */
export function busyError(reason: BusyReason, message: string): DatabaseWattError {
  countBusyRejection(reason)
  return new DatabaseWattError('BUSY', message)
}

/** The request was cancelled by the storage worker that sent it. */
export function cancelledError(): DatabaseWattError {
  return new DatabaseWattError('CANCELLED', 'The request was cancelled')
}

// Stack traces are not part of the wire contract: the storage worker keeps the stack
// of its own call site, which is what points at the failing operation.
export function toErrorResponse(error: unknown, context: ErrorContext = {}): DatabaseErrorResponse {
  if (error instanceof DatabaseWattError) {
    return {
      code: error.code,
      message: error.message,
      requestId: context.requestId,
      operationName: context.operationName,
      destination: context.destination,
      lockId: context.lockId,
      sqlState: error.sqlState,
      connectionDiscarded: error.connectionDiscarded,
    }
  }

  if (error instanceof DatabaseError) {
    return {
      code: 'POSTGRES_ERROR',
      message: error.message,
      requestId: context.requestId,
      operationName: context.operationName,
      destination: context.destination,
      lockId: context.lockId,
      sqlState: error.code,
      postgres: pickPostgresErrorFields(error),
    }
  }

  if (isConnectionTimeoutError(error)) {
    const connectionError = error as Error
    return {
      code: 'CONNECTION_TIMEOUT',
      message: connectionError.message,
      requestId: context.requestId,
      operationName: context.operationName,
      destination: context.destination,
      lockId: context.lockId,
    }
  }

  const fallback = error instanceof Error ? error : new Error(String(error))
  return {
    code: 'MESSAGING_ERROR',
    message: fallback.message,
    requestId: context.requestId,
    operationName: context.operationName,
    destination: context.destination,
    lockId: context.lockId,
  }
}

export function isErrorResponse(value: unknown): boolean {
  return Boolean(value && typeof value === 'object' && 'code' in value && 'message' in value)
}

function pickPostgresErrorFields(error: DatabaseError): PostgresErrorFields | undefined {
  let fields: PostgresErrorFields | undefined
  for (const field of POSTGRES_ERROR_FIELDS) {
    const value = error[field]
    if (typeof value === 'string') {
      fields ??= {}
      fields[field] = value
    }
  }
  return fields
}
