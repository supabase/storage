import { DatabaseError } from 'pg'
import type { DatabaseErrorCode, DatabaseErrorResponse } from './protocol.js'

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
      stack: error.stack,
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
      stack: error.stack,
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
      stack: connectionError.stack,
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
    stack: fallback.stack,
  }
}

export function isErrorResponse(value: unknown): boolean {
  return Boolean(value && typeof value === 'object' && 'code' in value && 'message' in value)
}

function isConnectionTimeoutError(error: unknown): boolean {
  return (
    error instanceof Error &&
    (error.message === 'timeout expired' ||
      error.message === 'timeout exceeded when trying to connect')
  )
}
