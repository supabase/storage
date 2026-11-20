import { DatabaseWattError } from './errors.js'

export function validateNonLockRequestEnvelope(request: unknown): void {
  validateBaseEnvelope(request)
  const destination = (request as { destination?: unknown }).destination
  if (!destination || typeof destination !== 'object') {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'destination must be an object')
  }

  const { connectionString, id, isExternalPool, maxConnections } = destination as Record<
    string,
    unknown
  >

  if (typeof id !== 'string' || id.length === 0) {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'destination.id must be a non-empty string')
  }

  if (typeof connectionString !== 'string' || connectionString.length === 0) {
    throw new DatabaseWattError(
      'PROTOCOL_ERROR',
      'destination.connectionString must be a non-empty string'
    )
  }

  if (typeof isExternalPool !== 'boolean') {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'destination.isExternalPool must be a boolean')
  }

  if (!Number.isInteger(maxConnections) || (maxConnections as number) < 0) {
    throw new DatabaseWattError(
      'PROTOCOL_ERROR',
      'destination.maxConnections must be a non-negative integer'
    )
  }
}

export function validateLockRequestEnvelope(request: unknown): void {
  validateBaseEnvelope(request)
  const lockId = (request as { lockId?: unknown }).lockId
  if (typeof lockId !== 'string' || lockId.length === 0) {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'lockId must be a non-empty string')
  }
}

export function validateQueryEnvelope(request: unknown): void {
  const sql = (request as { sql?: unknown }).sql
  const values = (request as { values?: unknown }).values

  if (typeof sql !== 'string') {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'sql must be a string')
  }

  if (values !== undefined && !Array.isArray(values)) {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'values must be an array when present')
  }
}

export function validateCancelRequest(request: unknown): void {
  if (!request || typeof request !== 'object') {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'request must be an object')
  }

  const requestId = (request as { requestId?: unknown }).requestId
  if (typeof requestId !== 'string' || requestId.length === 0) {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'requestId must be a non-empty string')
  }
}

function validateBaseEnvelope(request: unknown): void {
  if (!request || typeof request !== 'object') {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'request must be an object')
  }
}
