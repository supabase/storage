import { Buffer } from 'node:buffer'
import type { DatabaseConfig } from './config.js'
import { DatabaseWattError } from './errors.js'

export function validateNonLockRequestEnvelope(request: unknown, config: DatabaseConfig): void {
  validateBaseEnvelope(request, config)
  const destination = (request as { destination?: unknown }).destination
  if (typeof destination !== 'string' || destination.length === 0) {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'destination must be a non-empty string')
  }
}

export function validateLockRequestEnvelope(request: unknown, config: DatabaseConfig): void {
  validateBaseEnvelope(request, config)
  const lockId = (request as { lockId?: unknown }).lockId
  if (typeof lockId !== 'string' || lockId.length === 0) {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'lockId must be a non-empty string')
  }
}

export function validateQueryEnvelope(request: unknown, config: DatabaseConfig): void {
  const sql = (request as { sql?: unknown }).sql
  const values = (request as { values?: unknown }).values

  if (typeof sql !== 'string') {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'sql must be a string')
  }

  if (Buffer.byteLength(sql) > config.maxSqlBytes) {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'sql exceeds maxSqlBytes')
  }

  if (values !== undefined && !Array.isArray(values)) {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'values must be an array when present')
  }

  if (Array.isArray(values) && values.length > config.maxParameterCount) {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'values exceeds maxParameterCount')
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

function validateBaseEnvelope(request: unknown, config: DatabaseConfig): void {
  if (!request || typeof request !== 'object') {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'request must be an object')
  }

  const serializedSize = Buffer.byteLength(JSON.stringify(request))
  if (serializedSize > config.maxSerializedRequestBytes) {
    throw new DatabaseWattError('PROTOCOL_ERROR', 'request exceeds maxSerializedRequestBytes')
  }

  const requestId = (request as { requestId?: unknown }).requestId
  if (requestId !== undefined) {
    if (typeof requestId !== 'string' || requestId.length > config.maxRequestIdLength) {
      throw new DatabaseWattError('PROTOCOL_ERROR', 'requestId is invalid')
    }
  }

  const operationName = (request as { operationName?: unknown }).operationName
  if (operationName !== undefined) {
    if (typeof operationName !== 'string' || operationName.length > config.maxOperationNameLength) {
      throw new DatabaseWattError('PROTOCOL_ERROR', 'operationName is invalid')
    }
  }
}
