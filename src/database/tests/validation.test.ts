import { describe, expect, it } from 'vitest'
import { readConfig } from '../config.js'
import { DatabaseWattError } from '../errors.js'
import {
  validateCancelRequest,
  validateLockRequestEnvelope,
  validateNonLockRequestEnvelope,
  validateQueryEnvelope,
} from '../validation.js'

describe('database request validation', () => {
  const config = readConfig({
    DATABASE_WATT_MAX_OPERATION_NAME_LENGTH: '8',
    DATABASE_WATT_MAX_PARAMETER_COUNT: '2',
    DATABASE_WATT_MAX_REQUEST_ID_LENGTH: '8',
    DATABASE_WATT_MAX_SERIALIZED_REQUEST_BYTES: '200',
    DATABASE_WATT_MAX_SQL_BYTES: '10',
  })

  it('accepts valid stateless query envelopes', () => {
    const request = {
      destination: 'tenant-a',
      operationName: 'select',
      requestId: 'req-1',
      sql: 'SELECT 1',
      values: [1],
    }

    expect(() => validateNonLockRequestEnvelope(request, config)).not.toThrow()
    expect(() => validateQueryEnvelope(request, config)).not.toThrow()
  })

  it('rejects missing destinations before query execution', () => {
    expect(() => validateNonLockRequestEnvelope({ sql: 'SELECT 1' }, config)).toThrow(
      DatabaseWattError
    )
  })

  it('rejects missing lock identifiers for lock-bound requests', () => {
    expect(() => validateLockRequestEnvelope({ sql: 'SELECT 1' }, config)).toThrow(
      /lockId/
    )
  })

  it('rejects invalid SQL and parameter payloads', () => {
    expect(() => validateQueryEnvelope({ sql: 1 }, config)).toThrow(/sql/)
    expect(() => validateQueryEnvelope({ sql: 'SELECT 1', values: 'bad' }, config)).toThrow(
      /values/
    )
    expect(() => validateQueryEnvelope({ sql: 'SELECT 1', values: [1, 2, 3] }, config)).toThrow(
      /maxParameterCount/
    )
    expect(() => validateQueryEnvelope({ sql: 'SELECT 1234567890' }, config)).toThrow(
      /maxSqlBytes/
    )
  })

  it('bounds metadata and serialized request size', () => {
    expect(() =>
      validateNonLockRequestEnvelope({ destination: 'a', requestId: 'too-long-request-id' }, config)
    ).toThrow(/requestId/)

    expect(() =>
      validateNonLockRequestEnvelope({ destination: 'a', operationName: 'too-long-operation' }, config)
    ).toThrow(/operationName/)

    expect(() =>
      validateNonLockRequestEnvelope({ destination: 'a', padding: 'x'.repeat(500) }, config)
    ).toThrow(/maxSerializedRequestBytes/)
  })

  it('validates cancellation requests', () => {
    expect(() => validateCancelRequest({ requestId: 'req-1' })).not.toThrow()
    expect(() => validateCancelRequest({})).toThrow(/requestId/)
  })
})
