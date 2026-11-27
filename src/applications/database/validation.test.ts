import { describe, expect, it } from 'vitest'
import { DatabaseWattError } from './errors.js'
import {
  validateBatchEnvelope,
  validateCancelRequest,
  validateLockRequestEnvelope,
  validateNonLockRequestEnvelope,
  validateQueryEnvelope,
} from './validation.js'

describe('database request validation', () => {
  it('accepts valid stateless query envelopes', () => {
    const request = {
      destination: createDestination(),
      operationName: 'select',
      requestId: 'req-1',
      sql: 'SELECT 1',
      values: [1],
    }

    expect(() => validateNonLockRequestEnvelope(request)).not.toThrow()
    expect(() => validateQueryEnvelope(request)).not.toThrow()
  })

  it('rejects missing destinations before query execution', () => {
    expect(() => validateNonLockRequestEnvelope({ sql: 'SELECT 1' })).toThrow(DatabaseWattError)
  })

  it('rejects incomplete physical pool destinations', () => {
    expect(() =>
      validateNonLockRequestEnvelope({
        destination: { id: 'tenant-a' },
        sql: 'SELECT 1',
      })
    ).toThrow(/connectionString/)
  })

  it('rejects missing lock identifiers for lock-bound requests', () => {
    expect(() => validateLockRequestEnvelope({ sql: 'SELECT 1' })).toThrow(/lockId/)
  })

  it('rejects invalid SQL and parameter payloads', () => {
    expect(() => validateQueryEnvelope({ sql: 1 })).toThrow(/sql/)
    expect(() => validateQueryEnvelope({ sql: 'SELECT 1', values: 'bad' })).toThrow(/values/)
  })

  it('accepts holdOpen only as a boolean', () => {
    const batch = {
      commit: false,
      destination: createDestination(),
      statements: [{ sql: 'SELECT pg_try_advisory_xact_lock(1)' }],
    }

    expect(() => validateBatchEnvelope({ ...batch, holdOpen: true })).not.toThrow()
    expect(() => validateBatchEnvelope({ ...batch, holdOpen: 'yes' })).toThrow(/holdOpen/)
  })

  it('validates cancellation requests', () => {
    expect(() => validateCancelRequest({ requestId: 'req-1' })).not.toThrow()
    expect(() => validateCancelRequest({})).toThrow(/requestId/)
  })
})

function createDestination() {
  return {
    connectionString: 'postgres://tenant-db',
    id: 'tenant-a',
    isExternalPool: false,
    maxConnections: 10,
  }
}
