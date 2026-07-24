import { describe, expect, it } from 'vitest'
import { DatabaseWattError } from './errors.js'
import {
  validateCancelRequest,
  validateLockRequestEnvelope,
  validateNonLockRequestEnvelope,
  validateQueryEnvelope,
} from './validation.js'

describe('database request validation', () => {
  it('accepts valid stateless query envelopes', () => {
    const request = {
      destination: 'tenant-a',
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

  it('rejects missing lock identifiers for lock-bound requests', () => {
    expect(() => validateLockRequestEnvelope({ sql: 'SELECT 1' })).toThrow(/lockId/)
  })

  it('rejects invalid SQL and parameter payloads', () => {
    expect(() => validateQueryEnvelope({ sql: 1 })).toThrow(/sql/)
    expect(() => validateQueryEnvelope({ sql: 'SELECT 1', values: 'bad' })).toThrow(/values/)
  })

  it('validates cancellation requests', () => {
    expect(() => validateCancelRequest({ requestId: 'req-1' })).not.toThrow()
    expect(() => validateCancelRequest({})).toThrow(/requestId/)
  })
})
