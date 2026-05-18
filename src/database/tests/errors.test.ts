import { DatabaseError } from 'pg'
import { describe, expect, it } from 'vitest'
import { DatabaseWattError, isErrorResponse, toErrorResponse } from '../errors.js'

describe('database error contract', () => {
  it('serializes DatabaseWattError with safe context', () => {
    const response = toErrorResponse(new DatabaseWattError('BUSY', 'queue full'), {
      destination: 'tenant-a',
      operationName: 'op',
      requestId: 'req-1',
    })

    expect(response).toMatchObject({
      code: 'BUSY',
      destination: 'tenant-a',
      message: 'queue full',
      operationName: 'op',
      requestId: 'req-1',
    })
  })

  it('maps PostgreSQL errors to POSTGRES_ERROR and SQLSTATE', () => {
    const error = new DatabaseError('duplicate key', 1, 'error')
    error.code = '23505'

    const response = toErrorResponse(error, { lockId: 'lock-a' })

    expect(response).toMatchObject({
      code: 'POSTGRES_ERROR',
      lockId: 'lock-a',
      message: 'duplicate key',
      sqlState: '23505',
    })
  })

  it('maps unknown errors to MESSAGING_ERROR', () => {
    const response = toErrorResponse(new Error('transport failed'))

    expect(response).toMatchObject({
      code: 'MESSAGING_ERROR',
      message: 'transport failed',
    })
    expect(isErrorResponse(response)).toBe(true)
  })
})
