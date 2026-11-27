import { metrics } from '@opentelemetry/api'
import { DatabaseError } from 'pg'
import { describe, expect, it, vi } from 'vitest'
import { DatabaseWattError, isErrorResponse, toErrorResponse } from './errors.js'

describe('database error contract', () => {
  it('counts each BUSY refusal by the limit that refused it', async () => {
    vi.resetModules()
    const add = vi.fn()
    const getMeter = vi
      .spyOn(metrics, 'getMeter')
      .mockReturnValue({ createCounter: () => ({ add }) } as never)

    const { busyError } = await import('./errors.js')
    const error = busyError('global_acquire_queue', 'Global acquisition queue is full')
    busyError('destination_not_owned', 'Destination belongs to Database worker database:1')

    expect(error).toMatchObject({ code: 'BUSY', message: 'Global acquisition queue is full' })
    expect(add.mock.calls).toEqual([
      [1, { reason: 'global_acquire_queue' }],
      [1, { reason: 'destination_not_owned' }],
    ])
    getMeter.mockRestore()
  })

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

  it('carries the PostgreSQL error fields Storage maps on, without stack traces', () => {
    const error = new DatabaseError('violates foreign key constraint', 1, 'error')
    error.code = '23503'
    error.constraint = 's3_multipart_uploads_bucket_id_fkey'
    error.detail = 'Key (id)=(bucket) is still referenced from table "s3_multipart_uploads".'
    error.where = 'SQL function "x"'

    const response = toErrorResponse(error)

    expect(response.postgres).toEqual({
      constraint: 's3_multipart_uploads_bucket_id_fkey',
      detail: 'Key (id)=(bucket) is still referenced from table "s3_multipart_uploads".',
      where: 'SQL function "x"',
    })
    expect(response).not.toHaveProperty('stack')
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
