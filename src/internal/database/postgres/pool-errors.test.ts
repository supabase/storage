import { EventEmitter } from 'node:events'
import type { Pool } from 'pg'
import { describe, expect, it, vi } from 'vitest'
import { attachPoolErrorHandler, isConnectionStateError } from './pool-errors'

describe('PostgreSQL pool errors', () => {
  it('attaches caller-owned idle client error handling', () => {
    const pool = new EventEmitter() as Pool
    const onError = vi.fn()
    const error = new Error('idle client failed')

    expect(attachPoolErrorHandler(pool, onError)).toBe(pool)

    pool.emit('error', error)
    expect(onError).toHaveBeenCalledWith(error)
  })

  it.each([
    '08000',
    '08003',
    '08006',
    '08P01',
  ])('recognizes SQLSTATE %s as a connection-state error', (code) => {
    const error = Object.assign(new Error('connection failed'), { code })

    expect(isConnectionStateError(error)).toBe(true)
  })

  it.each([
    'received invalid response: 58',
    'Received unexpected authentication request',
    'Unknown authenticationOk message type',
  ])('recognizes PostgreSQL protocol failure %s', (message) => {
    expect(isConnectionStateError(new Error(message))).toBe(true)
  })

  it('rejects unrelated and non-error values', () => {
    expect(isConnectionStateError(new Error('duplicate key'))).toBe(false)
    expect(isConnectionStateError({ code: '08006' })).toBe(false)
  })
})
