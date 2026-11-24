import { EventEmitter } from 'node:events'
import { DatabaseError, Pool } from 'pg'
import { describe, expect, it, vi } from 'vitest'
import {
  attachPoolErrorHandler,
  checkoutClient,
  createAbortError,
  isConnectionStateError,
  markClientDisposable,
  shouldDisposeClient,
} from './pool-errors'

function createDatabaseError(code: string | undefined, message = 'connection failed') {
  const error = new DatabaseError(message, message.length, 'error')
  error.code = code
  return error
}

describe('PostgreSQL pool errors', () => {
  it('attaches caller-owned idle client error handling', () => {
    const pool = new EventEmitter() as Pool
    const onError = vi.fn()
    const error = new Error('idle client failed')

    expect(attachPoolErrorHandler(pool, onError)).toBe(pool)

    pool.emit('error', error)
    expect(onError).toHaveBeenCalledWith(error)
  })

  it.each(['08000', '08003', '08006', '08P01'])(
    'recognizes DatabaseError SQLSTATE %s as a connection-state error',
    (code) => {
      expect(isConnectionStateError(createDatabaseError(code))).toBe(true)
    }
  )

  it.each([
    'received invalid response: 58',
    'Received unexpected authentication request',
    'Unknown authenticationOk message type',
  ])('recognizes PostgreSQL protocol failure %s', (message) => {
    expect(isConnectionStateError(new Error(message))).toBe(true)
  })

  it('does not treat unrelated or plain coded errors as SQLSTATE failures', () => {
    expect(isConnectionStateError(new Error('duplicate key'))).toBe(false)
    expect(isConnectionStateError(Object.assign(new Error('socket'), { code: '08006' }))).toBe(
      false
    )
    expect(isConnectionStateError({ code: '08006' })).toBe(false)
  })

  it('marks exact query errors for disposal and always disposes abort errors', () => {
    const queryError = new Error('bad connection')
    expect(shouldDisposeClient(queryError)).toBe(false)

    markClientDisposable(queryError)

    expect(shouldDisposeClient(queryError)).toBe(true)
    expect(shouldDisposeClient(createAbortError())).toBe(true)
  })

  it('creates a fresh AbortError for each rejected operation', () => {
    const first = createAbortError()
    const second = createAbortError()

    expect(first).toMatchObject({
      name: 'AbortError',
      code: 'ABORT_ERR',
      message: 'Query was aborted',
    })
    expect(second).not.toBe(first)
  })
})

describe('checkoutClient', () => {
  class SyncErrorClient extends EventEmitter {
    _queryable = true
    _ending = false
    connect(cb: (err?: Error) => void) {
      process.nextTick(() => {
        cb()
        this.emit('error', new Error('boom'))
      })
    }
    end(cb?: () => void) {
      cb?.()
    }
    ref() {}
    unref() {}
  }

  it('delivers an error emitted in the same stack as the checkout callback and releases once', async () => {
    const pool = new Pool({ Client: SyncErrorClient as never })
    pool.on('error', () => undefined)
    const onError = vi.fn()

    try {
      const checkout = await checkoutClient(pool, onError)

      expect(onError).toHaveBeenCalledWith(expect.objectContaining({ message: 'boom' }))
      expect(checkout.error).toMatchObject({ message: 'boom' })
      expect(() => checkout.throwIfErrored()).toThrow('boom')
      expect(shouldDisposeClient(checkout.error)).toBe(true)

      checkout.release()
      checkout.release()

      checkout.client.emit('error', new Error('after release'))
      expect(onError).toHaveBeenCalledTimes(1)
      expect(pool.totalCount).toBe(0)
    } finally {
      await pool.end()
    }
  })

  it('rejects when the checkout fails and preserves async caller stack frames', async () => {
    class HangClient extends EventEmitter {
      connect() {}
      end(cb?: () => void) {
        cb?.()
      }
      isConnected() {
        return false
      }
      ref() {}
      unref() {}
    }

    const pool = new Pool({ Client: HangClient as never, connectionTimeoutMillis: 15, max: 1 })
    pool.on('error', () => undefined)
    // Fill the single slot with a hanging connect so the second waiter hits the queue timer.
    pool.connect(() => undefined)

    async function invokeCheckout() {
      return await checkoutClient(pool)
    }

    const error = await invokeCheckout().catch((e: unknown) => e)
    expect(error).toBeInstanceOf(Error)
    expect((error as Error).message).toBe('timeout exceeded when trying to connect')
    expect((error as Error).stack).toContain('invokeCheckout')
  })

  it('rejects with an Error when checkout yields neither error nor client', async () => {
    const pool = {
      connect: (cb: (err?: Error, client?: unknown) => void) => cb(undefined, undefined),
    } as unknown as Pool

    await expect(checkoutClient(pool)).rejects.toThrow('Pool checkout returned no client')
  })
})
