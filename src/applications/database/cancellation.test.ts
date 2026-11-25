import { describe, expect, it, vi } from 'vitest'
import { CancellationRegistry, releaseErrorFor } from './cancellation.js'

describe('database cancellation registry', () => {
  it('tracks missing and completed requests', async () => {
    const registry = new CancellationRegistry()

    expect(await registry.cancel('missing')).toEqual({ cancelled: false })

    registry.start('req-1')
    registry.finish('req-1')

    expect(await registry.cancel('req-1')).toEqual({ cancelled: false })
  })

  it('aborts a cancelled request so its next statement does not start', async () => {
    const registry = new CancellationRegistry()
    const operation = registry.start('req-1', 'lock-a')
    const client = {} as never

    operation.runOn(client)
    expect(await registry.cancel('req-1', 'lock-a')).toEqual({ cancelled: true })

    expect(operation.signal.aborted).toBe(true)
    expect(() => operation.runOn(client)).toThrow(
      expect.objectContaining({ code: 'CANCELLED', message: 'The request was cancelled' })
    )
  })

  it('does not cancel operations for a different lock', async () => {
    const registry = new CancellationRegistry()
    const operation = registry.start('req-1', 'lock-a')

    expect(await registry.cancel('req-1', 'lock-b')).toEqual({ cancelled: false })
    expect(operation.cancelled).toBe(false)
  })

  it('tracks a request without an ID only for itself', async () => {
    const registry = new CancellationRegistry()
    const operation = registry.start(undefined)

    expect(operation.cancelled).toBe(false)
    expect(() => operation.runOn({} as never)).not.toThrow()
  })

  it('sends no PostgreSQL cancel for a request whose statement has not started', async () => {
    const registry = new CancellationRegistry()
    const client = { processID: 1, secretKey: 2 }
    registry.start('req-1')

    expect(await registry.cancel('req-1')).toEqual({ cancelled: true })
    expect(releaseErrorFor(client)).toBeUndefined()
  })

  it('releases connections untouched by a cancel as they are', () => {
    const failure = new Error('Connection terminated unexpectedly')

    expect(releaseErrorFor({})).toBeUndefined()
    expect(releaseErrorFor({}, failure)).toBe(failure)
  })

  it('sends PostgreSQL cancel to the running statement and closes its connection after', async () => {
    vi.resetModules()

    const cancel = vi.fn()
    const end = vi.fn()
    const unref = vi.fn()
    vi.doMock('pg/lib/connection', () => ({
      default: class MockPgConnection {
        private callbacks = new Map<string, () => void>()
        cancel = cancel
        end = end
        unref = unref
        on(event: string, callback: () => void) {
          this.callbacks.set(event, callback)
        }
        connect() {
          setImmediate(() => {
            this.callbacks.get('connect')?.()
            this.callbacks.get('end')?.()
          })
        }
      },
    }))

    const mocked = await import('./cancellation.js')
    const registry = new mocked.CancellationRegistry()
    const client = {
      connectionParameters: { host: 'localhost', port: 5432 },
      processID: 1,
      secretKey: 2,
    }
    registry.start('req-1').runOn(client as never)

    expect(await registry.cancel('req-1')).toEqual({ cancelled: true })
    expect(cancel).toHaveBeenCalledWith(1, 2)
    expect(end).toHaveBeenCalled()
    // A late cancel must not reach the next request's statement on this backend.
    expect(mocked.releaseErrorFor(client)).toEqual(
      new Error('A cancel request was sent on this connection')
    )
    vi.doUnmock('pg/lib/connection')
  })
})
