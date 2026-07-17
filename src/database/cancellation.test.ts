import { describe, expect, it, vi } from 'vitest'
import { CancellationRegistry } from './cancellation.js'

describe('database cancellation registry', () => {
  it('tracks missing and completed requests', async () => {
    const registry = new CancellationRegistry()

    expect(await registry.cancel('missing')).toEqual({ cancelled: false })

    registry.start('req-1', { cancelled: false })
    registry.finish('req-1')

    expect(await registry.cancel('req-1')).toEqual({ cancelled: false })
  })

  it('marks an in-flight operation as cancelled', async () => {
    const registry = new CancellationRegistry()
    const operation = { cancelled: false, lockId: 'lock-a' }

    registry.start('req-1', operation)

    expect(await registry.cancel('req-1', 'lock-a')).toEqual({ cancelled: true })
    expect(operation.cancelled).toBe(true)
  })

  it('does not cancel operations for a different lock', async () => {
    const registry = new CancellationRegistry()
    const operation = { cancelled: false, lockId: 'lock-a' }

    registry.start('req-1', operation)

    expect(await registry.cancel('req-1', 'lock-b')).toEqual({ cancelled: false })
    expect(operation.cancelled).toBe(false)
  })

  it('sends PostgreSQL cancel when client backend identifiers are present', async () => {
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

    const { CancellationRegistry: MockedCancellationRegistry } = await import('./cancellation.js')
    const registry = new MockedCancellationRegistry()
    registry.start('req-1', { cancelled: false })
    registry.setClient('req-1', {
      connectionParameters: { host: 'localhost', port: 5432 },
      processID: 1,
      secretKey: 2,
    } as never)

    expect(await registry.cancel('req-1')).toEqual({ cancelled: true })
    expect(cancel).toHaveBeenCalledWith(1, 2)
    expect(end).toHaveBeenCalled()
    vi.doUnmock('pg/lib/connection')
  })
})
