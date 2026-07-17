import { describe, expect, it, vi } from 'vitest'
import { readConfig } from './config.js'
import { DatabaseWattError } from './errors.js'
import { LockRegistry } from './locks.js'
import type { DestinationConfig } from './types.js'

type FakeClient = {
  query: ReturnType<typeof vi.fn>
  release: ReturnType<typeof vi.fn>
}

function createDestination(): DestinationConfig {
  return {
    connectionString: 'postgres://example',
    id: 'tenant-a',
    isExternalPool: false,
    maxConnections: 10,
  }
}

function createClient(): FakeClient {
  return {
    query: vi.fn().mockResolvedValue({ rowCount: 1, rows: [{ ok: true }] }),
    release: vi.fn(),
  }
}

describe('database lock registry', () => {
  it('runs locked queries on the pinned client', async () => {
    const locks = new LockRegistry(readConfig())
    const client = createClient()
    const lockId = locks.create(createDestination(), client as never)

    const result = await locks.query(lockId, 'SELECT 1', [1])

    expect(result).toEqual({ rowCount: 1, rows: [{ ok: true }] })
    expect(client.query).toHaveBeenCalledWith('SELECT 1', [1])
    expect(client.release).not.toHaveBeenCalled()

    await locks.release(lockId)
    expect(client.release).toHaveBeenCalledTimes(1)
    await locks.close()
  })

  it('rejects reuse after release', async () => {
    const locks = new LockRegistry(readConfig())
    const client = createClient()
    const lockId = locks.create(createDestination(), client as never)

    await locks.release(lockId)

    await expect(locks.query(lockId, 'SELECT 1')).rejects.toMatchObject({
      code: 'PROTOCOL_ERROR',
      message: 'Unknown lock ID',
    })
    await locks.close()
  })

  it('does not release transaction locks through release()', async () => {
    const locks = new LockRegistry(readConfig())
    const client = createClient()
    const lockId = locks.create(createDestination(), client as never, true)

    await expect(locks.release(lockId)).rejects.toBeInstanceOf(DatabaseWattError)
    expect(client.release).not.toHaveBeenCalled()

    await locks.rollback(lockId)
    expect(client.query).toHaveBeenCalledWith('ROLLBACK', undefined)
    expect(client.release).toHaveBeenCalledTimes(1)
    await locks.close()
  })

  it('commits transactions and removes terminal locks', async () => {
    const locks = new LockRegistry(readConfig())
    const client = createClient()
    const lockId = locks.create(createDestination(), client as never, true)

    await locks.commit(lockId)

    expect(client.query).toHaveBeenCalledWith('COMMIT', undefined)
    expect(client.release).toHaveBeenCalledTimes(1)
    await expect(locks.rollback(lockId)).rejects.toMatchObject({ code: 'PROTOCOL_ERROR' })
    await locks.close()
  })

  it('serializes lock-bound work for the same lock', async () => {
    const locks = new LockRegistry(readConfig())
    const client = createClient()
    const order: string[] = []
    let releaseFirstQuery!: () => void

    client.query.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          order.push('first-start')
          releaseFirstQuery = () => {
            order.push('first-end')
            resolve({ rowCount: 1, rows: [] })
          }
        })
    )
    client.query.mockImplementationOnce(async () => {
      order.push('second-start')
      return { rowCount: 1, rows: [] }
    })

    const lockId = locks.create(createDestination(), client as never)
    const first = locks.query(lockId, 'SELECT 1')
    const second = locks.query(lockId, 'SELECT 2')

    await new Promise((resolve) => setImmediate(resolve))
    expect(order).toEqual(['first-start'])

    releaseFirstQuery()
    await Promise.all([first, second])

    expect(order).toEqual(['first-start', 'first-end', 'second-start'])
    await locks.release(lockId)
    await locks.close()
  })
})
