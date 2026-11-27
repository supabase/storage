import hyperid from 'hyperid'
import { describe, expect, it, vi } from 'vitest'
import { getConfig } from '../../config.js'
import { DatabaseWattError } from './errors.js'
import { HELD_LOCK_MAX_LIFETIME_MS, LockRegistry } from './locks.js'
import type { DatabasePoolTarget } from './protocol.js'

type FakeClient = {
  query: ReturnType<typeof vi.fn>
  release: ReturnType<typeof vi.fn>
}

function createDestination(): DatabasePoolTarget {
  return {
    connectionString: 'postgres://example',
    id: 'tenant-a',
    isExternalPool: false,
    maxConnections: 10,
  }
}

// What pg returns for a statement run in array row mode with raw (unparsed) values.
const rawResult = { rowCount: 1, rows: [['t']], fields: [{ name: 'ok', dataTypeID: 16 }] }

// The messaging channel that owns the locks of these tests.
const owner = {}

function createClient(): FakeClient {
  return {
    query: vi.fn().mockResolvedValue(rawResult),
    release: vi.fn(),
  }
}

describe('database lock registry', () => {
  it('runs locked queries on the pinned client', async () => {
    const locks = new LockRegistry(getConfig())
    const client = createClient()
    const lockId = locks.create(createDestination(), client as never, false, owner)

    const result = await locks.query(lockId, 'SELECT 1', [1], owner)

    // Rows go back as raw text: the storage worker parses them.
    expect(result).toEqual({ rowCount: 1, rows: [['t']], fields: [{ name: 'ok', dataTypeID: 16 }] })
    expect(client.query).toHaveBeenCalledWith(
      expect.objectContaining({ text: 'SELECT 1', values: [1], rowMode: 'array' })
    )
    expect(client.release).not.toHaveBeenCalled()

    await locks.release(lockId, owner)
    expect(client.release).toHaveBeenCalledTimes(1)
    await locks.close()
  })

  it('rejects reuse after release', async () => {
    const locks = new LockRegistry(getConfig())
    const client = createClient()
    const lockId = locks.create(createDestination(), client as never, false, owner)

    await locks.release(lockId, owner)

    await expect(locks.query(lockId, 'SELECT 1', undefined, owner)).rejects.toMatchObject({
      code: 'PROTOCOL_ERROR',
      message: 'Unknown lock ID',
    })
    await locks.close()
  })

  it('does not release transaction locks through release()', async () => {
    const locks = new LockRegistry(getConfig())
    const client = createClient()
    const lockId = locks.create(createDestination(), client as never, true, owner)

    await expect(locks.release(lockId, owner)).rejects.toBeInstanceOf(DatabaseWattError)
    expect(client.release).not.toHaveBeenCalled()

    await locks.rollback(lockId, owner)
    expect(client.query).toHaveBeenCalledWith('ROLLBACK')
    expect(client.release).toHaveBeenCalledTimes(1)
    await locks.close()
  })

  it('commits transactions and removes terminal locks', async () => {
    const locks = new LockRegistry(getConfig())
    const client = createClient()
    const lockId = locks.create(createDestination(), client as never, true, owner)

    await locks.commit(lockId, owner)

    expect(client.query).toHaveBeenCalledWith('COMMIT')
    expect(client.release).toHaveBeenCalledTimes(1)
    await expect(locks.rollback(lockId, owner)).rejects.toMatchObject({ code: 'PROTOCOL_ERROR' })
    await locks.close()
  })

  it('serializes lock-bound work for the same lock', async () => {
    const locks = new LockRegistry(getConfig())
    const client = createClient()
    const order: string[] = []
    let releaseFirstQuery!: () => void

    client.query.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          order.push('first-start')
          releaseFirstQuery = () => {
            order.push('first-end')
            resolve({ rowCount: 1, rows: [], fields: [] })
          }
        })
    )
    client.query.mockImplementationOnce(async () => {
      order.push('second-start')
      return { rowCount: 1, rows: [], fields: [] }
    })

    const lockId = locks.create(createDestination(), client as never, false, owner)
    const first = locks.query(lockId, 'SELECT 1', undefined, owner)
    const second = locks.query(lockId, 'SELECT 2', undefined, owner)

    await new Promise((resolve) => setImmediate(resolve))
    expect(order).toEqual(['first-start'])

    releaseFirstQuery()
    await Promise.all([first, second])

    expect(order).toEqual(['first-start', 'first-end', 'second-start'])
    await locks.release(lockId, owner)
    await locks.close()
  })

  it('rolls back and releases only the locks of a closed owner', async () => {
    const locks = new LockRegistry(getConfig())
    const ownerA = {}
    const ownerB = {}
    const clientA = createClient()
    const clientB = createClient()
    const lockA = locks.create(createDestination(), clientA as never, true, ownerA)
    const lockB = locks.create(createDestination(), clientB as never, true, ownerB)

    const released = await locks.releaseOwner(ownerA)

    expect(released).toBe(1)
    expect(clientA.query).toHaveBeenCalledWith('ROLLBACK')
    expect(clientA.release).toHaveBeenCalledWith(undefined)
    expect(locks.has(lockA)).toBe(false)
    expect(locks.has(lockB)).toBe(true)
    expect(clientB.release).not.toHaveBeenCalled()

    await locks.close()
  })

  it('disposes the connection when the owner cleanup rollback fails', async () => {
    const locks = new LockRegistry(getConfig())
    const client = createClient()
    client.query.mockRejectedValueOnce(new Error('Connection terminated unexpectedly'))
    locks.create(createDestination(), client as never, true, owner)

    await locks.releaseOwner(owner)

    expect(client.release).toHaveBeenCalledWith(expect.any(Error))
    await locks.close()
  })

  it('only lets the owner of a lock use it', async () => {
    const locks = new LockRegistry(getConfig())
    const client = createClient()
    const lockId = locks.create(createDestination(), client as never, true, owner)

    for (const caller of [{}, undefined]) {
      await expect(locks.query(lockId, 'SELECT 1', undefined, caller)).rejects.toMatchObject({
        code: 'PROTOCOL_ERROR',
        message: 'Unknown lock ID',
      })
      await expect(locks.commit(lockId, caller)).rejects.toMatchObject({ code: 'PROTOCOL_ERROR' })
      await expect(locks.rollback(lockId, caller)).rejects.toMatchObject({
        code: 'PROTOCOL_ERROR',
      })
    }
    expect(locks.has(lockId)).toBe(true)
    expect(client.query).not.toHaveBeenCalled()

    await locks.query(lockId, 'SELECT 1', undefined, owner)
    await locks.commit(lockId, owner)
    expect(client.release).toHaveBeenCalledTimes(1)
    await locks.close()
  })

  it('creates distinct lock IDs that differ between registries', async () => {
    const first = new LockRegistry(getConfig())
    const second = new LockRegistry(getConfig())

    const ids = [
      first.create(createDestination(), createClient() as never, false, owner),
      first.create(createDestination(), createClient() as never, false, owner),
      second.create(createDestination(), createClient() as never, false, owner),
    ]

    expect(new Set(ids).size).toBe(3)
    // A restarted Database application must not reuse the lock IDs of the previous one.
    expect(hyperid.decode(ids[2]).uuid).not.toBe(hyperid.decode(ids[0]).uuid)
    await first.close()
    await second.close()
  })
})

describe('database lock expiry', () => {
  // Expire after 1s idle or 5s alive; the cleanup interval runs every second.
  function createRegistry() {
    return new LockRegistry({
      ...getConfig(),
      databaseWattLockIdleTimeout: 1_000,
      databaseWattLockMaxLifetime: 5_000,
    })
  }

  function rolledBack(client: FakeClient): boolean {
    return client.query.mock.calls.some(([statement]) => statement === 'ROLLBACK')
  }

  it('rolls back a transaction left idle past the idle timeout', async () => {
    vi.useFakeTimers()
    try {
      const locks = createRegistry()
      const client = createClient()
      const lockId = locks.create(createDestination(), client as never, true, owner)

      await vi.advanceTimersByTimeAsync(2_500)

      expect(rolledBack(client)).toBe(true)
      expect(client.release).toHaveBeenCalledTimes(1)
      await expect(locks.query(lockId, 'SELECT 1', undefined, owner)).rejects.toMatchObject({
        message: 'Unknown lock ID',
      })
      await locks.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('keeps a held-open transaction past the idle timeout and the lock lifetime', async () => {
    vi.useFakeTimers()
    try {
      const locks = createRegistry()
      const client = createClient()
      const lockId = locks.create(createDestination(), client as never, true, owner, {
        holdOpen: true,
      })

      // A TUS upload holds its lock transaction idle for as long as the upload lasts.
      await vi.advanceTimersByTimeAsync(10_000)

      expect(rolledBack(client)).toBe(false)
      await locks.query(lockId, 'SELECT 1', undefined, owner)
      await locks.commit(lockId, owner)
      expect(client.query).toHaveBeenLastCalledWith('COMMIT')
      await locks.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('still bounds a held-open transaction by HELD_LOCK_MAX_LIFETIME_MS', async () => {
    vi.useFakeTimers()
    try {
      const locks = createRegistry()
      const client = createClient()
      locks.create(createDestination(), client as never, true, owner, { holdOpen: true })

      await vi.advanceTimersByTimeAsync(HELD_LOCK_MAX_LIFETIME_MS + 2_000)

      expect(rolledBack(client)).toBe(true)
      expect(client.release).toHaveBeenCalledTimes(1)
      await locks.close()
    } finally {
      vi.useRealTimers()
    }
  })

  it('does not count a statement that runs longer than the idle timeout as idle time', async () => {
    vi.useFakeTimers()
    try {
      const locks = createRegistry()
      const client = createClient()
      client.query.mockImplementationOnce(
        () => new Promise((resolve) => setTimeout(() => resolve(rawResult), 3_000))
      )
      const lockId = locks.create(createDestination(), client as never, true, owner)

      const query = locks.query(lockId, 'SELECT pg_sleep(3)', undefined, owner)
      await vi.advanceTimersByTimeAsync(3_000)
      await expect(query).resolves.toMatchObject({ rowCount: 1 })

      // The lock went idle when the statement ended, so it can still commit.
      await locks.commit(lockId, owner)
      expect(rolledBack(client)).toBe(false)
      expect(client.query).toHaveBeenLastCalledWith('COMMIT')
      await locks.close()
    } finally {
      vi.useRealTimers()
    }
  })
})
