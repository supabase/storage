import { afterEach, describe, expect, it, vi } from 'vitest'

const pgMock = vi.hoisted(() => {
  type PendingConnect = {
    resolve: (client: unknown) => void
    reject: (error: unknown) => void
  }

  const state = {
    pending: [] as PendingConnect[],
    pools: [] as Array<{ options: Record<string, unknown> }>,
  }

  class Pool {
    totalCount = 0
    idleCount = 0
    waitingCount = 0
    options: Record<string, unknown>

    constructor(options: Record<string, unknown>) {
      this.options = { ...options }
      state.pools.push(this)
    }

    on() {
      return this
    }

    connect() {
      return new Promise((resolve, reject) => {
        state.pending.push({ resolve, reject })
      })
    }

    async end() {}
  }

  return { Pool, state, types: { setTypeParser: () => undefined } }
})

vi.mock('pg', () => ({
  DatabaseError: class DatabaseError extends Error {},
  Pool: pgMock.Pool,
  types: pgMock.types,
  default: { types: pgMock.types },
}))

import { getConfig } from '../../config.js'
import { PoolRegistry } from './pools.js'

afterEach(() => {
  pgMock.state.pending.length = 0
  pgMock.state.pools.length = 0
  vi.useRealTimers()
})

describe('database pool registry', () => {
  it('releases a client that is handed out after the acquire timeout fired', async () => {
    vi.useFakeTimers()
    const registry = new PoolRegistry({ ...getConfig(), databaseWattAcquireTimeout: 100 })

    const acquiring = registry.acquire({
      connectionString: 'postgres://tenant-db',
      id: 'tenant-a',
      isExternalPool: false,
      maxConnections: 1,
    })
    const rejection = expect(acquiring).rejects.toMatchObject({ code: 'ACQUIRE_TIMEOUT' })

    await vi.advanceTimersByTimeAsync(100)
    await rejection

    const lateClient = { release: vi.fn() }
    pgMock.state.pending[0].resolve(lateClient)
    await vi.advanceTimersByTimeAsync(0)

    expect(lateClient.release).toHaveBeenCalledTimes(1)
    await registry.close()
  })

  it('does not let the pg-pool connect timeout cut the acquire wait short', async () => {
    const registry = new PoolRegistry({
      ...getConfig(),
      databaseConnectionTimeout: 3_000,
      databaseWattAcquireTimeout: 25_000,
    })

    void registry
      .acquire({
        connectionString: 'postgres://tenant-db',
        id: 'tenant-a',
        isExternalPool: false,
        maxConnections: 1,
      })
      .catch(() => undefined)

    expect(pgMock.state.pools[0].options.connectionTimeoutMillis).toBe(25_000)
    await registry.close()
  })
})
