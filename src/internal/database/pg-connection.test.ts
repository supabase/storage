import { logger, logSchema } from '@internal/monitoring'
import { EventEmitter } from 'events'
import { DatabaseError, Pool as PgPool, type Pool, type PoolClient } from 'pg'
import { vi } from 'vitest'
import {
  getPgCancelConnectionTarget,
  PgPoolExecutor,
  PgPoolStrategy,
  PgTenantConnection,
  PgTransaction,
} from './pg-connection'

class TestablePgPoolStrategy extends PgPoolStrategy {
  getCurrentPoolForTest(): Pool {
    return this.getPool()
  }

  setCurrentPoolForTest(pool: Pool): void {
    this.pool = pool
  }
}

function createPoolStrategySettings(
  overrides: Partial<ConstructorParameters<typeof PgPoolStrategy>[0]> = {}
): ConstructorParameters<typeof PgPoolStrategy>[0] {
  return {
    tenantId: 'pg-pool-strategy-test',
    dbUrl: 'postgres://postgres:postgres@localhost:5432/postgres',
    maxConnections: 8,
    numWorkers: 1,
    isExternalPool: true,
    user: {
      jwt: 'jwt',
      payload: {
        role: 'authenticated',
      },
    },
    superUser: {
      jwt: 'service',
      payload: {
        role: 'service_role',
      },
    },
    ...overrides,
  }
}

function createDatabaseError(code: string | undefined, message = 'database error'): DatabaseError {
  const error = new DatabaseError(message, message.length, 'error')
  error.code = code
  return error
}

async function expectQueryErrorRelease(error: Error): Promise<ReturnType<typeof vi.fn>> {
  const release = vi.fn()
  const client = {
    query: vi.fn().mockRejectedValue(error),
    release,
  } as unknown as PoolClient
  const pool = {
    connect: vi.fn().mockResolvedValue(client),
  } as unknown as Pool
  const executor = new PgPoolExecutor(pool)

  await expect(executor.query('SELECT 1')).rejects.toBe(error)

  return release
}

class FakePgPoolClient extends EventEmitter {
  _ending = false
  _poolUseCount = 0
  _queryable = true

  connect(callback: (error?: Error) => void): void {
    setImmediate(() => callback())
  }

  query(
    _statement: string,
    callback?: (error: Error | undefined, result: { rows: unknown[] }) => void
  ): Promise<{ rows: unknown[] }> | void {
    const result = { rows: [] }

    if (callback) {
      setImmediate(() => callback(undefined, result))
      return
    }

    return Promise.resolve(result)
  }

  end(callback?: () => void): void {
    this._ending = true
    setImmediate(() => {
      this.emit('end')
      callback?.()
    })
  }

  ref(): void {
    // no-op
  }

  unref(): void {
    // no-op
  }
}

function createDrainablePoolForTest(
  initialStats: {
    waitingCount?: number
    totalCount?: number
    idleCount?: number
    ending?: boolean
    ended?: boolean
  } = {}
) {
  const stats = {
    waitingCount: 0,
    totalCount: 0,
    idleCount: 0,
    ending: false,
    ended: false,
    ...initialStats,
  }
  const end = vi.fn().mockImplementation(async () => {
    stats.ending = true
    stats.ended = true
  })
  const pool = {
    options: {
      max: 8,
    },
    get waitingCount() {
      return stats.waitingCount
    },
    get totalCount() {
      return stats.totalCount
    },
    get idleCount() {
      return stats.idleCount
    },
    get ending() {
      return stats.ending
    },
    get ended() {
      return stats.ended
    },
    end,
  } as unknown as Pool

  return {
    pool,
    end,
    setStats(nextStats: Partial<typeof stats>) {
      Object.assign(stats, nextStats)
    },
  }
}

async function waitForDrainCheck(): Promise<void> {
  await new Promise((resolve) => setImmediate(resolve))
}

describe('getPgCancelConnectionTarget', () => {
  it('uses direct client host and port for TCP cancel connections', () => {
    expect(
      getPgCancelConnectionTarget({
        host: 'db.example.test',
        port: 6432,
      })
    ).toEqual({
      type: 'tcp',
      host: 'db.example.test',
      port: 6432,
    })
  })

  it('falls back to connection parameters for TCP cancel connections', () => {
    expect(
      getPgCancelConnectionTarget({
        connectionParameters: {
          host: 'pool.example.test',
          port: 5433,
        },
      })
    ).toEqual({
      type: 'tcp',
      host: 'pool.example.test',
      port: 5433,
    })
  })

  it('uses the first connection-parameter host for multi-host TCP cancel connections', () => {
    expect(
      getPgCancelConnectionTarget({
        connectionParameters: {
          host: ['primary.example.test', 'standby.example.test'],
          port: 5433,
        },
      })
    ).toEqual({
      type: 'tcp',
      host: 'primary.example.test',
      port: 5433,
    })
  })

  it('uses localhost and the default postgres port when the client does not expose a target', () => {
    expect(getPgCancelConnectionTarget({})).toEqual({
      type: 'tcp',
      host: 'localhost',
      port: 5432,
    })
  })

  it('builds a Unix socket path from direct client connection fields', () => {
    expect(
      getPgCancelConnectionTarget({
        host: '/var/run/postgresql',
        port: 6432,
      })
    ).toEqual({
      type: 'socket',
      path: '/var/run/postgresql/.s.PGSQL.6432',
    })
  })

  it('prefers direct client fields over connection parameter fallbacks', () => {
    expect(
      getPgCancelConnectionTarget({
        host: '/tmp/pg',
        port: 6543,
        connectionParameters: {
          host: 'pool.example.test',
          port: 5433,
        },
      })
    ).toEqual({
      type: 'socket',
      path: '/tmp/pg/.s.PGSQL.6543',
    })
  })
})

describe('PgPoolExecutor', () => {
  it('returns clients to the pool after regular SQL errors', async () => {
    for (const code of ['42P01', '23505', '23503', '42501', '22P02', '42703']) {
      const error = createDatabaseError(code)
      const release = await expectQueryErrorRelease(error)

      expect(release).toHaveBeenCalledWith(undefined)
    }
  })

  it('returns clients to the pool after statement_timeout errors', async () => {
    const timeoutError = createDatabaseError(
      '57014',
      'canceling statement due to statement timeout'
    )
    const release = await expectQueryErrorRelease(timeoutError)

    expect(release).toHaveBeenCalledWith(undefined)
  })

  it('disposes clients after connection-state query errors', async () => {
    const connectionError = createDatabaseError('08006')
    const connectionErrorRelease = await expectQueryErrorRelease(connectionError)

    expect(connectionErrorRelease).toHaveBeenCalledWith(connectionError)

    const protocolError = createDatabaseError(undefined, 'received invalid response: 58')
    const protocolErrorRelease = await expectQueryErrorRelease(protocolError)

    expect(protocolErrorRelease).toHaveBeenCalledWith(protocolError)
  })
})

describe('PgTransaction', () => {
  it('rejects queries after commit releases the client', async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [] }),
      release: vi.fn(),
    } as unknown as PoolClient
    const transaction = new PgTransaction(client)

    await transaction.commit()

    await expect(transaction.query('SELECT 1')).rejects.toThrow(
      'Cannot query a completed transaction'
    )
    expect(client.query).toHaveBeenCalledTimes(1)
    expect(client.release).toHaveBeenCalledTimes(1)
  })

  it('rejects queries after rollback releases the client', async () => {
    const client = {
      query: vi.fn().mockResolvedValue({ rows: [] }),
      release: vi.fn(),
    } as unknown as PoolClient
    const transaction = new PgTransaction(client)

    await transaction.rollback()

    await expect(transaction.query('SELECT 1')).rejects.toThrow(
      'Cannot query a completed transaction'
    )
    expect(client.query).toHaveBeenCalledTimes(1)
    expect(client.release).toHaveBeenCalledTimes(1)
  })
})

describe('PgTenantConnection', () => {
  it('rejects pool acquisition after disposal', async () => {
    const pool = {
      acquire: vi.fn(),
      destroy: vi.fn().mockResolvedValue(undefined),
    } as unknown as PgPoolStrategy
    const connection = new PgTenantConnection(
      pool,
      createPoolStrategySettings({
        isExternalPool: true,
        isSingleUse: true,
      })
    )

    await connection.dispose()

    await expect(connection.query('SELECT 1')).rejects.toThrow(
      'Cannot use a disposed PgTenantConnection'
    )
    await expect(connection.beginTransaction()).rejects.toThrow(
      'Cannot use a disposed PgTenantConnection'
    )
    await expect(connection.transaction()).rejects.toThrow(
      'Cannot use a disposed PgTenantConnection'
    )
    expect(() => connection.asSuperUser()).toThrow('Cannot use a disposed PgTenantConnection')
    expect(pool.acquire).not.toHaveBeenCalled()
    expect(pool.destroy).toHaveBeenCalledTimes(1)
  })

  it('rejects cacheable connection use after disposal without destroying the retained pool', async () => {
    const pool = {
      acquire: vi.fn(),
      destroy: vi.fn().mockResolvedValue(undefined),
    } as unknown as PgPoolStrategy
    const connection = new PgTenantConnection(
      pool,
      createPoolStrategySettings({
        isExternalPool: false,
        isSingleUse: false,
      })
    )

    await connection.dispose()

    await expect(connection.query('SELECT 1')).rejects.toThrow(
      'Cannot use a disposed PgTenantConnection'
    )
    expect(() => connection.asSuperUser()).toThrow('Cannot use a disposed PgTenantConnection')
    expect(pool.acquire).not.toHaveBeenCalled()
    expect(pool.destroy).not.toHaveBeenCalled()
  })

  it('stops transaction retries after disposal', async () => {
    vi.useFakeTimers()

    const connectionLimitError = createDatabaseError('08P01', 'no more connections allowed')
    const beginTransaction = vi.fn().mockRejectedValue(connectionLimitError)
    const pool = {
      acquire: vi.fn().mockReturnValue({
        beginTransaction,
      }),
      destroy: vi.fn().mockResolvedValue(undefined),
    } as unknown as PgPoolStrategy
    const connection = new PgTenantConnection(
      pool,
      createPoolStrategySettings({
        isExternalPool: true,
        isSingleUse: true,
      })
    )

    try {
      const transactionPromise = connection.transaction()
      const transactionErrorPromise = transactionPromise.catch((error) => error)

      await vi.advanceTimersByTimeAsync(0)
      expect(pool.acquire).toHaveBeenCalledTimes(1)

      await connection.dispose()
      await vi.advanceTimersByTimeAsync(200)

      await expect(transactionErrorPromise).resolves.toMatchObject({
        message: 'Cannot use a disposed PgTenantConnection',
      })
      expect(pool.acquire).toHaveBeenCalledTimes(1)
      expect(beginTransaction).toHaveBeenCalledTimes(1)
      expect(pool.destroy).toHaveBeenCalledTimes(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it('maps pg-pool acquisition timeouts to DatabaseTimeout', async () => {
    const timeoutError = new Error('timeout exceeded when trying to connect')
    const pool = {
      acquire: vi.fn().mockReturnValue({
        beginTransaction: vi.fn().mockRejectedValue(timeoutError),
      }),
    } as unknown as PgPoolStrategy
    const connection = new PgTenantConnection(pool, createPoolStrategySettings())

    await expect(connection.transaction()).rejects.toMatchObject({
      code: 'DatabaseTimeout',
      originalError: timeoutError,
    })
  })

  it('preserves setup errors when external-pool rollback fails', async () => {
    const setupError = new Error('search_path setup failed')
    const rollbackError = new Error('rollback failed')
    const transaction = {
      query: vi.fn().mockRejectedValue(setupError),
      rollback: vi.fn().mockRejectedValue(rollbackError),
    } as unknown as PgTransaction
    const pool = {
      acquire: vi.fn().mockReturnValue({
        beginTransaction: vi.fn().mockResolvedValue(transaction),
      }),
    } as unknown as PgPoolStrategy
    const connection = new PgTenantConnection(pool, createPoolStrategySettings())
    const logSpy = vi.spyOn(logSchema, 'warning').mockImplementation(() => undefined)

    try {
      await expect(connection.transaction()).rejects.toBe(setupError)

      expect(transaction.rollback).toHaveBeenCalledTimes(1)
      expect(logSpy).toHaveBeenCalledWith(
        logger,
        '[PgTenantConnection] Failed to rollback transaction',
        expect.objectContaining({
          type: 'db',
          tenantId: 'pg-pool-strategy-test',
          project: 'pg-pool-strategy-test',
          error: rollbackError,
        })
      )
    } finally {
      logSpy.mockRestore()
    }
  })
})

describe('PgPoolStrategy', () => {
  it('logs idle pg pool errors without rethrowing them', async () => {
    const strategy = new TestablePgPoolStrategy(createPoolStrategySettings())
    const logSpy = vi.spyOn(logSchema, 'warning').mockImplementation(() => undefined)

    try {
      const pool = strategy.getCurrentPoolForTest()
      const error = Object.assign(new Error('Connection terminated unexpectedly'), {
        client: { ssl: { ca: 'secret root cert' } },
      })

      expect(() => pool.emit('error', error, {})).not.toThrow()
      expect(logSpy).toHaveBeenCalledWith(
        logger,
        '[PgPoolStrategy] Idle pg client error',
        expect.objectContaining({
          type: 'db',
          tenantId: 'pg-pool-strategy-test',
          project: 'pg-pool-strategy-test',
          error,
        })
      )
    } finally {
      logSpy.mockRestore()
      await strategy.destroy()
    }
  })

  it('documents that pg-pool end does not service already queued acquires', async () => {
    const pool = new PgPool({
      Client: FakePgPoolClient,
      max: 1,
    } as unknown as ConstructorParameters<typeof PgPool>[0])
    const acquirePromises = Array.from({ length: 5 }, () => pool.connect())
    const checkedOutClient = await acquirePromises[0]

    await waitForDrainCheck()
    expect(pool.waitingCount).toBe(4)

    const endPromise = pool.end()
    checkedOutClient.release()
    await endPromise
    await waitForDrainCheck()

    expect(pool.ended).toBe(true)
    expect(pool.waitingCount).toBe(4)
    await expect(
      Promise.race([
        Promise.allSettled(acquirePromises.slice(1)).then(() => 'settled'),
        waitForDrainCheck().then(() => 'pending'),
      ])
    ).resolves.toBe('pending')
  })

  it('drains queued acquires on a real pg-pool before destroying it', async () => {
    const strategy = new TestablePgPoolStrategy(createPoolStrategySettings())
    const pool = new PgPool({
      Client: FakePgPoolClient,
      max: 1,
    } as unknown as ConstructorParameters<typeof PgPool>[0])
    const checkedOutClient = await pool.connect()
    const queuedConnect = pool.connect()

    await waitForDrainCheck()
    expect(pool.waitingCount).toBe(1)

    strategy.setCurrentPoolForTest(pool)
    const destroyPromise = strategy.destroy()

    checkedOutClient.release()
    const queuedClient = await queuedConnect

    await expect(queuedClient.query('SELECT 1')).resolves.toEqual({ rows: [] })
    queuedClient.release()
    await destroyPromise

    expect(pool.ended).toBe(true)
  })

  it('updates the current pg pool max after cluster-size rebalance', async () => {
    const strategy = new TestablePgPoolStrategy(createPoolStrategySettings())

    try {
      const originalPool = strategy.getCurrentPoolForTest()
      expect(originalPool.options.max).toBe(8)

      strategy.rebalance({ clusterSize: 4 })

      const rebalancedPool = strategy.getCurrentPoolForTest()
      expect(rebalancedPool).toBe(originalPool)
      expect(originalPool.ended).toBe(false)
      expect(rebalancedPool.options.max).toBe(2)
    } finally {
      await strategy.destroy()
    }
  })

  it('updates the current pg pool max after max-connections rebalance', async () => {
    const strategy = new TestablePgPoolStrategy(createPoolStrategySettings())

    try {
      const originalPool = strategy.getCurrentPoolForTest()
      expect(originalPool.options.max).toBe(8)

      strategy.rebalance({ maxConnections: 12 })

      const rebalancedPool = strategy.getCurrentPoolForTest()
      expect(rebalancedPool).toBe(originalPool)
      expect(originalPool.ended).toBe(false)
      expect(rebalancedPool.options.max).toBe(12)
    } finally {
      await strategy.destroy()
    }
  })

  it('keeps min at 0 across pg pool rebalances', async () => {
    const strategy = new TestablePgPoolStrategy(createPoolStrategySettings())

    try {
      const pool = strategy.getCurrentPoolForTest()
      expect(pool.options.min).toBe(0)

      strategy.rebalance({ maxConnections: 1 })
      expect(pool.options.min).toBe(0)

      strategy.rebalance({ maxConnections: 50 })
      expect(pool.options.min).toBe(0)

      strategy.rebalance({ clusterSize: 100 })
      expect(pool.options.min).toBe(0)
    } finally {
      await strategy.destroy()
    }
  })

  it('treats max-connections scale-down as a soft cap for checked-out pg clients', async () => {
    const strategy = new TestablePgPoolStrategy(createPoolStrategySettings())
    const pool = new PgPool({
      Client: FakePgPoolClient,
      max: 4,
    } as unknown as ConstructorParameters<typeof PgPool>[0])

    try {
      strategy.setCurrentPoolForTest(pool)
      const checkedOutClients = await Promise.all(Array.from({ length: 4 }, () => pool.connect()))
      expect(pool.totalCount).toBe(4)

      strategy.rebalance({ maxConnections: 1 })
      expect(pool.options.max).toBe(1)
      expect(pool.totalCount).toBe(4)

      const blockedAcquire = pool.connect()
      await waitForDrainCheck()
      expect(pool.waitingCount).toBe(1)

      checkedOutClients[0].release()
      const queuedClient = await blockedAcquire
      expect(pool.totalCount).toBe(4)

      queuedClient.release()
      checkedOutClients.slice(1).forEach((client) => client.release())
    } finally {
      await pool.end()
    }
  })

  it('serves queued pg acquires immediately after scaling max up', async () => {
    const strategy = new TestablePgPoolStrategy(createPoolStrategySettings())
    const pool = new PgPool({
      Client: FakePgPoolClient,
      max: 1,
    } as unknown as ConstructorParameters<typeof PgPool>[0])

    try {
      strategy.setCurrentPoolForTest(pool)
      const checkedOutClient = await pool.connect()
      const queuedConnect = pool.connect()

      await waitForDrainCheck()
      expect(pool.waitingCount).toBe(1)

      strategy.rebalance({ maxConnections: 2 })

      const queuedClient = await Promise.race([
        queuedConnect,
        waitForDrainCheck().then(() => undefined),
      ])
      expect(queuedClient).toBeDefined()
      expect(pool.waitingCount).toBe(0)

      checkedOutClient.release()
      queuedClient?.release()
    } finally {
      await pool.end()
    }
  })

  it('does not drain the current pg pool after rebalance', async () => {
    const strategy = new TestablePgPoolStrategy(createPoolStrategySettings())
    const error = new Error('pool drain failed')
    const originalPool = {
      options: {
        max: 8,
      },
      end: vi.fn().mockRejectedValue(error),
    } as unknown as Pool
    const logSpy = vi.spyOn(logSchema, 'warning').mockImplementation(() => undefined)

    try {
      strategy.setCurrentPoolForTest(originalPool)
      strategy.rebalance({ clusterSize: 2 })
      await new Promise((resolve) => setImmediate(resolve))

      expect(originalPool.end).not.toHaveBeenCalled()
      expect(originalPool.options.max).toBe(4)
      expect(logSpy).not.toHaveBeenCalledWith(
        logger,
        '[PgPoolStrategy] Failed to drain old pool during rebalance',
        expect.anything()
      )
    } finally {
      logSpy.mockRestore()
    }
  })

  it('keeps queued acquires on the current pg pool after rebalance', async () => {
    vi.useFakeTimers()
    const strategy = new TestablePgPoolStrategy(createPoolStrategySettings())
    const { pool, end, setStats } = createDrainablePoolForTest({
      waitingCount: 1,
      totalCount: 1,
      idleCount: 0,
    })

    try {
      strategy.setCurrentPoolForTest(pool)
      strategy.rebalance({ clusterSize: 2 })

      await vi.advanceTimersByTimeAsync(200)
      expect(end).not.toHaveBeenCalled()

      setStats({
        waitingCount: 0,
        totalCount: 1,
        idleCount: 0,
      })
      await vi.advanceTimersByTimeAsync(200)

      expect(pool.options.max).toBe(4)
      expect(end).not.toHaveBeenCalled()
    } finally {
      vi.useRealTimers()
    }
  })

  it('waits for queued acquires to drain before destroying a pg pool', async () => {
    vi.useFakeTimers()
    const strategy = new TestablePgPoolStrategy(createPoolStrategySettings())
    const { pool, end, setStats } = createDrainablePoolForTest({
      waitingCount: 1,
      totalCount: 1,
      idleCount: 0,
    })

    try {
      strategy.setCurrentPoolForTest(pool)
      const destroyPromise = strategy.destroy()

      await vi.advanceTimersByTimeAsync(200)
      expect(end).not.toHaveBeenCalled()

      setStats({
        waitingCount: 0,
        totalCount: 1,
        idleCount: 0,
      })
      await vi.advanceTimersByTimeAsync(200)

      await destroyPromise
      expect(end).toHaveBeenCalledTimes(1)
    } finally {
      vi.useRealTimers()
    }
  })

  it('does not wait for idle-only pg pools to age out before ending them', async () => {
    const strategy = new TestablePgPoolStrategy(createPoolStrategySettings())
    const { pool, end } = createDrainablePoolForTest({
      waitingCount: 0,
      totalCount: 1,
      idleCount: 1,
    })

    strategy.setCurrentPoolForTest(pool)
    await strategy.destroy()

    expect(end).toHaveBeenCalledTimes(1)
  })

  it('logs residual work and ends the pg pool when drain timeout elapses', async () => {
    vi.useFakeTimers()

    const strategy = new TestablePgPoolStrategy(createPoolStrategySettings())
    const { pool, end } = createDrainablePoolForTest({
      waitingCount: 2,
      totalCount: 3,
      idleCount: 1,
    })
    const logSpy = vi.spyOn(logSchema, 'warning').mockImplementation(() => undefined)

    try {
      strategy.setCurrentPoolForTest(pool)
      const destroyPromise = strategy.destroy()

      await vi.advanceTimersByTimeAsync(30_000)
      await destroyPromise

      expect(end).toHaveBeenCalledTimes(1)
      const timeoutLog = logSpy.mock.calls.find(
        ([, message]) => message === '[PgPoolStrategy] Timed out waiting for pg pool to drain'
      )
      expect(timeoutLog).toBeDefined()
      const timeoutPayload = timeoutLog?.[2] as { metadata: string }

      expect(timeoutPayload).toMatchObject({
        type: 'db',
        tenantId: 'pg-pool-strategy-test',
        project: 'pg-pool-strategy-test',
        metadata: expect.any(String),
      })
      expect(JSON.parse(timeoutPayload.metadata)).toMatchObject({
        reason: 'destroy',
        drainTimeoutMs: 30_000,
        waitingCount: 2,
        activeCount: 2,
        totalCount: 3,
        idleCount: 1,
      })
    } finally {
      logSpy.mockRestore()
      vi.useRealTimers()
    }
  })
})
