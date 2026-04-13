'use strict'

import { TENANT_POOL_CACHE_NAME } from '@internal/cache'
import type { PoolStrategy, TenantConnectionOptions } from '../internal/database/pool'

type TestPool = {
  acquire: jest.Mock
  rebalance: jest.Mock
  destroy: jest.Mock<Promise<void>, []>
  getPoolStats: jest.Mock
}

type PoolModule = typeof import('../internal/database/pool')

function createPoolSettings(tenantId: string) {
  return {
    tenantId,
    dbUrl: 'postgres://example',
    maxConnections: 10,
    user: { jwt: 'jwt', payload: { role: 'authenticated' } },
    superUser: { jwt: 'service', payload: { role: 'service_role' } },
  }
}

function createTestPool(stats: { used: number; total: number } | null = null): TestPool {
  return {
    acquire: jest.fn(),
    rebalance: jest.fn(),
    destroy: jest.fn().mockResolvedValue(undefined),
    getPoolStats: jest.fn().mockReturnValue(stats),
  }
}

async function loadPoolModule(ttlMs: number, maxEntries?: number): Promise<PoolModule> {
  jest.resetModules()

  const configModule = await import('../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig({ isMultitenant: true })

  const cacheOptionOverrides = {
    ttl: ttlMs,
    ...(maxEntries === undefined ? {} : { max: maxEntries }),
  }

  jest.doMock('@internal/cache', () => {
    const actual = jest.requireActual('@internal/cache') as typeof import('@internal/cache')

    return {
      ...actual,
      createTtlCache: ((optionsOrName: unknown, maybeOptions?: Record<string, unknown>) => {
        if (typeof optionsOrName === 'string') {
          return actual.createTtlCache(
            optionsOrName as never,
            {
              ...(maybeOptions || {}),
              ...cacheOptionOverrides,
            } as never
          )
        }

        return actual.createTtlCache({
          ...(optionsOrName as Record<string, unknown>),
          ...cacheOptionOverrides,
        } as never)
      }) as typeof actual.createTtlCache,
    }
  })

  return import('../internal/database/pool')
}

describe('PoolManager cache lifecycle', () => {
  beforeEach(() => {
    jest.useFakeTimers()
  })

  afterEach(() => {
    jest.useRealTimers()
    jest.dontMock('@internal/cache')
    jest.resetModules()
    jest.clearAllMocks()
  })

  test('expires cached pools and disposes them after inactivity', async () => {
    const poolModule = await loadPoolModule(20)

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool: TestPool = {
          acquire: jest.fn(),
          rebalance: jest.fn(),
          destroy: jest.fn().mockResolvedValue(undefined),
          getPoolStats: jest.fn().mockReturnValue(null),
        }
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('tenant-a')

    const first = poolManager.getPool(settings)

    expect(poolManager.created).toHaveLength(1)

    jest.advanceTimersByTime(40)

    expect(poolManager.created[0].destroy).toHaveBeenCalledTimes(1)

    const second = poolManager.getPool(settings)

    expect(second).not.toBe(first)
    expect(poolManager.created).toHaveLength(2)

    await poolManager.destroyAll()
  })

  test('refreshes pool ttl when an existing pool is reused', async () => {
    const poolModule = await loadPoolModule(25)

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool: TestPool = {
          acquire: jest.fn(),
          rebalance: jest.fn(),
          destroy: jest.fn().mockResolvedValue(undefined),
          getPoolStats: jest.fn().mockReturnValue(null),
        }
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('tenant-b')

    const first = poolManager.getPool(settings)

    jest.advanceTimersByTime(15)

    const reused = poolManager.getPool(settings)

    expect(reused).toBe(first)
    expect(poolManager.created[0].destroy).not.toHaveBeenCalled()

    jest.advanceTimersByTime(15)

    expect(poolManager.created[0].destroy).not.toHaveBeenCalled()

    jest.advanceTimersByTime(20)

    expect(poolManager.created[0].destroy).toHaveBeenCalledTimes(1)

    await poolManager.destroyAll()
  })

  test('records logical pool cache misses and hits', async () => {
    const poolModule = await loadPoolModule(10_000)
    const metricsModule = await import('../internal/monitoring/metrics')
    const addSpy = jest.spyOn(metricsModule.cacheRequestsTotal, 'add')

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('tenant-cache-metrics')

    const first = poolManager.getPool(settings)
    const second = poolManager.getPool(settings)

    expect(second).toBe(first)
    expect(poolManager.created).toHaveLength(1)
    expect(addSpy.mock.calls).toEqual(
      expect.arrayContaining([
        [1, { cache: TENANT_POOL_CACHE_NAME, outcome: 'miss' }],
        [1, { cache: TENANT_POOL_CACHE_NAME, outcome: 'hit' }],
      ])
    )

    await poolManager.destroyAll()
  })

  test('records pool cache evictions when inactivity ttl removes cached pools', async () => {
    const poolModule = await loadPoolModule(20)
    const metricsModule = await import('../internal/monitoring/metrics')
    const evictionSpy = jest.spyOn(metricsModule.cacheEvictionsTotal, 'add')

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    poolManager.getPool(createPoolSettings('tenant-cache-ttl-eviction'))

    jest.advanceTimersByTime(40)

    expect(evictionSpy).toHaveBeenCalledWith(1, {
      cache: TENANT_POOL_CACHE_NAME,
    })
    expect(poolManager.created[0].destroy).toHaveBeenCalledTimes(1)

    await poolManager.destroyAll()
  })

  test('records pool cache evictions when capacity removes cached pools', async () => {
    const poolModule = await loadPoolModule(10_000, 1)
    const metricsModule = await import('../internal/monitoring/metrics')
    const evictionSpy = jest.spyOn(metricsModule.cacheEvictionsTotal, 'add')

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    poolManager.getPool(createPoolSettings('tenant-cache-capacity-eviction-a'))
    poolManager.getPool(createPoolSettings('tenant-cache-capacity-eviction-b'))

    expect(evictionSpy).toHaveBeenCalledWith(1, {
      cache: TENANT_POOL_CACHE_NAME,
    })
    expect(poolManager.created[0].destroy).toHaveBeenCalledTimes(1)

    await poolManager.destroyAll()
  })

  test('does not record pool cache evictions for explicit destroys', async () => {
    const poolModule = await loadPoolModule(10_000)
    const metricsModule = await import('../internal/monitoring/metrics')
    const evictionSpy = jest.spyOn(metricsModule.cacheEvictionsTotal, 'add')

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    poolManager.getPool(createPoolSettings('tenant-cache-explicit-destroy-a'))
    poolManager.getPool(createPoolSettings('tenant-cache-explicit-destroy-b'))

    await poolManager.destroy('tenant-cache-explicit-destroy-a')
    await poolManager.destroyAll()

    expect(evictionSpy.mock.calls).not.toContainEqual([
      1,
      {
        cache: TENANT_POOL_CACHE_NAME,
      },
    ])
    expect(poolManager.created[0].destroy).toHaveBeenCalledTimes(1)
    expect(poolManager.created[1].destroy).toHaveBeenCalledTimes(1)
  })

  test('does not record pool cache misses for single-use external pools without cached pools', async () => {
    const poolModule = await loadPoolModule(10_000)
    const metricsModule = await import('../internal/monitoring/metrics')
    const addSpy = jest.spyOn(metricsModule.cacheRequestsTotal, 'add')

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = {
      ...createPoolSettings('tenant-single-use-external'),
      isSingleUse: true,
      isExternalPool: true,
    }

    const first = poolManager.getPool(settings)
    const second = poolManager.getPool(settings)

    expect(second).not.toBe(first)
    expect(poolManager.created).toHaveLength(2)
    expect(
      addSpy.mock.calls.filter(([, attrs]) => {
        return attrs && typeof attrs === 'object' && attrs.cache === TENANT_POOL_CACHE_NAME
      })
    ).toEqual([])

    await Promise.all([first.destroy(), second.destroy()])
    await poolManager.destroyAll()
  })

  test('reuses cached pools for single-use external requests and records a hit', async () => {
    const poolModule = await loadPoolModule(10_000)
    const metricsModule = await import('../internal/monitoring/metrics')
    const addSpy = jest.spyOn(metricsModule.cacheRequestsTotal, 'add')

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const tenantId = 'tenant-single-use-external-reuses-cache'
    const cachedPool = poolManager.getPool(createPoolSettings(tenantId))
    addSpy.mockClear()

    const reusedPool = poolManager.getPool({
      ...createPoolSettings(tenantId),
      isSingleUse: true,
      isExternalPool: true,
    })

    expect(reusedPool).toBe(cachedPool)
    expect(poolManager.created).toHaveLength(1)
    expect(addSpy.mock.calls).toEqual([[1, { cache: TENANT_POOL_CACHE_NAME, outcome: 'hit' }]])

    await poolManager.destroyAll()
  })

  test('iterates cached pools for monitor snapshots', async () => {
    jest.useFakeTimers()

    const poolModule = await loadPoolModule(10_000)
    const metricsModule = await import('../internal/monitoring/metrics')
    const addBatchObservableCallbackSpy = jest.spyOn(
      metricsModule.meter,
      'addBatchObservableCallback'
    )
    let batchObserver: ((observer: { observe: (...args: unknown[]) => void }) => void) | undefined

    addBatchObservableCallbackSpy.mockImplementation((callback) => {
      batchObserver = callback as typeof batchObserver
      return undefined as never
    })

    class TestPoolManager extends poolModule.PoolManager {
      created: Record<string, TestPool> = {}

      protected newPool(settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool(
          settings.tenantId === 'tenant-a' ? { used: 2, total: 5 } : { used: 3, total: 7 }
        )
        this.created[settings.tenantId] = pool
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    poolManager.getPool(createPoolSettings('tenant-a'))
    poolManager.getPool(createPoolSettings('tenant-b'))

    poolManager.monitor()
    jest.advanceTimersByTime(5_000)

    const observeSpy = jest.fn()
    batchObserver?.({ observe: observeSpy })

    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbActivePool, 2)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbActiveConnection, 12)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbInUseConnection, 5)

    await poolManager.destroyAll()
  })

  test('iterates cached pools for rebalanceAll and destroyAll', async () => {
    const poolModule = await loadPoolModule(10_000)

    class TestPoolManager extends poolModule.PoolManager {
      created: Record<string, TestPool> = {}

      protected newPool(settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created[settings.tenantId] = pool
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const first = poolManager.getPool(createPoolSettings('tenant-c'))
    const second = poolManager.getPool(createPoolSettings('tenant-d'))

    poolManager.rebalanceAll({ clusterSize: 4 })

    expect(first.rebalance).toHaveBeenCalledWith({ clusterSize: 4 })
    expect(second.rebalance).toHaveBeenCalledWith({ clusterSize: 4 })

    await poolManager.destroyAll()

    expect(first.destroy).toHaveBeenCalledTimes(1)
    expect(second.destroy).toHaveBeenCalledTimes(1)

    const recreated = poolManager.getPool(createPoolSettings('tenant-c'))

    expect(recreated).not.toBe(first)
  })

  test('propagates explicit destroy failures without double-destroying pools', async () => {
    const poolModule = await loadPoolModule(10_000)

    class TestPoolManager extends poolModule.PoolManager {
      created: Record<string, TestPool> = {}

      protected newPool(settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        pool.destroy.mockRejectedValue(new Error(`destroy failed for ${settings.tenantId}`))
        this.created[settings.tenantId] = pool
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const tenantId = 'tenant-destroy-error'

    poolManager.getPool(createPoolSettings(tenantId))

    await expect(poolManager.destroy(tenantId)).rejects.toThrow(`destroy failed for ${tenantId}`)
    expect(poolManager.created[tenantId].destroy).toHaveBeenCalledTimes(1)
  })

  test('preserves rejected destroyAll settlements when pool teardown fails', async () => {
    const poolModule = await loadPoolModule(10_000)

    class TestPoolManager extends poolModule.PoolManager {
      created: Record<string, TestPool> = {}

      protected newPool(settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()

        if (settings.tenantId === 'tenant-destroyall-error') {
          pool.destroy.mockRejectedValue(new Error('destroyAll failed'))
        }

        this.created[settings.tenantId] = pool
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    poolManager.getPool(createPoolSettings('tenant-destroyall-ok'))
    poolManager.getPool(createPoolSettings('tenant-destroyall-error'))

    const results = await poolManager.destroyAll()
    const rejected = results.find((result) => result.status === 'rejected')

    expect(results).toHaveLength(2)
    expect(rejected).toBeDefined()
    expect(rejected).toMatchObject({
      status: 'rejected',
      reason: expect.objectContaining({ message: 'destroyAll failed' }),
    })
    expect(poolManager.created['tenant-destroyall-ok'].destroy).toHaveBeenCalledTimes(1)
    expect(poolManager.created['tenant-destroyall-error'].destroy).toHaveBeenCalledTimes(1)
  })

  test('does not extend pool ttl when iterating for monitor snapshots', async () => {
    const poolModule = await loadPoolModule(25)
    const metricsModule = await import('../internal/monitoring/metrics')
    const addBatchObservableCallbackSpy = jest.spyOn(
      metricsModule.meter,
      'addBatchObservableCallback'
    )
    let batchObserver: ((observer: { observe: (...args: unknown[]) => void }) => void) | undefined

    addBatchObservableCallbackSpy.mockImplementation((callback) => {
      batchObserver = callback as typeof batchObserver
      return undefined as never
    })

    class TestPoolManager extends poolModule.PoolManager {
      created: Record<string, TestPool> = {}

      protected newPool(settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool({ used: 1, total: 2 })
        this.created[settings.tenantId] = pool
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    poolManager.getPool(createPoolSettings('tenant-monitor'))

    poolManager.monitor()

    jest.advanceTimersByTime(5_000)
    batchObserver?.({ observe: jest.fn() })

    jest.advanceTimersByTime(30)

    expect(poolManager.created['tenant-monitor'].destroy).toHaveBeenCalledTimes(1)
  })
})
