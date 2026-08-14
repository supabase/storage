import { TENANT_POOL_CACHE_NAME } from '@internal/cache/names'
import { captureBatchObserver } from '@internal/testing/metrics'
import { type Mock, vi } from 'vitest'
import type { PoolStrategy, PoolStrategySettings } from './pool'

type TestPool = {
  acquire: Mock
  rebalance: Mock
  destroy: Mock<() => Promise<void>>
  retire: Mock<() => Promise<void>>
  retain: Mock
  release: Mock
  retireWhenReleased: Mock<() => Promise<void>>
  getPoolStats: Mock
}

type PoolModule = typeof import('./pool')
type MetricsModule = typeof import('@internal/monitoring/metrics')

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
  const destroy = vi.fn().mockResolvedValue(undefined)
  let retirement: Promise<void> | undefined
  const retire = vi.fn((): Promise<void> => {
    if (retirement) {
      return retirement
    }
    const nextRetirement = destroy()
    retirement = nextRetirement
    return nextRetirement
  })

  return {
    acquire: vi.fn(),
    rebalance: vi.fn(),
    destroy,
    retire,
    retain: vi.fn(),
    release: vi.fn(),
    retireWhenReleased: vi.fn(() => retire()),
    getPoolStats: vi.fn().mockReturnValue(stats),
  }
}

function mockIdempotentRetire(pool: TestPool, startRetirement: () => Promise<void>): void {
  let retirement: Promise<void> | undefined
  pool.retire.mockImplementation(() => {
    retirement ??= startRetirement()
    return retirement
  })
}

const autoTeardownManagers: { destroyAll(): Promise<unknown> }[] = []

function createTestPoolManager(
  poolModule: PoolModule,
  makePool: (settings: PoolStrategySettings, created: readonly TestPool[]) => TestPool = () =>
    createTestPool()
) {
  const created: TestPool[] = []
  const poolManager = new (class extends poolModule.PoolManager {
    protected newPool(settings: PoolStrategySettings): PoolStrategy {
      const pool = makePool(settings, created)
      created.push(pool)
      return pool
    }
  })()
  autoTeardownManagers.push(poolManager)
  return { poolManager, created }
}

async function loadPoolModule(
  maxEntries?: number,
  configOverrides: Record<string, unknown> = {},
  beforePoolImport?: (metrics: MetricsModule) => void
): Promise<PoolModule> {
  vi.resetModules()

  const configModule = await import('../../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig({
    isMultitenant: true,
    ...(maxEntries === undefined ? {} : { tenantPoolCacheMaxEntries: maxEntries }),
    ...configOverrides,
  } as Parameters<typeof configModule.mergeConfig>[0])

  if (beforePoolImport) {
    beforePoolImport(await import('@internal/monitoring/metrics'))
  }

  return import('./pool')
}

describe('PoolManager cache lifecycle', () => {
  beforeAll(() => {
    vi.useFakeTimers()
  })

  beforeEach(() => {
    vi.clearAllTimers()
    vi.setSystemTime(1_000)
  })

  afterEach(async () => {
    for (const poolManager of autoTeardownManagers.splice(0)) {
      await poolManager.destroyAll()
    }
    vi.doUnmock('@internal/cache')
    vi.resetModules()
    vi.restoreAllMocks()
  })

  afterAll(() => {
    vi.useRealTimers()
  })

  test('retains inactive pools until LRU capacity evicts the least recent entry', async () => {
    const poolModule = await loadPoolModule(2)

    const { poolManager } = createTestPoolManager(poolModule)
    const first = poolManager.getPool(createPoolSettings('tenant-a'))
    const second = poolManager.getPool(createPoolSettings('tenant-b'))

    await vi.advanceTimersByTimeAsync(24 * 60 * 60 * 1000)
    expect(poolManager.getPool(createPoolSettings('tenant-a'))).toBe(first)
    expect(first.retire).not.toHaveBeenCalled()
    expect(second.retire).not.toHaveBeenCalled()

    poolManager.getPool(createPoolSettings('tenant-c'))

    expect(first.retire).not.toHaveBeenCalled()
    expect(second.retireWhenReleased).toHaveBeenCalledTimes(1)
    expect(second.retire).toHaveBeenCalledTimes(1)
  })

  test('rebalance does not promote pool LRU recency', async () => {
    const poolModule = await loadPoolModule(2)

    const { poolManager } = createTestPoolManager(poolModule)
    const first = poolManager.getPool(createPoolSettings('tenant-a'))
    const second = poolManager.getPool(createPoolSettings('tenant-b'))

    poolManager.rebalance('tenant-a', { maxConnections: 20 })
    poolManager.getPool(createPoolSettings('tenant-c'))

    expect(first.rebalance).toHaveBeenCalledWith({ maxConnections: 20 })
    expect(first.retireWhenReleased).toHaveBeenCalledTimes(1)
    expect(second.retireWhenReleased).not.toHaveBeenCalled()
  })

  test('keeps a real evicted strategy usable until every request lease is released', async () => {
    await loadPoolModule(1)
    const { PgPoolManager, PgTenantConnection } = await import('./pg-connection')
    const poolManager = new PgPoolManager()
    const tenantA = createPoolSettings('tenant-real-lease-a')
    const tenantB = createPoolSettings('tenant-real-lease-b')
    const strategyA = poolManager.getPool(tenantA)
    const firstConnection = new PgTenantConnection(strategyA, tenantA)
    const secondConnection = new PgTenantConnection(strategyA, tenantA)
    const physicalPool = (strategyA.acquire() as unknown as { pool: unknown }).pool
    const connectionB = new PgTenantConnection(poolManager.getPool(tenantB), tenantB)

    try {
      const retirement = strategyA.retireWhenReleased()
      let retirementSettled = false
      void retirement.then(() => {
        retirementSettled = true
      })

      expect(() => new PgTenantConnection(strategyA, tenantA)).toThrow(
        expect.objectContaining({
          code: 'InternalError',
          message: 'Cannot retain a retiring pool strategy',
        })
      )

      firstConnection.dispose()
      firstConnection.dispose()
      await Promise.resolve()

      expect(retirementSettled).toBe(false)
      expect((strategyA.acquire() as unknown as { pool: unknown }).pool).toBe(physicalPool)

      secondConnection.dispose()
      await retirement

      expect(() => strategyA.acquire()).toThrow(
        expect.objectContaining({
          code: 'InternalError',
          message: 'Cannot acquire from a retired pool strategy',
        })
      )
    } finally {
      firstConnection.dispose()
      secondConnection.dispose()
      connectionB.dispose()
      await poolManager.destroyAll()
    }
  })

  test('forces a real evicted strategy to retire on explicit invalidation', async () => {
    await loadPoolModule(1)
    const { PgPoolManager, PgTenantConnection } = await import('./pg-connection')
    const poolManager = new PgPoolManager()
    const tenantA = createPoolSettings('tenant-real-force-a')
    const tenantB = createPoolSettings('tenant-real-force-b')
    const strategyA = poolManager.getPool(tenantA)
    const connectionA = new PgTenantConnection(strategyA, tenantA)
    strategyA.acquire()
    const connectionB = new PgTenantConnection(poolManager.getPool(tenantB), tenantB)

    try {
      await poolManager.destroy(tenantA.tenantId)

      expect(() => strategyA.acquire()).toThrow(
        expect.objectContaining({
          code: 'InternalError',
          message: 'Cannot acquire from a retired pool strategy',
        })
      )
    } finally {
      connectionA.dispose()
      connectionB.dispose()
      await poolManager.destroyAll()
    }
  })

  test('keeps an evicted old-URL strategy isolated from the replacement URL', async () => {
    await loadPoolModule(1)
    const { PgPoolManager, PgTenantConnection } = await import('./pg-connection')
    const poolManager = new PgPoolManager()
    const tenantA = createPoolSettings('tenant-real-reconcile-a')
    const tenantB = createPoolSettings('tenant-real-reconcile-b')
    const strategyA = poolManager.getPool(tenantA)
    const connectionA = new PgTenantConnection(strategyA, tenantA)
    const oldPhysicalPool = (strategyA.acquire() as unknown as { pool: unknown }).pool
    const connectionB = new PgTenantConnection(poolManager.getPool(tenantB), tenantB)
    const updatedTenantA = {
      ...tenantA,
      dbUrl: 'postgres://new-host.example.test/new-database',
    }

    try {
      const updatedStrategy = poolManager.getPool(updatedTenantA)
      const updatedPhysicalPool = (
        updatedStrategy.acquire() as unknown as {
          pool: { options: { connectionString?: string } }
        }
      ).pool

      expect(updatedStrategy).not.toBe(strategyA)
      expect((strategyA.acquire() as unknown as { pool: unknown }).pool).toBe(oldPhysicalPool)
      expect(updatedPhysicalPool.options.connectionString).toBe(updatedTenantA.dbUrl)
    } finally {
      connectionA.dispose()
      connectionB.dispose()
      await poolManager.destroyAll()
    }
  })

  test('destroyAll waits for an explicit destroy already in flight', async () => {
    const poolModule = await loadPoolModule()
    const destroyDeferred = Promise.withResolvers<void>()

    const { poolManager, created } = createTestPoolManager(poolModule, () => {
      const pool = createTestPool()
      pool.destroy.mockReturnValue(destroyDeferred.promise)
      return pool
    })
    const tenantId = 'tenant-explicit-destroy-in-flight'
    poolManager.getPool(createPoolSettings(tenantId))

    const explicitDestroy = poolManager.destroy(tenantId)
    const shutdown = poolManager.destroyAll()
    let shutdownSettled = false
    void shutdown.then(() => {
      shutdownSettled = true
    })

    await Promise.resolve()
    expect(shutdownSettled).toBe(false)

    destroyDeferred.resolve()
    await explicitDestroy
    await expect(shutdown).resolves.toEqual([{ status: 'fulfilled', value: undefined }])
    expect(created[0].destroy).toHaveBeenCalledTimes(1)
  })

  test('destroy waits for every same-tenant teardown before propagating an error', async () => {
    const poolModule = await loadPoolModule()
    const firstDestroy = Promise.withResolvers<void>()
    const secondDestroy = Promise.withResolvers<void>()

    const { poolManager, created } = createTestPoolManager(poolModule, (_settings, pools) => {
      const pool = createTestPool()
      pool.destroy.mockReturnValue(
        pools.length === 0 ? firstDestroy.promise : secondDestroy.promise
      )
      return pool
    })
    const tenantId = 'tenant-overlapping-destroy'
    const settings = createPoolSettings(tenantId)
    poolManager.getPool(settings)

    const first = poolManager.destroy(tenantId)
    const firstResult = first.catch((error: unknown) => error)
    poolManager.getPool(settings)
    const second = poolManager.destroy(tenantId)
    let secondSettled = false
    void second.then(
      () => {
        secondSettled = true
      },
      () => {
        secondSettled = true
      }
    )

    firstDestroy.reject(new Error('first destroy failed'))
    await firstResult
    await Promise.resolve()
    expect(secondSettled).toBe(false)

    secondDestroy.resolve()
    await expect(second).rejects.toThrow('first destroy failed')
    expect(created[0].destroy).toHaveBeenCalledTimes(1)
    expect(created[1].destroy).toHaveBeenCalledTimes(1)
    expect(created[0].retire).toHaveBeenCalled()
    expect(created[1].retire).toHaveBeenCalled()
  })

  test('keeps tenant destroy waits isolated while shutdown snapshots every pending teardown', async () => {
    const poolModule = await loadPoolModule()
    const tenantA = 'tenant-indexed-destroy-a'
    const tenantB = 'tenant-indexed-destroy-b'
    const destroys = new Map([
      [tenantA, Promise.withResolvers<void>()],
      [tenantB, Promise.withResolvers<void>()],
    ])

    const { poolManager } = createTestPoolManager(poolModule, (settings) => {
      const pool = createTestPool()
      const destroy = destroys.get(settings.tenantId)
      if (!destroy) {
        throw new Error(`missing destroy resolver for ${settings.tenantId}`)
      }
      pool.destroy.mockReturnValue(destroy.promise)
      return pool
    })
    poolManager.getPool(createPoolSettings(tenantA))
    poolManager.getPool(createPoolSettings(tenantB))

    const destroyA = poolManager.destroy(tenantA)
    const destroyB = poolManager.destroy(tenantB)
    const shutdown = poolManager.destroyAll()
    let destroyBSettled = false
    let shutdownSettled = false
    void destroyB.then(() => {
      destroyBSettled = true
    })
    void shutdown.then(() => {
      shutdownSettled = true
    })

    destroys.get(tenantA)?.resolve()
    await destroyA

    expect(destroyBSettled).toBe(false)
    expect(shutdownSettled).toBe(false)

    destroys.get(tenantB)?.resolve()
    await destroyB
    await expect(shutdown).resolves.toEqual([
      { status: 'fulfilled', value: undefined },
      { status: 'fulfilled', value: undefined },
    ])
  })

  test('concurrent destroyAll calls wait for the same in-flight teardown', async () => {
    const poolModule = await loadPoolModule()
    const destroyDeferred = Promise.withResolvers<void>()

    const { poolManager, created } = createTestPoolManager(poolModule, () => {
      const pool = createTestPool()
      pool.destroy.mockReturnValue(destroyDeferred.promise)
      return pool
    })
    poolManager.getPool(createPoolSettings('tenant-concurrent-shutdown'))

    const firstShutdown = poolManager.destroyAll()
    const secondShutdown = poolManager.destroyAll()
    let secondSettled = false
    void secondShutdown.then(() => {
      secondSettled = true
    })

    await Promise.resolve()
    expect(secondSettled).toBe(false)

    destroyDeferred.resolve()
    await expect(firstShutdown).resolves.toEqual([{ status: 'fulfilled', value: undefined }])
    await expect(secondShutdown).resolves.toEqual([{ status: 'fulfilled', value: undefined }])
    expect(created[0].destroy).toHaveBeenCalledTimes(1)
  })

  test('does not create a timer for hot pool-cache hits', async () => {
    const poolModule = await loadPoolModule()

    const { poolManager } = createTestPoolManager(poolModule)
    const settings = createPoolSettings('tenant-hot-cache-entry')
    const first = poolManager.getPool(settings)
    const setTimeoutSpy = vi.spyOn(globalThis, 'setTimeout')

    setTimeoutSpy.mockClear()
    for (let index = 0; index < 1_000; index++) {
      expect(poolManager.getPool(settings)).toBe(first)
    }

    expect(setTimeoutSpy).not.toHaveBeenCalled()
  })

  test('keeps the single-tenant pool alive until explicit teardown', async () => {
    const poolModule = await loadPoolModule(undefined, { isMultitenant: false })

    const { poolManager, created } = createTestPoolManager(poolModule)
    const settings = createPoolSettings('single-tenant-cache-entry')
    const first = poolManager.getPool(settings)

    await vi.advanceTimersByTimeAsync(200)

    expect(poolManager.getPool(settings)).toBe(first)
    expect(created).toHaveLength(1)
    expect(first.retire).not.toHaveBeenCalled()
  })

  test('records logical pool cache misses and hits', async () => {
    const poolModule = await loadPoolModule()
    const metricsModule = await import('@internal/monitoring/metrics')
    const recordSpy = vi.spyOn(metricsModule, 'recordCacheRequest')

    const { poolManager, created } = createTestPoolManager(poolModule)
    const settings = createPoolSettings('tenant-cache-metrics')

    const first = poolManager.getPool(settings)
    const second = poolManager.getPool(settings)

    expect(second).toBe(first)
    expect(created).toHaveLength(1)
    expect(recordSpy.mock.calls).toEqual(
      expect.arrayContaining([
        [TENANT_POOL_CACHE_NAME, 'miss'],
        [TENANT_POOL_CACHE_NAME, 'hit'],
      ])
    )
  })

  test('shares cached tenant pools across manager instances', async () => {
    const poolModule = await loadPoolModule()

    const { poolManager: firstManager, created: firstCreated } = createTestPoolManager(poolModule)
    const { poolManager: secondManager, created: secondCreated } = createTestPoolManager(poolModule)
    const settings = createPoolSettings('tenant-shared-manager-cache')

    const first = firstManager.getPool(settings)
    const second = secondManager.getPool(settings)

    expect(second).toBe(first)
    expect(firstCreated).toHaveLength(1)
    expect(secondCreated).toHaveLength(0)
  })

  test('uses tenant and database URL as immutable strategy identity', async () => {
    const poolModule = await loadPoolModule()

    const { poolManager, created } = createTestPoolManager(poolModule)
    const originalSettings = createPoolSettings('tenant-pool-identity')
    const first = poolManager.getPool(originalSettings)
    const currentSettings = {
      ...originalSettings,
      dbUrl: 'postgres://moved',
      isExternalPool: true,
    }
    const second = poolManager.getPool(currentSettings)

    expect(second).not.toBe(first)
    expect(created).toHaveLength(2)
    expect(first.retire).not.toHaveBeenCalled()
    expect(poolManager.getPool(originalSettings)).toBe(first)
    expect(poolManager.getPool(currentSettings)).toBe(second)
    expect(created).toHaveLength(2)

    await poolManager.destroy(originalSettings.tenantId)
    expect(created[0].destroy).toHaveBeenCalledTimes(1)
    expect(created[1].destroy).toHaveBeenCalledTimes(1)
  })

  test('reuses same-URL strategies and applies capacity changes in place', async () => {
    const poolModule = await loadPoolModule()

    const { poolManager, created } = createTestPoolManager(poolModule)
    const settings = createPoolSettings('tenant-pool-budget')
    const pool = poolManager.getPool(settings)

    expect(
      poolManager.getPool({
        ...settings,
        maxConnections: 20,
        clusterSize: 4,
      })
    ).toBe(pool)
    expect(created).toHaveLength(1)
    expect(pool.rebalance).toHaveBeenCalledWith({ maxConnections: 20 })

    created[0].rebalance.mockClear()
    poolManager.rebalance(settings.tenantId, {
      maxConnections: 30,
      clusterSize: 5,
    })
    poolManager.rebalanceAll({ clusterSize: 6 })

    expect(created[0].rebalance.mock.calls).toEqual([
      [{ maxConnections: 30, clusterSize: 5 }],
      [{ clusterSize: 6 }],
    ])
  })

  test('destroyAll waits for a deferred capacity-eviction teardown', async () => {
    const poolModule = await loadPoolModule(1)
    const retirementRequested = Promise.withResolvers<void>()
    const evictedDestroy = Promise.withResolvers<void>()

    const { poolManager, created } = createTestPoolManager(poolModule, (_settings, pools) => {
      const pool = createTestPool()
      if (pools.length === 0) {
        pool.retireWhenReleased.mockReturnValue(retirementRequested.promise)
        pool.destroy.mockReturnValue(evictedDestroy.promise)
        mockIdempotentRetire(pool, () => {
          retirementRequested.resolve()
          return pool.destroy()
        })
      }
      return pool
    })
    poolManager.getPool(createPoolSettings('tenant-capacity-drain-a'))
    poolManager.getPool(createPoolSettings('tenant-capacity-drain-b'))

    expect(created[0].retireWhenReleased).toHaveBeenCalledTimes(1)
    expect(created[0].retire).not.toHaveBeenCalled()

    const shutdown = poolManager.destroyAll()
    let shutdownSettled = false
    void shutdown.then(() => {
      shutdownSettled = true
    })

    await Promise.resolve()
    expect(shutdownSettled).toBe(false)

    evictedDestroy.resolve()
    const results = await shutdown

    expect(created[0].destroy).toHaveBeenCalledTimes(1)
    expect(created[1].destroy).toHaveBeenCalledTimes(1)
    expect(created[0].retireWhenReleased).toHaveBeenCalledTimes(1)
    expect(created[0].retire).toHaveBeenCalledTimes(1)
    expect(created[1].retireWhenReleased).not.toHaveBeenCalled()
    expect(created[1].retire).toHaveBeenCalled()
    expect(results).toHaveLength(2)
  })

  test('logs a failed deferred retirement once without replaying it during shutdown', async () => {
    const poolModule = await loadPoolModule(1)
    const monitoringModule = await import('@internal/monitoring')
    const retirementError = new Error('capacity retirement failed')
    const logSpy = vi.spyOn(monitoringModule.logSchema, 'error').mockImplementation(() => undefined)

    const { poolManager } = createTestPoolManager(poolModule, (_settings, pools) => {
      const pool = createTestPool()
      if (pools.length === 0) {
        pool.retireWhenReleased.mockRejectedValue(retirementError)
      }
      return pool
    })
    poolManager.getPool(createPoolSettings('tenant-capacity-error-a'))
    poolManager.getPool(createPoolSettings('tenant-capacity-error-b'))

    await vi.waitFor(() => {
      expect(logSpy).toHaveBeenCalledWith(
        expect.anything(),
        'pool was not able to be destroyed',
        expect.objectContaining({ type: 'db', error: retirementError })
      )
    })

    await expect(poolManager.destroyAll()).resolves.toEqual([
      { status: 'fulfilled', value: undefined },
    ])
    expect(logSpy).toHaveBeenCalledTimes(1)
  })

  test('leaves an observed forced-retirement failure for the shutdown caller to report', async () => {
    const poolModule = await loadPoolModule(1)
    const monitoringModule = await import('@internal/monitoring')
    const retirementWaiter = Promise.withResolvers<void>()
    const forcedRetirement = Promise.withResolvers<void>()
    const retirementError = new Error('forced capacity retirement failed')
    const logSpy = vi.spyOn(monitoringModule.logSchema, 'error').mockImplementation(() => undefined)

    const { poolManager, created } = createTestPoolManager(poolModule, (_settings, pools) => {
      const pool = createTestPool()
      if (pools.length === 0) {
        pool.retireWhenReleased.mockReturnValue(retirementWaiter.promise)
        pool.destroy.mockReturnValue(forcedRetirement.promise)
        mockIdempotentRetire(pool, () => {
          const retirement = pool.destroy()
          void retirement.then(retirementWaiter.resolve, retirementWaiter.reject)
          return retirement
        })
      }
      return pool
    })
    poolManager.getPool(createPoolSettings('tenant-capacity-forced-error-a'))
    poolManager.getPool(createPoolSettings('tenant-capacity-forced-error-b'))

    const shutdown = poolManager.destroyAll()
    forcedRetirement.reject(retirementError)
    const results = await shutdown

    expect(results).toHaveLength(2)
    expect(results.filter((result) => result.status === 'rejected')).toEqual([
      { status: 'rejected', reason: retirementError },
    ])
    expect(logSpy).not.toHaveBeenCalled()
    expect(created[0].retire).toHaveBeenCalledTimes(1)
    expect(created[0].destroy).toHaveBeenCalledTimes(1)
  })

  test('explicit destroy forces a capacity-evicted strategy to retire', async () => {
    const poolModule = await loadPoolModule(1)
    const retirementRequested = Promise.withResolvers<void>()

    const { poolManager, created } = createTestPoolManager(poolModule, (_settings, pools) => {
      const pool = createTestPool()
      if (pools.length === 0) {
        pool.retireWhenReleased.mockReturnValue(retirementRequested.promise)
        mockIdempotentRetire(pool, () => {
          retirementRequested.resolve()
          return pool.destroy()
        })
      }
      return pool
    })
    const tenantId = 'tenant-capacity-explicit-destroy-a'
    poolManager.getPool(createPoolSettings(tenantId))
    poolManager.getPool(createPoolSettings('tenant-capacity-explicit-destroy-b'))

    expect(created[0].retireWhenReleased).toHaveBeenCalledTimes(1)
    expect(created[0].retire).not.toHaveBeenCalled()

    await poolManager.destroy(tenantId)

    expect(created[0].retire).toHaveBeenCalledTimes(1)
    expect(created[0].destroy).toHaveBeenCalledTimes(1)
  })

  test('records pool cache evictions when capacity removes cached pools', async () => {
    const poolModule = await loadPoolModule(1)
    const metricsModule = await import('@internal/monitoring/metrics')
    const evictionSpy = vi.spyOn(metricsModule, 'recordCacheEviction')

    const { poolManager, created } = createTestPoolManager(poolModule)
    poolManager.getPool(createPoolSettings('tenant-cache-capacity-eviction-a'))
    poolManager.getPool(createPoolSettings('tenant-cache-capacity-eviction-b'))

    expect(evictionSpy).toHaveBeenCalledWith(TENANT_POOL_CACHE_NAME)
    expect(created[0].destroy).toHaveBeenCalledTimes(1)
    expect(created[0].retireWhenReleased).toHaveBeenCalledTimes(1)
    expect(created[0].retire).toHaveBeenCalledTimes(1)
  })

  test('does not record pool cache evictions for explicit destroys', async () => {
    const poolModule = await loadPoolModule()
    const metricsModule = await import('@internal/monitoring/metrics')
    const evictionSpy = vi.spyOn(metricsModule, 'recordCacheEviction')

    const { poolManager, created } = createTestPoolManager(poolModule)
    poolManager.getPool(createPoolSettings('tenant-cache-explicit-destroy-a'))
    poolManager.getPool(createPoolSettings('tenant-cache-explicit-destroy-b'))

    await poolManager.destroy('tenant-cache-explicit-destroy-a')
    await poolManager.destroyAll()

    expect(evictionSpy.mock.calls.filter(([cache]) => cache === TENANT_POOL_CACHE_NAME)).toEqual([])
    expect(created[0].destroy).toHaveBeenCalledTimes(1)
    expect(created[1].destroy).toHaveBeenCalledTimes(1)
    expect(created[0].retireWhenReleased).not.toHaveBeenCalled()
    expect(created[1].retireWhenReleased).not.toHaveBeenCalled()
    expect(created[0].retire).toHaveBeenCalled()
    expect(created[1].retire).toHaveBeenCalled()
  })

  test('caches external pools across lookups and records miss then hit', async () => {
    const poolModule = await loadPoolModule()
    const metricsModule = await import('@internal/monitoring/metrics')
    const recordSpy = vi.spyOn(metricsModule, 'recordCacheRequest')

    const { poolManager, created } = createTestPoolManager(poolModule)
    const settings = {
      ...createPoolSettings('tenant-external-pool-cache'),
      isExternalPool: true,
    }

    const first = poolManager.getPool(settings)
    const second = poolManager.getPool(settings)

    expect(second).toBe(first)
    expect(created).toHaveLength(1)
    expect(recordSpy.mock.calls.filter(([cache]) => cache === TENANT_POOL_CACHE_NAME)).toEqual([
      [TENANT_POOL_CACHE_NAME, 'miss'],
      [TENANT_POOL_CACHE_NAME, 'hit'],
    ])
  })

  test('iterates cached pools for monitor snapshots', async () => {
    const poolModule = await loadPoolModule(undefined, {
      otelMetricsEnabled: true,
      prometheusMetricsEnabled: true,
    })
    const metricsModule = await import('@internal/monitoring/metrics')
    const batchObserver = captureBatchObserver(metricsModule)

    const { poolManager } = createTestPoolManager(poolModule, (settings) =>
      createTestPool(
        settings.tenantId === 'tenant-a' ? { used: 2, total: 5 } : { used: 3, total: 7 }
      )
    )
    const firstPool = poolManager.getPool(createPoolSettings('tenant-a'))
    poolManager.getPool(createPoolSettings('tenant-b'))

    poolManager.monitor()
    await vi.advanceTimersByTimeAsync(15_000)

    expect(batchObserver.spy.mock.calls[0]?.[1]).toEqual([
      metricsModule.dbActivePool,
      metricsModule.dbActiveConnection,
      metricsModule.dbInUseConnection,
      metricsModule.dbPoolsPendingRetirement,
      metricsModule.dbPoolOldestPendingRetirementAge,
    ])

    const observeSpy = vi.fn()
    batchObserver.observe(observeSpy)

    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbActivePool, 2)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbActiveConnection, 12)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbInUseConnection, 5)

    await vi.advanceTimersByTimeAsync(20_000)

    expect(poolManager.getPool(createPoolSettings('tenant-a'))).toBe(firstPool)
    expect(firstPool.retire).not.toHaveBeenCalled()
  })

  test('includes lease-deferred evictions in live connection snapshots', async () => {
    const poolModule = await loadPoolModule(1, {
      otelMetricsEnabled: true,
      prometheusMetricsEnabled: true,
    })
    const metricsModule = await import('@internal/monitoring/metrics')
    const batchObserver = captureBatchObserver(metricsModule)
    const retirementRequested = Promise.withResolvers<void>()

    const { poolManager } = createTestPoolManager(poolModule, (_settings, pools) => {
      const pool = createTestPool(
        pools.length === 0 ? { used: 2, total: 5 } : { used: 3, total: 7 }
      )
      if (pools.length === 0) {
        pool.retireWhenReleased.mockReturnValue(retirementRequested.promise)
        mockIdempotentRetire(pool, () => {
          retirementRequested.resolve()
          return pool.destroy()
        })
      }
      return pool
    })
    poolManager.getPool(createPoolSettings('tenant-deferred-stats-a'))
    poolManager.getPool(createPoolSettings('tenant-deferred-stats-b'))
    poolManager.monitor()

    await vi.advanceTimersByTimeAsync(15_000)

    const observeSpy = vi.fn()
    batchObserver.observe(observeSpy)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbActivePool, 2)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbActiveConnection, 12)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbInUseConnection, 5)
  })

  test('observes pending retirement count and oldest age from lifecycle state', async () => {
    const poolModule = await loadPoolModule(1, {
      otelMetricsEnabled: true,
      prometheusMetricsEnabled: true,
    })
    const metricsModule = await import('@internal/monitoring/metrics')
    const batchObserver = captureBatchObserver(metricsModule)

    const firstRetirement = Promise.withResolvers<void>()
    const secondRetirement = Promise.withResolvers<void>()
    const retirements = [firstRetirement, secondRetirement]
    let now = 0
    vi.spyOn(performance, 'now').mockImplementation(() => now)

    const { poolManager, created } = createTestPoolManager(poolModule, (_settings, pools) => {
      const pool = createTestPool({ used: 1, total: 2 })
      const retirement = retirements[pools.length]
      if (retirement) {
        pool.retireWhenReleased.mockReturnValue(retirement.promise)
      }
      return pool
    })

    metricsModule.setMetricsEnabled([
      { name: 'db_active_local_pools', enabled: false },
      { name: 'db_connections', enabled: false },
      { name: 'db_connections_in_use', enabled: false },
    ])
    poolManager.getPool(createPoolSettings('tenant-retirement-age-a'))
    poolManager.getPool(createPoolSettings('tenant-retirement-age-b'))
    poolManager.monitor()

    const observeSpy = vi.fn()
    batchObserver.observe(observeSpy)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbPoolsPendingRetirement, 1)

    now = 5_000
    poolManager.getPool(createPoolSettings('tenant-retirement-age-c'))
    now = 7_000

    observeSpy.mockClear()
    batchObserver.observe(observeSpy)

    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbPoolsPendingRetirement, 2)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbPoolOldestPendingRetirementAge, 7)
    expect(created.every((pool) => pool.getPoolStats.mock.calls.length === 0)).toBe(true)

    firstRetirement.resolve()
    await firstRetirement.promise
    observeSpy.mockClear()
    batchObserver.observe(observeSpy)

    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbPoolsPendingRetirement, 1)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbPoolOldestPendingRetirementAge, 2)

    secondRetirement.resolve()
    await secondRetirement.promise
    observeSpy.mockClear()
    batchObserver.observe(observeSpy)

    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbPoolsPendingRetirement, 0)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbPoolOldestPendingRetirementAge, 0)
  })

  test('collects stable pool stats while cache hits reorder the LRU', async () => {
    const poolModule = await loadPoolModule(undefined, {
      otelMetricsEnabled: true,
      prometheusMetricsEnabled: true,
    })
    const metricsModule = await import('@internal/monitoring/metrics')
    const batchObserver = captureBatchObserver(metricsModule)

    let reordered = false
    const { poolManager } = createTestPoolManager(poolModule, (settings) => {
      const pool = createTestPool({ used: 1, total: 1 })

      if (settings.tenantId === 'tenant-c') {
        pool.getPoolStats.mockImplementation(() => {
          if (!reordered) {
            reordered = true
            poolManager.getPool(createPoolSettings('tenant-c'))
          }
          return { used: 1, total: 1 }
        })
      }

      return pool
    })
    for (const tenantId of ['tenant-a', 'tenant-b', 'tenant-c', 'tenant-d']) {
      poolManager.getPool(createPoolSettings(tenantId))
    }

    poolManager.monitor()
    await vi.advanceTimersByTimeAsync(15_000)

    const observeSpy = vi.fn()
    batchObserver.observe(observeSpy)

    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbActivePool, 4)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbActiveConnection, 4)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbInUseConnection, 4)
  })

  test('collects pool stats only while an exporter and a pool gauge are enabled', async () => {
    const poolModule = await loadPoolModule(undefined, {
      otelMetricsEnabled: true,
      prometheusMetricsEnabled: true,
    })
    const metricsModule = await import('@internal/monitoring/metrics')

    const { poolManager, created } = createTestPoolManager(poolModule, () =>
      createTestPool({ used: 1, total: 2 })
    )
    poolManager.getPool(createPoolSettings('tenant-gated-pool-stats'))
    metricsModule.setMetricsEnabled([
      { name: 'db_active_local_pools', enabled: false },
      { name: 'db_connections', enabled: false },
      { name: 'db_connections_in_use', enabled: false },
    ])

    poolManager.monitor()
    await vi.advanceTimersByTimeAsync(15_000)

    expect(created[0].getPoolStats).not.toHaveBeenCalled()

    metricsModule.setMetricsEnabled([{ name: 'db_connections', enabled: true }])
    await vi.advanceTimersByTimeAsync(15_000)

    expect(created[0].getPoolStats).toHaveBeenCalledTimes(1)
  })

  test('observes tenant pool LRU entries without scanning pool strategies', async () => {
    let metricsModule: MetricsModule | undefined
    let batchObserver: ReturnType<typeof captureBatchObserver> | undefined
    const poolModule = await loadPoolModule(2, {}, (loadedMetrics) => {
      metricsModule = loadedMetrics
      batchObserver = captureBatchObserver(loadedMetrics, loadedMetrics.cacheEntries)
    })

    if (!metricsModule) {
      throw new Error('metrics module was not loaded')
    }

    const { poolManager, created } = createTestPoolManager(poolModule, () =>
      createTestPool({ used: 1, total: 2 })
    )
    poolManager.getPool(createPoolSettings('tenant-entries-a'))
    poolManager.getPool(createPoolSettings('tenant-entries-b'))
    poolManager.getPool(createPoolSettings('tenant-entries-c'))
    metricsModule.setMetricsEnabled([
      { name: 'cache_entries', enabled: true },
      { name: 'db_active_local_pools', enabled: false },
      { name: 'db_connections', enabled: false },
      { name: 'db_connections_in_use', enabled: false },
    ])

    const observeSpy = vi.fn()
    batchObserver?.observe(observeSpy)

    expect(observeSpy).toHaveBeenCalledWith(metricsModule.cacheEntries, 2, {
      cache: TENANT_POOL_CACHE_NAME,
    })
    expect(created.every((pool) => pool.getPoolStats.mock.calls.length === 0)).toBe(true)

    await poolManager.destroy('tenant-entries-b')
    observeSpy.mockClear()
    batchObserver?.observe(observeSpy)

    expect(observeSpy).toHaveBeenCalledWith(metricsModule.cacheEntries, 1, {
      cache: TENANT_POOL_CACHE_NAME,
    })
    expect(created.every((pool) => pool.getPoolStats.mock.calls.length === 0)).toBe(true)
  })

  test('does not collect pool stats without a metrics exporter', async () => {
    const poolModule = await loadPoolModule(undefined, {
      otelMetricsEnabled: false,
      // This flag alone does not create the Prometheus reader; it is nested
      // under the OTel provider gate in otel-metrics.ts.
      prometheusMetricsEnabled: true,
    })

    const { poolManager, created } = createTestPoolManager(poolModule, () =>
      createTestPool({ used: 1, total: 2 })
    )
    poolManager.getPool(createPoolSettings('tenant-pool-stats-without-exporter'))
    const setIntervalSpy = vi.spyOn(globalThis, 'setInterval')
    poolManager.monitor()

    expect(setIntervalSpy).not.toHaveBeenCalled()

    await vi.advanceTimersByTimeAsync(15_000)

    expect(created[0].getPoolStats).not.toHaveBeenCalled()
  })

  test('does not start pool stats without an attached metrics reader', async () => {
    const poolModule = await loadPoolModule(undefined, {
      otelMetricsEnabled: true,
      otlpMetricsEndpoint: undefined,
      prometheusMetricsEnabled: false,
    })

    const { poolManager } = createTestPoolManager(poolModule, () =>
      createTestPool({ used: 1, total: 2 })
    )
    poolManager.getPool(createPoolSettings('tenant-pool-stats-without-reader'))
    const setIntervalSpy = vi.spyOn(globalThis, 'setInterval')

    poolManager.monitor()

    expect(setIntervalSpy).not.toHaveBeenCalled()
  })

  test('starts pool stats for an attached OTLP metrics reader', async () => {
    const poolModule = await loadPoolModule(undefined, {
      otelMetricsEnabled: true,
      otlpMetricsEndpoint: 'http://otel-collector:4317',
      prometheusMetricsEnabled: false,
    })

    const { poolManager } = createTestPoolManager(poolModule, () =>
      createTestPool({ used: 1, total: 2 })
    )
    const setIntervalSpy = vi.spyOn(globalThis, 'setInterval')

    poolManager.monitor()

    expect(setIntervalSpy).toHaveBeenCalledTimes(1)
  })

  test('starts pool monitoring once and unregisters it during teardown', async () => {
    const poolModule = await loadPoolModule(undefined, {
      otelMetricsEnabled: true,
      prometheusMetricsEnabled: true,
    })
    const metricsModule = await import('@internal/monitoring/metrics')
    const addBatchObservableCallbackSpy = vi.spyOn(
      metricsModule.meter,
      'addBatchObservableCallback'
    )
    const removeBatchObservableCallbackSpy = vi.spyOn(
      metricsModule.meter,
      'removeBatchObservableCallback'
    )
    const setIntervalSpy = vi.spyOn(globalThis, 'setInterval')
    const clearIntervalSpy = vi.spyOn(globalThis, 'clearInterval')

    const { poolManager } = createTestPoolManager(poolModule)
    poolManager.monitor()
    poolManager.monitor()

    expect(setIntervalSpy).toHaveBeenCalledTimes(1)
    expect(addBatchObservableCallbackSpy).toHaveBeenCalledTimes(1)
    const [callback, observables] = addBatchObservableCallbackSpy.mock.calls[0]

    await poolManager.destroyAll()

    expect(clearIntervalSpy).toHaveBeenCalledTimes(1)
    expect(removeBatchObservableCallbackSpy).toHaveBeenCalledWith(callback, observables)

    poolManager.monitor()
    expect(setIntervalSpy).toHaveBeenCalledTimes(2)
    expect(addBatchObservableCallbackSpy).toHaveBeenCalledTimes(2)
  })

  test('iterates cached pools for rebalanceAll and destroyAll', async () => {
    const poolModule = await loadPoolModule()

    const { poolManager, created } = createTestPoolManager(poolModule)
    const first = poolManager.getPool(createPoolSettings('tenant-c'))
    const second = poolManager.getPool(createPoolSettings('tenant-d'))

    poolManager.rebalanceAll({ clusterSize: 4 })

    expect(first.rebalance).toHaveBeenCalledWith({ clusterSize: 4 })
    expect(second.rebalance).toHaveBeenCalledWith({ clusterSize: 4 })

    await poolManager.destroyAll()

    expect(created[0].destroy).toHaveBeenCalledTimes(1)
    expect(created[1].destroy).toHaveBeenCalledTimes(1)

    const recreated = poolManager.getPool(createPoolSettings('tenant-c'))

    expect(recreated).not.toBe(first)
  })

  test('passes all tenant rebalance options to the cached pool', async () => {
    const poolModule = await loadPoolModule()

    const { poolManager } = createTestPoolManager(poolModule)
    const pool = poolManager.getPool(createPoolSettings('tenant-rebalance-options'))

    poolManager.rebalance('tenant-rebalance-options', {
      clusterSize: 3,
      maxConnections: 14,
    })

    expect(pool.rebalance).toHaveBeenCalledWith({
      clusterSize: 3,
      maxConnections: 14,
    })
  })

  test('propagates explicit destroy failures without double-destroying pools', async () => {
    const poolModule = await loadPoolModule()

    const { poolManager, created } = createTestPoolManager(poolModule, (settings) => {
      const pool = createTestPool()
      pool.destroy.mockRejectedValue(new Error(`destroy failed for ${settings.tenantId}`))
      return pool
    })
    const tenantId = 'tenant-destroy-error'

    poolManager.getPool(createPoolSettings(tenantId))

    await expect(poolManager.destroy(tenantId)).rejects.toThrow(`destroy failed for ${tenantId}`)
    expect(created[0].destroy).toHaveBeenCalledTimes(1)
  })

  test('preserves rejected destroyAll settlements when pool teardown fails', async () => {
    const poolModule = await loadPoolModule()

    const { poolManager, created } = createTestPoolManager(poolModule, (settings) => {
      const pool = createTestPool()

      if (settings.tenantId === 'tenant-destroyall-error') {
        pool.destroy.mockRejectedValue(new Error('destroyAll failed'))
      }

      return pool
    })
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
    expect(created[0].destroy).toHaveBeenCalledTimes(1)
    expect(created[1].destroy).toHaveBeenCalledTimes(1)
  })
})
