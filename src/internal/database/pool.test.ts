import { TENANT_POOL_CACHE_NAME } from '@internal/cache/names'
import { Knex } from 'knex'
import { type Mock, vi } from 'vitest'
import type { PoolStrategy, TenantConnectionOptions } from './pool'

type TestPool = {
  acquire: Mock
  rebalance: Mock
  destroy: Mock<() => Promise<void>>
  getPoolStats: Mock
}

type PoolModule = typeof import('./pool')

// Mirrors enough of tarn.js's Pool internals for tests to read state and stub IO
// without standing up a real database. Names match tarn 3.0.2.
type TarnPoolLike = {
  max: number
  min: number
  creator: (...args: unknown[]) => Promise<unknown>
  destroyer: (resource: unknown) => Promise<void>
  validate: (resource: unknown) => boolean | Promise<boolean>
  acquire: () => { promise: Promise<unknown>; abort: () => void }
  release: (resource: unknown) => boolean
  numUsed: () => number
  numFree: () => number
  numPendingAcquires: () => number
  numPendingValidations: () => number
  numPendingCreates: () => number
  _tryAcquireOrCreate: () => void
}

function isTenantPoolCacheLookupCall(message: string) {
  return (call: unknown[]) => call[1] === message
}

function createPoolSettings(tenantId: string) {
  return {
    tenantId,
    dbUrl: 'postgres://example',
    maxConnections: 10,
    user: { jwt: 'jwt', payload: { role: 'authenticated' } },
    superUser: { jwt: 'service', payload: { role: 'service_role' } },
  }
}

function getTarnPool(knex: Knex): TarnPoolLike {
  return knex.client.pool as unknown as TarnPoolLike
}

/**
 * Replace the tarn pool's create/destroy/validate so we can exercise the real
 * acquire flow under fake timers without touching a database.
 */
function stubTarnIo(knex: Knex): TarnPoolLike {
  const tarnPool = getTarnPool(knex)
  let counter = 0
  tarnPool.creator = () => Promise.resolve({ id: ++counter })
  tarnPool.destroyer = () => Promise.resolve()
  tarnPool.validate = () => true
  return tarnPool
}

function createTestPool(stats: { used: number; total: number } | null = null): TestPool {
  return {
    acquire: vi.fn(),
    rebalance: vi.fn(),
    destroy: vi.fn().mockResolvedValue(undefined),
    getPoolStats: vi.fn().mockReturnValue(stats),
  }
}

async function loadPoolModule(
  ttlMs: number,
  maxEntries?: number,
  configOverrides: Record<string, unknown> = {}
): Promise<PoolModule> {
  vi.resetModules()

  const configModule = await import('../../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig({
    isMultitenant: true,
    ...configOverrides,
  } as Parameters<typeof configModule.mergeConfig>[0])

  const cacheOptionOverrides = {
    ttl: ttlMs,
    ...(maxEntries === undefined ? {} : { max: maxEntries }),
  }

  vi.doMock('@internal/cache', async () => {
    const actual = await vi.importActual<typeof import('@internal/cache')>('@internal/cache')

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

  return import('./pool')
}

async function loadPoolModuleWithConfig(
  configOverrides: Record<string, unknown> = {}
): Promise<PoolModule> {
  vi.resetModules()
  vi.doUnmock('@internal/cache')

  const configModule = await import('../../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig({
    isMultitenant: true,
    ...configOverrides,
  } as Parameters<typeof configModule.mergeConfig>[0])

  return import('./pool')
}

describe('PoolManager cache lifecycle', () => {
  beforeAll(() => {
    vi.useFakeTimers()
  })

  beforeEach(() => {
    vi.clearAllTimers()
    vi.setSystemTime(0)
  })

  afterEach(() => {
    vi.doUnmock('@internal/cache')
    vi.resetModules()
    vi.restoreAllMocks()
  })

  afterAll(() => {
    vi.useRealTimers()
  })

  test('expires cached pools and disposes them after inactivity', async () => {
    const poolModule = await loadPoolModule(20)

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool: TestPool = {
          acquire: vi.fn(),
          rebalance: vi.fn(),
          destroy: vi.fn().mockResolvedValue(undefined),
          getPoolStats: vi.fn().mockReturnValue(null),
        }
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('tenant-a')

    const first = poolManager.getPool(settings)

    expect(poolManager.created).toHaveLength(1)

    await vi.advanceTimersByTimeAsync(40)

    expect(poolManager.created[0].destroy).toHaveBeenCalledTimes(1)

    const second = poolManager.getPool(settings)

    expect(second).not.toBe(first)
    expect(poolManager.created).toHaveLength(2)

    await poolManager.destroyAll()
  })

  test('uses the configured tenant pool cache ttl', async () => {
    const poolModule = await loadPoolModuleWithConfig({
      tenantPoolCacheTtlMs: 20,
    })

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('tenant-configured-cache-ttl')

    const first = poolManager.getPool(settings)

    await vi.advanceTimersByTimeAsync(40)

    expect(poolManager.created[0].destroy).toHaveBeenCalledTimes(1)
    expect(poolManager.getPool(settings)).not.toBe(first)
    expect(poolManager.created).toHaveLength(2)

    await poolManager.destroyAll()
  })

  test('refreshes pool ttl when an existing pool is reused', async () => {
    const poolModule = await loadPoolModule(25)

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool: TestPool = {
          acquire: vi.fn(),
          rebalance: vi.fn(),
          destroy: vi.fn().mockResolvedValue(undefined),
          getPoolStats: vi.fn().mockReturnValue(null),
        }
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('tenant-b')

    const first = poolManager.getPool(settings)

    await vi.advanceTimersByTimeAsync(15)

    const reused = poolManager.getPool(settings)

    expect(reused).toBe(first)
    expect(poolManager.created[0].destroy).not.toHaveBeenCalled()

    await vi.advanceTimersByTimeAsync(15)

    expect(poolManager.created[0].destroy).not.toHaveBeenCalled()

    await vi.advanceTimersByTimeAsync(40)

    expect(poolManager.created[0].destroy).toHaveBeenCalledTimes(1)

    await poolManager.destroyAll()
  })

  test('records logical pool cache misses and hits', async () => {
    const poolModule = await loadPoolModule(10_000)
    const metricsModule = await import('@internal/monitoring/metrics')
    const addSpy = vi.spyOn(metricsModule.cacheRequestsTotal, 'add')

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

  test('logs sampled tenant pool cache misses and hits', async () => {
    const poolModule = await loadPoolModule(10_000, undefined, {
      tenantPoolCacheHitLogSampleRate: 1,
      tenantPoolCacheMissLogSampleRate: 1,
    })
    const loggerModule = await import('@internal/monitoring/logger')
    const infoSpy = vi.spyOn(loggerModule.logger, 'info').mockImplementation(() => undefined)
    const logSchemaInfoSpy = vi.spyOn(loggerModule.logSchema, 'info')

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('tenant-cache-lookup-logs')

    const first = poolManager.getPool(settings)
    const second = poolManager.getPool(settings)

    const expectedMissLog = expect.objectContaining({
      type: poolModule.TENANT_POOL_CACHE_LOOKUP_LOG_TYPE,
      cache: TENANT_POOL_CACHE_NAME,
      tenantId: 'tenant-cache-lookup-logs',
      project: 'tenant-cache-lookup-logs',
      outcome: 'miss',
      sampleRate: 1,
      sampleWeight: 1,
      isCacheable: true,
      isExternalPool: false,
      isSingleUse: false,
    })
    const expectedHitLog = expect.objectContaining({
      type: poolModule.TENANT_POOL_CACHE_LOOKUP_LOG_TYPE,
      cache: TENANT_POOL_CACHE_NAME,
      tenantId: 'tenant-cache-lookup-logs',
      project: 'tenant-cache-lookup-logs',
      outcome: 'hit',
      sampleRate: 1,
      sampleWeight: 1,
      isCacheable: true,
      isExternalPool: false,
      isSingleUse: false,
    })

    expect(second).toBe(first)
    expect(poolManager.created).toHaveLength(1)
    expect(logSchemaInfoSpy.mock.calls).toEqual(
      expect.arrayContaining([
        [loggerModule.logger, poolModule.TENANT_POOL_CACHE_LOOKUP_LOG_MESSAGE, expectedMissLog],
        [loggerModule.logger, poolModule.TENANT_POOL_CACHE_LOOKUP_LOG_MESSAGE, expectedHitLog],
      ])
    )
    expect(infoSpy.mock.calls).toEqual(
      expect.arrayContaining([
        [expectedMissLog, poolModule.TENANT_POOL_CACHE_LOOKUP_LOG_MESSAGE],
        [expectedHitLog, poolModule.TENANT_POOL_CACHE_LOOKUP_LOG_MESSAGE],
      ])
    )

    await poolManager.destroyAll()
  })

  test('does not log tenant pool cache lookups by default', async () => {
    const poolModule = await loadPoolModule(10_000)
    const loggerModule = await import('@internal/monitoring/logger')
    const infoSpy = vi.spyOn(loggerModule.logger, 'info').mockImplementation(() => undefined)

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('tenant-cache-lookup-logs-disabled')

    poolManager.getPool(settings)
    poolManager.getPool(settings)

    expect(
      infoSpy.mock.calls.filter(
        isTenantPoolCacheLookupCall(poolModule.TENANT_POOL_CACHE_LOOKUP_LOG_MESSAGE)
      )
    ).toEqual([])

    await poolManager.destroyAll()
  })

  test('does not log tenant pool cache lookups when sample rates are explicitly disabled', async () => {
    const poolModule = await loadPoolModule(10_000, undefined, {
      tenantPoolCacheHitLogSampleRate: 0,
      tenantPoolCacheMissLogSampleRate: 0,
    })
    const loggerModule = await import('@internal/monitoring/logger')
    const infoSpy = vi.spyOn(loggerModule.logger, 'info').mockImplementation(() => undefined)

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('tenant-cache-lookup-logs-explicitly-disabled')

    poolManager.getPool(settings)
    poolManager.getPool(settings)

    expect(
      infoSpy.mock.calls.filter(
        isTenantPoolCacheLookupCall(poolModule.TENANT_POOL_CACHE_LOOKUP_LOG_MESSAGE)
      )
    ).toEqual([])

    await poolManager.destroyAll()
  })

  test('does not log single-use external pool lookups without a cached pool', async () => {
    const poolModule = await loadPoolModule(10_000)
    const loggerModule = await import('@internal/monitoring/logger')
    const infoSpy = vi.spyOn(loggerModule.logger, 'info').mockImplementation(() => undefined)

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []

      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const pool = poolManager.getPool({
      ...createPoolSettings('tenant-single-use-external-log'),
      isSingleUse: true,
      isExternalPool: true,
    })

    expect(
      infoSpy.mock.calls.filter(
        isTenantPoolCacheLookupCall(poolModule.TENANT_POOL_CACHE_LOOKUP_LOG_MESSAGE)
      )
    ).toEqual([])

    await pool.destroy()
    await poolManager.destroyAll()
  })

  test('records pool cache evictions when inactivity ttl removes cached pools', async () => {
    const poolModule = await loadPoolModule(20)
    const metricsModule = await import('@internal/monitoring/metrics')
    const evictionSpy = vi.spyOn(metricsModule.cacheEvictionsTotal, 'add')

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

    await vi.advanceTimersByTimeAsync(40)

    expect(evictionSpy).toHaveBeenCalledWith(1, {
      cache: TENANT_POOL_CACHE_NAME,
    })
    expect(poolManager.created[0].destroy).toHaveBeenCalledTimes(1)

    await poolManager.destroyAll()
  })

  test('records pool cache evictions when capacity removes cached pools', async () => {
    const poolModule = await loadPoolModule(10_000, 1)
    const metricsModule = await import('@internal/monitoring/metrics')
    const evictionSpy = vi.spyOn(metricsModule.cacheEvictionsTotal, 'add')

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
    const metricsModule = await import('@internal/monitoring/metrics')
    const evictionSpy = vi.spyOn(metricsModule.cacheEvictionsTotal, 'add')

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
    const metricsModule = await import('@internal/monitoring/metrics')
    const addSpy = vi.spyOn(metricsModule.cacheRequestsTotal, 'add')

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
    const metricsModule = await import('@internal/monitoring/metrics')
    const addSpy = vi.spyOn(metricsModule.cacheRequestsTotal, 'add')

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
    const poolModule = await loadPoolModule(10_000)
    const metricsModule = await import('@internal/monitoring/metrics')
    const addBatchObservableCallbackSpy = vi.spyOn(
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
    const firstPool = poolManager.getPool(createPoolSettings('tenant-a'))
    poolManager.getPool(createPoolSettings('tenant-b'))

    poolManager.monitor()
    await vi.advanceTimersByTimeAsync(5_000)

    const observeSpy = vi.fn()
    batchObserver?.({ observe: observeSpy })

    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbActivePool, 2)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbActiveConnection, 12)
    expect(observeSpy).toHaveBeenCalledWith(metricsModule.dbInUseConnection, 5)

    await vi.advanceTimersByTimeAsync(20_000)

    const recreatedPool = poolManager.getPool(createPoolSettings('tenant-a'))

    expect(recreatedPool).not.toBe(firstPool)
    await vi.waitFor(() => {
      expect(firstPool.destroy).toHaveBeenCalledTimes(1)
    })

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

  test('updates cached tenant pool max connections in place after rebalance', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const pool = poolManager.getPool(createPoolSettings('tenant-max-connections-rebalance'))

    try {
      const originalKnex = pool.acquire()
      const destroySpy = vi.spyOn(originalKnex, 'destroy')
      expect((originalKnex.client.pool as { max: number }).max).toBe(10)

      pool.rebalance({ maxConnections: 14 })

      const rebalancedKnex = pool.acquire()
      expect(rebalancedKnex).toBe(originalKnex)
      expect(destroySpy).not.toHaveBeenCalled()
      expect((rebalancedKnex.client.pool as { max: number }).max).toBe(14)

      pool.rebalance({ clusterSize: 2 })

      expect(pool.acquire()).toBe(originalKnex)
      expect(destroySpy).not.toHaveBeenCalled()
      expect((originalKnex.client.pool as { max: number }).max).toBe(7)
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('scales max connections down in place without destroying the Knex instance', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const pool = poolManager.getPool({
      ...createPoolSettings('rebalance-scale-down-max'),
      maxConnections: 20,
    })

    try {
      const knex = pool.acquire()
      const destroySpy = vi.spyOn(knex, 'destroy')
      const tarnPool = getTarnPool(knex)

      expect(tarnPool.max).toBe(20)

      pool.rebalance({ maxConnections: 4 })

      expect(pool.acquire()).toBe(knex)
      expect(tarnPool.max).toBe(4)
      expect(tarnPool.min).toBe(0)
      expect(destroySpy).not.toHaveBeenCalled()
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('scales max up when clusterSize shrinks', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const pool = poolManager.getPool({
      ...createPoolSettings('rebalance-cluster-shrink'),
      maxConnections: 12,
      clusterSize: 4,
    })

    try {
      const knex = pool.acquire()
      const destroySpy = vi.spyOn(knex, 'destroy')
      const tarnPool = getTarnPool(knex)

      expect(tarnPool.max).toBe(3) // ceil(12 / 4)

      pool.rebalance({ clusterSize: 2 })

      expect(pool.acquire()).toBe(knex)
      expect(tarnPool.max).toBe(6) // ceil(12 / 2)
      expect(destroySpy).not.toHaveBeenCalled()
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('applies clusterSize and maxConnections together in a single rebalance', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const pool = poolManager.getPool({
      ...createPoolSettings('rebalance-combined'),
      maxConnections: 10,
      clusterSize: 1,
    })

    try {
      const knex = pool.acquire()
      const tarnPool = getTarnPool(knex)
      expect(tarnPool.max).toBe(10)

      pool.rebalance({ maxConnections: 30, clusterSize: 3 })

      expect(pool.acquire()).toBe(knex)
      expect(tarnPool.max).toBe(10) // ceil(30 / 3)
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('treats an empty rebalance call as a no-op', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const pool = poolManager.getPool({
      ...createPoolSettings('rebalance-empty'),
      maxConnections: 8,
    })

    try {
      const knex = pool.acquire()
      const tarnPool = getTarnPool(knex)
      const tryAcquireSpy = vi.spyOn(tarnPool, '_tryAcquireOrCreate')

      pool.rebalance({})

      expect(tarnPool.max).toBe(8)
      expect(tryAcquireSpy).not.toHaveBeenCalled()
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('treats clusterSize=0 as skipping the cluster dimension', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const pool = poolManager.getPool({
      ...createPoolSettings('rebalance-cluster-zero'),
      maxConnections: 6,
      clusterSize: 2,
    })

    try {
      const knex = pool.acquire()
      const tarnPool = getTarnPool(knex)
      expect(tarnPool.max).toBe(3) // ceil(6 / 2)

      // clusterSize=0 is ignored — the previous clusterSize (2) is preserved
      pool.rebalance({ clusterSize: 0 })
      expect(tarnPool.max).toBe(3)

      // ...and a maxConnections change in the same call still applies against
      // the preserved clusterSize.
      pool.rebalance({ clusterSize: 0, maxConnections: 8 })
      expect(tarnPool.max).toBe(4) // ceil(8 / 2)
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('is safe when called before the underlying Knex pool exists', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const pool = poolManager.getPool({
      ...createPoolSettings('rebalance-no-pool'),
      maxConnections: 10,
    })

    try {
      // No acquire yet — TenantPool.pool is undefined. The rebalance should
      // still update the in-memory settings without throwing.
      expect(() => pool.rebalance({ maxConnections: 30 })).not.toThrow()

      const knex = pool.acquire()
      expect(getTarnPool(knex).max).toBe(30)
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('keeps min at 0 across rebalances', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const pool = poolManager.getPool({
      ...createPoolSettings('rebalance-min'),
      maxConnections: 10,
    })

    try {
      const knex = pool.acquire()
      const tarnPool = getTarnPool(knex)

      expect(tarnPool.min).toBe(0)

      pool.rebalance({ maxConnections: 1 })
      expect(tarnPool.min).toBe(0)

      pool.rebalance({ maxConnections: 50 })
      expect(tarnPool.min).toBe(0)

      // ceil(50 / 100) = 1 — verifies that even pushing toward 1 doesn't bump min.
      pool.rebalance({ clusterSize: 100 })
      expect(tarnPool.min).toBe(0)
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('invokes the tarn _tryAcquireOrCreate hook after mutating max', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const pool = poolManager.getPool({
      ...createPoolSettings('rebalance-tryacquire-hook'),
      maxConnections: 10,
    })

    try {
      const knex = pool.acquire()
      const tarnPool = getTarnPool(knex)
      const tryAcquireSpy = vi.spyOn(tarnPool, '_tryAcquireOrCreate')

      pool.rebalance({ maxConnections: 15 })
      expect(tryAcquireSpy).toHaveBeenCalledTimes(1)

      pool.rebalance({ clusterSize: 3 })
      expect(tryAcquireSpy).toHaveBeenCalledTimes(2)

      // No-op rebalances should NOT poke the tarn hook.
      pool.rebalance({})
      pool.rebalance({ clusterSize: 0 })
      expect(tryAcquireSpy).toHaveBeenCalledTimes(2)
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('keeps the same Knex instance across many consecutive rebalances', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const pool = poolManager.getPool({
      ...createPoolSettings('rebalance-consecutive'),
      maxConnections: 10,
    })

    try {
      const knex = pool.acquire()
      const destroySpy = vi.spyOn(knex, 'destroy')

      for (let next = 1; next <= 10; next++) {
        pool.rebalance({ maxConnections: next })
        expect(pool.acquire()).toBe(knex)
        expect(getTarnPool(knex).max).toBe(next)
      }

      expect(destroySpy).not.toHaveBeenCalled()
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('falls back to databaseMaxConnections when rebalanced with maxConnections=0', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const pool = poolManager.getPool({
      ...createPoolSettings('rebalance-fallback'),
      maxConnections: 5,
    })

    try {
      const knex = pool.acquire()
      const tarnPool = getTarnPool(knex)
      expect(tarnPool.max).toBe(5)

      // 0 is falsy → getSettings falls back to databaseMaxConnections (default 20).
      pool.rebalance({ maxConnections: 0 })
      expect(tarnPool.max).toBe(20)
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('rebalanceAll mutates max for all cached pools in place', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const poolA = poolManager.getPool({
      ...createPoolSettings('rebalance-all-a'),
      maxConnections: 12,
    })
    const poolB = poolManager.getPool({
      ...createPoolSettings('rebalance-all-b'),
      maxConnections: 12,
    })

    try {
      const knexA = poolA.acquire()
      const knexB = poolB.acquire()
      const destroyA = vi.spyOn(knexA, 'destroy')
      const destroyB = vi.spyOn(knexB, 'destroy')

      poolManager.rebalanceAll({ clusterSize: 3 })

      expect(poolA.acquire()).toBe(knexA)
      expect(poolB.acquire()).toBe(knexB)
      expect(getTarnPool(knexA).max).toBe(4) // ceil(12 / 3)
      expect(getTarnPool(knexB).max).toBe(4)
      expect(destroyA).not.toHaveBeenCalled()
      expect(destroyB).not.toHaveBeenCalled()
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('serves a queued acquire immediately after scaling max up', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const pool = poolManager.getPool({
      ...createPoolSettings('rebalance-scale-up-serves-queue'),
      maxConnections: 2,
    })

    try {
      const knex = pool.acquire()
      const tarnPool = stubTarnIo(knex)

      const first = tarnPool.acquire()
      const second = tarnPool.acquire()
      await vi.advanceTimersByTimeAsync(0)

      expect(tarnPool.numUsed()).toBe(2)
      expect(tarnPool.numPendingAcquires()).toBe(0)

      // 3rd acquire is queued behind max=2.
      const blocked = tarnPool.acquire()
      await vi.advanceTimersByTimeAsync(0)

      expect(tarnPool.numPendingAcquires()).toBe(1)
      expect(tarnPool.numUsed()).toBe(2)

      // Raising max should immediately serve the queued acquire via
      // _tryAcquireOrCreate, without waiting for a release or reap tick.
      pool.rebalance({ maxConnections: 4 })
      await vi.advanceTimersByTimeAsync(0)

      expect(tarnPool.max).toBe(4)
      expect(tarnPool.numPendingAcquires()).toBe(0)
      expect(tarnPool.numUsed()).toBe(3)

      const resources = await Promise.all([first.promise, second.promise, blocked.promise])
      for (const r of resources) {
        tarnPool.release(r)
      }
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('blocks new acquires above the new max after scaling down', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const pool = poolManager.getPool({
      ...createPoolSettings('rebalance-scale-down-blocks'),
      maxConnections: 4,
    })

    try {
      const knex = pool.acquire()
      const tarnPool = stubTarnIo(knex)

      const a = tarnPool.acquire()
      const b = tarnPool.acquire()
      await vi.advanceTimersByTimeAsync(0)
      expect(tarnPool.numUsed()).toBe(2)

      // Scale below the current used count, but above 1 so we have headroom
      // for one more. Existing in-use connections are untouched; the cap
      // only constrains future creations.
      pool.rebalance({ maxConnections: 2 })
      expect(tarnPool.max).toBe(2)
      expect(tarnPool.numUsed()).toBe(2)

      // A new acquire above the new ceiling is queued, not created.
      const blocked = tarnPool.acquire()
      await vi.advanceTimersByTimeAsync(0)

      expect(tarnPool.numUsed()).toBe(2)
      expect(tarnPool.numPendingAcquires()).toBe(1)
      expect(tarnPool.numPendingCreates()).toBe(0)

      // Releasing one of the in-use resources frees a slot, which is then
      // handed to the queued acquire — proving the cap is enforced going
      // forward but does not strand pending callers.
      const aResource = await a.promise
      tarnPool.release(aResource)
      await vi.advanceTimersByTimeAsync(0)

      expect(tarnPool.numPendingAcquires()).toBe(0)
      expect(tarnPool.numUsed()).toBe(2)

      const remaining = await Promise.all([b.promise, blocked.promise])
      for (const r of remaining) {
        tarnPool.release(r)
      }
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('does not destroy in-use resources when scaling max down', async () => {
    const poolModule = await loadPoolModule(10_000)
    const poolManager = new poolModule.PoolManager()
    const pool = poolManager.getPool({
      ...createPoolSettings('rebalance-scale-down-keeps-used'),
      maxConnections: 4,
    })

    try {
      const knex = pool.acquire()
      const tarnPool = stubTarnIo(knex)
      const destroyerSpy = vi.spyOn(tarnPool, 'destroyer')

      const ops = [tarnPool.acquire(), tarnPool.acquire(), tarnPool.acquire(), tarnPool.acquire()]
      await vi.advanceTimersByTimeAsync(0)
      expect(tarnPool.numUsed()).toBe(4)

      // Aggressively scale down to well below the in-use count. The PR's
      // contract is: the cap is a soft ceiling going forward — existing
      // checked-out resources are NOT torn down.
      pool.rebalance({ maxConnections: 1 })

      expect(tarnPool.max).toBe(1)
      expect(tarnPool.numUsed()).toBe(4)
      expect(destroyerSpy).not.toHaveBeenCalled()

      const resources = await Promise.all(ops.map((op) => op.promise))
      for (const r of resources) {
        tarnPool.release(r)
      }
    } finally {
      await poolManager.destroyAll()
    }
  })

  test('recycle creates a fresh pool when no prior pool is cached', async () => {
    const poolModule = await loadPoolModule(10_000)

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []
      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('recycle-no-prior-pool')

    const pool = poolManager.recycle(settings.tenantId, settings)

    expect(poolManager.created).toHaveLength(1)
    expect(pool).toBe(poolManager.created[0])
    expect(poolManager.created[0].destroy).not.toHaveBeenCalled()

    // The newly recycled pool is cached and reused by subsequent getPool calls.
    expect(poolManager.getPool(settings)).toBe(pool)

    await poolManager.destroyAll()
  })

  test('recycle swaps the cached pool atomically and returns the new instance', async () => {
    const poolModule = await loadPoolModule(10_000)

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []
      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('recycle-swap')

    const original = poolManager.getPool(settings)
    expect(poolManager.created).toHaveLength(1)

    const recycled = poolManager.recycle(settings.tenantId, settings)

    expect(poolManager.created).toHaveLength(2)
    expect(recycled).toBe(poolManager.created[1])
    expect(recycled).not.toBe(original)

    // The cache swap is synchronous — any getPool right after sees the new pool.
    expect(poolManager.getPool(settings)).toBe(recycled)

    await poolManager.destroyAll()
  })

  test('recycle destroys the old pool in the background exactly once', async () => {
    const poolModule = await loadPoolModule(10_000)

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []
      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('recycle-bg-destroy')

    poolManager.getPool(settings)
    poolManager.recycle(settings.tenantId, settings)

    // destroyPoolSafely is fire-and-forget — wait for it.
    await vi.waitFor(() => {
      expect(poolManager.created[0].destroy).toHaveBeenCalledTimes(1)
    })

    // The new pool is untouched.
    expect(poolManager.created[1].destroy).not.toHaveBeenCalled()

    await poolManager.destroyAll()

    // After destroyAll the new pool is also destroyed — but only once.
    expect(poolManager.created[0].destroy).toHaveBeenCalledTimes(1)
    expect(poolManager.created[1].destroy).toHaveBeenCalledTimes(1)
  })

  test('recycle does not record a cache eviction for the replaced pool', async () => {
    const poolModule = await loadPoolModule(10_000)
    const metricsModule = await import('@internal/monitoring/metrics')
    const evictionSpy = vi.spyOn(metricsModule.cacheEvictionsTotal, 'add')

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []
      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('recycle-no-eviction')

    poolManager.getPool(settings)
    poolManager.recycle(settings.tenantId, settings)

    // Let the background destroy + dispose hook settle.
    await vi.waitFor(() => {
      expect(poolManager.created[0].destroy).toHaveBeenCalledTimes(1)
    })

    expect(evictionSpy.mock.calls).not.toContainEqual([1, { cache: TENANT_POOL_CACHE_NAME }])

    await poolManager.destroyAll()
  })

  test('destroy after recycle tears down both pools without double-destroying either', async () => {
    const poolModule = await loadPoolModule(10_000)

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []
      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('recycle-then-destroy')

    poolManager.getPool(settings)
    poolManager.recycle(settings.tenantId, settings)
    await poolManager.destroy(settings.tenantId)

    await vi.waitFor(() => {
      expect(poolManager.created[0].destroy).toHaveBeenCalledTimes(1)
      expect(poolManager.created[1].destroy).toHaveBeenCalledTimes(1)
    })
  })

  test('back-to-back recycles destroy every prior pool and cache the latest', async () => {
    const poolModule = await loadPoolModule(10_000)

    class TestPoolManager extends poolModule.PoolManager {
      created: TestPool[] = []
      protected newPool(_settings: TenantConnectionOptions): PoolStrategy {
        const pool = createTestPool()
        this.created.push(pool)
        return pool
      }
    }

    const poolManager = new TestPoolManager()
    const settings = createPoolSettings('recycle-back-to-back')

    poolManager.getPool(settings)
    poolManager.recycle(settings.tenantId, settings)
    const latest = poolManager.recycle(settings.tenantId, settings)

    expect(poolManager.created).toHaveLength(3)
    expect(poolManager.getPool(settings)).toBe(latest)

    // The two earlier pools both drain in the background; the latest stays cached.
    await vi.waitFor(() => {
      expect(poolManager.created[0].destroy).toHaveBeenCalledTimes(1)
      expect(poolManager.created[1].destroy).toHaveBeenCalledTimes(1)
    })
    expect(poolManager.created[2].destroy).not.toHaveBeenCalled()

    await poolManager.destroyAll()
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
})
