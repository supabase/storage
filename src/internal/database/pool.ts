import { type CacheLookupOutcome, createTtlCache, TENANT_POOL_CACHE_NAME } from '@internal/cache'
import { logger, logSchema } from '@internal/monitoring'
import {
  cacheEvictionsTotal,
  cacheRequestsTotal,
  dbActiveConnection,
  dbActivePool,
  dbInUseConnection,
  isMetricEnabled,
  meter,
} from '@internal/monitoring/metrics'
import { JWTPayload } from 'jose'
import { getConfig } from '../../config'

const {
  isMultitenant,
  dbSearchPath,
  tenantPoolCacheTtlMs,
  tenantPoolCacheHitLogSampleRate,
  tenantPoolCacheMissLogSampleRate,
} = getConfig()

export const TENANT_POOL_CACHE_LOOKUP_LOG_TYPE = 'cache'
export const TENANT_POOL_CACHE_LOOKUP_LOG_MESSAGE = '[Cache] Tenant pool lookup'

export interface TenantConnectionOptions {
  tenantId: string
  dbUrl: string
  isExternalPool?: boolean
  isSingleUse?: boolean
  idleTimeoutMillis?: number
  reapIntervalMillis?: number
  maxConnections: number
  clusterSize?: number
  numWorkers?: number
  user: User
  superUser: User
  headers?: Record<string, string | undefined | string[]>
  method?: string
  path?: string
  operation?: () => string | undefined
}

export interface User {
  jwt: string
  payload: { role?: string } & JWTPayload
}

export interface PoolStats {
  used: number
  total: number
}

export interface PoolRebalanceOptions {
  clusterSize?: number
  maxConnections?: number
}

export interface PoolStrategy {
  rebalance(options: PoolRebalanceOptions): void
  destroy(): Promise<void>
  getPoolStats(): PoolStats | null
}

export const searchPath = ['storage', 'public', 'extensions', ...dbSearchPath.split(',')].filter(
  Boolean
)

const multiTenantTtlConfig = {
  ttl: tenantPoolCacheTtlMs,
  updateAgeOnGet: true,
  checkAgeOnGet: true,
}

const manuallyDestroyedPools = new WeakSet<PoolStrategy>()

function logPoolDestroyError(error: unknown): void {
  logSchema.error(logger, 'pool was not able to be destroyed', {
    type: 'db',
    error,
  })
}

async function destroyPool(pool: PoolStrategy): Promise<void> {
  await pool.destroy()
}

async function destroyPoolSafely(pool: PoolStrategy): Promise<void> {
  try {
    await destroyPool(pool)
  } catch (e) {
    logPoolDestroyError(e)
  }
}

function recordTenantPoolCacheEviction(reason: string): void {
  // Explicit destroy paths are filtered before this helper is called.
  if (reason === 'stale' || reason === 'evict' || reason === 'delete') {
    cacheEvictionsTotal.add(1, {
      cache: TENANT_POOL_CACHE_NAME,
    })
  }
}

function recordTenantPoolCacheRequest(outcome: CacheLookupOutcome): void {
  cacheRequestsTotal.add(1, {
    cache: TENANT_POOL_CACHE_NAME,
    outcome,
  })
}

function recordTenantPoolCacheLookup(
  settings: TenantConnectionOptions,
  isCacheable: boolean,
  outcome: CacheLookupOutcome
): void {
  recordTenantPoolCacheRequest(outcome)
  logTenantPoolCacheLookup(settings, isCacheable, outcome)
}

function shouldLogTenantPoolCacheLookup(sampleRate: number): boolean {
  return sampleRate >= 1 || (sampleRate > 0 && Math.random() < sampleRate)
}

function logTenantPoolCacheLookup(
  settings: TenantConnectionOptions,
  isCacheable: boolean,
  outcome: CacheLookupOutcome
): void {
  const sampleRate =
    outcome === 'hit' ? tenantPoolCacheHitLogSampleRate : tenantPoolCacheMissLogSampleRate

  if (!shouldLogTenantPoolCacheLookup(sampleRate)) {
    return
  }

  const log = {
    type: TENANT_POOL_CACHE_LOOKUP_LOG_TYPE,
    cache: TENANT_POOL_CACHE_NAME,
    tenantId: settings.tenantId,
    project: settings.tenantId,
    outcome,
    sampleRate,
    sampleWeight: 1 / sampleRate,
    isCacheable,
    isExternalPool: Boolean(settings.isExternalPool),
    isSingleUse: Boolean(settings.isSingleUse),
  }

  logSchema.info(logger, TENANT_POOL_CACHE_LOOKUP_LOG_MESSAGE, log)
}

const tenantPools = createTtlCache<string, PoolStrategy>({
  ...(isMultitenant ? multiTenantTtlConfig : { max: 1, ttl: Infinity }),
  dispose: async (pool, _tenantId, reason) => {
    if (!pool || manuallyDestroyedPools.has(pool)) {
      return
    }

    recordTenantPoolCacheEviction(reason)

    await destroyPoolSafely(pool)
  },
})

// ============================================================================
// Pool stats collection — chunked to avoid blocking the event loop
// ============================================================================
interface PoolStatsSnapshot {
  poolCount: number
  totalConnections: number
  totalInUse: number
}

const STATS_CHUNK_SIZE = 100
const STATS_INTERVAL_MS = 5_000

let cachedPoolStats: PoolStatsSnapshot = {
  poolCount: 0,
  totalConnections: 0,
  totalInUse: 0,
}
let collectInProgress = false

async function collectPoolStats() {
  if (collectInProgress) return
  collectInProgress = true

  try {
    let poolCount = 0
    let totalConnections = 0
    let totalInUse = 0
    let chunkCount = 0

    for (const [, pool] of tenantPools.entries()) {
      poolCount++
      const stats = pool.getPoolStats()
      if (stats) {
        totalConnections += stats.total
        totalInUse += stats.used
      }
      // Yield to the event loop between chunks
      if (++chunkCount % STATS_CHUNK_SIZE === 0) {
        await new Promise<void>((resolve) => setImmediate(resolve))
      }
    }

    cachedPoolStats = {
      poolCount,
      totalConnections,
      totalInUse,
    }
  } finally {
    collectInProgress = false
  }
}

/**
 * PoolManager manages tenant-specific database pools and the shared cache metrics.
 * Concrete connection implementations provide the actual pool strategy.
 */
export abstract class PoolManager<TPool extends PoolStrategy = PoolStrategy> {
  protected numWorkers: number = 1

  setNumWorkers(numWorkers: number) {
    this.numWorkers = Math.max(numWorkers ?? 1, 1)
  }

  monitor() {
    // Periodically collect stats in a non-blocking way
    const interval = setInterval(() => {
      void collectPoolStats()
    }, STATS_INTERVAL_MS)
    interval.unref()

    // Observable callback reads the cached snapshot — O(1)
    meter.addBatchObservableCallback(
      (observer) => {
        if (isMetricEnabled('db_active_local_pools')) {
          observer.observe(dbActivePool, cachedPoolStats.poolCount)
        }
        if (isMetricEnabled('db_connections')) {
          observer.observe(dbActiveConnection, cachedPoolStats.totalConnections)
        }
        if (isMetricEnabled('db_connections_in_use')) {
          observer.observe(dbInUseConnection, cachedPoolStats.totalInUse)
        }
      },
      [dbActivePool, dbActiveConnection, dbInUseConnection]
    )
  }

  rebalanceAll(data: { clusterSize: number }) {
    for (const pool of tenantPools.values()) {
      pool.rebalance({
        clusterSize: data.clusterSize,
      })
    }
  }

  rebalance(tenantId: string, data: PoolRebalanceOptions) {
    const pool = tenantPools.get(tenantId)
    if (pool) {
      pool.rebalance({ ...data })
    }
  }

  getPool(settings: TenantConnectionOptions): TPool {
    const isCacheable = (settings.isSingleUse && !settings.isExternalPool) || !settings.isSingleUse
    const { value: existingPool, outcome } = tenantPools.getWithOutcome(settings.tenantId)
    recordTenantPoolCacheLookup(settings, isCacheable, outcome)

    if (existingPool) {
      return existingPool as TPool
    }

    if (!isCacheable) {
      return this.newPool({ ...settings, numWorkers: this.numWorkers })
    }

    const newPool = this.newPool({ ...settings, numWorkers: this.numWorkers })

    tenantPools.set(settings.tenantId, newPool)
    return newPool
  }

  destroy(tenantId: string) {
    const pool = tenantPools.get(tenantId)
    if (pool) {
      manuallyDestroyedPools.add(pool)
      tenantPools.delete(tenantId)
      return destroyPool(pool).finally(() => {
        manuallyDestroyedPools.delete(pool)
      })
    }
    return Promise.resolve()
  }

  destroyAll() {
    const promises: Promise<void>[] = []

    for (const [connectionString, pool] of tenantPools) {
      manuallyDestroyedPools.add(pool)
      tenantPools.delete(connectionString)
      promises.push(
        destroyPool(pool).finally(() => {
          manuallyDestroyedPools.delete(pool)
        })
      )
    }
    return Promise.allSettled(promises)
  }

  protected abstract newPool(settings: TenantConnectionOptions): TPool
}
