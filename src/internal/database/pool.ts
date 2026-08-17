import { createLruCache, TENANT_POOL_CACHE_NAME } from '@internal/cache'
import { logger, logSchema } from '@internal/monitoring'
import {
  dbActiveConnection,
  dbActivePool,
  dbInUseConnection,
  dbPoolOldestPendingRetirementAge,
  dbPoolsPendingRetirement,
  isMetricEnabled,
  meter,
} from '@internal/monitoring/metrics'
import { isOtelMetricsReaderEnabled } from '@internal/monitoring/otel-metrics-config'
import { JWTPayload } from 'jose'
import { getConfig } from '../../config'
import { PoolRetirementCoordinator, type RetirablePool } from './pool-retirement'

const config = getConfig()
const { isMultitenant, dbSearchPath, tenantPoolCacheMaxEntries } = config

export interface TenantConnectionOptions {
  tenantId: string
  dbUrl: string
  isExternalPool?: boolean
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

// Pool cache entries are retained by recency up to TENANT_POOL_CACHE_MAX_ENTRIES.
// Strategies must retain only pool settings and can't capture the request
// that created them (headers, the operation closure over the whole Fastify request).
export type PoolStrategySettings = Pick<
  TenantConnectionOptions,
  'tenantId' | 'dbUrl' | 'isExternalPool' | 'maxConnections' | 'clusterSize' | 'numWorkers'
>

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

export interface PoolStrategy extends RetirablePool {
  rebalance(options: PoolRebalanceOptions): void
  retain(): void
  release(): void
  getPoolStats(): PoolStats | null
}

export const searchPath = ['storage', 'public', 'extensions', ...dbSearchPath.split(',')].filter(
  Boolean
)

const tenantPoolCacheKeySeparator = '\x00'

function tenantPoolCacheKey(settings: Pick<TenantConnectionOptions, 'tenantId' | 'dbUrl'>): string {
  return `${settings.tenantId}${tenantPoolCacheKeySeparator}${settings.dbUrl}`
}

function belongsToTenant(cacheKey: string, tenantId: string): boolean {
  return cacheKey.startsWith(`${tenantId}${tenantPoolCacheKeySeparator}`)
}

function tenantIdFromPoolCacheKey(cacheKey: string): string {
  return cacheKey.slice(0, cacheKey.indexOf(tenantPoolCacheKeySeparator))
}

function logPoolDestroyError(tenantId: string, error: unknown): void {
  logSchema.error(logger, 'pool was not able to be destroyed', {
    type: 'db',
    error,
    tenantId,
    project: tenantId,
  })
}

const poolRetirements = new PoolRetirementCoordinator<PoolStrategy>({
  onDetachedFailure: logPoolDestroyError,
})

const tenantPools = createLruCache<string, PoolStrategy>(TENANT_POOL_CACHE_NAME, {
  max: isMultitenant ? tenantPoolCacheMaxEntries : 1,
  disposeAfter: (pool, cacheKey, reason) => {
    if (reason !== 'evict') {
      return
    }

    poolRetirements.defer(tenantIdFromPoolCacheKey(cacheKey), pool)
  },
})

function createPoolSettings(
  settings: TenantConnectionOptions | PoolStrategySettings,
  numWorkers: number
): PoolStrategySettings {
  return {
    tenantId: settings.tenantId,
    dbUrl: settings.dbUrl,
    isExternalPool: Boolean(settings.isExternalPool),
    maxConnections: settings.maxConnections,
    clusterSize: settings.clusterSize,
    numWorkers,
  }
}

// ============================================================================
// Pool stats collection — chunked to avoid blocking the event loop
// ============================================================================
interface PoolStatsSnapshot {
  poolCount: number
  totalConnections: number
  totalInUse: number
}

const STATS_CHUNK_SIZE = 4096
const STATS_INTERVAL_MS = 15_000
const POOL_OBSERVABLES = [
  dbActivePool,
  dbActiveConnection,
  dbInUseConnection,
  dbPoolsPendingRetirement,
  dbPoolOldestPendingRetirementAge,
]

const poolStatsExporterEnabled = isOtelMetricsReaderEnabled(config)

let cachedPoolStats: PoolStatsSnapshot = {
  poolCount: 0,
  totalConnections: 0,
  totalInUse: 0,
}
let collectInProgress = false

function shouldCollectPoolStats(): boolean {
  return (
    isMetricEnabled('db_active_local_pools') ||
    isMetricEnabled('db_connections') ||
    isMetricEnabled('db_connections_in_use')
  )
}

async function collectPoolStats(shouldCommit: () => boolean) {
  if (collectInProgress) return
  collectInProgress = true

  try {
    let poolCount = 0
    let totalConnections = 0
    let totalInUse = 0
    let chunkCount = 0

    // Capacity-evicted strategies can remain live until their
    // owning request releases its lease, include them.
    const pools = [...tenantPools.values()]
    poolRetirements.appendDeferredSnapshotTo(pools)
    for (const pool of pools) {
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

    if (shouldCommit()) {
      cachedPoolStats = {
        poolCount,
        totalConnections,
        totalInUse,
      }
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
  private statsInterval?: ReturnType<typeof setInterval>
  private statsObserver?: Parameters<typeof meter.addBatchObservableCallback>[0]

  setNumWorkers(numWorkers: number) {
    this.numWorkers = Math.max(numWorkers ?? 1, 1)
  }

  monitor() {
    if (this.statsObserver) {
      return
    }

    const statsObserver: Parameters<typeof meter.addBatchObservableCallback>[0] = (observer) => {
      if (isMetricEnabled('db_active_local_pools')) {
        observer.observe(dbActivePool, cachedPoolStats.poolCount)
      }
      if (isMetricEnabled('db_connections')) {
        observer.observe(dbActiveConnection, cachedPoolStats.totalConnections)
      }
      if (isMetricEnabled('db_connections_in_use')) {
        observer.observe(dbInUseConnection, cachedPoolStats.totalInUse)
      }
      if (isMetricEnabled('db_pools_pending_retirement')) {
        observer.observe(dbPoolsPendingRetirement, poolRetirements.deferredCount)
      }
      if (isMetricEnabled('db_pool_oldest_pending_retirement_age_seconds')) {
        observer.observe(
          dbPoolOldestPendingRetirementAge,
          poolRetirements.getOldestDeferredAgeSeconds()
        )
      }
    }
    this.statsObserver = statsObserver
    meter.addBatchObservableCallback(statsObserver, POOL_OBSERVABLES)

    if (poolStatsExporterEnabled) {
      this.statsInterval = setInterval(() => {
        if (shouldCollectPoolStats()) {
          void collectPoolStats(() => this.statsObserver === statsObserver)
        }
      }, STATS_INTERVAL_MS)
      this.statsInterval.unref()
    }
  }

  rebalanceAll(data: { clusterSize: number }) {
    for (const pool of tenantPools.values()) {
      pool.rebalance({
        clusterSize: data.clusterSize,
      })
    }
  }

  rebalance(tenantId: string, data: PoolRebalanceOptions) {
    for (const [cacheKey, pool] of tenantPools.entries()) {
      if (belongsToTenant(cacheKey, tenantId)) {
        pool.rebalance({ ...data })
      }
    }
  }

  getPool(settings: TenantConnectionOptions): TPool {
    const cacheKey = tenantPoolCacheKey(settings)
    const existingPool = tenantPools.get(cacheKey)

    if (existingPool) {
      // The URL is immutable strategy identity. Same-URL capacity changes are
      // eventually consistent and the next current-config request reapplies them.
      existingPool.rebalance({ maxConnections: settings.maxConnections })
      return existingPool as TPool
    }

    const newPool = this.newPool(createPoolSettings(settings, this.numWorkers))
    tenantPools.set(cacheKey, newPool)
    return newPool
  }

  destroy(tenantId: string) {
    for (const [cacheKey, pool] of [...tenantPools.entries()]) {
      if (!belongsToTenant(cacheKey, tenantId)) {
        continue
      }

      tenantPools.delete(cacheKey)
      poolRetirements.retireNow(tenantId, pool)
    }

    return poolRetirements.waitForTenant(tenantId)
  }

  async destroyAll() {
    this.stopMonitoring()

    for (const [cacheKey, pool] of [...tenantPools.entries()]) {
      const tenantId = tenantIdFromPoolCacheKey(cacheKey)
      tenantPools.delete(cacheKey)
      poolRetirements.retireNow(tenantId, pool)
    }

    return poolRetirements.waitForAll()
  }

  private stopMonitoring(): void {
    if (this.statsInterval) {
      clearInterval(this.statsInterval)
      this.statsInterval = undefined
    }
    if (this.statsObserver) {
      meter.removeBatchObservableCallback(this.statsObserver, POOL_OBSERVABLES)
      this.statsObserver = undefined
    }
    cachedPoolStats = {
      poolCount: 0,
      totalConnections: 0,
      totalInUse: 0,
    }
  }

  protected abstract newPool(settings: PoolStrategySettings): TPool
}
