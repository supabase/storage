import { type CacheLookupOutcome, createTtlCache, TENANT_POOL_CACHE_NAME } from '@internal/cache'
import { logger, logSchema } from '@internal/monitoring'
import {
  dbActiveConnection,
  dbActivePool,
  dbInUseConnection,
  isMetricEnabled,
  meter,
  recordCacheEviction,
  recordCacheRequest,
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
  configRevision?: number
}

// Pool cache entries are long-lived:
//  * minimum TENANT_POOL_CACHE_TTL_MS
//  * refreshed on access
// Strategies must retain only pool settings and can't capture the request
// that created them (headers, the operation closure over the whole Fastify request).
export type PoolStrategySettings = Pick<
  TenantConnectionOptions,
  | 'tenantId'
  | 'dbUrl'
  | 'isExternalPool'
  | 'maxConnections'
  | 'clusterSize'
  | 'numWorkers'
  | 'configRevision'
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
  numWorkers?: number
}

export interface PoolStrategy {
  isCurrent(
    configRevision: number | undefined,
    clusterSize: number | undefined,
    numWorkers: number
  ): boolean
  hasNewerConfigRevision(configRevision: number): boolean
  rebalance(options: PoolRebalanceOptions): void
  reconcile(settings: PoolStrategySettings): void
  closeCurrentPool(): Promise<void>
  retire(error: Error): Promise<void>
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

const retiringPools = new Map<PoolStrategy, Promise<void>>()
const retireAllMaxRounds = 32

function logPoolCloseError(error: unknown): void {
  logSchema.error(logger, 'Failed to close evicted database pool', {
    type: 'db',
    error,
  })
}

async function closeCurrentPoolSafely(pool: PoolStrategy): Promise<void> {
  try {
    await pool.closeCurrentPool()
  } catch (e) {
    logPoolCloseError(e)
  }
}

function recordTenantPoolCacheEviction(reason: string): void {
  // Only cache-driven removals count as cache evictions; explicit retirement does not.
  if (reason === 'stale' || reason === 'evict' || reason === 'delete') {
    recordCacheEviction(TENANT_POOL_CACHE_NAME)
  }
}

function recordTenantPoolCacheRequest(outcome: CacheLookupOutcome): void {
  recordCacheRequest(TENANT_POOL_CACHE_NAME, outcome)
}

function trackRetirement(pool: PoolStrategy, error: Error): Promise<void> {
  const existingRetirement = retiringPools.get(pool)
  if (existingRetirement) {
    return existingRetirement
  }

  const retirement = pool.retire(error)
  const trackedRetirement = retirement.finally(() => {
    if (retiringPools.get(pool) === trackedRetirement) {
      retiringPools.delete(pool)
    }
  })
  retiringPools.set(pool, trackedRetirement)
  return trackedRetirement
}

function recordTenantPoolCacheLookup(
  settings: TenantConnectionOptions,
  outcome: CacheLookupOutcome
): void {
  recordTenantPoolCacheRequest(outcome)
  logTenantPoolCacheLookup(settings, outcome)
}

function shouldLogTenantPoolCacheLookup(sampleRate: number): boolean {
  return sampleRate >= 1 || (sampleRate > 0 && Math.random() < sampleRate)
}

function logTenantPoolCacheLookup(
  settings: TenantConnectionOptions,
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
    isExternalPool: Boolean(settings.isExternalPool),
  }

  logSchema.info(logger, TENANT_POOL_CACHE_LOOKUP_LOG_MESSAGE, log)
}

const tenantPools = createTtlCache<string, PoolStrategy>({
  ...(isMultitenant ? multiTenantTtlConfig : { max: 1, ttl: Infinity }),
  dispose: async (pool, _tenantId, reason) => {
    if (!pool || retiringPools.has(pool)) {
      return
    }

    recordTenantPoolCacheEviction(reason)

    await closeCurrentPoolSafely(pool)
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

  hasPool(tenantId: string): boolean {
    return tenantPools.has(tenantId)
  }

  renewPoolIfNeeded(settings: PoolStrategySettings): void {
    tenantPools.peek(settings.tenantId)?.reconcile(settings)
  }

  getPool(settings: TenantConnectionOptions): TPool {
    const existingPool = tenantPools.get(settings.tenantId)
    const outcome: CacheLookupOutcome = existingPool ? 'hit' : 'miss'
    recordTenantPoolCacheLookup(settings, outcome)

    if (existingPool) {
      if (!existingPool.isCurrent(settings.configRevision, settings.clusterSize, this.numWorkers)) {
        existingPool.reconcile({
          tenantId: settings.tenantId,
          dbUrl: settings.dbUrl,
          isExternalPool: settings.isExternalPool,
          maxConnections: settings.maxConnections,
          clusterSize: settings.clusterSize,
          numWorkers: this.numWorkers,
          configRevision: settings.configRevision,
        })
      }
      return existingPool as TPool
    }

    const newPool = this.newPool({
      tenantId: settings.tenantId,
      dbUrl: settings.dbUrl,
      isExternalPool: settings.isExternalPool,
      maxConnections: settings.maxConnections,
      clusterSize: settings.clusterSize,
      numWorkers: this.numWorkers,
      configRevision: settings.configRevision,
    })

    tenantPools.set(settings.tenantId, newPool)
    return newPool
  }

  retire(tenantId: string, error: Error, configRevision?: number) {
    const pool = tenantPools.peek(tenantId)
    if (!pool) {
      return Promise.resolve()
    }
    if (configRevision !== undefined && pool.hasNewerConfigRevision(configRevision)) {
      return Promise.resolve()
    }

    const retirement = trackRetirement(pool, error)
    tenantPools.delete(tenantId)

    return retirement
  }

  private async retireAll(error: Error) {
    const results: PromiseSettledResult<void>[] = []

    for (let round = 0; ; round++) {
      const residentPools = [...tenantPools.entries()]
      if (residentPools.length === 0 && retiringPools.size === 0) {
        return results
      }
      if (round === retireAllMaxRounds) {
        results.push({
          status: 'rejected',
          reason: new Error(
            `Tenant pool manager did not become idle after ${retireAllMaxRounds} retirement rounds`
          ),
        })
        return results
      }

      for (const [tenantId, pool] of residentPools) {
        void trackRetirement(pool, error)
        tenantPools.delete(tenantId)
      }

      // Awaiting this snapshot lets tracked retirements remove themselves
      // before the next round, preventing duplicate results.
      results.push(...(await Promise.allSettled([...retiringPools.values()])))
    }
  }

  shutdown() {
    return this.retireAll(new Error('Tenant pool manager is shutting down'))
  }

  protected abstract newPool(settings: PoolStrategySettings): TPool
}
