import { createTtlCache, TENANT_POOL_CACHE_NAME } from '@internal/cache'
import { wait } from '@internal/concurrency'
import { getSslSettings } from '@internal/database/ssl'
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
import { Knex, knex } from 'knex'
import { getConfig } from '../../config'

const {
  isMultitenant,
  databaseSSLRootCert,
  databaseMaxConnections,
  databaseFreePoolAfterInactivity,
  databaseConnectionTimeout,
  dbSearchPath,
  dbPostgresVersion,
  databaseApplicationName,
} = getConfig()

export interface TenantConnectionOptions {
  user: User
  superUser: User

  tenantId: string
  dbUrl: string
  isExternalPool?: boolean
  isSingleUse?: boolean
  idleTimeoutMillis?: number
  reapIntervalMillis?: number
  maxConnections: number
  clusterSize?: number
  numWorkers?: number
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

export interface PoolStrategy {
  acquire(): Knex
  rebalance(options: { clusterSize: number }): void
  destroy(): Promise<void>
  getPoolStats(): PoolStats | null
}

export const searchPath = ['storage', 'public', 'extensions', ...dbSearchPath.split(',')].filter(
  Boolean
)

const multiTenantTtlConfig = {
  ttl: 1000 * 10,
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

function recordTenantPoolCacheRequest(outcome: string): void {
  cacheRequestsTotal.add(1, {
    cache: TENANT_POOL_CACHE_NAME,
    outcome,
  })
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
 * PoolManager is a class that manages a pool of Knex connections.
 * It creates a new pool for each tenant and reuses existing pools.
 */
export class PoolManager {
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

  rebalance(tenantId: string, data: { clusterSize: number }) {
    const pool = tenantPools.get(tenantId)
    if (pool) {
      pool.rebalance({
        clusterSize: data.clusterSize,
      })
    }
  }

  getPool(settings: TenantConnectionOptions) {
    const isCacheable = (settings.isSingleUse && !settings.isExternalPool) || !settings.isSingleUse
    const { value: existingPool, outcome } = tenantPools.getWithOutcome(settings.tenantId)

    if (existingPool) {
      recordTenantPoolCacheRequest(outcome)

      return existingPool
    }

    if (!isCacheable) {
      return this.newPool({ ...settings, numWorkers: this.numWorkers })
    }

    recordTenantPoolCacheRequest(outcome)

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

  protected newPool(settings: TenantConnectionOptions): PoolStrategy {
    return new TenantPool(settings)
  }
}

/**
 * TenantPool create a new Knex pool for each tenant, with rebalance
 * functionality to adjust the number of connections based on the cluster size.
 */
class TenantPool implements PoolStrategy {
  protected pool?: Knex

  constructor(protected readonly options: TenantConnectionOptions) {}

  acquire() {
    if (this.pool) {
      return this.pool
    }

    this.pool = this.createKnexPool()
    return this.pool
  }

  destroy(): Promise<void> {
    const originalPool = this.pool

    if (!originalPool) {
      return Promise.resolve()
    }

    this.pool = undefined
    return this.drainPool(originalPool)
  }

  getPoolStats(): PoolStats | null {
    const tarnPool = this.pool?.client?.pool
    if (!tarnPool) return null
    return {
      used: tarnPool.numUsed(),
      total: tarnPool.numUsed() + tarnPool.numFree(),
    }
  }

  getSettings() {
    const isSingleUseExternalPool = this.options.isSingleUse && this.options.isExternalPool

    const numWorkers = Math.max(this.options.numWorkers ?? 1, 1)
    const clusterSize = this.options.clusterSize || 0
    let maxConnection = this.options.maxConnections || databaseMaxConnections

    const divisor = Math.max(clusterSize, 1) * numWorkers
    if (divisor > 1) {
      maxConnection = Math.ceil(maxConnection / divisor) || 1
    }

    if (isSingleUseExternalPool) {
      maxConnection = 1
    }

    return {
      ...this.options,
      searchPath: this.options.isExternalPool ? undefined : searchPath,
      idleTimeoutMillis: isSingleUseExternalPool ? 100 : databaseFreePoolAfterInactivity,
      reapIntervalMillis: isSingleUseExternalPool ? 50 : undefined,
      maxConnections: maxConnection,
    }
  }

  rebalance(options: { clusterSize: number }) {
    if (options.clusterSize === 0) {
      return
    }

    const originalPool = this.pool

    this.options.clusterSize = options.clusterSize
    this.pool = undefined

    if (originalPool) {
      this.drainPool(originalPool).catch((e) => {
        logger.error({ type: 'pool', error: e })
      })
    }
  }

  protected async drainPool(pool: Knex) {
    for (; pool?.client?.pool; ) {
      let waiting = 0
      waiting += pool.client.pool.numPendingAcquires()
      waiting += pool.client.pool.numPendingValidations()
      waiting += pool.client.pool.numPendingCreates()

      if (waiting === 0) {
        break
      }

      await wait(200)
    }

    return pool.destroy()
  }

  protected createKnexPool() {
    const settings = this.getSettings()
    const sslSettings = getSslSettings({
      connectionString: settings.dbUrl,
      databaseSSLRootCert,
    })

    const maxConnections = settings.maxConnections

    return knex({
      client: 'pg',
      version: dbPostgresVersion,
      searchPath: settings.searchPath,
      pool: {
        min: 0,
        max: maxConnections,
        acquireTimeoutMillis: databaseConnectionTimeout,
        idleTimeoutMillis: settings.idleTimeoutMillis,
        reapIntervalMillis: 1000,
      },
      connection: {
        connectionString: settings.dbUrl,
        connectionTimeoutMillis: databaseConnectionTimeout,
        ssl: sslSettings ? { ...sslSettings } : undefined,
        application_name: databaseApplicationName,
      },
      acquireConnectionTimeout: databaseConnectionTimeout,
    })
  }
}
