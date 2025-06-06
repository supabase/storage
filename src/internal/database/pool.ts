import { getConfig } from '../../config'
import TTLCache from '@isaacs/ttlcache'
import { knex, Knex } from 'knex'
import { logger, logSchema } from '@internal/monitoring'
import { getSslSettings } from '@internal/database/ssl'
import { wait } from '@internal/concurrency'
import { JWTPayload } from 'jose'
import { DbActivePool } from '@internal/monitoring/metrics'

const {
  region,
  isMultitenant,
  databaseSSLRootCert,
  databaseMaxConnections,
  databaseFreePoolAfterInactivity,
  databaseConnectionTimeout,
  dbSearchPath,
  dbPostgresVersion,
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
  headers?: Record<string, string | undefined | string[]>
  method?: string
  path?: string
  operation?: () => string | undefined
}

export interface User {
  jwt: string
  payload: { role?: string } & JWTPayload
}

export interface PoolStrategy {
  acquire(): Knex
  rebalance(options: { clusterSize: number }): void
  destroy(): Promise<void>
}

export const searchPath = ['storage', 'public', 'extensions', ...dbSearchPath.split(',')].filter(
  Boolean
)

const multiTenantLRUConfig = {
  ttl: 1000 * 10,
  updateAgeOnGet: true,
  checkAgeOnGet: true,
}

const tenantPools = new TTLCache<string, PoolStrategy>({
  ...(isMultitenant ? multiTenantLRUConfig : { max: 1, ttl: Infinity }),
  dispose: async (pool) => {
    if (!pool) return
    try {
      await pool.destroy()
    } catch (e) {
      logSchema.error(logger, 'pool was not able to be destroyed', {
        type: 'db',
        error: e,
      })
    }
  },
})

/**
 * PoolManager is a class that manages a pool of Knex connections.
 * It creates a new pool for each tenant and reuses existing pools.
 */
export class PoolManager {
  monitor(signal: AbortSignal) {
    const monitorInterval = setInterval(() => {
      DbActivePool.set(
        {
          region,
        },
        tenantPools.size
      )
    }, 2000)

    signal.addEventListener(
      'abort',
      () => {
        clearInterval(monitorInterval)
      },
      { once: true }
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
    const existingPool = tenantPools.get(settings.tenantId)
    if (existingPool) {
      return existingPool
    }

    const newPool = this.newPool(settings)

    if ((settings.isSingleUse && !settings.isExternalPool) || !settings.isSingleUse) {
      tenantPools.set(settings.tenantId, newPool)
    }
    return newPool
  }

  destroy(tenantId: string) {
    const pool = tenantPools.get(tenantId)
    if (pool) {
      tenantPools.delete(tenantId)
      return pool.destroy()
    }
    return Promise.resolve()
  }

  destroyAll() {
    const promises: Promise<void>[] = []

    for (const [connectionString, pool] of tenantPools) {
      promises.push(pool.destroy())
      tenantPools.delete(connectionString)
    }
    return Promise.allSettled(promises)
  }

  protected newPool(settings: TenantConnectionOptions) {
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
    if (!this.pool) {
      return Promise.resolve()
    }
    return this.drainPool(this.pool)
  }

  getSettings() {
    const isSingleUseExternalPool = this.options.isSingleUse && this.options.isExternalPool

    const clusterSize = this.options.clusterSize || 0
    let maxConnection = this.options.maxConnections || databaseMaxConnections

    if (clusterSize > 0) {
      maxConnection = Math.ceil(maxConnection / clusterSize)
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
    if (!pool?.client?.pool) {
      if (pool) return pool.destroy()
      return
    }

    while (true) {
      if (!pool?.client?.pool) {
        if (pool) return pool.destroy()
        return
      }

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
      },
      acquireConnectionTimeout: databaseConnectionTimeout,
    })
  }
}
