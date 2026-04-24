import { logger, logSchema } from '@internal/monitoring'
import { Pool, PoolConfig } from 'pg'
import { getConfig } from '../../config'
import { PgPoolExecutor } from './pg-connection'
import { getSslSettings } from './ssl'

// multitenantPgPool is a Proxy that re-creates the underlying pg.Pool when config
// changes. Do NOT register persistent event handlers (e.g., pool.on('error', ...))
// directly on this object; they will be lost on config swap. Register handlers
// inside MultitenantPgPoolOwner.getPool() if needed.
export function getMultitenantPgPoolConfig(): PoolConfig {
  return buildMultitenantPgPoolConfig(getConfig())
}

function buildMultitenantPgPoolConfig(config: ReturnType<typeof getConfig>): PoolConfig {
  const {
    databaseApplicationName,
    databaseSSLRootCert,
    multitenantDatabasePoolUrl,
    multitenantDatabaseUrl,
    multitenantMaxConnections,
  } = config

  const connectionString = multitenantDatabasePoolUrl || multitenantDatabaseUrl
  const poolSize = multitenantDatabasePoolUrl
    ? multitenantMaxConnections * 10
    : multitenantMaxConnections
  const sslSettings = connectionString
    ? getSslSettings({
        connectionString,
        databaseSSLRootCert,
      })
    : undefined

  return {
    connectionString,
    connectionTimeoutMillis: 5000,
    application_name: databaseApplicationName,
    min: 0,
    max: poolSize,
    idleTimeoutMillis: 5000,
    ssl: sslSettings ? { ...sslSettings } : undefined,
  }
}

class MultitenantPgPoolOwner {
  private pool?: Pool
  private config?: ReturnType<typeof getConfig>
  private configSignature?: string

  getPool(): Pool {
    const currentConfig = getConfig()

    if (this.pool && this.config === currentConfig) {
      return this.pool
    }

    const poolConfig = buildMultitenantPgPoolConfig(currentConfig)
    const configSignature = getPoolConfigSignature(poolConfig)

    if (!this.pool || this.configSignature !== configSignature) {
      const oldPool = this.pool
      this.pool = new Pool(poolConfig)
      this.configSignature = configSignature

      if (oldPool) {
        void oldPool.end().catch((error) => {
          logSchema.warning(logger, '[MultitenantPg] Failed to close replaced pg pool', {
            type: 'db',
            error,
          })
        })
      }
    }

    this.config = currentConfig

    return this.pool
  }

  async close(): Promise<void> {
    const pool = this.pool

    this.pool = undefined
    this.config = undefined
    this.configSignature = undefined

    await pool?.end()
  }
}

function getPoolConfigSignature(config: PoolConfig): string {
  return JSON.stringify({
    application_name: config.application_name,
    connectionString: config.connectionString,
    connectionTimeoutMillis: config.connectionTimeoutMillis,
    idleTimeoutMillis: config.idleTimeoutMillis,
    max: config.max,
    min: config.min,
    ssl: config.ssl,
  })
}

const multitenantPgPoolOwner = new MultitenantPgPoolOwner()

export const multitenantPgPool = new Proxy({} as Pool, {
  get(_target, property) {
    const pool = multitenantPgPoolOwner.getPool()
    const value = Reflect.get(pool, property, pool)

    return typeof value === 'function' ? value.bind(pool) : value
  },
})

export const multitenantPgExecutor = new PgPoolExecutor(multitenantPgPool)

export function closeMultitenantPg(): Promise<void> {
  return multitenantPgPoolOwner.close()
}
