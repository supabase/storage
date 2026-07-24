import { decrypt } from '@internal/auth'
import { attachPoolErrorHandler } from '@internal/database/postgres/pool-errors'
import { getSslSettings } from '@internal/database/postgres/ssl'
import { createPostgresTypeParsers } from '@internal/database/postgres/type-parsers'
import { getLogger } from '@platformatic/globals'
import { Pool } from 'pg'
import type { DatabaseConfig } from './config.js'
import { DatabaseWattError } from './errors.js'
import type { DestinationConfig } from './types.js'

type TenantRow = {
  database_pool_url?: string | null
  database_url: string
  max_connections?: number | null
}

export class DestinationResolver {
  private masterPool?: Pool
  private readonly config: DatabaseConfig

  constructor(config: DatabaseConfig) {
    this.config = config
  }

  async resolve(destination: string): Promise<DestinationConfig> {
    if (destination === 'master') {
      return this.resolveMasterDestination()
    }

    if (this.config.masterConnectionString) {
      return this.resolveTenant(destination)
    }

    const connectionString = this.config.poolConnectionString
    if (!connectionString) {
      throw new DatabaseWattError('DESTINATION_UNKNOWN', 'No database connection string configured')
    }

    return {
      connectionString,
      id: destination,
      isExternalPool: this.config.poolIsExternal,
      maxConnections: this.config.destinationMaxConnections,
    }
  }

  async close(): Promise<void> {
    const pool = this.masterPool
    this.masterPool = undefined
    if (pool) {
      await pool.end()
    }
  }

  private async resolveTenant(destination: string): Promise<DestinationConfig> {
    const result = await this.getMasterPool().query<TenantRow>(
      `
        SELECT database_url, database_pool_url, max_connections
        FROM tenants
        WHERE id = $1
        LIMIT 1
      `,
      [destination]
    )

    const tenant = result.rows[0]
    if (!tenant) {
      throw new DatabaseWattError('DESTINATION_UNKNOWN', 'Destination is unknown')
    }

    const connectionString = decrypt(tenant.database_pool_url || tenant.database_url)
    if (!connectionString) {
      throw new DatabaseWattError('DESTINATION_UNKNOWN', 'Destination has no database credentials')
    }

    return {
      connectionString,
      id: destination,
      isExternalPool: Boolean(tenant.database_pool_url),
      maxConnections: tenant.max_connections || this.config.destinationMaxConnections,
    }
  }

  private resolveMasterDestination(): DestinationConfig {
    const connectionString = this.config.masterConnectionString
    if (!connectionString) {
      throw new DatabaseWattError('DESTINATION_UNKNOWN', 'No multitenant database configured')
    }

    return {
      connectionString,
      id: 'master',
      isExternalPool: this.config.masterIsExternalPool,
      maxConnections: this.config.masterMaxConnections,
    }
  }

  private getMasterPool(): Pool {
    const connectionString = this.config.masterConnectionString
    if (!connectionString) {
      throw new DatabaseWattError('DESTINATION_UNKNOWN', 'No multitenant database configured')
    }

    if (!this.masterPool) {
      this.masterPool = attachPoolErrorHandler(
        new Pool({
          application_name: this.config.applicationName,
          connectionString,
          connectionTimeoutMillis: this.config.connectionTimeoutMs,
          idleTimeoutMillis: this.config.idlePoolTimeoutMs,
          max: this.config.masterMaxConnections,
          min: 0,
          ssl: getSslSettings({
            connectionString,
            databaseSSLRootCert: this.config.rootCert,
          }),
          types: createPostgresTypeParsers(),
        }),
        (error) => {
          getLogger({ throwOnMissing: false })?.warn(
            { err: error, type: 'db' },
            '[DatabaseWatt] Idle master pg client error'
          )
        }
      )
    }

    return this.masterPool
  }
}
