import pg, { DatabaseError } from 'pg'
import { Knex, knex } from 'knex'
import { JwtPayload } from 'jsonwebtoken'
import retry from 'async-retry'
import TTLCache from '@isaacs/ttlcache'
import { getConfig } from '../../config'
import { DbActiveConnection, DbActivePool } from '../monitoring/metrics'
import KnexTimeoutError = knex.KnexTimeoutError
import { logger, logSchema } from '../monitoring'
import { ERRORS } from '@internal/errors'

// https://github.com/knex/knex/issues/387#issuecomment-51554522
pg.types.setTypeParser(20, 'text', parseInt)

const {
  isMultitenant,
  databaseSSLRootCert,
  databaseMaxConnections,
  databaseFreePoolAfterInactivity,
  databaseConnectionTimeout,
  dbSearchPath,
  dbPostgresVersion,
} = getConfig()

interface TenantConnectionOptions {
  user: User
  superUser: User

  tenantId: string
  dbUrl: string
  isExternalPool?: boolean
  idleTimeoutMillis?: number
  maxConnections: number
  headers?: Record<string, string | undefined | string[]>
  method?: string
  path?: string
  operation?: string
}

export interface User {
  jwt: string
  payload: { role?: string } & JwtPayload
}

const multiTenantLRUConfig = {
  ttl: 1000 * 30,
  updateAgeOnGet: true,
  checkAgeOnGet: true,
  noDisponseOnSet: true,
}

export const searchPath = ['storage', 'public', 'extensions', ...dbSearchPath.split(',')].filter(
  Boolean
)

/**
 * Manages connections to tenant databases
 * Connections pools expire after a certain amount of time as well as idle connections
 */
export class ConnectionManager {
  /**
   * Connections map, the string is the connection string and the value is the knex pool
   * @protected
   */
  protected static connections = new TTLCache<string, Knex>({
    ...multiTenantLRUConfig,
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
   * Stop the pool manager and destroy all connections
   */
  static stop() {
    this.connections.cancelTimer()
    const promises: Promise<void>[] = []

    for (const [connectionString, pool] of this.connections) {
      promises.push(pool.destroy())
      this.connections.delete(connectionString)
    }

    return Promise.allSettled(promises)
  }

  /**
   * Acquire a pool for a tenant database
   * @param options
   */
  static acquirePool(options: TenantConnectionOptions): Knex {
    const connectionString = options.dbUrl

    let knexPool = this.connections.get(connectionString)

    if (knexPool) {
      return knexPool
    }

    const isExternalPool = Boolean(options.isExternalPool)

    knexPool = knex({
      client: 'pg',
      version: dbPostgresVersion,
      searchPath: isExternalPool ? undefined : searchPath,
      pool: {
        min: 0,
        max: isExternalPool
          ? options.maxConnections || databaseMaxConnections
          : databaseMaxConnections,
        acquireTimeoutMillis: databaseConnectionTimeout,
        idleTimeoutMillis: isExternalPool
          ? options.idleTimeoutMillis || 5000
          : databaseFreePoolAfterInactivity,
        reapIntervalMillis: isExternalPool ? 1000 : undefined,
      },
      connection: {
        connectionString: connectionString,
        connectionTimeoutMillis: databaseConnectionTimeout,
        ...this.sslSettings(),
      },
      acquireConnectionTimeout: databaseConnectionTimeout,
    })

    DbActivePool.inc({ is_external: isExternalPool.toString() })

    knexPool.client.pool.on('createSuccess', () => {
      DbActiveConnection.inc({
        is_external: isExternalPool.toString(),
      })
    })

    knexPool.client.pool.on('destroySuccess', () => {
      DbActiveConnection.dec({
        is_external: isExternalPool.toString(),
      })
    })

    knexPool.client.pool.on('poolDestroySuccess', () => {
      DbActivePool.dec({ is_external: isExternalPool.toString() })
    })

    this.connections.set(connectionString, knexPool)

    return knexPool
  }

  protected static sslSettings() {
    if (databaseSSLRootCert) {
      return { ssl: { ca: databaseSSLRootCert } }
    }
    return {}
  }
}

/**
 * Represent a connection to a tenant database
 */
export class TenantConnection {
  public readonly role: string

  constructor(protected readonly options: TenantConnectionOptions) {
    this.role = options.user.payload.role || 'anon'
  }

  async dispose() {
    // TODO: remove this method
  }

  async transaction(instance?: Knex) {
    try {
      const tnx = await retry(
        async (bail) => {
          try {
            const pool = instance || ConnectionManager.acquirePool(this.options)
            return await pool.transaction()
          } catch (e) {
            if (
              e instanceof DatabaseError &&
              e.code === '08P01' &&
              e.message.includes('no more connections allowed')
            ) {
              throw e
            }

            bail(e as Error)
            return
          }
        },
        {
          minTimeout: 50,
          maxTimeout: 200,
          maxRetryTime: 3000,
          retries: 10,
        }
      )

      if (!tnx) {
        throw ERRORS.InternalError(undefined, 'Could not create transaction')
      }

      if (!instance && this.options.isExternalPool) {
        // Note: in knex there is a bug when using `knex.transaction()` which doesn't bubble up the error to the catch block
        // in case the transaction was not able to be created. This is a workaround to make sure the error is thrown.
        // Ref: https://github.com/knex/knex/issues/4709
        if (tnx.isCompleted()) {
          await tnx.executionPromise

          // This should never be reached, since the above promise is always rejected in this edge case.
          throw ERRORS.DatabaseError('Transaction already completed')
        }

        try {
          await tnx.raw(`SELECT set_config('search_path', ?, true)`, [searchPath.join(', ')])
        } catch (e) {
          await tnx.rollback()
          throw e
        }
      }

      return tnx
    } catch (e) {
      if (e instanceof KnexTimeoutError) {
        throw ERRORS.DatabaseTimeout(e)
      }

      throw e
    }
  }

  transactionProvider(instance?: Knex): Knex.TransactionProvider {
    return async () => {
      return this.transaction(instance)
    }
  }

  asSuperUser() {
    const newOptions = {
      ...this.options,
      user: this.options.superUser,
    }
    return new TenantConnection(newOptions)
  }

  async setScope(tnx: Knex) {
    const headers = JSON.stringify(this.options.headers || {})
    await tnx.raw(
      `
        SELECT
          set_config('role', ?, true),
          set_config('request.jwt.claim.role', ?, true),
          set_config('request.jwt', ?, true),
          set_config('request.jwt.claim.sub', ?, true),
          set_config('request.jwt.claims', ?, true),
          set_config('request.headers', ?, true),
          set_config('request.method', ?, true),
          set_config('request.path', ?, true),
          set_config('storage.operation', ?, true);
    `,
      [
        this.role,
        this.role,
        this.options.user.jwt || '',
        this.options.user.payload.sub || '',
        JSON.stringify(this.options.user.payload),
        headers,
        this.options.method || '',
        this.options.path || '',
        this.options.operation || '',
      ]
    )
  }
}
