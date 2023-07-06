import pg from 'pg'
import { Knex, knex } from 'knex'
import { JwtPayload } from 'jsonwebtoken'
import { getConfig } from '../config'
import { DbActiveConnection, DbActivePool } from '../monitoring/metrics'

// https://github.com/knex/knex/issues/387#issuecomment-51554522
pg.types.setTypeParser(20, 'text', parseInt)

const { databaseMaxConnections, databaseFreePoolAfterInactivity, databaseConnectionTimeout } =
  getConfig()

interface TenantConnectionOptions {
  user: User
  superUser: User

  tenantId: string
  dbUrl: string
  isExternalPool?: boolean
  maxConnections: number
  headers?: Record<string, string | undefined | string[]>
  method?: string
  path?: string
}

export interface User {
  jwt: string
  payload: { role?: string } & JwtPayload
}

export const connections = new Map<string, Knex>()
const searchPath = ['storage', 'public', 'extensions']

export class TenantConnection {
  public readonly role: string

  protected constructor(
    protected readonly pool: Knex,
    protected readonly options: TenantConnectionOptions
  ) {
    this.role = options.user.payload.role || 'anon'
  }

  static stop() {
    const promises: Promise<void>[] = []
    for (const [connectionString, pool] of connections) {
      promises.push(pool.destroy())
      connections.delete(connectionString)
    }
    return Promise.allSettled(promises)
  }

  static async create(options: TenantConnectionOptions) {
    const connectionString = options.dbUrl

    let knexPool = connections.get(connectionString)

    if (knexPool) {
      return new this(knexPool, options)
    }

    const isExternalPool = Boolean(options.isExternalPool)

    knexPool = knex({
      client: 'pg',
      searchPath: isExternalPool ? undefined : searchPath,
      pool: {
        min: 0,
        max: isExternalPool ? 1 : options.maxConnections || databaseMaxConnections,
        acquireTimeoutMillis: databaseConnectionTimeout,
        idleTimeoutMillis: isExternalPool ? 100 : databaseFreePoolAfterInactivity,
        reapIntervalMillis: isExternalPool ? 110 : undefined,
      },
      connection: connectionString,
      acquireConnectionTimeout: databaseConnectionTimeout,
    })

    DbActivePool.inc({ tenant_id: options.tenantId, is_external: isExternalPool.toString() })

    knexPool.client.pool.on('createSuccess', () => {
      DbActiveConnection.inc({
        tenant_id: options.tenantId,
        is_external: isExternalPool.toString(),
      })
    })

    knexPool.client.pool.on('destroySuccess', () => {
      DbActiveConnection.dec({
        tenant_id: options.tenantId,
        is_external: isExternalPool.toString(),
      })
    })

    knexPool.client.pool.on('poolDestroySuccess', () => {
      DbActivePool.dec({ tenant_id: options.tenantId, is_external: isExternalPool.toString() })
    })

    if (!isExternalPool) {
      knexPool.client.pool.on('poolDestroySuccess', () => {
        if (connections.get(connectionString) === knexPool) {
          connections.delete(connectionString)
        }
      })

      connections.set(connectionString, knexPool)
    }

    return new this(knexPool, options)
  }

  async dispose() {
    if (this.options.isExternalPool) {
      return this.pool.destroy()
    }
  }

  async transaction(instance?: Knex) {
    const pool = instance || this.pool
    const tnx = await pool.transaction()

    if (!instance && this.options.isExternalPool) {
      await tnx.raw(`SELECT set_config('search_path', ?, true)`, [searchPath.join(', ')])
    }
    return tnx
  }

  transactionProvider(instance?: Knex): Knex.TransactionProvider {
    return async () => {
      return this.transaction(instance)
    }
  }

  asSuperUser() {
    return new TenantConnection(this.pool, {
      ...this.options,
      user: this.options.superUser,
    })
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
          set_config('request.path', ?, true);
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
      ]
    )
  }
}
