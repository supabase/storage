import pg from 'pg'
import { ConnectionString } from 'connection-string'
import LRU from 'lru-cache'

import { StorageBackendError } from '../storage'
import { Knex, knex } from 'knex'
import { verifyJWT } from '../auth'
import { JwtPayload } from 'jsonwebtoken'

pg.types.setTypeParser(20, 'text', parseInt)

const poolSize = 200 // TODO: dynamic value per tenant

interface TenantConnectionOptions {
  url: string
  role: string
  jwt: JwtPayload
  jwtRaw: string
  jwtSecret: string
}

export const connections = {
  leastUsed: undefined,
  values: new LRU<string, Knex>({
    max: 200,
    dispose: async (value) => {
      await value.destroy()
    },
  }),
}

export class TenantConnection {
  public readonly role: string

  protected constructor(
    protected readonly pool: Knex,
    protected readonly options: TenantConnectionOptions
  ) {
    this.role = options.role
  }

  protected _isExternal = false

  get isExternal() {
    return this._isExternal
  }

  static async create(pool: LRU<string, Knex>, options: TenantConnectionOptions) {
    const verifiedJWT = await verifyJWT(options.jwtRaw, options.jwtSecret)

    if (!verifiedJWT) {
      throw new StorageBackendError('invalid_jwt', 403, 'invalid jwt')
    }

    options.role = verifiedJWT?.role || 'anon'

    const internalConnectionPool = new ConnectionString(options.url)
    const externalConnectionPool = new ConnectionString(options.url)

    if (externalConnectionPool.hosts) {
      externalConnectionPool.hosts[0].port = 6543
    }

    const connNumber = 2

    let tried = 0
    let error: unknown | undefined = undefined
    let connectionPool = externalConnectionPool

    while (tried !== connNumber) {
      let knexPool = connections.values.get(connectionPool.toString())

      if (knexPool) {
        return new this(knexPool, options)
      }

      const isExternalPool = connectionPool.port === 6543
      try {
        knexPool = knex({
          client: 'pg',
          pool: {
            min: 0,
            max: isExternalPool ? 200 : poolSize,
            idleTimeoutMillis: 5000,
          },
          connection: connectionPool.toString(),
          acquireConnectionTimeout: 1000,
        })

        await knexPool.raw(`SELECT 1`)

        if (!isExternalPool) {
          connections.values.set(connectionPool.toString(), knexPool)
        }

        const conn = new this(knexPool, options)
        conn._isExternal = isExternalPool
        return conn
      } catch (e: any) {
        if ('code' in e && e.code === 'ECONNREFUSED') {
          tried++
          connectionPool = internalConnectionPool
          error = e
        } else {
          throw e
        }
      }
    }

    throw error
  }

  dispose() {
    if (this.isExternal) {
      return this.pool.destroy()
    }
  }

  transaction(isolation?: Knex.IsolationLevels, instance?: Knex) {
    return (instance || this.pool).transactionProvider({
      isolationLevel: isolation,
    })
  }

  async setScope(tnx: Knex) {
    await tnx.raw(`
      SET LOCAL ROLE ${this.options.role};
      SET LOCAL request.jwt.claim.role='${this.options.role}';
      SET LOCAL request.jwt='${this.options.jwtRaw}';
      SET LOCAL request.jwt.claim.sub='${this.options.jwt.sub || ''}';
      SET LOCAL request.jwt.claims='${JSON.stringify(this.options.jwt)}';
    `)
  }
}
