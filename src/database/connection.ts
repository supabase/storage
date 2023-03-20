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
  tenantId: string
  url: string
  role: string
  jwt: JwtPayload
  jwtRaw: string
  jwtSecret: string
}

export const connections = {
  leastUsed: undefined,
  values: new LRU<string, Promise<Knex>>({
    max: 200,
    dispose: async (value) => {
      const k = await value
      await k.destroy()
    },
  }),
}

export class TenantConnection {
  public readonly role: string

  protected constructor(
    public readonly pool: Knex,
    protected readonly options: TenantConnectionOptions
  ) {
    this.role = options.role
  }

  protected _usesExternalPool = false

  get usesExternalPool() {
    return this._usesExternalPool
  }

  static async create(pool: LRU<string, Promise<Knex>>, options: TenantConnectionOptions) {
    const verifiedJWT = await verifyJWT(options.jwtRaw, options.jwtSecret)

    if (!verifiedJWT) {
      throw new StorageBackendError('invalid_jwt', 403, 'invalid jwt')
    }

    options.role = verifiedJWT?.role || 'anon'

    const connectionString = new ConnectionString(options.url)

    let knexPool = connections.values.get(connectionString.toString())

    if (knexPool) {
      return new this(await knexPool, options)
    }

    const isExternalPool = connectionString.port === 6543

    knexPool = new Promise<Knex>(async (resolve) => {
      const k = knex({
        client: 'pg',
        pool: {
          min: 0,
          max: isExternalPool ? 200 : poolSize,
          idleTimeoutMillis: 5000,
        },
        connection: connectionString.toString(),
        acquireConnectionTimeout: 1000,
      })

      await k.raw(`SELECT 1`)
      resolve(k)
    })

    if (!isExternalPool) {
      connections.values.set(connectionString.toString(), knexPool)
    }

    const conn = new this(await knexPool, options)
    conn._usesExternalPool = isExternalPool

    return conn
  }

  dispose() {
    if (this.usesExternalPool) {
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
