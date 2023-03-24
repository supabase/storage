import pg from 'pg'
import { Knex, knex } from 'knex'
import { JwtPayload } from 'jsonwebtoken'
import { getConfig } from '../config'

pg.types.setTypeParser(20, 'text', parseInt)

const { databaseMaxConnections } = getConfig()

interface TenantConnectionOptions {
  tenantId: string
  dbUrl: string
  role: string
  jwt: JwtPayload
  jwtRaw: string
  isExternalPool?: boolean
  maxConnections?: number
}

export const connections = new Map<string, Knex>()

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
    return Boolean(this.options.isExternalPool)
  }

  static async create(options: TenantConnectionOptions) {
    const connectionString = options.dbUrl

    let knexPool = connections.get(connectionString)

    if (knexPool) {
      return new this(await knexPool, options)
    }

    const isExternalPool = Boolean(options.isExternalPool)

    knexPool = knex({
      client: 'pg',
      searchPath: 'storage',
      pool: {
        min: 0,
        max: isExternalPool ? undefined : options.maxConnections || databaseMaxConnections,
        idleTimeoutMillis: 5000,
      },
      connection: connectionString,
      acquireConnectionTimeout: 1000,
    })

    if (!isExternalPool) {
      knexPool.on('poolDestroySuccess', () => {
        connections.delete(connectionString)
      })
      connections.set(connectionString, knexPool)
    }

    const conn = new this(knexPool, options)
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
