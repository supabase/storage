import pg from 'pg'
import { Knex, knex } from 'knex'
import { JwtPayload } from 'jsonwebtoken'
import { getConfig } from '../config'
import { logger } from '../monitoring'

// https://github.com/knex/knex/issues/387#issuecomment-51554522
pg.types.setTypeParser(20, 'text', parseInt)

const { databaseMaxConnections, databaseFreePoolAfterInactivity, databaseConnectionTimeout } =
  getConfig()

interface TenantConnectionOptions {
  tenantId: string
  dbUrl: string
  role: string
  jwt: JwtPayload
  jwtRaw: string
  isExternalPool?: boolean
  maxConnections: number
  headers?: Record<string, string | undefined | string[]>
  method?: string
  path?: string
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
      return new this(await knexPool, options)
    }

    const isExternalPool = Boolean(options.isExternalPool)

    knexPool = knex({
      client: 'pg',
      searchPath: ['public', 'storage'],
      pool: {
        min: 0,
        max: isExternalPool ? 1 : options.maxConnections || databaseMaxConnections,
      },
      connection: connectionString,
      acquireConnectionTimeout: databaseConnectionTimeout,
    })

    if (!isExternalPool) {
      let freePoolIntervalFn: NodeJS.Timeout | undefined

      knexPool.client.pool.on('poolDestroySuccess', () => {
        if (freePoolIntervalFn) {
          clearTimeout(freePoolIntervalFn)
        }

        if (connections.get(connectionString) === knexPool) {
          connections.delete(connectionString)
        }
      })

      knexPool.client.pool.on('stopReaping', () => {
        if (freePoolIntervalFn) {
          clearTimeout(freePoolIntervalFn)
        }

        freePoolIntervalFn = setTimeout(async () => {
          connections.delete(connectionString)
          knexPool?.destroy().catch((e) => {
            logger.error(e, 'Error destroying pool')
          })
          clearTimeout(freePoolIntervalFn)
        }, databaseFreePoolAfterInactivity)
      })

      knexPool.client.pool.on('startReaping', () => {
        if (freePoolIntervalFn) {
          clearTimeout(freePoolIntervalFn)
          freePoolIntervalFn = undefined
        }
      })
      connections.set(connectionString, knexPool)
    }

    return new this(knexPool, options)
  }

  dispose() {
    if (this.options.isExternalPool) {
      return this.pool.destroy()
    }
  }

  transaction(isolation?: Knex.IsolationLevels, instance?: Knex) {
    return (instance || this.pool).transactionProvider({
      isolationLevel: isolation,
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
        this.options.role || 'anon',
        this.options.role || 'anon',
        this.options.jwtRaw || '',
        this.options.jwt.sub || '',
        JSON.stringify(this.options.jwt),
        headers,
        this.options.method || '',
        this.options.path || '',
      ]
    )
  }
}
