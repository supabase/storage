import pg, { DatabaseError } from 'pg'
import { Knex, knex } from 'knex'
import retry from 'async-retry'
import KnexTimeoutError = knex.KnexTimeoutError
import { ERRORS } from '@internal/errors'
import {
  PoolStrategy,
  PoolManager,
  searchPath,
  TenantConnectionOptions,
} from '@internal/database/pool'
import { getConfig } from '../../config'
import { TransactionOptions } from '@storage/database'

const { databaseStatementTimeout } = getConfig()

// https://github.com/knex/knex/issues/387#issuecomment-51554522
pg.types.setTypeParser(20, 'text', parseInt)

export class TenantConnection {
  static poolManager = new PoolManager()
  public readonly role: string

  constructor(
    public readonly pool: PoolStrategy,
    protected readonly options: TenantConnectionOptions
  ) {
    this.role = options.user.payload.role || 'anon'
  }

  static stop() {
    return TenantConnection.poolManager.destroyAll()
  }

  static async create(options: TenantConnectionOptions) {
    const knexPool = TenantConnection.poolManager.getPool(options)
    return new this(knexPool, options)
  }

  dispose() {
    if (this.options.isSingleUse && this.options.isExternalPool) {
      return this.pool.destroy()
    }

    return Promise.resolve()
  }

  async transaction(instance?: Knex, opts?: TransactionOptions): Promise<Knex.Transaction> {
    try {
      const tnx = await retry(
        async (bail) => {
          try {
            const pool = instance || this.pool.acquire()
            return await pool.transaction()
          } catch (e) {
            if (
              e instanceof DatabaseError &&
              ((e.code === '08P01' && e.message.includes('no more connections allowed')) ||
                e.message.includes('Max client connections reached'))
            ) {
              throw e
            }

            bail(e as Error)
            return
          }
        },
        {
          minTimeout: 50,
          factor: 2,
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

      // Apply statement timeout at start of transaction (PgBouncer-compatible)
      // SET LOCAL scopes the timeout to the current transaction only
      if (typeof opts?.timeout !== 'undefined' || databaseStatementTimeout > 0) {
        const statementTimeout = opts?.timeout ?? databaseStatementTimeout

        // Apply statement timeout if set
        if (statementTimeout > 0) {
          try {
            await tnx.raw(`SET LOCAL statement_timeout TO '${statementTimeout}ms'`)
          } catch (e) {
            await tnx.rollback()
            throw e
          }
        }
      }

      return tnx
    } catch (e) {
      if (e instanceof KnexTimeoutError) {
        throw ERRORS.DatabaseTimeout(e)
      }

      // Handle pg client connectionTimeoutMillis timeout
      // This fires when the connection timeout happens before pool acquisition timeout
      if (e instanceof Error && e.message === 'timeout expired') {
        throw ERRORS.DatabaseTimeout(e)
      }

      throw e
    }
  }

  transactionProvider(instance?: Knex, opts?: TransactionOptions): Knex.TransactionProvider {
    return async () => {
      return this.transaction(instance, opts)
    }
  }

  asSuperUser() {
    return new TenantConnection(this.pool, {
      ...this.options,
      user: this.options.superUser,
    })
  }

  async setScope(tnx: Knex, opts?: { signal?: AbortSignal }) {
    const headers = JSON.stringify(this.options.headers || {})
    await tnx
      .raw(
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
          set_config('storage.operation', ?, true),
          set_config('storage.allow_delete_query', 'true', true);
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
          this.options.operation?.() || '',
        ]
      )
      .abortOnSignal(opts?.signal)
  }
}
