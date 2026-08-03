import EventEmitter from 'node:events'
import type { DatabaseExecutor } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { Pool } from 'pg'
import type { Db } from 'pg-boss'
import { getConfig } from '../../config'

/** Connection selection mirrors v1: the explicit queue URL wins; multitenant deployments
 * fall back to the multitenant DB. `pooledUrl` is what the runtime pools connect to — the
 * transaction pooler when one is configured, otherwise the same as `directUrl`. `directUrl`
 * is ALWAYS a direct connection and is what BOTH adapters' migrations ride — a transaction
 * pooler cannot run them (see `runPgBossMigrations` in `boss.ts` and `runPgqueMigrations`
 * in `pgque.ts`). Shared by both adapters' factories. */
export function queueConnectionConfig(): { pooledUrl: string; directUrl: string } {
  const {
    isMultitenant,
    databaseURL,
    multitenantDatabasePoolUrl,
    multitenantDatabaseUrl,
    pgQueueConnectionURL,
  } = getConfig()

  let directUrl = pgQueueConnectionURL ?? databaseURL
  let pooledUrl = directUrl

  if (isMultitenant && !pgQueueConnectionURL) {
    if (!multitenantDatabaseUrl) {
      throw new Error('running storage in multi-tenant but DB_MULTITENANT_DATABASE_URL is not set')
    }
    directUrl = multitenantDatabaseUrl
    pooledUrl = multitenantDatabasePoolUrl || multitenantDatabaseUrl
  }

  return { pooledUrl, directUrl }
}

/**
 * The pg pool the pgque adapter runs on (`PG_QUEUE_ADAPTER=pgque`) — same connection
 * selection, pool size, application_name, and statement_timeout bound as the pg-boss pool,
 * so an adapter benchmark compares engines, not connection budgets. pgQue's statements are
 * all short (receives return immediately; there is no server-side long-poll), verified under
 * a 200k-backlog drain with the bound active. This pool may sit behind a transaction pooler:
 * the adapter runs single-statement autocommit queries only, and its schema is provisioned
 * on the direct URL by `pgque.ts` before the wave starts.
 */
export function createQueuePgPool(): Pool {
  const { pgQueueMaxConnections, pgQueueReadWriteTimeout, databaseApplicationName } = getConfig()
  const { pooledUrl } = queueConnectionConfig()

  const pool = new Pool({
    connectionString: pooledUrl,
    min: 0,
    max: pgQueueMaxConnections,
    application_name: databaseApplicationName,
    statement_timeout: pgQueueReadWriteTimeout > 0 ? pgQueueReadWriteTimeout : undefined,
  })

  // An unlistened pool 'error' (idle client dropped) kills the process.
  pool.on('error', (error) => {
    logSchema.error(logger, '[Queue] pgque pool error', {
      type: 'queue',
      error,
    })
  })

  return pool
}

/**
 * Adapts a live tenant transaction (`DatabaseExecutor`) into the pg-boss `Db` shape wave's
 * pgboss adapter accepts as a produce `ctx` — the transactional-enqueue seam: the append
 * commits or rolls back with the caller's own transaction (v1's `PgQueueDB`).
 */
export class TransactionalQueueDb extends EventEmitter implements Db {
  events = {
    error: 'error',
  }

  constructor(protected readonly db: DatabaseExecutor) {
    super()
  }

  async executeSql(text: string, values: unknown[]): Promise<{ rows: unknown[] }> {
    const result = await this.db.query({
      text,
      values: values.map((value) => (value === undefined ? null : value)),
    })

    return { rows: result.rows }
  }
}

/** The produce `ctx` for enqueuing on the caller's own transaction. */
export function txQueueCtx(tnx: DatabaseExecutor): TransactionalQueueDb {
  return new TransactionalQueueDb(tnx)
}
