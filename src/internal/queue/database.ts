import EventEmitter from 'node:events'
import type { DatabaseExecutor } from '@internal/database'
import type { Db } from 'pg-boss'
import { getConfig } from '../../config'

/** Connection selection mirrors v1: the explicit queue URL wins; multitenant deployments
 * fall back to the multitenant DB. `pooledUrl` is what the runtime pool connects to: the
 * transaction pooler when one is configured, otherwise the same as `directUrl`. `directUrl`
 * is ALWAYS a direct connection and is what the pg-boss migrations ride, since a transaction
 * pooler cannot run them (see `runPgBossMigrations` in `boss.ts`). */
export function queueConnectionConfig(): { pooledUrl: string; directUrl: string } {
  const {
    isMultitenant,
    databaseURL,
    multitenantDatabasePoolUrl,
    multitenantDatabaseUrl,
    pgQueueConnectionURL,
  } = getConfig()

  // `||`: an empty PG_QUEUE_CONNECTION_URL (as in .env.sample) means unset.
  let directUrl = pgQueueConnectionURL || databaseURL
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
 * Adapts a live tenant transaction (`DatabaseExecutor`) into the pg-boss `Db` shape the
 * pg-boss adapter accepts as a produce `ctx`: the send commits or rolls back with the
 * caller's own transaction (v1's `PgQueueDB`).
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
