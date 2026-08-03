import EventEmitter from 'node:events'
import type { DatabaseExecutor } from '@internal/database'
import type { Db } from 'pg-boss'

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
