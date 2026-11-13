import type { Db } from 'pg-boss'

/** A minimal query executor: what a `pg` client, pool or transaction already is. */
export interface PgExecutor {
  query(text: string, values?: unknown[]): Promise<{ rows: unknown[] }>
}

/**
 * The produce context `send` accepts, typically the caller's open transaction: a plain
 * {@link PgExecutor}, or a pg-boss `Db` (what `fromDrizzle`/`fromKysely`/… produce).
 */
export type PgBossCtx = PgExecutor | Db

const isDb = (ctx: PgBossCtx): ctx is Db =>
  typeof (ctx as { executeSql?: unknown }).executeSql === 'function'

/** Adapt a produce ctx to the `Db` shape pg-boss calls take. */
export const toDb = (ctx: PgBossCtx): Db =>
  isDb(ctx)
    ? ctx
    : {
        executeSql: async (text, values) => ({ rows: (await ctx.query(text, values)).rows }),
      }

/** pg-boss's pool-backed Db pins one client for a callback; custom `db` adapters may not. */
export const hasTransaction = (
  db: Db
): db is Db & { withTransaction<T>(fn: (db: Db) => Promise<T>): Promise<T> } =>
  typeof (db as { withTransaction?: unknown }).withTransaction === 'function'
