import knex, { Knex } from 'knex'

/**
 * Test DB singleton.
 *
 * We open one postgres-superuser knex per vitest worker (per file). Factories
 * use this connection to INSERT/DELETE directly, bypassing RLS and the
 * tenant-routed connection pool entirely. The *app under test* still uses the
 * real routed connection — only setup/teardown cheats.
 */
let pool: Knex | undefined

export function getTestKnex(): Knex {
  if (pool) return pool
  const url = process.env.DATABASE_URL
  if (!url) {
    throw new Error('DATABASE_URL must be set for test_v2 factories')
  }
  pool = knex({
    client: 'pg',
    connection: url,
    pool: { min: 1, max: 4 },
  })
  return pool
}

export async function disposeTestKnex(): Promise<void> {
  if (pool) {
    await pool.destroy()
    pool = undefined
  }
}

/**
 * storage.objects and storage.buckets have a BEFORE DELETE trigger that blocks
 * direct deletes unless `storage.allow_delete_query` is set. Test cleanup needs
 * this set, so we run the destructive query inside a transaction with the GUC
 * applied locally.
 */
export async function withDeleteEnabled<T>(
  db: Knex,
  fn: (trx: Knex.Transaction) => Promise<T>
): Promise<T> {
  const trx = await db.transaction()
  try {
    await trx.raw(`SELECT set_config('storage.allow_delete_query', 'true', true)`)
    const result = await fn(trx)
    await trx.commit()
    return result
  } catch (err) {
    await trx.rollback()
    throw err
  }
}
