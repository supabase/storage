import dotenv from 'dotenv'

dotenv.config()

import { runMigrationsOnTenant } from '@internal/database/migrations'
import { Client } from 'pg'
import { getConfig } from '../config'

// Runs every tenant migration, then checks all storage plpgsql functions with
// plpgsql_check, which is what `supabase db lint` runs against user projects.
// Requires the plpgsql_check extension to be installable on the target database.
const FAILING_LEVELS = ['error', 'warning']

const CHECK_SQL = `
SELECT p.oid::regprocedure::text AS function, r.lineno, r.level, r.sqlstate, r.message, r.detail
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
JOIN pg_catalog.pg_language l ON l.oid = p.prolang
CROSS JOIN LATERAL plpgsql_check_function_tb(
  p.oid,
  COALESCE((SELECT t.tgrelid FROM pg_catalog.pg_trigger t WHERE t.tgfoid = p.oid LIMIT 1), 0)
) r
WHERE l.lanname = 'plpgsql'
  AND n.nspname = 'storage'
  AND (p.prorettype <> 'trigger'::regtype OR EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t WHERE t.tgfoid = p.oid))
ORDER BY 1, 2
`

void (async () => {
  const { databaseURL, dbMigrationFreezeAt } = getConfig()
  await runMigrationsOnTenant({
    databaseUrl: databaseURL,
    upToMigration: dbMigrationFreezeAt,
  })

  const client = new Client({ connectionString: databaseURL })
  await client.connect()
  let rows: Record<string, string>[]
  try {
    // Rolled back so a pre-existing plpgsql_check install is left untouched
    await client.query('BEGIN')
    await client.query('CREATE EXTENSION IF NOT EXISTS plpgsql_check')
    ;({ rows } = await client.query(CHECK_SQL))
  } finally {
    await client.query('ROLLBACK').catch(() => {})
    await client.end()
  }

  const failures = rows.filter((row) => FAILING_LEVELS.includes(row.level))
  for (const row of failures) {
    const location = row.lineno ? `${row.function} line ${row.lineno}` : row.function
    console.error(`${row.level} ${location}: ${row.message} (${row.sqlstate})`)
    if (row.detail) {
      console.error(`  ${row.detail}`)
    }
  }

  if (failures.length > 0) {
    console.error(
      `\n${failures.length} plpgsql_check finding(s) at level ${FAILING_LEVELS.join('/')}. ` +
        'These surface to users via `supabase db lint`. For records populated by dynamic SQL, ' +
        "declare their shape with PERFORM 'PRAGMA:TYPE:<var> (<col> <type>, ...)'."
    )
    process.exit(1)
  }

  console.log(`plpgsql_check passed (${rows.length} lower-level finding(s) ignored)`)
  process.exit(0)
})().catch((e) => {
  console.error(e)
  process.exit(1)
})
