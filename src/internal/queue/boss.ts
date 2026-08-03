import { logger, logSchema } from '@internal/monitoring'
import type { QueueDefaults } from '@supabase-labs/wave-adapter-pgboss'
import { Pool, type PoolConfig } from 'pg'
import { getConstructionPlans, getMigrationPlans, PgBoss } from 'pg-boss'
// pg-boss's target schema version lives in its package manifest (`pgboss.schema`) — the same
// value its own contractor compares against. The exported plan functions are static SQL
// generators that never inspect the database, so knowing "already current" is on us.
import { pgboss as pgBossManifest } from 'pg-boss/package.json'
import { getConfig } from '../../config'
import { queueConnectionConfig } from './database'

/**
 * The pg-boss v12 instance the queue runs on — caller-owned (shared with wave's pgboss
 * adapter AND any direct admin queries), on its own `pgQueueSchemaV2` schema so it never
 * touches the legacy v10-fork schema.
 */
export function createQueueBoss(): PgBoss {
  const {
    pgQueueMaxConnections,
    pgQueueReadWriteTimeout,
    pgQueueSchemaV2,
    databaseApplicationName,
  } = getConfig()
  const { pooledUrl } = queueConnectionConfig()

  // pg-boss hands this whole object to `new pg.Pool(...)`, so `statement_timeout` (absent
  // from pg-boss's own option types) reaches every pooled connection — v1's bound on all
  // queue statements (produce inserts, worker fetch/complete, maintenance). The transactional
  // produce path rides the caller's transaction and is deliberately NOT bounded here.
  const poolTimeout: Pick<PoolConfig, 'statement_timeout'> = {
    statement_timeout: pgQueueReadWriteTimeout > 0 ? pgQueueReadWriteTimeout : undefined,
  }

  const boss = new PgBoss({
    connectionString: pooledUrl,
    schema: pgQueueSchemaV2,
    // Never migrates at runtime: this pool may sit behind a transaction pooler, which
    // cannot run migrations. The schema is provisioned on the direct URL before the wave
    // starts (`runPgBossMigrations`), mirroring the pgque adapter's `skipMigrations`.
    migrate: false,
    max: pgQueueMaxConnections,
    application_name: databaseApplicationName,
    // pg-boss's internal supervision timer stays OFF: wave's maintenance runner owns the
    // cadence (started on worker nodes in instance.ts), calling the public boss.supervise()
    // one-pass — a superset of the timer's sweep (it also ensures upcoming partitions and
    // writes queue stats) — plus the adapter's retired-group reaper. Wave forces cron
    // scheduling off regardless.
    supervise: false,
    schedule: false,
    ...poolTimeout,
  })

  // v1 parity: supervision/maintenance failures surface on the emitter, and an unlistened
  // 'error' event kills the process — e.g. statement_timeout canceling a monitor sweep
  // under a deep backlog. Log and keep running; workers are unaffected.
  boss.on('error', (error) => {
    logSchema.error(logger, '[Queue] error', {
      type: 'queue',
      error,
    })
  })

  return boss
}

/**
 * The per-queue defaults wave stamps on every queue it creates — v1's global send/queue
 * posture mapped to v12's queue-level vocabulary. v12 has no archive table, so
 * `PG_QUEUE_ARCHIVE_COMPLETED_AFTER_SECONDS` has no equivalent: completed jobs are deleted
 * directly after the delete window.
 */
export function queueDefaults(): QueueDefaults {
  const { pgQueueDeleteAfterDays, pgQueueDeleteAfterHours, pgQueueRetentionDays } = getConfig()

  const deleteAfterHours = pgQueueDeleteAfterHours ?? (pgQueueDeleteAfterDays ?? 2) * 24
  return {
    expireInSeconds: 23 * 3600,
    retentionSeconds: (pgQueueRetentionDays ?? 2) * 86_400,
    deleteAfterSeconds: deleteAfterHours * 3600,
  }
}

/**
 * Provision/upgrade the pg-boss v12 schema on a DIRECT connection before the wave starts —
 * the pg-boss twin of `runPgqueMigrations`. The runtime boss always starts with
 * `migrate: false` (see `createQueueBoss`), so this is the ONLY thing that touches the
 * schema's DDL.
 *
 * pg-boss's exported plans are static SQL generators — they never inspect the database and
 * never return an empty script — so its contractor's check-then-act logic is replicated
 * here: no version table → run the full construction plans; installed but behind → run the
 * migration plans from that version; current (or ahead, after a rollback deploy) → nothing.
 */
export async function runPgBossMigrations(): Promise<void> {
  const { pgQueueSchemaV2: schema } = getConfig()
  const { directUrl } = queueConnectionConfig()

  const pool = new Pool({ connectionString: directUrl, max: 1 })
  // An unlistened pool 'error' (the client dropping outside a query) kills the process.
  pool.on('error', (error) => {
    logSchema.error(logger, '[Queue] pgboss migration pool error', { type: 'queue', error })
  })
  const client = await pool.connect()
  try {
    // The plan scripts' own advisory lock is transaction-scoped, which leaves a
    // check-then-create race between concurrently booting nodes (both see "not installed",
    // the loser fails on CREATE TYPE). This session-scoped lock serializes the whole
    // check+apply; it is released when the connection closes below.
    await client.query(
      `SELECT pg_advisory_lock(('x' || encode(sha224(($1)::bytea), 'hex'))::bit(64)::bigint)`,
      [`pgboss.${schema}.provision`]
    )

    const installed = await client.query<{ name: string | null }>(
      `SELECT to_regclass($1) AS name`,
      [`${schema}.version`]
    )

    if (!installed.rows[0]?.name) {
      await client.query(getConstructionPlans(schema))
      logSchema.info(logger, '[Queue] pgboss schema installed', { type: 'queue' })
      return
    }

    const result = await client.query<{ version: string }>(`SELECT version FROM ${schema}.version`)
    const version = result.rows.length ? parseInt(result.rows[0].version, 10) : null

    if (version === null || version >= pgBossManifest.schema) {
      return
    }

    for (const statement of splitMigrationScript(getMigrationPlans(schema, version))) {
      await client.query(statement)
    }
    logSchema.info(logger, '[Queue] pgboss schema migrated', {
      type: 'queue',
      metadata: JSON.stringify({ from: version, to: pgBossManifest.schema }),
    })
  } finally {
    client.release()
    await pool.end()
  }
}

/**
 * `getMigrationPlans` renders one printable script: a BEGIN..COMMIT block plus any inlined
 * `CREATE INDEX CONCURRENTLY` builds appended AFTER the COMMIT, each introduced by an
 * `-- inlined from` provenance comment. CONCURRENTLY refuses to run inside a transaction —
 * and a multi-statement pg query IS an implicit transaction — so the trailing builds must
 * be executed one statement at a time.
 */
function splitMigrationScript(script: string): string[] {
  const marker = '\n-- inlined from '
  const idx = script.indexOf(marker)
  if (idx === -1) {
    return [script]
  }
  const concurrent = script
    .slice(idx)
    .split(/\n(?=-- inlined from )/)
    .map((statement) => statement.trim())
    .filter(Boolean)
  return [script.slice(0, idx), ...concurrent]
}
