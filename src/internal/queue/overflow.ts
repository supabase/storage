import { multitenantKnex } from '@internal/database'
import { Knex } from 'knex'
import { PG_BOSS_SCHEMA } from './queue'

const CREATED_STATE = 'created'
const JOB_TABLE = `${PG_BOSS_SCHEMA}.job`
export const JOB_OVERFLOW_BACKUP_TABLE = `${PG_BOSS_SCHEMA}.job_overflow_backup`
export const JOB_OVERFLOW_LIST_LIMIT_DEFAULT = 50
export const JOB_OVERFLOW_RESTORE_LIMIT_DEFAULT = 50000

export type QueueOverflowSource = 'job' | 'backup'
export type QueueOverflowGroupBy = 'summary' | 'tenant'

export interface QueueOverflowFilters {
  eventTypes?: readonly string[]
  name?: string
  tenantRefs?: readonly string[]
}

export interface ListQueueOverflowOptions extends QueueOverflowFilters {
  groupBy?: QueueOverflowGroupBy
  limit?: number
  source?: QueueOverflowSource
}

export interface MoveQueueOverflowOptions extends QueueOverflowFilters {
  limit?: number
}

export interface QueueOverflowSummaryRow {
  count: number
  eventType: string | null
  name: string
}

export interface QueueOverflowTenantRow {
  count: number
  tenantRef: string | null
}

interface QueueOverflowWhereClause {
  bindings: unknown[]
  sql: string
}

type KnexLike = Knex | Knex.Transaction

export function parseQueueOverflowCsv(value: string | undefined) {
  if (!value) {
    return undefined
  }

  const normalized = Array.from(
    new Set(
      value
        .split(',')
        .map((item) => item.trim())
        .filter((item) => item.length > 0)
    )
  )

  return normalized.length > 0 ? normalized : undefined
}

function normalizeOverflowString(value: string | undefined) {
  const normalized = value?.trim()
  return normalized ? normalized : undefined
}

function normalizeOverflowStringList(values: readonly string[] | undefined) {
  if (!values) {
    return undefined
  }

  const normalized = Array.from(
    new Set(values.map((value) => value.trim()).filter((value) => value.length > 0))
  )

  return normalized.length > 0 ? normalized : undefined
}

export function normalizeQueueOverflowFilters(filters: QueueOverflowFilters): QueueOverflowFilters {
  return {
    name: normalizeOverflowString(filters.name),
    eventTypes: normalizeOverflowStringList(filters.eventTypes),
    tenantRefs: normalizeOverflowStringList(filters.tenantRefs),
  }
}

export function buildQueueOverflowWhereClause(
  filters: QueueOverflowFilters
): QueueOverflowWhereClause {
  const normalizedFilters = normalizeQueueOverflowFilters(filters)
  const clauses = ['state = ?']
  const bindings: unknown[] = [CREATED_STATE]

  if (normalizedFilters.name) {
    clauses.push('name = ?')
    bindings.push(normalizedFilters.name)
  }

  if (normalizedFilters.eventTypes?.length) {
    clauses.push(
      `data->'event'->>'type' IN (${normalizedFilters.eventTypes.map(() => '?').join(', ')})`
    )
    bindings.push(...normalizedFilters.eventTypes)
  }

  if (normalizedFilters.tenantRefs?.length) {
    clauses.push(
      `data->'tenant'->>'ref' IN (${normalizedFilters.tenantRefs.map(() => '?').join(', ')})`
    )
    bindings.push(...normalizedFilters.tenantRefs)
  }

  return {
    sql: clauses.join(' AND '),
    bindings,
  }
}

export async function queueOverflowBackupTableExists(db: KnexLike = multitenantKnex) {
  const result = await db.raw('SELECT to_regclass(?) AS table_name', [JOB_OVERFLOW_BACKUP_TABLE])
  return Boolean(result.rows[0]?.table_name)
}

export async function ensureQueueOverflowBackupTable(db: KnexLike = multitenantKnex) {
  const existed = await queueOverflowBackupTableExists(db)

  if (!existed) {
    await db.raw(
      `CREATE TABLE IF NOT EXISTS ${JOB_OVERFLOW_BACKUP_TABLE} (LIKE ${JOB_TABLE} INCLUDING ALL)`
    )
  }

  return {
    created: !existed,
  }
}

function resolveQueueOverflowTable(source: QueueOverflowSource) {
  return source === 'backup' ? JOB_OVERFLOW_BACKUP_TABLE : JOB_TABLE
}

function buildMatchingJobIdsQuery(
  db: KnexLike,
  tableName: string,
  filters: QueueOverflowFilters,
  limit?: number
) {
  const whereClause = buildQueueOverflowWhereClause(filters)
  const query = db(tableName)
    .select('id')
    .whereRaw(whereClause.sql, whereClause.bindings)
    .orderBy('id')

  if (limit !== undefined) {
    query.limit(limit)
  }

  return query
}

export async function listQueueOverflow(
  options: ListQueueOverflowOptions,
  db: KnexLike = multitenantKnex
) {
  const source = options.source ?? 'job'
  const groupBy = options.groupBy ?? 'summary'
  const limit = options.limit ?? JOB_OVERFLOW_LIST_LIMIT_DEFAULT
  const filters = normalizeQueueOverflowFilters(options)
  const backupTableExists = source === 'backup' ? await queueOverflowBackupTableExists(db) : true

  if (!backupTableExists) {
    return {
      backupTableExists,
      data: [] as QueueOverflowSummaryRow[] | QueueOverflowTenantRow[],
      filters,
      groupBy,
      source,
    }
  }

  const tableName = resolveQueueOverflowTable(source)
  const whereClause = buildQueueOverflowWhereClause(filters)

  if (groupBy === 'tenant') {
    const rows = (await db(tableName)
      .select(db.raw("data->'tenant'->>'ref' AS tenant_ref"))
      .count<{ count: string; tenant_ref: string | null }[]>('* AS count')
      .whereRaw(whereClause.sql, whereClause.bindings)
      .groupByRaw("data->'tenant'->>'ref'")
      .orderBy('count', 'desc')
      .orderBy('tenant_ref', 'asc')
      .limit(limit)) as { count: string; tenant_ref: string | null }[]

    return {
      backupTableExists,
      data: rows.map((row) => ({
        count: Number(row.count),
        tenantRef: row.tenant_ref,
      })),
      filters,
      groupBy,
      source,
    }
  }

  const rows = (await db(tableName)
    .select('name')
    .select(db.raw("data->'event'->>'type' AS event_type"))
    .count<{ count: string; event_type: string | null; name: string }[]>('* AS count')
    .whereRaw(whereClause.sql, whereClause.bindings)
    .groupBy('name')
    .groupByRaw("data->'event'->>'type'")
    .orderBy('count', 'desc')
    .orderBy('name', 'asc')
    .orderBy('event_type', 'asc')
    .limit(limit)) as { count: string; event_type: string | null; name: string }[]

  return {
    backupTableExists,
    data: rows.map((row) => ({
      count: Number(row.count),
      eventType: row.event_type,
      name: row.name,
    })),
    filters,
    groupBy,
    source,
  }
}

async function moveQueueOverflowJobs(
  db: KnexLike,
  options: MoveQueueOverflowOptions,
  sourceTable: string,
  targetTable: string
) {
  const selectedIdsQuery = buildMatchingJobIdsQuery(db, sourceTable, options, options.limit)
  const compiledQuery = selectedIdsQuery.toSQL()
  const result = await db.raw(
    `
      WITH moved AS (
        DELETE FROM ${sourceTable}
        WHERE id IN (${compiledQuery.sql})
        RETURNING *
      ),
      inserted AS (
        INSERT INTO ${targetTable}
        SELECT * FROM moved
        RETURNING 1
      )
      SELECT COUNT(*)::bigint AS moved_count FROM inserted
    `,
    compiledQuery.bindings
  )

  return Number(result.rows[0]?.moved_count ?? 0)
}

export async function backupQueueOverflow(
  options: MoveQueueOverflowOptions,
  db: KnexLike = multitenantKnex
) {
  return db.transaction(async (tnx) => {
    const { created } = await ensureQueueOverflowBackupTable(tnx)
    await tnx.raw(`LOCK TABLE ${JOB_TABLE} IN SHARE ROW EXCLUSIVE MODE`)

    const movedCount = await moveQueueOverflowJobs(
      tnx,
      options,
      JOB_TABLE,
      JOB_OVERFLOW_BACKUP_TABLE
    )

    return {
      backupTableCreated: created,
      filters: normalizeQueueOverflowFilters(options),
      limit: options.limit ?? null,
      movedCount,
    }
  })
}

export async function restoreQueueOverflow(
  options: MoveQueueOverflowOptions,
  db: KnexLike = multitenantKnex
) {
  const backupTableExists = await queueOverflowBackupTableExists(db)

  if (!backupTableExists) {
    return {
      backupTableExists,
      filters: normalizeQueueOverflowFilters(options),
      limit: options.limit ?? JOB_OVERFLOW_RESTORE_LIMIT_DEFAULT,
      movedCount: 0,
    }
  }

  return db.transaction(async (tnx) => {
    const movedCount = await moveQueueOverflowJobs(
      tnx,
      { ...options, limit: options.limit ?? JOB_OVERFLOW_RESTORE_LIMIT_DEFAULT },
      JOB_OVERFLOW_BACKUP_TABLE,
      JOB_TABLE
    )

    return {
      backupTableExists: true,
      filters: normalizeQueueOverflowFilters(options),
      limit: options.limit ?? JOB_OVERFLOW_RESTORE_LIMIT_DEFAULT,
      movedCount,
    }
  })
}
