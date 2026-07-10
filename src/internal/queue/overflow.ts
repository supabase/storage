import { ErrorCode, StorageBackendError } from '@internal/errors'
import type { QueryResultRow } from 'pg'
import type {
  DatabaseExecutor,
  DatabaseQueryOptions,
  DatabaseTransaction,
  TransactionOptions,
} from '../database/connection'
import { quoteQualifiedIdentifier } from '../database/sql'
import { PG_BOSS_SCHEMA } from './constants'

const CREATED_STATE = 'created'
const QUEUE_OVERFLOW_ADVISORY_LOCK_KEY = '-5525285245963000612'
export const JOB_OVERFLOW_LIST_LIMIT_DEFAULT = 50
export const JOB_OVERFLOW_RESTORE_LIMIT_DEFAULT = 10000
export const QUEUE_OVERFLOW_UNSCOPED_BACKUP_MESSAGE =
  'Backup requires at least one queue, event type, or tenant filter unless confirmAll is true'

export type QueueOverflowSource = 'job' | 'backup'
export type QueueOverflowGroupBy = 'summary' | 'tenant'

export interface QueueOverflowFilters {
  eventTypes?: readonly string[]
  name?: string
  tenantRefs?: readonly string[]
}

interface QueueOverflowOperationOptions {
  signal?: AbortSignal
}

export interface ListQueueOverflowOptions
  extends QueueOverflowFilters,
    QueueOverflowOperationOptions {
  groupBy?: QueueOverflowGroupBy
  limit?: number
  source?: QueueOverflowSource
}

export interface MoveQueueOverflowOptions
  extends QueueOverflowFilters,
    QueueOverflowOperationOptions {
  limit?: number
}

export interface BackupQueueOverflowOptions extends MoveQueueOverflowOptions {
  confirmAll?: boolean
}

interface QueueOverflowWhereClause {
  sql: string
  values: unknown[]
}

interface QueueOverflowAggregateRow extends QueryResultRow {
  count: number | string
  group_count: number | string
  total_count: number | string
}

interface QueueOverflowTotalCountRow extends QueryResultRow {
  total_count: number | string
}

interface QueueOverflowSummaryDbRow extends QueueOverflowAggregateRow {
  event_type: string | null
  name: string
}

interface QueueOverflowTenantDbRow extends QueueOverflowAggregateRow {
  tenant_ref: string | null
}

interface QueueOverflowCountRow extends QueryResultRow {
  moved_count: number | string
}

interface QueueOverflowRestoreCountRow extends QueueOverflowCountRow {
  selected_count: number | string
}

interface QueueOverflowEngineRow extends QueryResultRow {
  is_oriole: boolean
}

interface QueueOverflowTableRow extends QueryResultRow {
  table_name: string | null
}

interface QueueOverflowIndexRow extends QueryResultRow {
  index_name: string
}

export interface QueueOverflowDatabase {
  beginTransaction(
    options?: TransactionOptions & DatabaseQueryOptions
  ): Promise<DatabaseTransaction>
}

function normalizeOverflowString(value: string | undefined) {
  const normalized = value?.trim()
  return normalized ? normalized : undefined
}

function normalizeStringList(values: readonly string[] | undefined): string[] | undefined {
  if (!values) {
    return undefined
  }

  const normalized = [...new Set(values.map((value) => value.trim()).filter(Boolean))]
  return normalized.length ? normalized : undefined
}

export function parseCommaSeparatedList(value: string | undefined) {
  return value ? normalizeStringList(value.split(',')) : undefined
}

export function normalizeQueueOverflowFilters(filters: QueueOverflowFilters): QueueOverflowFilters {
  return {
    name: normalizeOverflowString(filters.name),
    eventTypes: normalizeStringList(filters.eventTypes),
    tenantRefs: normalizeStringList(filters.tenantRefs),
  }
}

export function buildQueueOverflowWhereClause(
  filters: QueueOverflowFilters
): QueueOverflowWhereClause {
  const normalizedFilters = normalizeQueueOverflowFilters(filters)
  const clauses = ['state = $1']
  const values: unknown[] = [CREATED_STATE]

  if (normalizedFilters.name) {
    values.push(normalizedFilters.name)
    clauses.push(`name = $${values.length}`)
  }

  if (normalizedFilters.eventTypes?.length) {
    values.push(normalizedFilters.eventTypes)
    clauses.push(`data->'event'->>'type' = ANY($${values.length}::text[])`)
  }

  if (normalizedFilters.tenantRefs?.length) {
    values.push(normalizedFilters.tenantRefs)
    clauses.push(`data->'tenant'->>'ref' = ANY($${values.length}::text[])`)
  }

  return {
    sql: clauses.join(' AND '),
    values,
  }
}

export class QueueOverflowStorePg {
  private readonly backupTableName: string
  private readonly jobTable: string
  private readonly backupTable: string

  constructor(
    private readonly db: QueueOverflowDatabase,
    schema = PG_BOSS_SCHEMA
  ) {
    const jobTableName = `${schema}.job`
    this.backupTableName = `${schema}.job_overflow_backup`
    this.jobTable = quoteQualifiedIdentifier(jobTableName)
    this.backupTable = quoteQualifiedIdentifier(this.backupTableName)
  }

  async countCreated(options: QueueOverflowOperationOptions = {}) {
    return this.withMaintenanceTransaction(options.signal, async (transaction) => {
      const result = await transaction.query<QueueOverflowTotalCountRow>(
        {
          text: `
            SELECT COUNT(*)::bigint AS total_count
            FROM ${this.jobTable}
            WHERE state = $1
          `,
          values: [CREATED_STATE],
        },
        { signal: options.signal }
      )

      return {
        totalCount: Number(result.rows[0]?.total_count ?? 0),
      }
    })
  }

  async list(options: ListQueueOverflowOptions) {
    const source = options.source ?? 'job'
    const groupBy = options.groupBy ?? 'summary'
    const limit = options.limit ?? JOB_OVERFLOW_LIST_LIMIT_DEFAULT
    const filters = normalizeQueueOverflowFilters(options)

    assertPositiveSafeInteger(limit, 'limit')

    return this.withMaintenanceTransaction(options.signal, async (transaction) => {
      const sourceTableExists =
        source === 'backup' ? await this.backupTableExists(transaction, options.signal) : true

      if (!sourceTableExists) {
        return {
          sourceTableExists,
          data: [],
          filters,
          groupBy,
          groupCount: 0,
          hasMore: false,
          source,
          totalCount: 0,
        }
      }

      const tableName = source === 'backup' ? this.backupTable : this.jobTable
      const whereClause = buildQueueOverflowWhereClause(filters)
      const values = [...whereClause.values, limit]
      const limitPlaceholder = `$${values.length}`

      if (groupBy === 'tenant') {
        const result = await transaction.query<QueueOverflowTenantDbRow>(
          {
            text: `
              SELECT
                data->'tenant'->>'ref' AS tenant_ref,
                COUNT(*)::bigint AS count,
                COUNT(*) OVER ()::bigint AS group_count,
                SUM(COUNT(*)) OVER ()::bigint AS total_count
              FROM ${tableName}
              WHERE ${whereClause.sql}
              GROUP BY data->'tenant'->>'ref'
              ORDER BY count DESC, tenant_ref ASC
              LIMIT ${limitPlaceholder}
            `,
            values,
          },
          { signal: options.signal }
        )

        const groupCount = Number(result.rows[0]?.group_count ?? 0)

        return {
          sourceTableExists,
          data: result.rows.map((row) => ({
            count: Number(row.count),
            tenantRef: row.tenant_ref,
          })),
          filters,
          groupBy,
          groupCount,
          hasMore: groupCount > result.rows.length,
          source,
          totalCount: Number(result.rows[0]?.total_count ?? 0),
        }
      }

      const result = await transaction.query<QueueOverflowSummaryDbRow>(
        {
          text: `
            SELECT
              name,
              data->'event'->>'type' AS event_type,
              COUNT(*)::bigint AS count,
              COUNT(*) OVER ()::bigint AS group_count,
              SUM(COUNT(*)) OVER ()::bigint AS total_count
            FROM ${tableName}
            WHERE ${whereClause.sql}
            GROUP BY name, data->'event'->>'type'
            ORDER BY count DESC, name ASC, event_type ASC
            LIMIT ${limitPlaceholder}
          `,
          values,
        },
        { signal: options.signal }
      )

      const groupCount = Number(result.rows[0]?.group_count ?? 0)

      return {
        sourceTableExists,
        data: result.rows.map((row) => ({
          count: Number(row.count),
          eventType: row.event_type,
          name: row.name,
        })),
        filters,
        groupBy,
        groupCount,
        hasMore: groupCount > result.rows.length,
        source,
        totalCount: Number(result.rows[0]?.total_count ?? 0),
      }
    })
  }

  async backup(options: BackupQueueOverflowOptions) {
    const filters = normalizeQueueOverflowFilters(options)
    if (
      options.confirmAll !== true &&
      !filters.name &&
      !filters.eventTypes?.length &&
      !filters.tenantRefs?.length
    ) {
      throw new Error(QUEUE_OVERFLOW_UNSCOPED_BACKUP_MESSAGE)
    }

    if (options.limit !== undefined) {
      assertPositiveSafeInteger(options.limit, 'limit')
    }

    return this.withMaintenanceTransaction(options.signal, async (transaction) => {
      await this.acquireMaintenanceLock(transaction, options.signal)
      const backupTableCreated = await this.ensureBackupTable(transaction, options.signal)

      await transaction.query(`LOCK TABLE ${this.jobTable} IN SHARE ROW EXCLUSIVE MODE`, {
        signal: options.signal,
      })

      const movedCount = await this.moveJobs(
        transaction,
        filters,
        this.jobTable,
        this.backupTable,
        options.limit,
        options.signal
      )

      return {
        backupTableCreated,
        filters,
        limit: options.limit ?? null,
        movedCount,
      }
    })
  }

  async restore(options: MoveQueueOverflowOptions) {
    const limit = options.limit ?? JOB_OVERFLOW_RESTORE_LIMIT_DEFAULT
    assertPositiveSafeInteger(limit, 'limit')
    const filters = normalizeQueueOverflowFilters(options)

    return this.withMaintenanceTransaction(options.signal, async (transaction) => {
      await this.acquireMaintenanceLock(transaction, options.signal)
      const backupTableExists = await this.backupTableExists(transaction, options.signal)

      if (!backupTableExists) {
        return {
          backupTableExists,
          conflictCount: 0,
          filters,
          hasMore: false,
          limit,
          movedCount: 0,
        }
      }

      const engine = await transaction.query<QueueOverflowEngineRow>(
        `SELECT EXISTS (
          SELECT 1 FROM pg_extension WHERE extname = 'orioledb'
        ) AS is_oriole`,
        { signal: options.signal }
      )

      if (engine.rows[0]?.is_oriole) {
        throw new StorageBackendError({
          code: ErrorCode.NotSupported,
          httpStatusCode: 409,
          message: 'Queue overflow restore is not supported on OrioleDB',
        })
      }

      await transaction.query(`LOCK TABLE ${this.jobTable} IN SHARE ROW EXCLUSIVE MODE`, {
        signal: options.signal,
      })

      const { conflictCount, hasMore, movedCount } = await this.restoreJobs(
        transaction,
        filters,
        limit,
        options.signal
      )

      return {
        backupTableExists,
        conflictCount,
        filters,
        hasMore,
        limit,
        movedCount,
      }
    })
  }

  private async backupTableExists(db: DatabaseExecutor, signal?: AbortSignal) {
    const result = await db.query<QueueOverflowTableRow>(
      {
        text: 'SELECT to_regclass($1) AS table_name',
        values: [this.backupTableName],
      },
      { signal }
    )

    return Boolean(result.rows[0]?.table_name)
  }

  private async ensureBackupTable(db: DatabaseExecutor, signal?: AbortSignal) {
    const existed = await this.backupTableExists(db, signal)

    await db.query(
      `CREATE TABLE IF NOT EXISTS ${this.backupTable} (
        LIKE ${this.jobTable} INCLUDING DEFAULTS,
        PRIMARY KEY (name, id)
      )`,
      {
        signal,
      }
    )

    const legacyIndexes = await db.query<QueueOverflowIndexRow>(
      {
        text: `
          SELECT format('%I.%I', index_namespace.nspname, index_class.relname) AS index_name
          FROM pg_index AS backup_index
          JOIN pg_class AS index_class ON index_class.oid = backup_index.indexrelid
          JOIN pg_namespace AS index_namespace ON index_namespace.oid = index_class.relnamespace
          WHERE backup_index.indrelid = to_regclass($1)
            AND backup_index.indisunique
            AND backup_index.indpred IS NOT NULL
        `,
        values: [this.backupTableName],
      },
      { signal }
    )

    for (const index of legacyIndexes.rows) {
      await db.query(`DROP INDEX IF EXISTS ${index.index_name}`, { signal })
    }

    return !existed
  }

  private async moveJobs(
    db: DatabaseExecutor,
    filters: QueueOverflowFilters,
    sourceTable: string,
    targetTable: string,
    limit?: number,
    signal?: AbortSignal
  ) {
    const whereClause = buildQueueOverflowWhereClause(filters)
    const values = [...whereClause.values]
    let selectionBoundary = ''

    if (limit !== undefined) {
      values.push(limit)
      selectionBoundary = `ORDER BY name, id LIMIT $${values.length}`
    }

    const result = await db.query<QueueOverflowCountRow>(
      {
        text: `
          WITH selected AS (
            SELECT name, id
            FROM ${sourceTable}
            WHERE ${whereClause.sql}
            ${selectionBoundary}
          ),
          moved AS (
            DELETE FROM ${sourceTable} AS source_job
            USING selected
            WHERE source_job.name = selected.name
              AND source_job.id = selected.id
            RETURNING source_job.*
          ),
          inserted AS (
            INSERT INTO ${targetTable}
            SELECT * FROM moved
            RETURNING 1
          )
          SELECT COUNT(*)::bigint AS moved_count FROM inserted
        `,
        values,
      },
      { signal }
    )

    return Number(result.rows[0]?.moved_count ?? 0)
  }

  private async restoreJobs(
    db: DatabaseExecutor,
    filters: QueueOverflowFilters,
    limit: number,
    signal?: AbortSignal
  ) {
    const whereClause = buildQueueOverflowWhereClause(filters)
    const values = [...whereClause.values]
    values.push(limit)
    const limitPlaceholder = `$${values.length}`
    const result = await db.query<QueueOverflowRestoreCountRow>(
      {
        text: `
          WITH selected AS (
            SELECT *
            FROM ${this.backupTable}
            WHERE ${whereClause.sql}
            ORDER BY name, id
            LIMIT ${limitPlaceholder}
          ),
          inserted AS (
            INSERT INTO ${this.jobTable}
            SELECT * FROM selected
            ORDER BY name, id
            ON CONFLICT DO NOTHING
            RETURNING 1
          ),
          deleted AS (
            DELETE FROM ${this.backupTable} AS source_job
            USING selected
            WHERE source_job.name = selected.name
              AND source_job.id = selected.id
            RETURNING 1
          )
          SELECT
            (SELECT COUNT(*) FROM selected) AS selected_count,
            (SELECT COUNT(*) FROM inserted) AS moved_count
        `,
        values,
      },
      { signal }
    )

    const selectedCount = Number(result.rows[0]?.selected_count ?? 0)
    const movedCount = Number(result.rows[0]?.moved_count ?? 0)

    return {
      conflictCount: selectedCount - movedCount,
      hasMore: selectedCount === limit,
      movedCount,
    }
  }

  private async acquireMaintenanceLock(db: DatabaseExecutor, signal?: AbortSignal): Promise<void> {
    await db.query(
      {
        text: 'SELECT pg_advisory_xact_lock($1::bigint)',
        values: [QUEUE_OVERFLOW_ADVISORY_LOCK_KEY],
      },
      { signal }
    )
  }

  private async withMaintenanceTransaction<T>(
    signal: AbortSignal | undefined,
    fn: (transaction: DatabaseTransaction) => Promise<T>
  ): Promise<T> {
    const transaction = await this.db.beginTransaction({ signal })

    try {
      await transaction.query(
        {
          text: `
            SELECT
              set_config('statement_timeout', $1, true),
              set_config('lock_timeout', $2, true)
          `,
          values: ['0', '30s'],
        },
        { signal }
      )

      const result = await fn(transaction)
      await transaction.commit()
      return result
    } catch (error) {
      try {
        await transaction.rollback()
      } catch (rollbackError) {
        await logQueueOverflowRollbackFailure(error, rollbackError)
      }

      throw error
    }
  }
}

async function logQueueOverflowRollbackFailure(
  originalError: unknown,
  rollbackError: unknown
): Promise<void> {
  try {
    const { logger, logSchema } = await import('@internal/monitoring')
    logSchema.warning(logger, '[QueueOverflow] Failed to rollback maintenance transaction', {
      type: 'pgboss',
      error: rollbackError,
      metadata: JSON.stringify({ originalError: String(originalError) }),
    })
  } catch (loggingError) {
    console.error('[QueueOverflow] Failed to log maintenance transaction rollback failure', {
      originalError,
      rollbackError,
      loggingError,
    })
  }
}

function assertPositiveSafeInteger(value: number, field: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${field} must be a positive safe integer`)
  }
}
