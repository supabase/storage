import { randomUUID } from 'node:crypto'
import {
  type DatabaseExecutor,
  type DatabaseStatement,
  type DatabaseTransaction,
  quoteIdentifier,
  quoteQualifiedIdentifier,
  type TenantConnection,
  type TransactionOptions,
} from '@internal/database'
import { DBMigration, tenantHasMigrations } from '@internal/database/migrations'
import { ERRORS, ErrorCode, isStorageError, StorageBackendError } from '@internal/errors'
import { hashStringToInt } from '@internal/hashing'
import { logger, logSchema } from '@internal/monitoring'
import { dbQueryPerformance } from '@internal/monitoring/metrics'
import { ObjectMetadata } from '@storage/backend'
import { DatabaseError, QueryResultRow } from 'pg'
import { DatabaseEngine, getConfig } from '../../config'
import { isUuid } from '../limits'
import { Bucket, IcebergCatalog, Obj, S3MultipartUpload, S3PartUpload } from '../schemas'
import {
  Database,
  FindBucketFilters,
  FindObjectFilters,
  ListBucketOptions,
  ScannerS3Key,
  SearchObjectOption,
} from './adapter'
import { DBError, mapPgTransactionAbortedError, PgErrorContext } from './errors'

const { databaseEngine, databaseStatementTimeout, isMultitenant, databaseHealthcheckUnscoped } =
  getConfig()
// Scanner cache tables are unlogged scratch tables, not session temp tables.
// They must survive across pooled pg clients used by separate unscoped queries.
const S3_KEYS_SCRATCH_TABLE_SCHEMA = 'storage'
const S3_KEYS_SCRATCH_TABLE_PREFIX = '_s3_remote_keys_'
const S3_KEYS_SCRATCH_TABLE_MAX_AGE_MS = 24 * 60 * 60 * 1000
const S3_KEYS_SCRATCH_TABLE_PATTERN = `^${S3_KEYS_SCRATCH_TABLE_PREFIX}([0-9]{13})(?:_[A-Za-z0-9_]+)?$`

export function escapeLike(str: string) {
  return str.replace(/\\/g, '\\\\').replace(/([%_])/g, '\\$1')
}

interface PgDatabaseOptions {
  tenantId: string
  reqId?: string
  sbReqId?: string
  latestMigration?: keyof typeof DBMigration
  databaseEngine?: DatabaseEngine
  host: string
  tnx?: DatabaseTransaction
  parentTnx?: DatabaseTransaction
  parentConnection?: TenantConnection
}

interface UnscopedQueryOptions {
  readonly timeoutMs?: number
}

const HEALTHCHECK_SQL = 'SELECT id from storage.buckets limit 1'
const HEALTHCHECK_QUERY_OPTIONS: UnscopedQueryOptions = Object.freeze({
  timeoutMs: databaseStatementTimeout,
})

async function executeQuery<T extends QueryResultRow = QueryResultRow>(
  db: DatabaseExecutor,
  statement: string | DatabaseStatement,
  signal?: AbortSignal
) {
  try {
    return await db.query<T>(statement, { signal })
  } catch (error) {
    throw mapPgError(error, typeof statement === 'string' ? statement : statement.text)
  }
}

function healthcheckProbe(db: DatabaseExecutor, signal?: AbortSignal) {
  return executeQuery(db, HEALTHCHECK_SQL, signal)
}

class TestPermissionRollbackError extends Error {
  constructor() {
    super('Rollback test permission transaction')
    this.name = 'TestPermissionRollbackError'
    Object.setPrototypeOf(this, TestPermissionRollbackError.prototype)
  }
}

const testPermissionRollbackError = new TestPermissionRollbackError()
testPermissionRollbackError.stack = undefined
Object.freeze(testPermissionRollbackError)

/**
 * Pg-backed storage metadata adapter.
 */
export class StoragePgDB implements Database {
  public readonly tenantHost: string
  public readonly tenantId: string
  public readonly reqId: string | undefined
  public readonly sbReqId: string | undefined
  public readonly role?: string
  public readonly latestMigration?: keyof typeof DBMigration

  constructor(
    public readonly connection: TenantConnection,
    private readonly options: PgDatabaseOptions
  ) {
    this.tenantHost = options.host
    this.tenantId = options.tenantId
    this.reqId = options.reqId
    this.sbReqId = options.sbReqId
    this.role = connection.role
    this.latestMigration = options.latestMigration
  }

  async withTransaction<T>(
    fn: (db: StoragePgDB) => Promise<T>,
    opts?: TransactionOptions
  ): Promise<T> {
    const parentTnx = this.options.tnx
    const tnx = parentTnx ?? (await this.connection.transaction(opts))
    const savepoint = parentTnx ? nextSavepointName() : undefined
    let savepointEstablished = false

    try {
      if (savepoint) {
        await createSavepoint(tnx, savepoint)
        savepointEstablished = true
      }

      await this.connection.setScope(tnx)

      const storageWithTnx = new StoragePgDB(this.connection, {
        ...this.options,
        tnx,
      })

      const result = await fn(storageWithTnx)

      if (savepoint) {
        if (
          this.options.parentConnection?.role &&
          this.connection.role !== this.options.parentConnection.role
        ) {
          // Keep scope restoration inside the savepoint. If it fails, rolling back
          // the nested unit is preferable to leaking elevated scope into the parent transaction.
          await this.options.parentConnection.setScope(tnx)
        }

        await tnx.query(`RELEASE SAVEPOINT ${savepoint}`)
      } else {
        await tnx.commit()
      }

      return result
    } catch (e) {
      if (savepointEstablished && savepoint && !tnx.isCompleted()) {
        try {
          await rollbackSavepoint(tnx, savepoint)
        } catch (rollbackError) {
          logSchema.warning(logger, '[StoragePgDB] Failed to rollback savepoint', {
            type: 'db',
            tenantId: this.tenantId,
            project: this.tenantId,
            reqId: this.reqId,
            sbReqId: this.sbReqId,
            error: rollbackError,
            metadata: JSON.stringify({ originalError: String(e), savepoint }),
          })
        }
      } else if (!savepoint) {
        try {
          await tnx.rollback()
        } catch (rollbackError) {
          logSchema.warning(logger, '[StoragePgDB] Failed to rollback transaction', {
            type: 'db',
            tenantId: this.tenantId,
            project: this.tenantId,
            reqId: this.reqId,
            sbReqId: this.sbReqId,
            error: rollbackError,
            metadata: JSON.stringify({ originalError: String(e) }),
          })
        }
      }

      throw mapPgError(e)
    }
  }

  tenant() {
    return {
      ref: this.tenantId,
      host: this.tenantHost,
    }
  }

  asSuperUser() {
    return new StoragePgDB(this.connection.asSuperUser(), {
      ...this.options,
      tnx: this.options.tnx,
      parentConnection: this.connection,
      parentTnx: this.options.tnx,
    })
  }

  async testPermission<T>(fn: (db: StoragePgDB) => T | Promise<T>): Promise<Awaited<T>> {
    let result: Awaited<T>
    try {
      await this.withTransaction(async (db) => {
        result = await fn(db)
        throw testPermissionRollbackError
      })
    } catch (e) {
      if (e === testPermissionRollbackError) {
        return result!
      }
      throw e
    }

    return result!
  }

  deleteAnalyticsBucket(id: string, opts?: { soft: boolean }): Promise<IcebergCatalog> {
    return this.runQuery('DeleteAnalyticsBucket', async (db, signal) => {
      const result = opts?.soft
        ? await this.query<IcebergCatalog>(
            db,
            {
              text: `
                UPDATE storage.buckets_analytics
                SET deleted_at = now()
                WHERE id = $1
                  AND deleted_at IS NULL
                RETURNING *
              `,
              values: [id],
            },
            signal
          )
        : await this.query<IcebergCatalog>(
            db,
            {
              text: `
                DELETE FROM storage.buckets_analytics
                WHERE id = $1
                RETURNING *
              `,
              values: [id],
            },
            signal
          )

      const deleted = result.rows[0]
      if (!deleted) {
        throw ERRORS.NoSuchBucket(id)
      }

      return deleted
    })
  }

  listAnalyticsBuckets(
    columns: string,
    options: ListBucketOptions | undefined
  ): Promise<IcebergCatalog[]> {
    return this.runQuery('ListIcebergBuckets', async (db, signal) => {
      const values: unknown[] = []
      const conditions = ['deleted_at IS NULL']

      if (options?.search !== undefined && options.search.length > 0) {
        values.push(`%${escapeLike(options.search)}%`)
        conditions.push(`name LIKE $${values.length}`)
      }

      const orderBy = options?.sortColumn
        ? `${quoteIdentifier(options.sortColumn)} ${normalizeSortOrder(options.sortOrder)}`
        : `"name" ASC`

      let pagination = ''
      if (options?.limit !== undefined) {
        values.push(options.limit)
        pagination += ` LIMIT $${values.length}`
      }
      if (options?.offset !== undefined) {
        values.push(options.offset)
        pagination += ` OFFSET $${values.length}`
      }

      const result = await this.query<IcebergCatalog>(
        db,
        {
          text: `
            SELECT ${selectColumns(columns)}
            FROM storage.buckets_analytics
            WHERE ${conditions.join(' AND ')}
            ORDER BY ${orderBy}
            ${pagination}
          `,
          values,
        },
        signal
      )

      return result.rows
    })
  }

  findAnalyticsBucketByName(name: string): Promise<IcebergCatalog> {
    return this.runQuery('FindAnalyticsBucketByName', async (db, signal) => {
      const result = await this.query<IcebergCatalog>(
        db,
        {
          text: `
            SELECT *
            FROM storage.buckets_analytics
            WHERE name = $1
              AND deleted_at IS NULL
            LIMIT 1
          `,
          values: [name],
        },
        signal
      )

      const bucket = result.rows[0]
      if (!bucket) {
        throw ERRORS.NoSuchBucket(name)
      }

      return bucket
    })
  }

  createAnalyticsBucket(data: Pick<Bucket, 'name'>): Promise<IcebergCatalog> {
    return this.runQuery('CreateAnalyticsBucket', async (db, signal) => {
      const result = await this.query<IcebergCatalog>(
        db,
        {
          text: `
            INSERT INTO storage.buckets_analytics (name)
            VALUES ($1)
            ON CONFLICT (name) WHERE deleted_at IS NULL DO NOTHING
            RETURNING *
          `,
          values: [data.name],
        },
        signal
      )

      const bucket = result.rows[0]
      if (!bucket) {
        throw ERRORS.ResourceAlreadyExists()
      }

      return bucket
    })
  }

  async createBucket(
    data: Pick<
      Bucket,
      'id' | 'name' | 'public' | 'owner' | 'file_size_limit' | 'allowed_mime_types' | 'type'
    >
  ) {
    const bucketData: Bucket = {
      id: data.id,
      name: data.name,
      owner: isUuid(data.owner || '') ? data.owner : undefined,
      owner_id: data.owner,
      public: data.public,
      allowed_mime_types: data.allowed_mime_types,
      file_size_limit: data.file_size_limit,
    }

    if (await tenantHasMigrations(this.tenantId, 'iceberg-catalog-flag-on-buckets')) {
      bucketData.type = 'STANDARD'
    }

    try {
      const result = await this.runQuery('CreateBucket', async (db, signal) => {
        const insert = buildInsert(bucketData as Record<string, unknown>)

        return this.query(
          db,
          {
            text: `
              INSERT INTO storage.buckets (${insert.columns})
              VALUES (${insert.placeholders})
            `,
            values: insert.values,
          },
          signal
        )
      })

      if (!result.rowCount) {
        throw ERRORS.NoSuchBucket(data.id)
      }

      return bucketData
    } catch (e) {
      if (isStorageError(ErrorCode.ResourceAlreadyExists, e)) {
        throw ERRORS.BucketAlreadyExists(data.id, e)
      }
      throw e
    }
  }

  async findBucketById(bucketId: string, columns = 'id', filters?: FindBucketFilters) {
    const result = await this.runQuery('FindBucketById', async (db, signal) => {
      let columnNames = columns.split(',').map((column) => column.trim())

      if (!(await tenantHasMigrations(this.tenantId, 'iceberg-catalog-flag-on-buckets'))) {
        columnNames = columnNames.filter((name) => name !== 'type')
      }

      const conditions = ['id = $1']
      const values: unknown[] = [bucketId]

      if (typeof filters?.isPublic !== 'undefined') {
        values.push(filters.isPublic)
        conditions.push(`public = $${values.length}`)
      }

      const result = await this.query<Bucket>(
        db,
        {
          text: `
            SELECT ${selectColumns(columnNames)}
            FROM storage.buckets
            WHERE ${conditions.join(' AND ')}
            LIMIT 1
            ${lockClause(filters)}
          `,
          values,
        },
        signal
      )

      return result.rows[0]
    })

    if (!result && !filters?.dontErrorOnEmpty) {
      throw ERRORS.NoSuchBucket(bucketId)
    }

    return result
  }

  async countObjectsInBucket(bucketId: string, limit?: number): Promise<number> {
    if (limit !== undefined) {
      const result = await this.runQuery('CountObjectsInBucketWithLimit', async (db, signal) => {
        return this.query(
          db,
          {
            text: `
              SELECT 1
              FROM storage.objects
              WHERE bucket_id = $1
              LIMIT $2
            `,
            values: [bucketId, limit],
          },
          signal
        )
      })

      return result.rows.length
    }

    const result = await this.runQuery('CountObjectsInBucket', async (db, signal) => {
      return this.query<{ count: number }>(
        db,
        {
          text: `
            SELECT COUNT(*)::int AS count
            FROM storage.objects
            WHERE bucket_id = $1
          `,
          values: [bucketId],
        },
        signal
      )
    })

    return result.rows[0]?.count || 0
  }

  async listObjects(
    bucketId: string,
    columns = 'id',
    limit = 10,
    before?: Date,
    nextToken?: string
  ) {
    const result = await this.runQuery('ListObjects', async (db, signal) => {
      const conditions = ['bucket_id = $1']
      const values: unknown[] = [bucketId]

      if (before) {
        values.push(before.toISOString())
        conditions.push(`created_at < $${values.length}`)
      }

      if (nextToken) {
        values.push(nextToken)
        conditions.push(`name COLLATE "C" > $${values.length}`)
      }

      values.push(limit)

      return this.query<Obj>(
        db,
        {
          text: `
            SELECT ${selectColumns(columns)}
            FROM storage.objects
            WHERE ${conditions.join(' AND ')}
            ORDER BY name COLLATE "C"
            LIMIT $${values.length}
          `,
          values,
        },
        signal
      )
    })

    return result.rows
  }

  async listObjectsV2(
    bucketId: string,
    options?: {
      prefix?: string
      delimiter?: string
      nextToken?: string
      maxKeys?: number
      startAfter?: string
      sortBy?: {
        order?: string
        column?: string
        after?: string
      }
    }
  ) {
    return this.runQuery('ListObjectsV2', async (db, signal) => {
      if (!options?.delimiter) {
        const values: unknown[] = [bucketId, options?.maxKeys || 100]
        const conditions = ['bucket_id = $1']

        const allowedSortColumns = new Set(['updated_at', 'created_at'])
        const allowedSortOrders = new Set(['asc', 'desc'])
        const sortColumn =
          options?.sortBy?.column && allowedSortColumns.has(options.sortBy.column)
            ? options.sortBy.column
            : undefined
        const sortOrder =
          options?.sortBy?.order && allowedSortOrders.has(options.sortBy.order)
            ? options.sortBy.order
            : 'asc'
        const pageOperator = sortOrder === 'asc' ? '>' : '<'

        if (options?.prefix) {
          values.push(`${escapeLike(options.prefix)}%`)
          conditions.push(`name LIKE $${values.length}`)
        }

        if (options?.startAfter && !options?.nextToken) {
          values.push(options.startAfter)
          conditions.push(`name COLLATE "C" > $${values.length}`)
        }

        if (options?.nextToken) {
          if (sortColumn && options.sortBy?.after) {
            values.push(options.sortBy.after, options.nextToken)
            conditions.push(
              `ROW(date_trunc('milliseconds', ${quoteIdentifier(
                sortColumn
              )}), name COLLATE "C") ${pageOperator} ROW(COALESCE(NULLIF($${
                values.length - 1
              }, '')::timestamptz, 'epoch'::timestamptz), $${values.length})`
            )
          } else {
            values.push(options.nextToken)
            conditions.push(`name COLLATE "C" ${pageOperator} $${values.length}`)
          }
        }

        const result = await this.query<Obj>(
          db,
          {
            text: `
              SELECT id, name, metadata, updated_at, created_at, last_accessed_at
              FROM storage.objects
              WHERE ${conditions.join(' AND ')}
              ORDER BY ${sortColumn ? `${quoteIdentifier(sortColumn)} ${sortOrder}, ` : ''}
                name COLLATE "C" ${sortOrder}
              LIMIT $2
            `,
            values,
          },
          signal
        )

        return result.rows
      }

      let useNewSearchVersion2 = true

      if (isMultitenant) {
        useNewSearchVersion2 = await tenantHasMigrations(this.tenantId, 'search-v2')
      }

      if (useNewSearchVersion2 && options?.delimiter === '/') {
        let paramPlaceholders = '$1,$2,$3,$4,$5'
        const sortParams: (string | null)[] = []
        const hasSortSupport =
          (await tenantHasMigrations(this.tenantId, 'add-search-v2-sort-support')) ||
          (await tenantHasMigrations(this.tenantId, 'search-v2-optimised'))

        if (hasSortSupport) {
          paramPlaceholders += ',$6,$7,$8'
          sortParams.push(
            options?.sortBy?.order || 'asc',
            options?.sortBy?.column || 'name',
            options?.sortBy?.after || null
          )
        }

        const levels = !options?.prefix ? 1 : options.prefix.split('/').length
        const searchParams = [
          options?.prefix || '',
          bucketId,
          options?.maxKeys || 1000,
          levels,
          options?.startAfter || '',
          ...sortParams,
        ]

        const result = await this.query<Obj>(
          db,
          {
            text: `select * from storage.search_v2(${paramPlaceholders})`,
            values: searchParams,
          },
          signal
        )

        return result.rows
      }

      const result = await this.query<Obj>(
        db,
        {
          text: 'select * from storage.list_objects_with_delimiter($1,$2,$3,$4,$5,$6)',
          values: [
            bucketId,
            options?.prefix,
            options?.delimiter,
            options?.maxKeys,
            options?.startAfter || '',
            options?.nextToken || '',
          ],
        },
        signal
      )

      return result.rows
    })
  }

  async deleteBucket(bucketId: string | string[]) {
    const result = await this.runQuery('DeleteBucket', async (db, signal) => {
      return this.query(
        db,
        {
          text: `
            DELETE FROM storage.buckets
            WHERE id = ANY($1::text[])
          `,
          values: [Array.isArray(bucketId) ? bucketId : [bucketId]],
        },
        signal
      )
    })

    return result.rowCount || 0
  }

  async listBuckets(columns = 'id', options?: ListBucketOptions) {
    return this.runQuery('ListBuckets', async (db, signal) => {
      const columnNames = columns.split(',').map((column) => column.trim())
      const selectColumnNames = columnNames.filter((name) => name !== 'type')
      const selectedColumns = selectColumnNames.length ? selectColumns(selectColumnNames) : ''
      const selectClause = columnNames.includes('type')
        ? [selectedColumns, `'STANDARD' AS "type"`].filter(Boolean).join(', ')
        : selectedColumns || quoteIdentifier('id')

      const conditions: string[] = []
      const values: unknown[] = []

      if (options?.search !== undefined && options.search.length > 0) {
        values.push(`%${escapeLike(options.search)}%`)
        conditions.push(`name ILIKE $${values.length}`)
      }

      const orderBy = options?.sortColumn
        ? ` ORDER BY ${quoteIdentifier(options.sortColumn)} ${normalizeSortOrder(
            options.sortOrder
          )}`
        : ''

      let pagination = ''
      if (options?.limit !== undefined) {
        values.push(options.limit)
        pagination += ` LIMIT $${values.length}`
      }
      if (options?.offset !== undefined) {
        values.push(options.offset)
        pagination += ` OFFSET $${values.length}`
      }

      const result = await this.query<Bucket>(
        db,
        {
          text: `
            SELECT ${selectClause}
            FROM storage.buckets
            ${conditions.length ? `WHERE ${conditions.join(' AND ')}` : ''}
            ${orderBy}
            ${pagination}
          `,
          values,
        },
        signal
      )

      return result.rows
    })
  }

  listMultipartUploads(
    bucketId: string,
    options?: {
      prefix?: string
      deltimeter?: string
      nextUploadToken?: string
      nextUploadKeyToken?: string
      maxKeys?: number
    }
  ) {
    return this.runQuery('ListMultipartsUploads', async (db, signal) => {
      if (!options?.deltimeter) {
        const conditions = ['bucket_id = $1']
        const values: unknown[] = [bucketId]

        if (options?.prefix) {
          values.push(`${escapeLike(options.prefix)}%`)
          conditions.push(`key ILIKE $${values.length}`)
        }

        if (options?.nextUploadKeyToken && !options.nextUploadToken) {
          values.push(options.nextUploadKeyToken)
          conditions.push(`key COLLATE "C" > $${values.length}`)
        }

        if (options?.nextUploadToken) {
          values.push(options.nextUploadToken)
          conditions.push(`id COLLATE "C" > $${values.length}`)
        }

        values.push(options?.maxKeys || 100)

        const result = await this.query<S3MultipartUpload>(
          db,
          {
            text: `
              SELECT id, key, created_at
              FROM storage.s3_multipart_uploads
              WHERE ${conditions.join(' AND ')}
              ORDER BY key COLLATE "C", created_at
              LIMIT $${values.length}
            `,
            values,
          },
          signal
        )

        return result.rows
      }

      const result = await this.query<S3MultipartUpload>(
        db,
        {
          text: 'select * from storage.list_multipart_uploads_with_delimiter($1,$2,$3,$4,$5,$6)',
          values: [
            bucketId,
            options?.prefix ? escapeLike(options.prefix) : options?.prefix,
            options?.deltimeter,
            options?.maxKeys,
            options?.nextUploadKeyToken || '',
            options.nextUploadToken || '',
          ],
        },
        signal
      )

      return result.rows
    })
  }

  async updateBucket(
    bucketId: string,
    fields: Pick<Bucket, 'public' | 'file_size_limit' | 'allowed_mime_types'>
  ) {
    const entries = Object.entries(fields).filter(([, value]) => value !== undefined)

    if (entries.length === 0) {
      return
    }

    const result = await this.runQuery('UpdateBucket', async (db, signal) => {
      const values = entries.map(([, value]) => value)
      const setClause = entries
        .map(([column], index) => `${quoteIdentifier(column)} = $${index + 1}`)
        .join(', ')

      return this.query(
        db,
        {
          text: `
            UPDATE storage.buckets
            SET ${setClause}
            WHERE id = $${entries.length + 1}
          `,
          values: [...values, bucketId],
        },
        signal
      )
    })

    if (result.rowCount === 0) {
      throw ERRORS.NoSuchBucket(bucketId)
    }
  }

  async upsertObject(
    data: Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata' | 'user_metadata' | 'version'>
  ) {
    const objectData = this.normalizeColumns({
      name: data.name,
      owner: isUuid(data.owner || '') ? data.owner : undefined,
      owner_id: data.owner,
      bucket_id: data.bucket_id,
      metadata: data.metadata,
      user_metadata: data.user_metadata,
      version: data.version,
    })
    const updateData = this.normalizeColumns({
      metadata: data.metadata,
      user_metadata: data.user_metadata,
      version: data.version,
      owner: isUuid(data.owner || '') ? data.owner : undefined,
      owner_id: data.owner,
    })

    const result = await this.runQuery('UpsertObject', async (db, signal) => {
      const insert = buildInsert(objectData)
      const updateRecord = updateData as Record<string, unknown>
      const updateClauses: string[] = []

      for (const column in updateRecord) {
        if (!Object.prototype.hasOwnProperty.call(updateRecord, column)) {
          continue
        }

        if (updateRecord[column] === undefined) {
          continue
        }

        updateClauses.push(`${quoteIdentifier(column)} = EXCLUDED.${quoteIdentifier(column)}`)
      }

      const updateClause = updateClauses.join(', ')

      return this.query<Obj>(
        db,
        {
          text: `
            INSERT INTO storage.objects (${insert.columns})
            VALUES (${insert.placeholders})
            ON CONFLICT (name, bucket_id)
            ${updateClause ? `DO UPDATE SET ${updateClause}` : 'DO NOTHING'}
            RETURNING *
          `,
          values: insert.values,
        },
        signal
      )
    })

    return result.rows[0]
  }

  async updateObject(
    bucketId: string,
    name: string,
    data: Pick<Obj, 'owner' | 'metadata' | 'version' | 'name' | 'bucket_id' | 'user_metadata'>
  ) {
    const objectData = this.normalizeColumns({
      name: data.name,
      bucket_id: data.bucket_id,
      owner: isUuid(data.owner || '') ? data.owner : undefined,
      owner_id: data.owner,
      metadata: data.metadata,
      user_metadata: data.user_metadata,
      version: data.version,
    })

    const result = await this.runQuery('UpdateObject', async (db, signal) => {
      const update = buildUpdate(objectData)
      return this.query<Obj>(
        db,
        {
          text: `
            UPDATE storage.objects
            SET ${update.setClause}
            WHERE bucket_id = $${update.values.length + 1}
              AND name = $${update.values.length + 2}
            RETURNING *
          `,
          values: [...update.values, bucketId, name],
        },
        signal
      )
    })

    const object = result.rows[0]
    if (!object) {
      throw ERRORS.NoSuchKey(name)
    }

    return object
  }

  async createObject(
    data: Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata' | 'version' | 'user_metadata'>
  ) {
    try {
      const object = this.normalizeColumns({
        name: data.name,
        owner: isUuid(data.owner || '') ? data.owner : undefined,
        owner_id: data.owner,
        bucket_id: data.bucket_id,
        metadata: data.metadata,
        version: data.version,
        user_metadata: data.user_metadata,
      })

      await this.runQuery('CreateObject', async (db, signal) => {
        const insert = buildInsert(object)
        return this.query(
          db,
          {
            text: `
              INSERT INTO storage.objects (${insert.columns})
              VALUES (${insert.placeholders})
            `,
            values: insert.values,
          },
          signal
        )
      })

      return object as Obj
    } catch (e) {
      if (isStorageError(ErrorCode.ResourceAlreadyExists, e)) {
        throw ERRORS.KeyAlreadyExists(data.name, e)
      }
      throw e
    }
  }

  async deleteObject(bucketId: string, objectName: string, version?: string) {
    const result = await this.runQuery('Delete Object', async (db, signal) => {
      const conditions = ['name = $1', 'bucket_id = $2']
      const values: unknown[] = [objectName, bucketId]

      if (version) {
        values.push(version)
        conditions.push(`version = $${values.length}`)
      }

      return this.query<Obj>(
        db,
        {
          text: `
            DELETE FROM storage.objects
            WHERE ${conditions.join(' AND ')}
            RETURNING *
          `,
          values,
        },
        signal
      )
    })

    return result.rows[0]
  }

  async deleteObjects(bucketId: string, objectNames: string[], by: keyof Obj = 'name') {
    if (objectNames.length === 0) {
      return []
    }

    const result = await this.runQuery('DeleteObjects', async (db, signal) => {
      return this.query<Obj>(
        db,
        {
          text: `
            DELETE FROM storage.objects
            WHERE bucket_id = $1
              AND ${quoteIdentifier(String(by))} = ANY($2)
            RETURNING *
          `,
          values: [bucketId, objectNames],
        },
        signal
      )
    })

    return result.rows
  }

  async deleteObjectVersions(bucketId: string, objectNames: { name: string; version: string }[]) {
    if (objectNames.length === 0) {
      return []
    }

    const result = await this.runQuery('DeleteObjects', async (db, signal) => {
      const { placeholders, values } = buildTupleValues(objectNames)

      return this.query<Obj>(
        db,
        {
          text: `
            DELETE FROM storage.objects
            WHERE bucket_id = $1
              AND (name, version) IN (${placeholders})
            RETURNING *
          `,
          values: [bucketId, ...values],
        },
        signal
      )
    })

    return result.rows
  }

  async updateObjectMetadata(bucketId: string, objectName: string, metadata: ObjectMetadata) {
    const result = await this.runQuery('UpdateObjectMetadata', async (db, signal) => {
      return this.query<Obj>(
        db,
        {
          text: `
            UPDATE storage.objects
            SET metadata = $1
            WHERE bucket_id = $2
              AND name = $3
            RETURNING *
          `,
          values: [metadata, bucketId, objectName],
        },
        signal
      )
    })

    return result.rows[0]
  }

  async updateObjectOwner(bucketId: string, objectName: string, owner?: string) {
    const result = await this.runQuery('UpdateObjectOwner', async (db, signal) => {
      return this.query<Obj>(
        db,
        {
          text: `
            UPDATE storage.objects
            SET
              last_accessed_at = now(),
              owner = $1,
              owner_id = $2
            WHERE bucket_id = $3
              AND name = $4
            RETURNING *
          `,
          values: [isUuid(owner || '') ? owner : null, owner, bucketId, objectName],
        },
        signal
      )
    })

    const object = result.rows[0]
    if (!object) {
      throw ERRORS.NoSuchKey(objectName)
    }

    return object
  }

  async findObject(
    bucketId: string,
    objectName: string,
    columns = 'id',
    filters?: FindObjectFilters
  ) {
    const result = await this.runQuery('FindObject', async (db, signal) => {
      return this.query<Obj>(
        db,
        {
          text: `
            SELECT ${selectColumns(this.normalizeColumns(columns))}
            FROM storage.objects
            WHERE name = $1
              AND bucket_id = $2
            LIMIT 1
            ${objectLockClause(filters)}
          `,
          values: [objectName, bucketId],
        },
        signal
      )
    })

    const object = result.rows[0]
    if (!object && !filters?.dontErrorOnEmpty) {
      throw ERRORS.NoSuchKey(objectName)
    }

    return object
  }

  async findObjects(bucketId: string, objectNames: string[], columns = 'id') {
    if (objectNames.length === 0) {
      return []
    }

    const result = await this.runQuery('FindObjects', async (db, signal) => {
      return this.query<Obj>(
        db,
        {
          text: `
            SELECT ${selectColumns(this.normalizeColumns(columns))}
            FROM storage.objects
            WHERE bucket_id = $1
              AND name = ANY($2::text[])
          `,
          values: [bucketId, objectNames],
        },
        signal
      )
    })

    return result.rows
  }

  async findObjectVersions(bucketId: string, obj: { name: string; version: string }[]) {
    if (obj.length === 0) {
      return []
    }

    const result = await this.runQuery('FindObjectVersions', async (db, signal) => {
      const { placeholders, values } = buildTupleValues(obj)

      return this.query<Pick<Obj, 'name' | 'version'>>(
        db,
        {
          text: `
            SELECT name, version
            FROM storage.objects
            WHERE bucket_id = $1
              AND (name, version) IN (${placeholders})
          `,
          values: [bucketId, ...values],
        },
        signal
      )
    })

    return result.rows
  }

  async mustLockObject(bucketId: string, objectName: string, version?: string) {
    return this.runQuery('MustLockObject', async (db, signal) => {
      const hash = hashStringToInt(`${bucketId}/${objectName}${version ? `/${version}` : ''}`)
      const result = await this.query<{ pg_try_advisory_xact_lock: boolean }>(
        db,
        {
          text: 'SELECT pg_try_advisory_xact_lock($1)',
          values: [hash],
        },
        signal
      )
      const lockAcquired = result.rows[0]?.pg_try_advisory_xact_lock || false

      if (!lockAcquired) {
        throw ERRORS.ResourceLocked()
      }

      return true
    })
  }

  async waitObjectLock(
    bucketId: string,
    objectName: string,
    version?: string,
    opts?: { timeout: number }
  ) {
    return this.runQuery('WaitObjectLock', async (db, signal) => {
      const hash = hashStringToInt(`${bucketId}/${objectName}${version ? `/${version}` : ''}`)
      const lockTimeout = opts?.timeout

      if (lockTimeout && lockTimeout > 0) {
        if (this.isMultigresDatabase()) {
          await this.waitObjectLockWithTopLevelLockTimeout(db, hash, lockTimeout, signal)
          return true
        }

        // Single round-trip: read current -> set new -> acquire -> restore.
        // MATERIALIZED forces each CTE to materialize before the next scans,
        // which sequences the side-effecting set_config and pg_advisory_xact_lock
        // calls in dependency order. Without MATERIALIZED the optimizer could
        // inline and reorder volatile-function evaluation.
        const query = `
          WITH previous_lock_timeout AS MATERIALIZED (
            SELECT current_setting('lock_timeout') AS value
          ),
          set_lock_timeout AS MATERIALIZED (
            SELECT
              set_config('lock_timeout', $2, true) AS applied_timeout,
              value
            FROM previous_lock_timeout
          ),
          acquire_lock AS MATERIALIZED (
            SELECT
              pg_advisory_xact_lock($1),
              applied_timeout,
              value
            FROM set_lock_timeout
          ),
          restore_lock_timeout AS MATERIALIZED (
            SELECT set_config('lock_timeout', value, true) AS restored_timeout
            FROM acquire_lock
          )
          SELECT true AS locked
          FROM restore_lock_timeout
        `

        try {
          await db.query(
            {
              text: query,
              values: [hash, `${lockTimeout}ms`],
            },
            { signal }
          )
        } catch (e) {
          if (isPgLockTimeoutError(e)) {
            throw ERRORS.LockTimeout(e)
          }

          throw mapPgError(e, 'WaitObjectLock CTE')
        }

        return true
      }

      try {
        await db.query(
          {
            text: 'SELECT pg_advisory_xact_lock($1)',
            values: [hash],
          },
          { signal }
        )
      } catch (e) {
        if (isPgLockTimeoutError(e)) {
          throw ERRORS.LockTimeout(e)
        }

        throw mapPgError(e, 'SELECT pg_advisory_xact_lock($1)')
      }

      return true
    })
  }

  private isMultigresDatabase(): boolean {
    return (this.options.databaseEngine ?? databaseEngine) === 'multigres'
  }

  private async waitObjectLockWithTopLevelLockTimeout(
    db: DatabaseExecutor,
    hash: number,
    lockTimeout: number,
    signal?: AbortSignal
  ): Promise<void> {
    let previousLockTimeout: string

    try {
      const currentLockTimeout = await db.query<{ value: string }>(
        {
          text: `SELECT current_setting('lock_timeout') AS value`,
          values: [],
        },
        { signal }
      )

      previousLockTimeout = currentLockTimeout.rows[0]?.value ?? '0'

      await db.query(
        {
          text: `SELECT set_config('lock_timeout', $1, true)`,
          values: [`${lockTimeout}ms`],
        },
        { signal }
      )
    } catch (e) {
      throw mapPgError(e, 'WaitObjectLock Multigres setup')
    }

    try {
      await db.query(
        {
          text: 'SELECT pg_advisory_xact_lock($1)',
          values: [hash],
        },
        { signal }
      )
    } catch (e) {
      if (isPgLockTimeoutError(e)) {
        throw ERRORS.LockTimeout(e)
      }

      throw mapPgError(e, 'WaitObjectLock Multigres lock')
    }

    try {
      await db.query(
        {
          text: `SELECT set_config('lock_timeout', $1, true)`,
          values: [previousLockTimeout],
        },
        { signal }
      )
    } catch (e) {
      throw mapPgError(e, 'WaitObjectLock Multigres restore')
    }
  }

  async searchObjects(bucketId: string, prefix: string, options: SearchObjectOption) {
    return this.runQuery('SearchObjects', async (db, signal) => {
      const sortColumn = options.sortBy?.column ?? 'name'
      const shouldEscapePattern = sortColumn !== 'name'
      const safePrefix = shouldEscapePattern ? escapeLike(prefix) : prefix
      const safeSearch = shouldEscapePattern
        ? escapeLike(options.search || '')
        : options.search || ''

      const result = await this.query<Obj>(
        db,
        {
          text: 'select * from storage.search($1,$2,$3,$4,$5,$6,$7,$8)',
          values: [
            safePrefix,
            bucketId,
            options.limit || 100,
            safePrefix.split('/').length,
            options.offset || 0,
            safeSearch,
            sortColumn,
            options.sortBy?.order ?? 'asc',
          ],
        },
        signal
      )

      return result.rows
    })
  }

  async createMultipartUpload(
    uploadId: string,
    bucketId: string,
    objectName: string,
    version: string,
    signature: string,
    owner?: string,
    userMetadata?: Record<string, string | null>,
    metadata?: Partial<ObjectMetadata>
  ) {
    return this.runQuery('CreateMultipartUpload', async (db, signal) => {
      const data: Record<string, unknown> = {
        id: uploadId,
        bucket_id: bucketId,
        key: objectName,
        version,
        upload_signature: signature,
        owner_id: owner,
        user_metadata: userMetadata,
      }

      if (this.hasMultipartMetadataColumn()) {
        data.metadata = metadata
      }

      const insert = buildInsert(this.normalizeColumns(data))
      const result = await this.query<S3MultipartUpload>(
        db,
        {
          text: `
            INSERT INTO storage.s3_multipart_uploads (${insert.columns})
            VALUES (${insert.placeholders})
            RETURNING *
          `,
          values: insert.values,
        },
        signal
      )

      return result.rows[0]
    })
  }

  async findMultipartUpload(uploadId: string, columns = 'id', options?: { forUpdate?: boolean }) {
    const result = await this.runQuery('FindMultipartUpload', async (db, signal) => {
      const normalizedColumns = this.normalizeMultipartUploadColumns(columns)

      return this.query<S3MultipartUpload>(
        db,
        {
          text: `
            SELECT ${selectColumns(normalizedColumns)}
            FROM storage.s3_multipart_uploads
            WHERE id = $1
            LIMIT 1
            ${options?.forUpdate ? 'FOR UPDATE' : ''}
          `,
          values: [uploadId],
        },
        signal
      )
    })

    const multipart = result.rows[0]
    if (!multipart) {
      throw ERRORS.NoSuchUpload(uploadId)
    }

    return multipart
  }

  async updateMultipartUploadProgress(uploadId: string, progress: number, signature: string) {
    return this.runQuery('UpdateMultipartUploadProgress', async (db, signal) => {
      await this.query(
        db,
        {
          text: `
            UPDATE storage.s3_multipart_uploads
            SET
              in_progress_size = $1,
              upload_signature = $2
            WHERE id = $3
          `,
          values: [progress, signature, uploadId],
        },
        signal
      )
    })
  }

  async deleteMultipartUpload(uploadId: string) {
    return this.runQuery('DeleteMultipartUpload', async (db, signal) => {
      await this.query(
        db,
        {
          text: `
            DELETE FROM storage.s3_multipart_uploads
            WHERE id = $1
          `,
          values: [uploadId],
        },
        signal
      )
    })
  }

  async insertUploadPart(part: S3PartUpload) {
    return this.runQuery('InsertUploadPart', async (db, signal) => {
      const insert = buildInsert(part as Record<string, unknown>)
      const result = await this.query<S3PartUpload>(
        db,
        {
          text: `
            INSERT INTO storage.s3_multipart_uploads_parts (${insert.columns})
            VALUES (${insert.placeholders})
            RETURNING *
          `,
          values: insert.values,
        },
        signal
      )

      return result.rows[0]
    })
  }

  async listParts(
    uploadId: string,
    options: { afterPart?: string; maxParts: number }
  ): Promise<S3PartUpload[]> {
    const result = await this.runQuery('ListParts', async (db, signal) => {
      const conditions = ['upload_id = $1']
      const values: unknown[] = [uploadId]

      if (options.afterPart) {
        values.push(options.afterPart)
        conditions.push(`part_number > $${values.length}`)
      }

      values.push(options.maxParts)

      return this.query<S3PartUpload>(
        db,
        {
          text: `
            SELECT etag, part_number, size, upload_id, created_at
            FROM storage.s3_multipart_uploads_parts
            WHERE ${conditions.join(' AND ')}
            ORDER BY part_number
            LIMIT $${values.length}
          `,
          values,
        },
        signal
      )
    })

    return result.rows
  }

  async createS3KeysTempTable(tableName: string): Promise<void> {
    await this.runUnscopedQuery('CreateS3KeysTempTable', async (db, signal) => {
      await this.dropStaleS3KeysScratchTables(db, signal)
      await this.query(
        db,
        `
          CREATE UNLOGGED TABLE IF NOT EXISTS ${quoteQualifiedIdentifier(tableName)} (
            key TEXT COLLATE "C" PRIMARY KEY,
            size BIGINT NOT NULL
          )
        `,
        signal
      )
    })
  }

  private async dropStaleS3KeysScratchTables(
    db: DatabaseExecutor,
    signal?: AbortSignal
  ): Promise<void> {
    const staleBefore = Date.now() - S3_KEYS_SCRATCH_TABLE_MAX_AGE_MS
    const result = await this.query<{ table_name: string }>(
      db,
      {
        text: `
          SELECT c.relname AS table_name
          FROM pg_class c
          INNER JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = $1
            AND c.relkind = 'r'
            AND c.relpersistence = 'u'
            AND c.relname ~ $2
            AND (regexp_match(c.relname, $2))[1]::bigint < $3
        `,
        values: [S3_KEYS_SCRATCH_TABLE_SCHEMA, S3_KEYS_SCRATCH_TABLE_PATTERN, staleBefore],
      },
      signal
    )

    for (const { table_name } of result.rows) {
      await this.query(
        db,
        `DROP TABLE IF EXISTS ${quoteQualifiedIdentifier(
          `${S3_KEYS_SCRATCH_TABLE_SCHEMA}.${table_name}`
        )}`,
        signal
      )
    }
  }

  async dropS3KeysTempTable(tableName: string): Promise<void> {
    await this.runUnscopedQuery('DropS3KeysTempTable', async (db, signal) => {
      await this.query(db, `DROP TABLE IF EXISTS ${quoteQualifiedIdentifier(tableName)}`, signal)
    })
  }

  async listS3KeysFromTempTable(
    tableName: string,
    nextItem: string,
    limit: number
  ): Promise<ScannerS3Key[]> {
    const result = await this.runUnscopedQuery('ListS3KeysFromTempTable', async (db, signal) => {
      const conditions: string[] = []
      const values: unknown[] = []

      if (nextItem) {
        values.push(nextItem)
        conditions.push(`key > $${values.length}`)
      }

      values.push(limit)

      return this.query<ScannerS3Key>(
        db,
        {
          text: `
            SELECT key, size
            FROM ${quoteQualifiedIdentifier(tableName)}
            ${conditions.length ? `WHERE ${conditions.join(' AND ')}` : ''}
            ORDER BY key ASC
            LIMIT $${values.length}
          `,
          values,
        },
        signal
      )
    })

    return result.rows
  }

  async findS3KeysInTempTable(
    tableName: string,
    keys: string[]
  ): Promise<Pick<ScannerS3Key, 'key'>[]> {
    if (keys.length === 0) {
      return []
    }

    const result = await this.runUnscopedQuery('FindS3KeysInTempTable', async (db, signal) => {
      return this.query<Pick<ScannerS3Key, 'key'>>(
        db,
        {
          text: `
            SELECT key
            FROM ${quoteQualifiedIdentifier(tableName)}
            WHERE key = ANY($1::text[])
          `,
          values: [keys],
        },
        signal
      )
    })

    return result.rows
  }

  async insertS3KeysIntoTempTable(tableName: string, keys: ScannerS3Key[]): Promise<void> {
    if (keys.length === 0) {
      return
    }

    await this.runUnscopedQuery('InsertS3KeysIntoTempTable', async (db, signal) => {
      const values: (string | number)[] = []
      const placeholders: string[] = []

      for (let index = 0; index < keys.length; index++) {
        const key = keys[index]
        placeholders.push(`($${index * 2 + 1}, $${index * 2 + 2})`)
        values.push(key.key, key.size)
      }

      await this.query(
        db,
        {
          text: `
            INSERT INTO ${quoteQualifiedIdentifier(tableName)} (key, size)
            VALUES ${placeholders.join(', ')}
            ON CONFLICT DO NOTHING
          `,
          values,
        },
        signal
      )
    })
  }

  async healthcheck() {
    if (databaseHealthcheckUnscoped) {
      await this.runUnscopedQuery('Healthcheck', healthcheckProbe, HEALTHCHECK_QUERY_OPTIONS)
    } else {
      await this.runQuery('Healthcheck', healthcheckProbe)
    }
  }

  destroyConnection(): void {
    this.connection.dispose()
  }

  /**
   * Excludes columns selection if a specific migration wasn't run.
   */
  protected normalizeColumns<T extends string | Record<string, unknown>>(columns: T): T {
    const latestMigration = this.latestMigration

    if (!latestMigration) {
      return columns
    }

    const rules = [{ migration: 'custom-metadata', newColumns: ['user_metadata'] }]

    for (const rule of rules) {
      if (DBMigration[latestMigration] >= DBMigration[rule.migration as keyof typeof DBMigration]) {
        continue
      }

      if (typeof columns === 'string') {
        const normalizedColumns: string[] = []
        for (const column of columns.split(',')) {
          const trimmed = column.trim()
          if (rule.newColumns.includes(trimmed)) {
            continue
          }

          normalizedColumns.push(trimmed)
        }

        return normalizedColumns.join(',') as T
      }

      const normalizedColumns: Record<string, unknown> = {}
      const sourceColumns = columns as Record<string, unknown>

      for (const column in sourceColumns) {
        if (!Object.prototype.hasOwnProperty.call(sourceColumns, column)) {
          continue
        }

        if (rule.newColumns.includes(column)) {
          continue
        }

        normalizedColumns[column] = sourceColumns[column]
      }

      return normalizedColumns as T
    }

    return columns
  }

  protected normalizeMultipartUploadColumns(columns: string): string[] {
    const normalizedColumns: string[] = []
    const hasMetadataColumn = this.hasMultipartMetadataColumn()

    for (const column of this.normalizeColumns(columns).split(',')) {
      const trimmed = column.trim()
      if (!trimmed || (!hasMetadataColumn && trimmed === 'metadata')) {
        continue
      }

      normalizedColumns.push(trimmed)
    }

    return normalizedColumns
  }

  protected hasMultipartMetadataColumn(): boolean {
    return (
      !this.latestMigration ||
      DBMigration[this.latestMigration] >= DBMigration['s3-multipart-uploads-metadata']
    )
  }

  protected async runQuery<T>(
    queryName: string,
    fn: (db: DatabaseExecutor, signal?: AbortSignal) => Promise<T>
  ): Promise<T> {
    const startTime = performance.now()
    const abortSignal = this.connection.getAbortSignal()
    const recordDuration = this.createDurationRecorder(queryName, startTime, abortSignal)

    let tnx = this.options.tnx
    let differentScopes = false
    let needsNewTransaction = !tnx
    let savepoint: string | undefined
    let savepointEstablished = false

    try {
      differentScopes = Boolean(
        this.options.parentConnection?.role &&
          this.connection.role !== this.options.parentConnection?.role
      )
      needsNewTransaction = !tnx
      const usingSavepoint = !needsNewTransaction && differentScopes

      if (needsNewTransaction) {
        tnx = await this.connection.transaction()
      }

      if (!tnx) {
        throw ERRORS.InternalError(undefined, 'Could not create transaction')
      }

      savepoint = usingSavepoint ? nextSavepointName() : undefined

      if (savepoint) {
        await createSavepoint(tnx, savepoint)
        savepointEstablished = true
      }

      if (needsNewTransaction || differentScopes) {
        await this.connection.setScope(tnx)
      }

      const result = await fn(tnx, abortSignal)

      if (needsNewTransaction) {
        await tnx.commit()
      } else if (savepoint) {
        // Keep scope restoration inside the savepoint. If it fails, rolling back
        // the nested unit is preferable to leaking elevated scope into the parent transaction.
        await this.options.parentConnection?.setScope(tnx)
        await tnx.query(`RELEASE SAVEPOINT ${savepoint}`)
      }

      return result
    } catch (e) {
      if (savepointEstablished && savepoint && tnx && !tnx.isCompleted()) {
        try {
          await rollbackSavepoint(tnx, savepoint)
        } catch (rollbackError) {
          logSchema.warning(logger, '[StoragePgDB] Failed to rollback savepoint', {
            type: 'db',
            tenantId: this.tenantId,
            project: this.tenantId,
            reqId: this.reqId,
            sbReqId: this.sbReqId,
            error: rollbackError,
            metadata: JSON.stringify({
              queryName,
              savepoint,
            }),
          })
        }
      } else if (needsNewTransaction && tnx && !tnx.isCompleted()) {
        try {
          await tnx.rollback()
        } catch (rollbackError) {
          logSchema.warning(logger, '[StoragePgDB] Failed to rollback transaction', {
            type: 'db',
            tenantId: this.tenantId,
            project: this.tenantId,
            reqId: this.reqId,
            sbReqId: this.sbReqId,
            error: rollbackError,
            metadata: JSON.stringify({
              queryName,
              originalError: String(e),
            }),
          })
        }
      }
      throw mapPgErrorWithQueryName(e, queryName)
    } finally {
      try {
        if (!savepoint && differentScopes) {
          await this.restoreParentScopeSafely(queryName)
        }
      } finally {
        recordDuration()
      }
    }
  }

  private async restoreParentScopeSafely(queryName: string): Promise<void> {
    const parentConnection = this.options.parentConnection
    const parentTnx = this.options.parentTnx

    if (!parentConnection || !parentTnx || parentTnx.isCompleted()) {
      return
    }

    try {
      await parentConnection.setScope(parentTnx)
    } catch (error) {
      logSchema.error(logger, '[StoragePgDB] Failed to restore parent transaction scope', {
        type: 'db',
        tenantId: this.tenantId,
        project: this.tenantId,
        reqId: this.reqId,
        sbReqId: this.sbReqId,
        error,
        metadata: JSON.stringify({
          queryName,
          role: this.connection.role,
          parentRole: parentConnection.role,
          errorName: error instanceof Error ? error.name : undefined,
          errorMessage: error instanceof Error ? error.message : String(error),
          errorCode: error instanceof Error ? (error as { code?: unknown }).code : undefined,
        }),
      })
    }
  }

  protected async runUnscopedQuery<T>(
    queryName: string,
    fn: (db: DatabaseExecutor, signal?: AbortSignal) => Promise<T>,
    options?: UnscopedQueryOptions
  ): Promise<T> {
    const startTime = performance.now()
    const requestAbortSignal = this.connection.getAbortSignal()
    const recordDuration = this.createDurationRecorder(queryName, startTime, requestAbortSignal)
    const timeoutMs = normalizeTimeoutMs(options?.timeoutMs)

    let controller: AbortController | undefined
    let timer: NodeJS.Timeout | undefined
    let onRequestAbort: (() => void) | undefined

    if (timeoutMs !== undefined) {
      controller = new AbortController()
      timer = setTimeout(abortFromTimer, timeoutMs, controller)
      timer.unref()

      if (requestAbortSignal?.aborted) {
        controller.abort()
      } else if (requestAbortSignal) {
        const timedController = controller
        onRequestAbort = () => timedController.abort()
        requestAbortSignal.addEventListener('abort', onRequestAbort, { once: true })
      }
    }

    try {
      return await fn(this.connection, controller?.signal ?? requestAbortSignal)
    } catch (e) {
      throw mapPgErrorWithQueryName(e, queryName)
    } finally {
      if (timer !== undefined) {
        clearTimeout(timer)
      }
      if (onRequestAbort) {
        requestAbortSignal?.removeEventListener('abort', onRequestAbort)
      }
      recordDuration()
    }
  }

  protected query<T extends QueryResultRow = QueryResultRow>(
    db: DatabaseExecutor,
    statement: string | DatabaseStatement,
    signal?: AbortSignal
  ) {
    return executeQuery<T>(db, statement, signal)
  }

  private createDurationRecorder(
    queryName: string,
    startTime: number,
    abortSignal?: AbortSignal
  ): () => void {
    const requestAbortedBeforeStart = Boolean(abortSignal?.aborted)

    return () => {
      const duration = (performance.now() - startTime) / 1000
      // This intentionally reads the signal after the query work settles. The
      // attributes describe request abort observation, not proof that PostgreSQL
      // cancelled this specific statement.
      const requestAbortedAtRecord = Boolean(abortSignal?.aborted)

      dbQueryPerformance.record(duration, {
        name: queryName,
        requestAborted: requestAbortedBeforeStart || requestAbortedAtRecord,
        requestAbortedBeforeStart,
        requestAbortedAfterStart: !requestAbortedBeforeStart && requestAbortedAtRecord,
      })
    }
  }
}

function normalizeTimeoutMs(timeoutMs: number | undefined): number | undefined {
  if (timeoutMs === undefined || !Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    return undefined
  }

  return timeoutMs
}

// Static setTimeout callback taking the controller as an argument
// so arming the deadline allocates no closure.
function abortFromTimer(controller: AbortController): void {
  controller.abort()
}

function selectColumns(columns: string | string[]): string {
  const selected: string[] = []

  if (Array.isArray(columns)) {
    for (const column of columns) {
      if (column.length === 0) {
        continue
      }

      if (column === '*') {
        selected.push('*')
      } else {
        selected.push(quoteIdentifier(column))
      }
    }
  } else {
    for (const column of columns.split(',')) {
      const trimmed = column.trim()
      if (trimmed.length === 0) {
        continue
      }

      if (trimmed === '*') {
        selected.push('*')
      } else {
        selected.push(quoteIdentifier(trimmed))
      }
    }
  }

  return selected.length ? selected.join(', ') : quoteIdentifier('id')
}

function normalizeSortOrder(sortOrder?: string): 'ASC' | 'DESC' {
  return sortOrder?.toLowerCase() === 'desc' ? 'DESC' : 'ASC'
}

function buildInsert(data: Record<string, unknown>): {
  columns: string
  placeholders: string
  values: unknown[]
} {
  const columns: string[] = []
  const placeholders: string[] = []
  const values: unknown[] = []

  for (const column in data) {
    if (!Object.prototype.hasOwnProperty.call(data, column)) {
      continue
    }

    const value = data[column]
    if (value === undefined) {
      continue
    }

    values.push(value)
    columns.push(quoteIdentifier(column))
    placeholders.push(`$${values.length}`)
  }

  if (values.length === 0) {
    throw ERRORS.NoContentProvided()
  }

  return {
    columns: columns.join(', '),
    placeholders: placeholders.join(', '),
    values,
  }
}

function buildUpdate(data: Record<string, unknown>): {
  setClause: string
  values: unknown[]
} {
  const setClauses: string[] = []
  const values: unknown[] = []

  for (const column in data) {
    if (!Object.prototype.hasOwnProperty.call(data, column)) {
      continue
    }

    const value = data[column]
    if (value === undefined) {
      continue
    }

    values.push(value)
    setClauses.push(`${quoteIdentifier(column)} = $${values.length}`)
  }

  if (values.length === 0) {
    throw ERRORS.NoContentProvided()
  }

  return {
    setClause: setClauses.join(', '),
    values,
  }
}

function buildTupleValues(values: { name: string; version: string }[]): {
  placeholders: string
  values: string[]
} {
  const placeholders: string[] = []
  const queryValues: string[] = []

  for (let index = 0; index < values.length; index++) {
    const { name, version } = values[index]
    placeholders.push(`($${index * 2 + 2}, $${index * 2 + 3})`)
    queryValues.push(name, version)
  }

  return {
    placeholders: placeholders.join(', '),
    values: queryValues,
  }
}

function lockClause(filters?: FindBucketFilters): string {
  if (filters?.forUpdate) {
    return 'FOR UPDATE'
  }

  if (filters?.forShare) {
    return 'FOR SHARE'
  }

  return ''
}

function objectLockClause(filters?: FindObjectFilters): string {
  const lock = filters?.forUpdate
    ? 'FOR UPDATE'
    : filters?.forShare
      ? 'FOR SHARE'
      : filters?.forKeyShare
        ? 'FOR KEY SHARE'
        : ''

  if (!lock) {
    return ''
  }

  return filters?.noWait ? `${lock} NOWAIT` : lock
}

function mapPgError(error: unknown, context?: string | PgErrorContext): unknown {
  if (error instanceof DatabaseError) {
    return DBError.fromDBError(error, context)
  }

  return error
}

function mapPgErrorWithQueryName(error: unknown, queryName: string): unknown {
  return ensurePgErrorQueryName(mapPgError(error), queryName)
}

function ensurePgErrorQueryName(error: unknown, queryName: string): unknown {
  if (!(error instanceof StorageBackendError) || !(error.originalError instanceof DatabaseError)) {
    return error
  }

  const metadata = error.metadata
  if (!metadata) {
    error.metadata = { queryName }
  } else if (metadata.queryName === undefined) {
    metadata.queryName = queryName
  }

  return error
}

function isPgLockTimeoutError(error: unknown): error is DatabaseError {
  return error instanceof DatabaseError && error.code === '55P03'
}

function nextSavepointName(): string {
  return quoteIdentifier(`storage_pg_query_${randomUUID().replace(/-/g, '_')}`)
}

async function createSavepoint(tnx: DatabaseTransaction, savepoint: string): Promise<void> {
  const query = `SAVEPOINT ${savepoint}`

  try {
    await tnx.query(query)
  } catch (error) {
    throw mapPgTransactionAbortedError(error, query)
  }
}

async function rollbackSavepoint(tnx: DatabaseTransaction, savepoint: string): Promise<void> {
  await tnx.query(`ROLLBACK TO SAVEPOINT ${savepoint}`)
  await tnx.query(`RELEASE SAVEPOINT ${savepoint}`)
}
