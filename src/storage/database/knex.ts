import { Bucket, S3MultipartUpload, Obj, S3PartUpload, IcebergCatalog } from '../schemas'
import {
  ErrorCode,
  ERRORS,
  isStorageError,
  RenderableError,
  StorageBackendError,
  StorageErrorOptions,
} from '@internal/errors'
import { ObjectMetadata } from '../backend'
import { Knex } from 'knex'
import {
  Database,
  DatabaseOptions,
  FindBucketFilters,
  FindObjectFilters,
  SearchObjectOption,
  ListBucketOptions,
  TransactionOptions,
} from './adapter'
import { DatabaseError } from 'pg'
import { TenantConnection } from '@internal/database'
import { dbQueryPerformance } from '@internal/monitoring/metrics'
import { hashStringToInt } from '@internal/hashing'
import { DBMigration, tenantHasMigrations } from '@internal/database/migrations'
import { getConfig } from '../../config'
import { isUuid } from '../limits'

const { isMultitenant } = getConfig()

/**
 * Database
 * the only source of truth for interacting with the storage database
 */
export class StorageKnexDB implements Database {
  public readonly tenantHost: string
  public readonly tenantId: string
  public readonly reqId: string | undefined
  public readonly role?: string
  public readonly latestMigration?: keyof typeof DBMigration

  constructor(
    public readonly connection: TenantConnection,
    private readonly options: DatabaseOptions<Knex.Transaction>
  ) {
    this.tenantHost = options.host
    this.tenantId = options.tenantId
    this.reqId = options.reqId
    this.role = connection?.role
    this.latestMigration = options.latestMigration
  }

  //eslint-disable-next-line @typescript-eslint/no-explicit-any
  async withTransaction<T extends (db: Database) => Promise<any>>(
    fn: T,
    opts?: TransactionOptions
  ) {
    const tnx = await this.connection.transactionProvider(this.options.tnx, opts)()

    try {
      await this.connection.setScope(tnx)

      tnx.once('query-error', (error, q) => {
        throw DBError.fromDBError(error, q.sql)
      })

      const opts = { ...this.options, tnx }
      const storageWithTnx = new StorageKnexDB(this.connection, opts)

      const result: Awaited<ReturnType<T>> = await fn(storageWithTnx)
      await tnx.commit()
      return result
    } catch (e) {
      await tnx.rollback()
      throw e
    }
  }

  tenant() {
    return {
      ref: this.tenantId,
      host: this.tenantHost,
    }
  }

  asSuperUser() {
    return new StorageKnexDB(this.connection.asSuperUser(), {
      ...this.options,
      parentConnection: this.connection,
      parentTnx: this.options.tnx,
    })
  }

  async testPermission<T extends (db: Database) => any>(fn: T) {
    let result: any
    try {
      await this.withTransaction(async (db) => {
        result = await fn(db)
        throw true
      })
    } catch (e) {
      if (e === true) {
        return result
      }
      throw e
    }
  }

  deleteAnalyticsBucket(id: string, opts?: { soft: boolean }): Promise<IcebergCatalog> {
    return this.runQuery('DeleteAnalyticsBucket', async (knex, signal) => {
      if (opts?.soft) {
        const softDeleted = await knex
          .from<IcebergCatalog>('buckets_analytics')
          .where('id', id)
          .whereNull('deleted_at')
          .update({ deleted_at: new Date() })
          .returning('*')
          .abortOnSignal(signal)

        if (softDeleted.length === 0) {
          throw ERRORS.NoSuchBucket(id)
        }

        return softDeleted[0]
      }

      const deleted = await knex
        .from<IcebergCatalog>('buckets_analytics')
        .where('id', id)
        .delete()
        .returning('*')
        .abortOnSignal(signal)

      if (deleted.length === 0) {
        throw ERRORS.NoSuchBucket(id)
      }

      return deleted[0]
    })
  }

  listAnalyticsBuckets(
    columns: string,
    options: ListBucketOptions | undefined
  ): Promise<IcebergCatalog[]> {
    return this.runQuery('ListIcebergBuckets', async (knex, signal) => {
      const query = knex
        .from<IcebergCatalog>('buckets_analytics')
        .select(columns.split(',').map((c) => c.trim()))
        .whereNull('deleted_at')

      if (options?.search !== undefined && options.search.length > 0) {
        query.where('name', 'like', `%${options.search}%`)
      }

      if (options?.sortColumn !== undefined) {
        query.orderBy(options.sortColumn, options.sortOrder || 'asc')
      } else {
        query.orderBy('name', 'asc')
      }

      if (options?.limit !== undefined) {
        query.limit(options.limit)
      }

      if (options?.offset !== undefined) {
        query.offset(options.offset)
      }

      return query.abortOnSignal(signal)
    })
  }

  findAnalyticsBucketByName(name: string) {
    return this.runQuery('FindAnalyticsBucketByName', async (knex, signal) => {
      const icebergBucket = await knex
        .from<IcebergCatalog>('buckets_analytics')
        .select('*')
        .where('name', name)
        .whereNull('deleted_at')
        .first()
        .abortOnSignal(signal)

      if (!icebergBucket) {
        throw ERRORS.NoSuchBucket(name)
      }

      return icebergBucket
    })
  }

  createAnalyticsBucket(data: Pick<Bucket, 'name'>): Promise<IcebergCatalog> {
    const bucketData: Pick<IcebergCatalog, 'name'> = {
      name: data.name,
    }

    return this.runQuery('CreateAnalyticsBucket', async (knex, signal) => {
      const icebergBucket = await knex
        .from<IcebergCatalog>('buckets_analytics')
        .insert(bucketData)
        .onConflict(knex.raw('(name) WHERE deleted_at IS NULL'))
        .ignore()
        .returning('*')
        .abortOnSignal(signal)

      if (icebergBucket.length === 0) {
        throw ERRORS.ResourceAlreadyExists()
      }

      return icebergBucket[0]
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
      const rowCount = await this.runQuery('CreateBucket', async (knex, signal) => {
        return knex.from<Bucket>('buckets').insert(bucketData).abortOnSignal(signal)
      })

      if (!rowCount || rowCount[0] === 0) {
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
    const result = await this.runQuery('FindBucketById', async (knex, signal) => {
      let columnNames = columns.split(',')

      if (!(await tenantHasMigrations(this.tenantId, 'iceberg-catalog-flag-on-buckets'))) {
        columnNames = columnNames.filter((name) => {
          return name.trim() !== 'type'
        })
      }

      const query = knex.from<Bucket>('buckets').select(columnNames).where('id', bucketId)

      if (typeof filters?.isPublic !== 'undefined') {
        query.where('public', filters.isPublic)
      }

      if (filters?.forUpdate) {
        query.forUpdate()
      }

      if (filters?.forShare) {
        query.forShare()
      }

      return query.abortOnSignal(signal).first() as Promise<Bucket>
    })

    if (!result && !filters?.dontErrorOnEmpty) {
      throw ERRORS.NoSuchBucket(bucketId)
    }

    return result
  }

  async countObjectsInBucket(bucketId: string, limit?: number): Promise<number> {
    // if we have a limit use select to only scan up to that limit
    if (limit !== undefined) {
      const result = await this.runQuery('CountObjectsInBucketWithLimit', (knex, signal) => {
        return knex
          .from('objects')
          .where('bucket_id', bucketId)
          .limit(limit)
          .select(knex.raw('1'))
          .abortOnSignal(signal)
      })
      return result.length
    }

    // do full count if there is no limit
    const result = await this.runQuery('CountObjectsInBucket', (knex, signal) => {
      return knex
        .from('objects')
        .where('bucket_id', bucketId)
        .count()
        .abortOnSignal(signal)
        .first<{ count: number }>()
    })

    return result?.count || 0
  }

  async deleteBucket(bucketId: string | string[]) {
    return await this.runQuery('DeleteBucket', (knex, signal) => {
      return knex<Bucket>('buckets')
        .whereIn('id', Array.isArray(bucketId) ? bucketId : [bucketId])
        .delete()
        .abortOnSignal(signal)
    })
  }

  async listObjects(
    bucketId: string,
    columns = 'id',
    limit = 10,
    before?: Date,
    nextToken?: string
  ) {
    const data = await this.runQuery('ListObjects', (knex, signal) => {
      const query = knex
        .from<Obj>('objects')
        .select(columns.split(','))
        .where('bucket_id', bucketId)
        // @ts-expect-error knex typing is wrong, it doesn't accept a knex raw on orderBy, even though is totally legit
        .orderBy(knex.raw('name COLLATE "C"'))
        .limit(limit)

      if (before) {
        query.andWhere('created_at', '<', before.toISOString())
      }

      if (nextToken) {
        query.andWhere(knex.raw('name COLLATE "C" > ?', [nextToken]))
      }

      return query.abortOnSignal(signal) as Promise<Obj[]>
    })

    return data
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
    return this.runQuery('ListObjectsV2', async (knex, signal) => {
      if (!options?.delimiter) {
        const query = knex
          .table('objects')
          .where('bucket_id', bucketId)
          .select(['id', 'name', 'metadata', 'updated_at', 'created_at', 'last_accessed_at'])
          .limit(options?.maxKeys || 100)

        // only allow these values for sort columns, "name" is excluded intentionally as it is the default and used as tie breaker when sorting by other columns
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

        if (sortColumn) {
          query.orderBy(sortColumn, sortOrder)
        }
        // knex typing is wrong, it doesn't accept a knex.raw on orderBy, even though is totally legit
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        query.orderBy(knex.raw(`name COLLATE "C"`), sortOrder)

        if (options?.prefix) {
          query.where('name', 'like', `${options.prefix}%`)
        }

        if (options?.startAfter && !options?.nextToken) {
          query.andWhere(knex.raw(`name COLLATE "C" > ?`, [options.startAfter]))
        }

        if (options?.nextToken) {
          const pageOperator = sortOrder === 'asc' ? '>' : '<'
          if (sortColumn && options.sortBy?.after) {
            query.andWhere(
              knex.raw(
                `ROW(date_trunc('milliseconds', ${sortColumn}), name COLLATE "C") ${pageOperator} ROW(COALESCE(NULLIF(?, '')::timestamptz, 'epoch'::timestamptz), ?)`,
                [options.sortBy.after, options.nextToken]
              )
            )
          } else {
            query.andWhere(knex.raw(`name COLLATE "C" ${pageOperator} ?`, [options.nextToken]))
          }
        }

        return query.abortOnSignal(signal)
      }

      let useNewSearchVersion2 = true

      if (isMultitenant) {
        useNewSearchVersion2 = await tenantHasMigrations(this.tenantId, 'search-v2')
      }

      if (useNewSearchVersion2 && options?.delimiter === '/') {
        let paramPlaceholders = '?,?,?,?,?'
        const sortParams: (string | null)[] = []
        // this migration adds 3 more parameters to search v2 support sorting
        // 'search-v2-optimised' also implies sort support (it's a newer migration)
        const hasSortSupport =
          (await tenantHasMigrations(this.tenantId, 'add-search-v2-sort-support')) ||
          (await tenantHasMigrations(this.tenantId, 'search-v2-optimised'))
        if (hasSortSupport) {
          paramPlaceholders += ',?,?,?'
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
        const result = await knex
          .raw(`select * from storage.search_v2(${paramPlaceholders})`, searchParams)
          .abortOnSignal(signal)
        return result.rows
      }

      const result = await knex
        .raw('select * from storage.list_objects_with_delimiter(?,?,?,?,?,?)', [
          bucketId,
          options?.prefix,
          options?.delimiter,
          options?.maxKeys,
          options?.startAfter || '',
          options?.nextToken || '',
        ])
        .abortOnSignal(signal)
      return result.rows
    })
  }

  async listBuckets(columns = 'id', options?: ListBucketOptions) {
    const data = await this.runQuery('ListBuckets', async (knex, signal) => {
      const columnNames = columns.split(',').map((c) => c.trim())

      const selectColumns = columnNames.filter((name) => {
        return name !== 'type'
      })

      if (columnNames.includes('type')) {
        selectColumns.push(knex.raw("'STANDARD' as type") as unknown as string)
      }

      const query = knex.from<Bucket>('buckets').select(selectColumns)

      if (options?.search !== undefined && options.search.length > 0) {
        query.where('name', 'ilike', `%${options.search}%`)
      }

      if (options?.sortColumn !== undefined) {
        query.orderBy(options.sortColumn, options.sortOrder || 'asc')
      }

      if (options?.limit !== undefined) {
        query.limit(options.limit)
      }

      if (options?.offset !== undefined) {
        query.offset(options.offset)
      }

      return query.abortOnSignal(signal)
    })

    return data as Bucket[]
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
    return this.runQuery('ListMultipartsUploads', async (knex, signal) => {
      if (!options?.deltimeter) {
        const query = knex
          .table('s3_multipart_uploads')
          .select(['id', 'key', 'created_at'])
          .where('bucket_id', bucketId)
          .limit(options?.maxKeys || 100)

        // knex typing is wrong, it doesn't accept a knex.raw on orderBy, even though is totally legit
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        query.orderBy(knex.raw('key COLLATE "C", created_at'))

        if (options?.prefix) {
          query.where('key', 'ilike', `${options.prefix}%`)
        }

        if (options?.nextUploadKeyToken && !options.nextUploadToken) {
          query.andWhere(knex.raw(`key COLLATE "C" > ?`, [options?.nextUploadKeyToken]))
        }

        if (options?.nextUploadToken) {
          query.andWhere(knex.raw('id COLLATE "C" > ?', [options?.nextUploadToken]))
        }

        return query.abortOnSignal(signal)
      }

      const result = await knex
        .raw('select * from storage.list_multipart_uploads_with_delimiter(?,?,?,?,?,?)', [
          bucketId,
          options?.prefix,
          options?.deltimeter,
          options?.maxKeys,
          options?.nextUploadKeyToken || '',
          options.nextUploadToken || '',
        ])
        .abortOnSignal(signal)
      return result.rows
    })
  }

  async updateBucket(
    bucketId: string,
    fields: Pick<Bucket, 'public' | 'file_size_limit' | 'allowed_mime_types'>
  ) {
    const bucket = await this.runQuery('UpdateBucket', (knex, signal) => {
      return knex
        .from('buckets')
        .where('id', bucketId)
        .update({
          public: fields.public,
          file_size_limit: fields.file_size_limit,
          allowed_mime_types: fields.allowed_mime_types,
        })
        .abortOnSignal(signal)
    })

    if (bucket === 0) {
      throw ERRORS.NoSuchBucket(bucketId)
    }

    return
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
    const [object] = await this.runQuery('UpsertObject', (knex, signal) => {
      return knex
        .from<Obj>('objects')
        .insert(objectData)
        .onConflict(['name', 'bucket_id'])
        .merge(
          this.normalizeColumns({
            metadata: data.metadata,
            user_metadata: data.user_metadata,
            version: data.version,
            owner: isUuid(data.owner || '') ? data.owner : undefined,
            owner_id: data.owner,
          })
        )
        .returning('*')
        .abortOnSignal(signal)
    })

    return object
  }

  async updateObject(
    bucketId: string,
    name: string,
    data: Pick<Obj, 'owner' | 'metadata' | 'version' | 'name' | 'bucket_id' | 'user_metadata'>
  ) {
    const [object] = await this.runQuery('UpdateObject', (knex, signal) => {
      return knex
        .from<Obj>('objects')
        .where('bucket_id', bucketId)
        .where('name', name)
        .update(
          this.normalizeColumns({
            name: data.name,
            bucket_id: data.bucket_id,
            owner: isUuid(data.owner || '') ? data.owner : undefined,
            owner_id: data.owner,
            metadata: data.metadata,
            user_metadata: data.user_metadata,
            version: data.version,
          }),
          '*'
        )
        .abortOnSignal(signal)
    })

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
      await this.runQuery('CreateObject', (knex, signal) => {
        return knex.from<Obj>('objects').insert(object).abortOnSignal(signal)
      })

      return object
    } catch (e) {
      if (isStorageError(ErrorCode.ResourceAlreadyExists, e)) {
        throw ERRORS.KeyAlreadyExists(data.name, e)
      }
      throw e
    }
  }

  async deleteObject(bucketId: string, objectName: string, version?: string) {
    const [data] = await this.runQuery('Delete Object', (knex, signal) => {
      return knex
        .from<Obj>('objects')
        .delete()
        .where({
          name: objectName,
          bucket_id: bucketId,
          ...(version ? { version } : {}),
        })
        .returning('*')
        .abortOnSignal(signal)
    })

    return data
  }

  async deleteObjects(bucketId: string, objectNames: string[], by: keyof Obj = 'name') {
    return this.runQuery('DeleteObjects', (knex, signal) => {
      return knex
        .from<Obj>('objects')
        .delete()
        .where('bucket_id', bucketId)
        .whereIn(by, objectNames)
        .returning('*')
        .abortOnSignal(signal)
    })
  }

  async deleteObjectVersions(bucketId: string, objectNames: { name: string; version: string }[]) {
    return this.runQuery('DeleteObjects', (knex, signal) => {
      const placeholders = objectNames.map(() => '(?, ?)').join(', ')

      // Step 2: Flatten the array of tuples into a single array of values
      const flatParams = objectNames.flatMap(({ name, version }) => [name, version])

      return knex
        .from<Obj>('objects')
        .delete()
        .where('bucket_id', bucketId)
        .whereRaw(`(name, version) IN (${placeholders})`, flatParams)
        .returning('*')
        .abortOnSignal(signal)
    })
  }

  async updateObjectMetadata(bucketId: string, objectName: string, metadata: ObjectMetadata) {
    const [object] = await this.runQuery('UpdateObjectMetadata', (knex, signal) => {
      return knex
        .from<Obj>('objects')
        .update({ metadata })
        .where({ bucket_id: bucketId, name: objectName })
        .returning('*')
        .abortOnSignal(signal)
    })

    return object
  }

  async updateObjectOwner(bucketId: string, objectName: string, owner?: string) {
    const [object] = await this.runQuery('UpdateObjectOwner', (knex, signal) => {
      return knex
        .from<Obj>('objects')
        .update({
          last_accessed_at: new Date().toISOString(),
          owner: isUuid(owner || '') ? owner : undefined,
          owner_id: owner,
        })
        .returning('*')
        .where({ bucket_id: bucketId, name: objectName })
        .abortOnSignal(signal)
    })

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
    const object = await this.runQuery('FindObject', (knex, signal) => {
      const query = knex
        .from<Obj>('objects')
        .select(this.normalizeColumns(columns).split(','))
        .where({
          name: objectName,
          bucket_id: bucketId,
        })

      if (filters?.forUpdate) {
        query.forUpdate()
      }

      if (filters?.forShare) {
        query.forShare()
      }

      if (filters?.forKeyShare) {
        query.forKeyShare()
      }

      if (filters?.noWait) {
        query.noWait()
      }

      return query.abortOnSignal(signal).first() as Promise<Obj | undefined>
    })

    if (!object && !filters?.dontErrorOnEmpty) {
      throw ERRORS.NoSuchKey(objectName)
    }

    return object as typeof filters extends FindObjectFilters
      ? FindObjectFilters['dontErrorOnEmpty'] extends true
        ? Obj | undefined
        : Obj
      : Obj
  }

  async findObjects(bucketId: string, objectNames: string[], columns = 'id') {
    return this.runQuery('FindObjects', (knex, signal) => {
      return knex
        .from<Obj>('objects')
        .select(columns)
        .where('bucket_id', bucketId)
        .whereIn('name', objectNames)
        .abortOnSignal(signal)
    })
  }

  async findObjectVersions(bucketId: string, obj: { name: string; version: string }[]) {
    return this.runQuery('FindObjectVersions', (knex, signal) => {
      // Step 1: Generate placeholders for each tuple
      const placeholders = obj.map(() => '(?, ?)').join(', ')

      // Step 2: Flatten the array of tuples into a single array of values
      const flatParams = obj.flatMap(({ name, version }) => [name, version])

      return knex
        .from<Obj>('objects')
        .select('objects.name', 'objects.version')
        .where('bucket_id', bucketId)
        .whereRaw(`(name, version) IN (${placeholders})`, flatParams)
        .abortOnSignal(signal)
    })
  }

  async mustLockObject(bucketId: string, objectName: string, version?: string) {
    return this.runQuery('MustLockObject', async (knex, signal) => {
      const hash = hashStringToInt(`${bucketId}/${objectName}${version ? `/${version}` : ''}`)
      const result = await knex
        .raw<{ rows: { pg_try_advisory_xact_lock: boolean }[] }>(
          `SELECT pg_try_advisory_xact_lock(?);`,
          [hash]
        )
        .abortOnSignal(signal)
      const lockAcquired = result.rows.shift()?.pg_try_advisory_xact_lock || false

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
    return this.runQuery('WaitObjectLock', async (knex, signal) => {
      const hash = hashStringToInt(`${bucketId}/${objectName}${version ? `/${version}` : ''}`)
      const query = knex.raw(`SELECT pg_advisory_xact_lock(?)`, [hash]).abortOnSignal(signal)

      if (opts?.timeout) {
        let timeoutInterval: undefined | NodeJS.Timeout

        try {
          await Promise.race([
            query,
            new Promise(
              (_, reject) =>
                (timeoutInterval = setTimeout(() => reject(ERRORS.LockTimeout()), opts.timeout))
            ),
          ])
        } catch (e) {
          throw e
        } finally {
          if (timeoutInterval) {
            clearTimeout(timeoutInterval)
          }
        }
      } else {
        await query
      }

      return true
    })
  }

  async searchObjects(bucketId: string, prefix: string, options: SearchObjectOption) {
    return this.runQuery('SearchObjects', async (knex, signal) => {
      const result = await knex
        .raw<{ rows: Obj[] }>('select * from storage.search(?,?,?,?,?,?,?,?)', [
          prefix,
          bucketId,
          options.limit || 100,
          prefix.split('/').length,
          options.offset || 0,
          options.search || '',
          options.sortBy?.column ?? 'name',
          options.sortBy?.order ?? 'asc',
        ])
        .abortOnSignal(signal)

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
    metadata?: Record<string, string | null>
  ) {
    return this.runQuery('CreateMultipartUpload', async (knex, signal) => {
      const multipart = await knex
        .table<S3MultipartUpload>('s3_multipart_uploads')
        .insert(
          this.normalizeColumns({
            id: uploadId,
            bucket_id: bucketId,
            key: objectName,
            version,
            upload_signature: signature,
            owner_id: owner,
            user_metadata: metadata,
          })
        )
        .returning('*')
        .abortOnSignal(signal)

      return multipart[0] as S3MultipartUpload
    })
  }

  async findMultipartUpload(uploadId: string, columns = 'id', options?: { forUpdate?: boolean }) {
    const multiPart = await this.runQuery('FindMultipartUpload', async (knex, signal) => {
      const query = knex
        .from('s3_multipart_uploads')
        .select(columns.split(','))
        .where('id', uploadId)

      if (options?.forUpdate) {
        return query.abortOnSignal(signal).forUpdate().first()
      }
      return query.abortOnSignal(signal).first()
    })

    if (!multiPart) {
      throw ERRORS.NoSuchUpload(uploadId)
    }
    return multiPart
  }

  async updateMultipartUploadProgress(uploadId: string, progress: number, signature: string) {
    return this.runQuery('UpdateMultipartUploadProgress', async (knex, signal) => {
      await knex
        .from('s3_multipart_uploads')
        .update({ in_progress_size: progress, upload_signature: signature })
        .where('id', uploadId)
        .abortOnSignal(signal)
    })
  }

  async deleteMultipartUpload(uploadId: string) {
    return this.runQuery('DeleteMultipartUpload', async (knex, signal) => {
      await knex.from('s3_multipart_uploads').delete().where('id', uploadId).abortOnSignal(signal)
    })
  }

  async insertUploadPart(part: S3PartUpload) {
    return this.runQuery('InsertUploadPart', async (knex, signal) => {
      const storedPart = await knex
        .table<S3PartUpload>('s3_multipart_uploads_parts')
        .insert(part)
        .returning('*')
        .abortOnSignal(signal)

      return storedPart[0]
    })
  }

  async listParts(
    uploadId: string,
    options: { afterPart?: string; maxParts: number }
  ): Promise<S3PartUpload[]> {
    return this.runQuery('ListParts', async (knex, signal) => {
      const query = knex
        .from<S3PartUpload>('s3_multipart_uploads_parts')
        .select('etag', 'part_number', 'size', 'upload_id', 'created_at')
        .where('upload_id', uploadId)
        .orderBy('part_number')
        .limit(options.maxParts)

      if (options.afterPart) {
        query.andWhere('part_number', '>', options.afterPart)
      }

      return query.abortOnSignal(signal)
    })
  }

  healthcheck() {
    return this.runQuery('Healthcheck', (knex, signal) => {
      return knex.raw('SELECT id from storage.buckets limit 1').abortOnSignal(signal)
    })
  }

  destroyConnection() {
    return this.connection.dispose()
  }

  /**
   * Excludes columns selection if a specific migration wasn't run
   * @param columns
   * @protected
   */
  protected normalizeColumns<T extends string | Record<string, any>>(columns: T): T {
    const latestMigration = this.latestMigration

    if (!latestMigration) {
      return columns
    }

    const rules = [{ migration: 'custom-metadata', newColumns: ['user_metadata'] }]

    rules.forEach((rule) => {
      if (DBMigration[latestMigration] < DBMigration[rule.migration as keyof typeof DBMigration]) {
        const value = rule.newColumns

        if (typeof columns === 'string') {
          columns = columns
            .split(',')
            .filter((column) => !value.includes(column))
            .join(',') as T
        }

        if (typeof columns === 'object') {
          value.forEach((column: string) => {
            delete (columns as Record<string, object>)[column]
          })
        }
      }
    })

    return columns
  }

  protected async runQuery<
    T extends (...args: [db: Knex.Transaction, signal?: AbortSignal]) => Promise<any>
  >(queryName: string, fn: T): Promise<Awaited<ReturnType<T>>> {
    const startTime = process.hrtime.bigint()
    const recordDuration = () => {
      const duration = Number(process.hrtime.bigint() - startTime) / 1e9
      dbQueryPerformance.record(duration, {
        name: queryName,
        tenantId: this.tenantId,
      })
    }

    const abortSignal = this.connection.getAbortSignal()

    let tnx = this.options.tnx

    const differentScopes = Boolean(
      this.options.parentConnection?.role &&
        this.connection.role !== this.options.parentConnection?.role
    )
    const needsNewTransaction = !tnx || differentScopes

    if (!tnx || needsNewTransaction) {
      tnx = await this.connection.transactionProvider(this.options.tnx)()
      tnx.once('query-error', (error: DatabaseError, q) => {
        throw DBError.fromDBError(error, q.sql)
      })
    }

    try {
      if (needsNewTransaction || differentScopes) {
        await this.connection.setScope(tnx)
      }

      const result: Awaited<ReturnType<T>> = await fn(tnx, abortSignal)

      if (needsNewTransaction) {
        await tnx.commit()
      }

      if (this.options.parentTnx && !this.options.parentTnx.isCompleted()) {
        if (differentScopes) {
          await this.options.parentConnection?.setScope(this.options.parentTnx)
        }
      }

      recordDuration()

      return result
    } catch (e) {
      if (needsNewTransaction) {
        await tnx.rollback()
      }
      recordDuration()
      throw e
    }
  }
}

export class DBError extends StorageBackendError implements RenderableError {
  constructor(options: StorageErrorOptions) {
    super(options)
    Object.setPrototypeOf(this, DBError.prototype)
  }

  static fromDBError(pgError: DatabaseError, query?: string) {
    switch (pgError.code) {
      case '42501':
        return ERRORS.AccessDenied(
          'new row violates row-level security policy',
          pgError
        ).withMetadata({
          query,
          code: pgError.code,
        })
      case '23505':
        return ERRORS.ResourceAlreadyExists(pgError).withMetadata({
          query,
          code: pgError.code,
        })
      case '23503':
        return ERRORS.RelatedResourceNotFound(pgError).withMetadata({
          query,
          code: pgError.code,
        })
      case '55P03':
      case 'resource_locked':
        return ERRORS.ResourceLocked(pgError).withMetadata({
          query,
          code: pgError.code,
        })
      case '57014': // query_canceled (statement_timeout or user cancel)
        return ERRORS.DatabaseTimeout(pgError).withMetadata({
          query,
          code: pgError.code,
        })
      default:
        return ERRORS.DatabaseError(`database error, code: ${pgError.code}`, pgError).withMetadata({
          query,
          code: pgError.code,
        })
    }
  }
}
