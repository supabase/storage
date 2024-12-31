import { Bucket, S3MultipartUpload, Obj, S3PartUpload } from '../schemas'
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
} from './adapter'
import { DatabaseError } from 'pg'
import { TenantConnection } from '@internal/database'
import { DbQueryPerformance } from '@internal/monitoring/metrics'
import { isUuid } from '../limits'
import { DBMigration } from '@internal/database/migrations'

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
  async withTransaction<T extends (db: Database) => Promise<any>>(fn: T) {
    const tnx = await this.connection.transactionProvider(this.options.tnx)()

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

  async createBucket(
    data: Pick<
      Bucket,
      'id' | 'name' | 'public' | 'owner' | 'file_size_limit' | 'allowed_mime_types'
    >
  ) {
    const bucketData = {
      id: data.id,
      name: data.name,
      owner: isUuid(data.owner || '') ? data.owner : undefined,
      owner_id: data.owner,
      public: data.public,
      allowed_mime_types: data.allowed_mime_types,
      file_size_limit: data.file_size_limit,
    }

    try {
      const bucket = await this.runQuery('CreateBucket', async (knex) => {
        return knex.from<Bucket>('buckets').insert(bucketData) as Promise<{ rowCount: number }>
      })

      if (bucket.rowCount === 0) {
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
    const result = await this.runQuery('FindBucketById', async (knex) => {
      const query = knex.from<Bucket>('buckets').select(columns.split(',')).where('id', bucketId)

      if (typeof filters?.isPublic !== 'undefined') {
        query.where('public', filters.isPublic)
      }

      if (filters?.forUpdate) {
        query.forUpdate()
      }

      if (filters?.forShare) {
        query.forShare()
      }

      return query.first() as Promise<Bucket>
    })

    if (!result && !filters?.dontErrorOnEmpty) {
      throw ERRORS.NoSuchBucket(bucketId)
    }

    return result
  }

  async countObjectsInBucket(bucketId: string) {
    const result = await this.runQuery('CountObjectsInBucket', (knex) => {
      return knex
        .from<{ count: number }>('objects')
        .where('bucket_id', bucketId)
        .limit(10)
        .count()
        .first()
    })

    return (result?.count as number) || 0
  }

  async deleteBucket(bucketId: string | string[]) {
    return await this.runQuery('DeleteBucket', (knex) => {
      return knex<Bucket>('buckets')
        .whereIn('id', Array.isArray(bucketId) ? bucketId : [bucketId])
        .delete()
    })
  }

  async listObjects(
    bucketId: string,
    columns = 'id',
    limit = 10,
    before?: Date,
    nextToken?: string
  ) {
    const data = await this.runQuery('ListObjects', (knex) => {
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

      return query as Promise<Obj[]>
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
    }
  ) {
    return this.runQuery('ListObjectsV2', async (knex) => {
      if (!options?.delimiter) {
        const query = knex
          .table('objects')
          .where('bucket_id', bucketId)
          .select(['id', 'name', 'metadata', 'updated_at'])
          .limit(options?.maxKeys || 100)

        // knex typing is wrong, it doesn't accept a knex.raw on orderBy, even though is totally legit
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        query.orderBy(knex.raw('name COLLATE "C"'))

        if (options?.prefix) {
          query.where('name', 'ilike', `${options.prefix}%`)
        }

        if (options?.nextToken) {
          query.andWhere(knex.raw('name COLLATE "C" > ?', [options?.nextToken]))
        }

        return query
      }

      const query = await knex.raw(
        'select * from storage.list_objects_with_delimiter(?,?,?,?,?,?)',
        [
          bucketId,
          options?.prefix,
          options?.delimiter,
          options?.maxKeys,
          options?.startAfter || '',
          options?.nextToken || '',
        ]
      )

      return query.rows
    })
  }

  async listBuckets(columns = 'id') {
    const data = await this.runQuery('ListBuckets', (knex) => {
      return knex.from<Bucket>('buckets').select(columns.split(','))
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
    return this.runQuery('ListMultipartsUploads', async (knex) => {
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

        return query
      }

      const query = await knex.raw(
        'select * from storage.list_multipart_uploads_with_delimiter(?,?,?,?,?,?)',
        [
          bucketId,
          options?.prefix,
          options?.deltimeter,
          options?.maxKeys,
          options?.nextUploadKeyToken || '',
          options.nextUploadToken || '',
        ]
      )

      return query.rows
    })
  }

  async updateBucket(
    bucketId: string,
    fields: Pick<Bucket, 'public' | 'file_size_limit' | 'allowed_mime_types'>
  ) {
    const bucket = await this.runQuery('UpdateBucket', (knex) => {
      return knex.from('buckets').where('id', bucketId).update({
        public: fields.public,
        file_size_limit: fields.file_size_limit,
        allowed_mime_types: fields.allowed_mime_types,
      })
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
    const [object] = await this.runQuery('UpsertObject', (knex) => {
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
    })

    return object
  }

  async updateObject(
    bucketId: string,
    name: string,
    data: Pick<Obj, 'owner' | 'metadata' | 'version' | 'name' | 'bucket_id' | 'user_metadata'>
  ) {
    const [object] = await this.runQuery('UpdateObject', (knex) => {
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
      await this.runQuery('CreateObject', (knex) => {
        return knex.from<Obj>('objects').insert(object)
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
    const [data] = await this.runQuery('Delete Object', (knex) => {
      return knex
        .from<Obj>('objects')
        .delete()
        .where({
          name: objectName,
          bucket_id: bucketId,
          ...(version ? { version } : {}),
        })
        .returning('*')
    })

    return data
  }

  async deleteObjects(bucketId: string, objectNames: string[], by: keyof Obj = 'name') {
    const objects = await this.runQuery('DeleteObjects', (knex) => {
      return knex
        .from<Obj>('objects')
        .delete()
        .where('bucket_id', bucketId)
        .whereIn(by, objectNames)
        .returning('*')
    })

    return objects
  }

  async deleteObjectVersions(bucketId: string, objectNames: { name: string; version: string }[]) {
    const objects = await this.runQuery('DeleteObjects', (knex) => {
      const placeholders = objectNames.map(() => '(?, ?)').join(', ')

      // Step 2: Flatten the array of tuples into a single array of values
      const flatParams = objectNames.flatMap(({ name, version }) => [name, version])

      return knex
        .from<Obj>('objects')
        .delete()
        .where('bucket_id', bucketId)
        .whereRaw(`(name, version) IN (${placeholders})`, flatParams)
        .returning('*')
    })

    return objects
  }

  async updateObjectMetadata(bucketId: string, objectName: string, metadata: ObjectMetadata) {
    const [object] = await this.runQuery('UpdateObjectMetadata', (knex) => {
      return knex
        .from<Obj>('objects')
        .update({
          metadata,
        })
        .where({ bucket_id: bucketId, name: objectName })
        .returning('*')
    })

    return object
  }

  async updateObjectOwner(bucketId: string, objectName: string, owner?: string) {
    const [object] = await this.runQuery('UpdateObjectOwner', (knex) => {
      return knex
        .from<Obj>('objects')
        .update({
          last_accessed_at: new Date().toISOString(),
          owner: isUuid(owner || '') ? owner : undefined,
          owner_id: owner,
        })
        .returning('*')
        .where({ bucket_id: bucketId, name: objectName })
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
    const object = await this.runQuery('FindObject', (knex) => {
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

      return query.first() as Promise<Obj | undefined>
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
    const objects = await this.runQuery('FindObjects', (knex) => {
      return knex
        .from<Obj>('objects')
        .select(columns)
        .where('bucket_id', bucketId)
        .whereIn('name', objectNames)
    })

    return objects
  }

  async findObjectVersions(bucketId: string, obj: { name: string; version: string }[]) {
    const objects = await this.runQuery('FindObjectVersions', (knex) => {
      // Step 1: Generate placeholders for each tuple
      const placeholders = obj.map(() => '(?, ?)').join(', ')

      // Step 2: Flatten the array of tuples into a single array of values
      const flatParams = obj.flatMap(({ name, version }) => [name, version])

      return knex
        .from<Obj>('objects')
        .select('objects.name', 'objects.version')
        .where('bucket_id', bucketId)
        .whereRaw(`(name, version) IN (${placeholders})`, flatParams)
    })

    return objects
  }

  async mustLockObject(bucketId: string, objectName: string, version?: string) {
    return this.runQuery('MustLockObject', async (knex) => {
      const hash = hashStringToInt(`${bucketId}/${objectName}${version ? `/${version}` : ''}`)
      const result = await knex.raw<any>(`SELECT pg_try_advisory_xact_lock(?);`, [hash])
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
    return this.runQuery('WaitObjectLock', async (knex) => {
      const hash = hashStringToInt(`${bucketId}/${objectName}${version ? `/${version}` : ''}`)
      const query = knex.raw<any>(`SELECT pg_advisory_xact_lock(?)`, [hash])

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
    return this.runQuery('SearchObjects', async (knex) => {
      const result = await knex.raw('select * from storage.search(?,?,?,?,?,?,?,?)', [
        prefix,
        bucketId,
        options.limit || 100,
        prefix.split('/').length,
        options.offset || 0,
        options.search || '',
        options.sortBy?.column ?? 'name',
        options.sortBy?.order ?? 'asc',
      ])

      return (result as any).rows
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
    return this.runQuery('CreateMultipartUpload', async (knex) => {
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

      return multipart[0] as S3MultipartUpload
    })
  }

  async findMultipartUpload(uploadId: string, columns = 'id', options?: { forUpdate?: boolean }) {
    const multiPart = await this.runQuery('FindMultipartUpload', async (knex) => {
      const query = knex
        .from('s3_multipart_uploads')
        .select(columns.split(','))
        .where('id', uploadId)

      if (options?.forUpdate) {
        return query.forUpdate().first()
      }
      return query.first()
    })

    if (!multiPart) {
      throw ERRORS.NoSuchUpload(uploadId)
    }
    return multiPart
  }

  async updateMultipartUploadProgress(uploadId: string, progress: number, signature: string) {
    return this.runQuery('UpdateMultipartUploadProgress', async (knex) => {
      await knex
        .from('s3_multipart_uploads')
        .update({ in_progress_size: progress, upload_signature: signature })
        .where('id', uploadId)
    })
  }

  async deleteMultipartUpload(uploadId: string) {
    return this.runQuery('DeleteMultipartUpload', async (knex) => {
      await knex.from('s3_multipart_uploads').delete().where('id', uploadId)
    })
  }

  async insertUploadPart(part: S3PartUpload) {
    return this.runQuery('InsertUploadPart', async (knex) => {
      const storedPart = await knex
        .table<S3PartUpload>('s3_multipart_uploads_parts')
        .insert(part)
        .returning('*')

      return storedPart[0]
    })
  }

  async listParts(
    uploadId: string,
    options: { afterPart?: string; maxParts: number }
  ): Promise<S3PartUpload[]> {
    return this.runQuery('ListParts', async (knex) => {
      const query = knex
        .from<S3PartUpload>('s3_multipart_uploads_parts')
        .select('etag', 'part_number', 'size', 'upload_id', 'created_at')
        .where('upload_id', uploadId)
        .orderBy('part_number')
        .limit(options.maxParts)

      if (options.afterPart) {
        query.andWhere('part_number', '>', options.afterPart)
      }

      return query
    })
  }

  healthcheck() {
    return this.runQuery('Healthcheck', (knex) => {
      return knex.raw('SELECT id from storage.buckets limit 1')
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
            delete (columns as Record<any, any>)[column]
          })
        }
      }
    })

    return columns
  }

  protected async runQuery<T extends (db: Knex.Transaction) => Promise<any>>(
    queryName: string,
    fn: T
  ): Promise<Awaited<ReturnType<T>>> {
    const timer = DbQueryPerformance.startTimer({
      name: queryName,
    })

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

      const result: Awaited<ReturnType<T>> = await fn(tnx)

      if (needsNewTransaction) {
        await tnx.commit()
      }

      if (this.options.parentTnx && !this.options.parentTnx.isCompleted()) {
        if (differentScopes) {
          await this.options.parentConnection?.setScope(this.options.parentTnx)
        }
      }

      timer()

      return result
    } catch (e) {
      if (needsNewTransaction) {
        await tnx.rollback()
      }
      timer()
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
      default:
        return ERRORS.DatabaseError(pgError.message, pgError).withMetadata({
          query,
          code: pgError.code,
        })
    }
  }
}

export default function hashStringToInt(str: string): number {
  let hash = 5381
  let i = -1
  while (i < str.length - 1) {
    i += 1
    hash = (hash * 33) ^ str.charCodeAt(i)
  }
  return hash >>> 0
}
