import { Bucket, Obj } from '../schemas'
import { RenderableError, StorageBackendError, StorageError } from '../errors'
import { ObjectMetadata } from '../backend'
import { Knex } from 'knex'
import {
  Database,
  DatabaseOptions,
  FindBucketFilters,
  FindObjectFilters,
  SearchObjectOption,
  TransactionOptions,
} from './adapter'
import { DatabaseError } from 'pg'
import { TenantConnection } from '../../database/connection'
import { DbQueryPerformance } from '../../monitoring/metrics'
import { isUuid } from '../limits'

/**
 * Database
 * the only source of truth for interacting with the storage database
 */
export class StorageKnexDB implements Database {
  public readonly tenantHost: string
  public readonly tenantId: string
  public readonly reqId: string | undefined
  public readonly role?: string

  constructor(
    public readonly connection: TenantConnection,
    private readonly options: DatabaseOptions<Knex.Transaction>
  ) {
    this.tenantHost = options.host
    this.tenantId = options.tenantId
    this.reqId = options.reqId
    this.role = connection?.role
  }

  async withTransaction<T extends (db: Database) => Promise<any>>(
    fn: T,
    transactionOptions?: TransactionOptions
  ) {
    let retryLeft = transactionOptions?.retry || 1
    let error: Error | undefined | unknown

    while (retryLeft > 0) {
      try {
        const tnx = await this.connection.transactionProvider(this.options.tnx)()

        try {
          await this.connection.setScope(tnx)

          tnx.once('query-error', (error) => {
            throw DBError.fromDBError(error)
          })

          const opts = { ...this.options, tnx }
          const storageWithTnx = new StorageKnexDB(this.connection, opts)

          const result: Awaited<ReturnType<T>> = await fn(storageWithTnx)
          await tnx.commit()
          return result
        } catch (e) {
          await tnx.rollback()
          throw e
        } finally {
          tnx.removeAllListeners()
        }
      } catch (e) {
        error = e
      } finally {
        retryLeft--
      }
    }

    if (error) {
      throw error
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
        throw new StorageBackendError('permission_ok', 200, 'permission pass')
      })
    } catch (e) {
      if (e instanceof StorageBackendError && e.name === 'permission_ok') {
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

    const bucket = await this.runQuery('CreateBucket', async (knex) => {
      return knex.from<Bucket>('buckets').insert(bucketData) as Promise<{ rowCount: number }>
    })

    if (bucket.rowCount === 0) {
      throw new DBError('Bucket not found', 404, 'Bucket not found', undefined, {
        bucketId: data.id,
      })
    }

    return bucketData
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
      throw new DBError('Bucket not found', 404, 'Bucket not found', undefined, {
        bucketId,
      })
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

  async listObjects(bucketId: string, columns = 'id', limit = 10) {
    const data = await this.runQuery('ListObjects', (knex) => {
      return knex
        .from<Obj>('objects')
        .select(columns.split(','))
        .where('bucket_id', bucketId)
        .limit(limit) as Promise<Obj[]>
    })

    return data
  }

  async listBuckets(columns = 'id') {
    const data = await this.runQuery('ListBuckets', (knex) => {
      return knex.from<Bucket>('buckets').select(columns.split(','))
    })

    return data as Bucket[]
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
      throw new DBError('Bucket not found', 404, 'Bucket not found', undefined, {
        bucketId,
      })
    }

    return
  }

  async upsertObject(data: Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata' | 'version'>) {
    const objectData = {
      name: data.name,
      owner: isUuid(data.owner || '') ? data.owner : undefined,
      owner_id: data.owner,
      bucket_id: data.bucket_id,
      metadata: data.metadata,
      version: data.version,
    }
    const [object] = await this.runQuery('UpsertObject', (knex) => {
      return knex
        .from<Obj>('objects')
        .insert(objectData)
        .onConflict(['name', 'bucket_id'])
        .merge({
          metadata: data.metadata,
          version: data.version,
          owner: isUuid(data.owner || '') ? data.owner : undefined,
          owner_id: data.owner,
        })
        .returning('*')
    })

    return object
  }

  async updateObject(
    bucketId: string,
    name: string,
    data: Pick<Obj, 'owner' | 'metadata' | 'version' | 'name'>
  ) {
    const [object] = await this.runQuery('UpdateObject', (knex) => {
      return knex
        .from<Obj>('objects')
        .where('bucket_id', bucketId)
        .where('name', name)
        .update(
          {
            name: data.name,
            owner: isUuid(data.owner || '') ? data.owner : undefined,
            owner_id: data.owner,
            metadata: data.metadata,
            version: data.version,
          },
          '*'
        )
    })

    if (!object) {
      throw new DBError('Not Found', 404, 'object not found')
    }

    return object
  }

  async createObject(data: Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata' | 'version'>) {
    const object = {
      name: data.name,
      owner: isUuid(data.owner || '') ? data.owner : undefined,
      owner_id: data.owner,
      bucket_id: data.bucket_id,
      metadata: data.metadata,
      version: data.version,
    }
    await this.runQuery('CreateObject', (knex) => {
      return knex.from<Obj>('objects').insert(object)
    })

    return object
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
      throw new DBError('Object not found', 404, 'not_found', undefined, {
        bucketId,
      })
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
      const query = knex.from<Obj>('objects').select(columns.split(',')).where({
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
      throw new DBError('Object not found', 404, 'not_found', undefined, {
        bucketId,
      })
    }

    return object as typeof filters extends FindObjectFilters
      ? typeof filters['dontErrorOnEmpty'] extends true
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

  async mustLockObject(bucketId: string, objectName: string, version?: string) {
    return this.runQuery('MustLockObject', async (knex) => {
      const hash = hashStringToInt(`${bucketId}/${objectName}${version ? `/${version}` : ''}`)
      const result = await knex.raw<any>(`SELECT pg_try_advisory_xact_lock(?);`, [hash])
      const lockAcquired = result.rows.shift()?.pg_try_advisory_xact_lock || false

      if (!lockAcquired) {
        throw new DBError('resource_locked', 409, 'Resource is locked')
      }

      return true
    })
  }

  async waitObjectLock(bucketId: string, objectName: string, version?: string) {
    return this.runQuery('WaitObjectLock', async (knex) => {
      const hash = hashStringToInt(`${bucketId}/${objectName}${version ? `/${version}` : ''}`)
      await knex.raw<any>(`SELECT pg_advisory_xact_lock(?)`, [hash])
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

  healthcheck() {
    return this.runQuery('Healthcheck', (knex) => {
      return knex.raw('SELECT id from storage.buckets limit 1')
    })
  }

  destroyConnection() {
    return this.connection.dispose()
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
      tnx.once('query-error', (error: DatabaseError) => {
        throw DBError.fromDBError(error)
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
    } finally {
      if (needsNewTransaction) {
        tnx.removeAllListeners()
      }
    }
  }
}

export class DBError extends Error implements RenderableError {
  constructor(
    message: string,
    public readonly statusCode: number,
    public readonly error: string,
    public readonly originalError?: Error,
    public readonly metadata?: Record<string, any>
  ) {
    super(message)
    this.message = message
    Object.setPrototypeOf(this, DBError.prototype)
  }

  static fromDBError(pgError: DatabaseError) {
    let message = 'Internal Server Error'
    let statusCode = 500
    let error = 'internal'

    switch (pgError.code) {
      case '42501':
        message = 'new row violates row-level security policy'
        statusCode = 403
        error = 'Unauthorized'
        break
      case '23505':
        message = 'The resource already exists'
        statusCode = 409
        error = 'Duplicate'
        break
      case '23503':
        message = 'The parent resource is not found'
        statusCode = 404
        error = 'Not Found'
        break
      case '55P03':
      case 'resource_locked':
        message = 'Resource Locked, an upload might be in progress for this resource'
        statusCode = 400
        error = 'resource_locked'
        break
    }

    return new DBError(message, statusCode, error, pgError)
  }

  render(): StorageError {
    return {
      message: this.message,
      statusCode: `${this.statusCode}`,
      error: this.error,
    }
  }

  getOriginalError() {
    return this.originalError
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
