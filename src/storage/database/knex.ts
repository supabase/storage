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

/**
 * Database
 * the only source of truth for interacting with the storage database
 */
export class StorageKnexDB implements Database {
  public readonly host: string
  public readonly tenantId: string
  public readonly role?: string

  constructor(
    public readonly connection: TenantConnection,
    private readonly options: DatabaseOptions<TenantConnection, Knex.Transaction>
  ) {
    this.host = options.host
    this.tenantId = options.tenantId
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
        const tnx = await this.connection.transaction(
          transactionOptions?.isolation as Knex.IsolationLevels,
          this.options.tnx
        )()

        await this.connection.setScope(tnx)

        tnx.on('query-error', (error) => {
          throw DBError.fromDBError(error)
        })

        const opts = { ...this.options, tnx }
        const storageWithTnx = new StorageKnexDB(this.connection, opts)

        try {
          const result: Awaited<ReturnType<T>> = await fn(storageWithTnx)
          await tnx.commit()
          return result
        } catch (e) {
          await tnx.rollback()
          throw e
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
      host: this.host,
    }
  }

  asSuperUser() {
    if (!this.options.superAdmin) {
      throw new Error('super admin client not instantiated')
    }

    return new StorageKnexDB(this.options.superAdmin, {
      ...this.options,
      parentConnection: this.connection,
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
      owner: data.owner,
      public: data.public,
      allowed_mime_types: data.allowed_mime_types,
      file_size_limit: data.file_size_limit,
    }
    const bucket = await this.runQuery(async (knex) => {
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
    const result = await this.runQuery(async (knex) => {
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
    const result = await this.runQuery((knex) => {
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
    return await this.runQuery((knex) => {
      return knex<Bucket>('buckets')
        .whereIn('id', Array.isArray(bucketId) ? bucketId : [bucketId])
        .delete()
    })
  }

  async listObjects(bucketId: string, columns = 'id', limit = 10) {
    const data = await this.runQuery((knex) => {
      return knex
        .from<Obj>('objects')
        .select(columns.split(','))
        .where('bucket_id', bucketId)
        .limit(limit) as Promise<Obj[]>
    })

    return data
  }

  async listBuckets(columns = 'id') {
    const data = await this.runQuery((knex) => {
      return knex.from<Bucket>('buckets').select(columns.split(','))
    })

    return data as Bucket[]
  }

  async updateBucket(
    bucketId: string,
    fields: Pick<Bucket, 'public' | 'file_size_limit' | 'allowed_mime_types'>
  ) {
    const [bucket] = await this.runQuery((knex) => {
      return knex
        .from<Bucket>('buckets')
        .update({
          public: fields.public,
          file_size_limit: fields.file_size_limit,
          allowed_mime_types: fields.allowed_mime_types === null ? [] : fields.allowed_mime_types,
        })
        .where('id', bucketId)
        .returning('*')
    })

    if (!bucket) {
      throw new DBError('Bucket not found', 404, 'Bucket not found', undefined, {
        bucketId,
      })
    }

    return bucket
  }

  async upsertObject(data: Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata' | 'version'>) {
    const objectData = {
      name: data.name,
      owner: data.owner,
      bucket_id: data.bucket_id,
      metadata: data.metadata,
      version: data.version,
    }
    await this.runQuery((knex) => {
      return knex.from<Obj>('objects').insert(objectData).onConflict(['name', 'bucket_id']).merge({
        metadata: data.metadata,
        version: data.version,
        owner: data.owner,
      })
    })

    return objectData
  }

  async updateObject(
    bucketId: string,
    name: string,
    data: Pick<Obj, 'owner' | 'metadata' | 'version' | 'name' | 'upload_state'>
  ) {
    const [object] = await this.runQuery((knex) => {
      return knex.from<Obj>('objects').where('bucket_id', bucketId).where('name', name).update(
        {
          name: data.name,
          owner: data.owner,
          metadata: data.metadata,
          version: data.version,
          upload_state: data.upload_state,
        },
        '*'
      )
    })

    if (!object) {
      throw new DBError('Not Found', 404, 'object not found')
    }

    return object
  }

  async createObject(
    data: Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata' | 'version' | 'upload_state'>
  ) {
    const object = {
      name: data.name,
      owner: data.owner,
      bucket_id: data.bucket_id,
      metadata: data.metadata,
      version: data.version,
      upload_state: data.upload_state,
    }
    await this.runQuery((knex) => {
      return knex.from<Obj>('objects').insert(object)
    })

    return object
  }

  async deleteObject(bucketId: string, objectName: string, version?: string) {
    const [data] = await this.runQuery((knex) => {
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
    const objects = await this.runQuery((knex) => {
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
    const [object] = await this.runQuery((knex) => {
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
    const [object] = await this.runQuery((knex) => {
      return knex
        .from<Obj>('objects')
        .update({
          last_accessed_at: new Date().toISOString(),
          owner,
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
    const object = await this.runQuery((knex) => {
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
    const objects = await this.runQuery((knex) => {
      return knex
        .from<Obj>('objects')
        .select(columns)
        .where('bucket_id', bucketId)
        .whereIn('name', objectNames)
    })

    return objects
  }

  async mustLockObject(bucketId: string, objectName: string, version?: string) {
    return this.runQuery(async (knex) => {
      const hash = hashStringToInt(`${bucketId}/${objectName}${version ? `/${version}` : ''}`)
      const result = await knex.raw<any>(`SELECT pg_try_advisory_xact_lock(${hash});`)
      const lockAcquired = result.rows.shift()?.pg_try_advisory_xact_lock || false

      if (!lockAcquired) {
        throw new DBError('resource_locked', 409, 'Resource is locked')
      }

      return true
    })
  }

  async waitObjectLock(bucketId: string, objectName: string, version?: string) {
    return this.runQuery(async (knex) => {
      const hash = hashStringToInt(`${bucketId}/${objectName}${version ? `/${version}` : ''}`)
      await knex.raw<any>(`SELECT pg_advisory_xact_lock(${hash});`)
      return true
    })
  }

  async searchObjects(bucketId: string, prefix: string, options: SearchObjectOption) {
    return this.runQuery(async (knex) => {
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

  protected async runQuery<T extends (db: Knex.Transaction) => Promise<any>>(
    fn: T,
    isolation?: Knex.IsolationLevels
  ): Promise<Awaited<ReturnType<T>>> {
    let tnx = this.options.tnx

    const differentScopes = Boolean(
      this.options.parentConnection?.role &&
        this.connection.role !== this.options.parentConnection?.role
    )
    const needsNewTransaction = !tnx || differentScopes

    if (!tnx || needsNewTransaction) {
      tnx = await this.connection.transaction(isolation, this.options.tnx)()
      tnx.on('query-error', (error: DatabaseError) => {
        throw DBError.fromDBError(error)
      })
      await this.connection.setScope(tnx)
    } else if (differentScopes) {
      await this.connection.setScope(tnx)
    }

    try {
      const result: Awaited<ReturnType<T>> = await fn(tnx)

      if (differentScopes) {
        await this.options.parentConnection?.setScope(tnx)
      }

      if (needsNewTransaction) {
        await tnx.commit()
      }

      return result
    } catch (e) {
      if (needsNewTransaction) {
        await tnx.rollback()
      }
      throw e
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
