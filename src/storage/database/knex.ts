import { Bucket, BucketWithCredentials, Credential, Obj } from '../schemas'
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
import { encrypt } from '../../auth'

/**
 * Database
 * the only source of truth for interacting with the storage database
 */
export class StorageKnexDB implements Database {
  public readonly tenantHost: string
  public readonly tenantId: string
  public readonly role?: string

  constructor(
    public readonly connection: TenantConnection,
    private readonly options: DatabaseOptions<Knex.Transaction>
  ) {
    this.tenantHost = options.host
    this.tenantId = options.tenantId
    this.role = connection?.role
  }

  async withTransaction<T extends (db: Database) => Promise<any>>(
    fn: T,
    transactionOptions?: TransactionOptions
  ) {
    try {
      const tnx = await this.connection.transaction(this.options.tnx)()

      try {
        await this.connection.setScope(tnx)

        tnx.on('query-error', (error) => {
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
      }
    } catch (e) {
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
      | 'id'
      | 'name'
      | 'public'
      | 'owner'
      | 'file_size_limit'
      | 'allowed_mime_types'
      | 'credential_id'
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
      credential_id: data.credential_id,
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

  async findBucketById<Filters extends FindBucketFilters = FindBucketFilters>(
    bucketId: string,
    columns = 'id',
    filters?: Filters
  ) {
    const result = await this.runQuery('FindBucketById', async (knex) => {
      let fields = columns.split(',')

      if (filters?.includeCredentials) {
        fields = fields.map((field) => {
          if (field.startsWith('buckets.')) {
            return field
          }
          return `buckets.${field}`
        })

        fields.push('bucket_credentials.access_key')
        fields.push('bucket_credentials.secret_key')
        fields.push('bucket_credentials.region')
        fields.push('bucket_credentials.role')
        fields.push('bucket_credentials.force_path_style')
        fields.push('bucket_credentials.endpoint')
      }

      const query = knex.from<Bucket>('buckets').select(fields).where('buckets.id', bucketId)

      if (typeof filters?.isPublic !== 'undefined') {
        query.where('public', filters.isPublic)
      }

      if (filters?.forUpdate) {
        query.forUpdate()
      }

      if (filters?.forShare) {
        query.forShare()
      }

      if (filters?.includeCredentials) {
        query.leftJoin('bucket_credentials', 'bucket_credentials.id', 'buckets.credential_id')
      }

      return query.first() as Promise<
        Filters['includeCredentials'] extends true ? BucketWithCredentials : Bucket
      >
    })

    if (!result && !filters?.dontErrorOnEmpty) {
      throw new DBError('Bucket not found', 404, 'Bucket not found', undefined, {
        bucketId,
      })
    }

    return result
  }

  async listBucketByExternalCredential(credentialId: string, columns = 'id') {
    return this.runQuery('FindBucketByExternalCredentialId', async (knex) => {
      return knex
        .from<Bucket>('buckets')
        .select(columns.split(','))
        .where('credential_id', credentialId) as Promise<Bucket[]>
    })
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
    fields: Pick<Bucket, 'public' | 'file_size_limit' | 'allowed_mime_types' | 'credential_id'>
  ) {
    const bucket = await this.runQuery('UpdateBucket', (knex) => {
      return knex.from('buckets').where('id', bucketId).update({
        public: fields.public,
        file_size_limit: fields.file_size_limit,
        allowed_mime_types: fields.allowed_mime_types,
        credential_id: fields.credential_id,
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

  async findObject<Filters extends FindObjectFilters = FindObjectFilters>(
    bucketId: string,
    objectName: string,
    columns = 'id',
    filters?: Filters
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
      ? Filters['dontErrorOnEmpty'] extends true
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

  async listCredentials() {
    return this.runQuery('CreateCredential', async (knex) => {
      const result = await knex<Credential>('bucket_credentials').select('id', 'name')
      return result as Pick<Credential, 'id' | 'name'>[]
    })
  }

  async createCredential(credential: Omit<Credential, 'id'>) {
    return this.runQuery('CreateCredential', async (knex) => {
      const [result] = await knex<Credential>('bucket_credentials')
        .insert({
          name: credential.name,
          access_key: credential.access_key ? encrypt(credential.access_key) : undefined,
          secret_key: credential.secret_key ? encrypt(credential.secret_key) : undefined,
          role: credential.role,
          endpoint: credential.endpoint,
          region: credential.region,
          force_path_style: Boolean(credential.force_path_style),
        })
        .returning('id')

      return result
    })
  }

  async deleteCredential(credentialId: string) {
    return this.runQuery('CreateCredential', async (knex) => {
      const [result] = await knex<Credential>('bucket_credentials')
        .where({ id: credentialId })
        .delete()
        .returning('id')

      if (!result) {
        throw new StorageBackendError('Credential not found', 404, 'not_found')
      }

      return result
    })
  }

  protected async runQuery<T extends (db: Knex.Transaction) => Promise<any>>(
    queryName: string,
    fn: T
  ): Promise<Awaited<ReturnType<T>>> {
    const timer = DbQueryPerformance.startTimer({
      name: queryName,
      tenant_id: this.options.tenantId,
    })

    let tnx = this.options.tnx

    const differentScopes = Boolean(
      this.options.parentConnection?.role &&
        this.connection.role !== this.options.parentConnection?.role
    )
    const needsNewTransaction = !tnx || differentScopes

    if (!tnx || needsNewTransaction) {
      tnx = await this.connection.transaction(this.options.tnx)()
      tnx.on('query-error', (error: DatabaseError) => {
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
