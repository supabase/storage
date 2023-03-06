import { PostgrestClient } from '@supabase/postgrest-js'
import { Bucket, Obj } from './schemas'
import { DatabaseError } from './errors'
import { ObjectMetadata } from './backend'

export interface DatabaseOptions {
  tenantId: string
  host: string
  superAdmin?: PostgrestClient
}

export interface SearchObjectOption {
  search?: string
  sortBy?: {
    column?: string
    order?: string
  }
  limit?: number
  offset?: number
}

export interface FindBucketFilters {
  isPublic?: boolean
}

/**
 * Database
 * the only source of truth for interacting with the storage database
 */
export class Database {
  public readonly host: string
  public readonly tenantId: string

  constructor(
    private readonly postgrest: PostgrestClient,
    private readonly options: DatabaseOptions
  ) {
    this.host = options.host
    this.tenantId = options.tenantId
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
    return new Database(this.options.superAdmin, this.options)
  }

  async createBucket(
    data: Pick<
      Bucket,
      'id' | 'name' | 'public' | 'owner' | 'file_size_limit' | 'allowed_mime_types'
    >
  ) {
    const {
      data: results,
      error,
      status,
    } = await this.postgrest
      .from<Bucket>('buckets')
      .insert(
        [
          {
            id: data.id,
            name: data.name,
            owner: data.owner,
            public: data.public,
            allowed_mime_types: data.allowed_mime_types,
            file_size_limit: data.file_size_limit,
          },
        ],
        {
          returning: 'minimal',
        }
      )
      .single()

    if (error) {
      throw new DatabaseError('failed to create bucket', status, error)
    }

    return results as Bucket
  }

  async findBucketById(bucketId: string, columns = 'id', filters?: FindBucketFilters) {
    const query = this.postgrest.from<Bucket>('buckets').select(columns).eq('id', bucketId)

    if (typeof filters?.isPublic !== 'undefined') {
      query.eq('public', filters.isPublic)
    }

    const { data, error, status } = await query.single()

    if (error) {
      throw new DatabaseError('failed to retrieve bucket', status, error, {
        bucketId,
      })
    }

    return data as Bucket
  }

  async countObjectsInBucket(bucketId: string) {
    const { count, error, status } = await this.postgrest
      .from<Obj>('objects')
      .select('id', { count: 'exact' })
      .eq('bucket_id', bucketId)
      .limit(10)

    if (error) {
      throw new DatabaseError('failed to count objects in bucket', status, error, {
        bucketId,
      })
    }

    return count as number
  }

  async deleteBucket(bucketId: string | string[]) {
    const { data, error, status } = await this.postgrest
      .from<Bucket>('buckets')
      .delete()
      .in('id', Array.isArray(bucketId) ? bucketId : [bucketId])

    if (error) {
      throw new DatabaseError('failed to delete bucket', status, error, {
        bucketId,
      })
    }

    return data as Bucket[]
  }

  async listObjects(bucketId: string, columns = 'id', limit = 10) {
    const { data, error, status } = await this.postgrest
      .from<Obj>('objects')
      .select(columns)
      .eq('bucket_id', bucketId)
      .limit(limit)

    if (error) {
      throw new DatabaseError('failed listing objects', status, error, {
        bucketId,
      })
    }

    return data as Obj[]
  }

  async listBuckets(columns = 'id') {
    const { data, error, status } = await this.postgrest.from<Bucket>('buckets').select(columns)

    if (error) {
      throw new DatabaseError('failed listing buckets', status, error)
    }

    return data as Bucket[]
  }

  async updateBucket(
    bucketId: string,
    fields: Pick<Bucket, 'public' | 'file_size_limit' | 'allowed_mime_types'>
  ) {
    const { error, status, data } = await this.postgrest
      .from<Bucket>('buckets')
      .update({
        public: fields.public,
        file_size_limit: fields.file_size_limit,
        allowed_mime_types: fields.allowed_mime_types === null ? [] : fields.allowed_mime_types,
      })
      .match({ id: bucketId })
      .single()

    if (error) {
      throw new DatabaseError('failed updating bucket', status, error, {
        bucketId,
      })
    }

    return data as Bucket
  }

  async upsertObject(data: Pick<Obj, 'name' | 'owner' | 'bucket_id'>) {
    const {
      error,
      status,
      data: result,
    } = await this.postgrest
      .from<Obj>('objects')
      .upsert(
        [
          {
            name: data.name,
            owner: data.owner,
            bucket_id: data.bucket_id,
          },
        ],
        {
          onConflict: 'name, bucket_id',
          returning: 'minimal',
        }
      )
      .single()

    if (error) {
      throw new DatabaseError('failed upserting object', status, error, {
        bucketId: data.bucket_id,
        name: data.name,
      })
    }

    return result as Obj
  }

  async createObject(data: Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata'>) {
    const { error, status } = await this.postgrest
      .from<Obj>('objects')
      .insert(
        [
          {
            name: data.name,
            owner: data.owner,
            bucket_id: data.bucket_id,
            metadata: data.metadata,
          },
        ],
        {
          returning: 'minimal',
        }
      )
      .single()

    if (error) {
      throw new DatabaseError('failed inserting object', status, error, {
        bucketId: data.bucket_id,
        name: data.name,
      })
    }

    return null
  }

  async deleteObject(bucketId: string, objectName: string) {
    const { error, status, data } = await this.postgrest
      .from<Obj>('objects')
      .delete()
      .match({
        name: objectName,
        bucket_id: bucketId,
      })
      .single()

    if (error) {
      throw new DatabaseError('failed deleting object', status, error, {
        bucketId: bucketId,
        name: objectName,
      })
    }

    return data as Obj
  }

  async deleteObjects(bucketId: string, objectNames: string[], by: keyof Obj = 'name') {
    const { error, status, data } = await this.postgrest
      .from<Obj>('objects')
      .delete()
      .eq('bucket_id', bucketId)
      .in(by, objectNames)

    if (error) {
      throw new DatabaseError('failed deleting object', status, error, {
        bucketId: bucketId,
        names: objectNames,
      })
    }

    return data as Obj[]
  }

  async updateObjectMetadata(bucketId: string, objectName: string, metadata: ObjectMetadata) {
    const { error, status, data } = await this.postgrest
      .from<Obj>('objects')
      .update({
        metadata,
      })
      .match({ bucket_id: bucketId, name: objectName })
      .single()

    if (error) {
      throw new DatabaseError('failed updating object metadata', status, error, {
        bucketId: bucketId,
        name: objectName,
      })
    }

    return data as Obj
  }

  async updateObjectOwner(bucketId: string, objectName: string, owner?: string) {
    const { error, status, data } = await this.postgrest
      .from<Obj>('objects')
      .update({
        last_accessed_at: new Date().toISOString(),
        owner,
      })
      .match({ bucket_id: bucketId, name: objectName })
      .single()

    if (error) {
      throw new DatabaseError('failed updating object owner', status, error, {
        bucketId: bucketId,
        name: objectName,
      })
    }

    return data as Obj
  }

  async updateObjectName(bucketId: string, sourceKey: string, destinationKey: string) {
    const { error, status, data } = await this.postgrest
      .from<Obj>('objects')
      .update({
        last_accessed_at: new Date().toISOString(),
        name: destinationKey,
      })
      .match({ bucket_id: bucketId, name: sourceKey })
      .single()

    if (error) {
      throw new DatabaseError('failed updating object name', status, error, {
        bucketId: bucketId,
        name: sourceKey,
      })
    }

    return data as Obj
  }

  async findObject(bucketId: string, objectName: string, columns = 'id') {
    const { error, status, data } = await this.postgrest
      .from<Obj>('objects')
      .select(columns)
      .match({
        name: objectName,
        bucket_id: bucketId,
      })
      .single()

    if (error) {
      throw new DatabaseError('failed finding object', status, error, {
        bucketId: bucketId,
        name: objectName,
      })
    }

    return data as Obj
  }

  async findObjects(bucketId: string, objectNames: string[], columns = 'id') {
    const { error, status, data } = await this.postgrest
      .from<Obj>('objects')
      .select('name')
      .eq('bucket_id', bucketId)
      .in('name', objectNames)

    if (error) {
      throw new DatabaseError('failed finding objects', status, error, {
        bucketId: bucketId,
      })
    }

    return data as Obj[]
  }

  async searchObjects(bucketId: string, prefix: string, options: SearchObjectOption) {
    const { data, error, status } = await this.postgrest.rpc<Obj>('search', {
      prefix,
      bucketname: bucketId,
      limits: options.limit,
      offsets: options.offset,
      levels: prefix.split('/').length,
      search: options.search,
      sortcolumn: options.sortBy?.column ?? 'name',
      sortorder: options.sortBy?.order ?? 'asc',
    })

    if (error) {
      throw new DatabaseError('failed listing objects', status, error, {
        bucketId: bucketId,
      })
    }

    return data as Obj[]
  }

  async canInsertObject(data: Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata'>) {
    const { error, status } = await this.postgrest.rpc<void>('can_insert_object', {
      bucketid: data.bucket_id,
      name: data.name,
      owner: data.owner ?? null,
      metadata: data.metadata,
    })

    if (error) {
      throw new DatabaseError('cannot insert object', status, error, {
        bucketId: data.bucket_id,
        name: data.name,
      })
    }

    return true
  }
}
