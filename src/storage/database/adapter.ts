import { Bucket, Obj } from '../schemas'
import { ObjectMetadata } from '../backend'
import { TenantConnection } from '../../database/connection'

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
  forUpdate?: boolean
  forShare?: boolean
  dontErrorOnEmpty?: boolean
}

export interface FindObjectFilters {
  forUpdate?: boolean
  forShare?: boolean
  forKeyShare?: boolean
  noWait?: boolean
  dontErrorOnEmpty?: boolean
}

export interface TransactionOptions {
  isolation?: string
  retry?: number
  readOnly?: boolean
}

export interface DatabaseOptions<TNX> {
  tenantId: string
  reqId?: string
  host: string
  tnx?: TNX
  parentTnx?: TNX
  parentConnection?: TenantConnection
}

export interface Database {
  tenantHost: string
  tenantId: string
  reqId?: string
  role?: string

  tenant(): { ref: string; host: string }

  asSuperUser(): Database

  withTransaction<T extends (db: Database) => Promise<any>>(
    fn: T,
    transactionOptions?: TransactionOptions
  ): Promise<ReturnType<T>>

  testPermission<T extends (db: Database) => any>(fn: T): Promise<Awaited<ReturnType<T>>>

  createBucket(
    data: Pick<
      Bucket,
      'id' | 'name' | 'public' | 'owner' | 'file_size_limit' | 'allowed_mime_types'
    >
  ): Promise<Pick<Bucket, 'id'>>

  findBucketById<Filters extends FindBucketFilters = FindObjectFilters>(
    bucketId: string,
    columns: string,
    filters?: Filters
  ): Promise<Filters['dontErrorOnEmpty'] extends true ? Bucket | undefined : Bucket>

  countObjectsInBucket(bucketId: string): Promise<number>

  deleteBucket(bucketId: string | string[]): Promise<Bucket[]>

  listObjects(bucketId: string, columns: string, limit: number): Promise<Obj[]>

  listBuckets(columns: string): Promise<Bucket[]>
  mustLockObject(bucketId: string, objectName: string, version?: string): Promise<boolean>
  waitObjectLock(bucketId: string, objectName: string, version?: string): Promise<boolean>

  updateBucket(
    bucketId: string,
    fields: Pick<Bucket, 'public' | 'file_size_limit' | 'allowed_mime_types'>
  ): Promise<void>

  upsertObject(
    data: Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata' | 'version'>
  ): Promise<Obj>
  updateObject(
    bucketId: string,
    name: string,
    data: Pick<Obj, 'owner' | 'metadata' | 'version' | 'name'>
  ): Promise<Obj>

  createObject(
    data: Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata' | 'version'>
  ): Promise<Obj>

  deleteObject(bucketId: string, objectName: string, version?: string): Promise<Obj | undefined>

  deleteObjects(bucketId: string, objectNames: string[], by: keyof Obj): Promise<Obj[]>

  updateObjectMetadata(bucketId: string, objectName: string, metadata: ObjectMetadata): Promise<Obj>

  updateObjectOwner(bucketId: string, objectName: string, owner?: string): Promise<Obj>

  findObjects(bucketId: string, objectNames: string[], columns: string): Promise<Obj[]>
  findObject<Filters extends FindObjectFilters = FindObjectFilters>(
    bucketId: string,
    objectName: string,
    columns: string,
    filters?: Filters
  ): Promise<Filters['dontErrorOnEmpty'] extends true ? Obj | undefined : Obj>

  searchObjects(bucketId: string, prefix: string, options: SearchObjectOption): Promise<Obj[]>
  healthcheck(): Promise<void>

  destroyConnection(): Promise<void>
}
