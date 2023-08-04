import { Bucket, Obj, BucketWithCredentials, Credential } from '../schemas'
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
  includeCredentials?: boolean
}

export interface FindObjectFilters {
  forUpdate?: boolean
  forShare?: boolean
  forKeyShare?: boolean
  noWait?: boolean
  dontErrorOnEmpty?: boolean
}

export interface TransactionOptions {
  retry?: number
  readOnly?: boolean
}

export interface DatabaseOptions<TNX> {
  tenantId: string
  host: string
  tnx?: TNX
  parentTnx?: TNX
  parentConnection?: TenantConnection
}

type MaybeType<Condition, T> = Condition extends true ? T | undefined : T

export interface Database {
  tenantHost: string
  tenantId: string
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
      | 'id'
      | 'name'
      | 'public'
      | 'owner'
      | 'file_size_limit'
      | 'allowed_mime_types'
      | 'credential_id'
    >
  ): Promise<Pick<Bucket, 'id'>>

  findBucketById<Filters extends FindBucketFilters = FindBucketFilters>(
    bucketId: string,
    columns: string,
    filters?: Filters
  ): Promise<
    Filters['dontErrorOnEmpty'] extends true
      ? Filters['includeCredentials'] extends true
        ? BucketWithCredentials | undefined
        : Bucket | undefined
      : Filters['includeCredentials'] extends true
      ? BucketWithCredentials
      : Bucket
  >

  listBucketByExternalCredential(credentialId: string, columns: string): Promise<Bucket[]>

  countObjectsInBucket(bucketId: string): Promise<number>

  deleteBucket(bucketId: string | string[]): Promise<Bucket[]>

  listObjects(bucketId: string, columns: string, limit: number): Promise<Obj[]>

  listBuckets(columns: string): Promise<Bucket[]>
  mustLockObject(bucketId: string, objectName: string, version?: string): Promise<boolean>
  waitObjectLock(bucketId: string, objectName: string, version?: string): Promise<boolean>

  updateBucket(
    bucketId: string,
    fields: Pick<Bucket, 'public' | 'file_size_limit' | 'allowed_mime_types' | 'credential_id'>
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

  listCredentials(): Promise<Pick<Credential, 'id' | 'name'>[]>
  createCredential(credential: Omit<Credential, 'id'>): Promise<Pick<Credential, 'id'>>
  deleteCredential(credentialId: string): Promise<Pick<Credential, 'id'>>
}
