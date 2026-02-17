import { Bucket, S3MultipartUpload, Obj, S3PartUpload, IcebergCatalog } from '../schemas'
import { ObjectMetadata } from '../backend'
import { TenantConnection } from '@internal/database'
import { DBMigration } from '@internal/database/migrations'
import { EventTransaction } from '@internal/queue/event-transaction'

export interface Cancellable {
  signal?: AbortSignal
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
  timeout?: number
  signal?: AbortSignal
}

export interface DatabaseOptions<TNX> {
  tenantId: string
  reqId?: string
  latestMigration?: keyof typeof DBMigration
  host: string
  tnx?: TNX
  parentTnx?: TNX
  parentConnection?: TenantConnection
  signal?: AbortSignal
}

export interface ListBucketOptions {
  limit?: number
  offset?: number
  sortColumn?: string
  sortOrder?: string
  search?: string
}

// --- Database method input interfaces ---

export interface FindBucketByIdInput extends Cancellable {
  bucketId: string
  columns?: string
  filters?: FindBucketFilters
}

export interface CountObjectsInBucketInput extends Cancellable {
  bucketId: string
  limit?: number
}

export interface DbDeleteBucketInput extends Cancellable {
  bucketId: string | string[]
}

export interface ListObjectsInput extends Cancellable {
  bucketId: string
  columns?: string
  limit?: number
  before?: Date
  nextToken?: string
}

export interface ListObjectsV2Input extends Cancellable {
  bucketId: string
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
}

export interface ListMultipartUploadsInput extends Cancellable {
  bucketId: string
  options?: {
    prefix?: string
    deltimeter?: string
    nextUploadToken?: string
    nextUploadKeyToken?: string
    maxKeys?: number
  }
}

export interface DbListBucketsInput extends Cancellable {
  columns?: string
  options?: ListBucketOptions
}

export interface MustLockObjectInput extends Cancellable {
  bucketId: string
  objectName: string
  version?: string
}

export interface WaitObjectLockInput extends Cancellable {
  bucketId: string
  objectName: string
  version?: string
  timeout?: number
}

export interface DbUpdateBucketInput extends Cancellable {
  bucketId: string
  fields: Pick<Bucket, 'public' | 'file_size_limit' | 'allowed_mime_types'>
}

export interface UpdateObjectInput extends Cancellable {
  bucketId: string
  name: string
  data: Pick<Obj, 'owner' | 'metadata' | 'version' | 'name' | 'bucket_id' | 'user_metadata'>
}

export interface DeleteObjectInput extends Cancellable {
  bucketId: string
  objectName: string
  version?: string
}

export interface DeleteObjectsInput extends Cancellable {
  bucketId: string
  objectNames: string[]
  by?: keyof Obj
}

export interface DeleteObjectVersionsInput extends Cancellable {
  bucketId: string
  objectNames: { name: string; version: string }[]
}

export interface UpdateObjectMetadataInput extends Cancellable {
  bucketId: string
  objectName: string
  metadata: ObjectMetadata
}

export interface UpdateObjectOwnerInput extends Cancellable {
  bucketId: string
  objectName: string
  owner?: string
}

export interface FindObjectsInput extends Cancellable {
  bucketId: string
  objectNames: string[]
  columns?: string
}

export interface FindObjectVersionsInput extends Cancellable {
  bucketId: string
  objectNames: { name: string; version: string }[]
  columns?: string
}

export interface FindObjectInput extends Cancellable {
  bucketId: string
  objectName: string
  columns?: string
  filters?: FindObjectFilters
}

export interface SearchObjectsInput extends Cancellable {
  bucketId: string
  prefix: string
  options: SearchObjectOption
}

export interface CreateMultipartUploadInput extends Cancellable {
  uploadId: string
  bucketId: string
  objectName: string
  version: string
  signature: string
  owner?: string
  metadata?: Record<string, string | null>
}

export interface FindMultipartUploadInput extends Cancellable {
  uploadId: string
  columns?: string
  options?: { forUpdate?: boolean }
}

export interface UpdateMultipartUploadProgressInput extends Cancellable {
  uploadId: string
  progress: number
  signature: string
}

export interface DeleteMultipartUploadInput extends Cancellable {
  uploadId: string
}

export interface ListPartsInput extends Cancellable {
  uploadId: string
  options: { afterPart?: string; maxParts: number }
}

export interface DeleteAnalyticsBucketInput extends Cancellable {
  id: string
  opts?: { soft: boolean }
}

export interface ListAnalyticsBucketsInput extends Cancellable {
  columns?: string
  options?: ListBucketOptions
}

export interface FindAnalyticsBucketByNameInput extends Cancellable {
  name: string
}

export type CreateBucketInput = Cancellable &
  Pick<Bucket, 'id' | 'name' | 'public' | 'owner' | 'file_size_limit' | 'allowed_mime_types'>

export type UpsertObjectInput = Cancellable &
  Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata' | 'version' | 'user_metadata'>

export type CreateObjectInput = Cancellable &
  Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata' | 'version' | 'user_metadata'>

export type InsertUploadPartInput = Cancellable & S3PartUpload

export type CreateAnalyticsBucketInput = Cancellable & Pick<Bucket, 'name'>

export interface Database {
  tenantHost: string
  tenantId: string
  reqId?: string
  role?: string
  connection: TenantConnection

  tenant(): { ref: string; host: string }

  asSuperUser(): Database

  get eventTransaction(): EventTransaction

  withTransaction<T extends (db: Database) => Promise<any>>(
    fn: T,
    transactionOptions?: TransactionOptions
  ): Promise<ReturnType<T>>

  testPermission<T extends (db: Database) => any>(
    fn: T,
    opts?: { signal?: AbortSignal }
  ): Promise<Awaited<ReturnType<T>>>

  createBucket(input: CreateBucketInput): Promise<Pick<Bucket, 'id'>>

  createAnalyticsBucket(input: CreateAnalyticsBucketInput): Promise<IcebergCatalog>

  findBucketById<Filters extends FindBucketFilters = FindObjectFilters>(
    input: FindBucketByIdInput & { filters?: Filters }
  ): Promise<Filters['dontErrorOnEmpty'] extends true ? Bucket | undefined : Bucket>

  countObjectsInBucket(input: CountObjectsInBucketInput): Promise<number>

  deleteBucket(input: DbDeleteBucketInput): Promise<number>

  listObjects(input: ListObjectsInput): Promise<Obj[]>

  listObjectsV2(input: ListObjectsV2Input): Promise<Obj[]>

  listMultipartUploads(input: ListMultipartUploadsInput): Promise<S3MultipartUpload[]>

  listBuckets(input: DbListBucketsInput): Promise<Bucket[]>
  mustLockObject(input: MustLockObjectInput): Promise<boolean>

  waitObjectLock(input: WaitObjectLockInput): Promise<boolean>

  updateBucket(input: DbUpdateBucketInput): Promise<void>

  upsertObject(input: UpsertObjectInput): Promise<Obj>

  updateObject(input: UpdateObjectInput): Promise<Obj>

  createObject(input: CreateObjectInput): Promise<Obj>

  deleteObject(input: DeleteObjectInput): Promise<Obj | undefined>

  deleteObjects(input: DeleteObjectsInput): Promise<Obj[]>

  deleteObjectVersions(input: DeleteObjectVersionsInput): Promise<Obj[]>

  updateObjectMetadata(input: UpdateObjectMetadataInput): Promise<Obj>

  updateObjectOwner(input: UpdateObjectOwnerInput): Promise<Obj>

  findObjects(input: FindObjectsInput): Promise<Obj[]>

  findObjectVersions(input: FindObjectVersionsInput): Promise<Obj[]>

  findObject<Filters extends FindObjectFilters = FindObjectFilters>(
    input: FindObjectInput & { filters?: Filters }
  ): Promise<Filters['dontErrorOnEmpty'] extends true ? Obj | undefined : Obj>

  searchObjects(input: SearchObjectsInput): Promise<Obj[]>

  healthcheck(): Promise<void>

  destroyConnection(): Promise<void>

  createMultipartUpload(input: CreateMultipartUploadInput): Promise<S3MultipartUpload>

  findMultipartUpload(input: FindMultipartUploadInput): Promise<S3MultipartUpload>

  updateMultipartUploadProgress(input: UpdateMultipartUploadProgressInput): Promise<void>

  deleteMultipartUpload(input: DeleteMultipartUploadInput): Promise<void>

  insertUploadPart(input: InsertUploadPartInput): Promise<S3PartUpload>

  listParts(input: ListPartsInput): Promise<S3PartUpload[]>

  deleteAnalyticsBucket(input: DeleteAnalyticsBucketInput): Promise<IcebergCatalog>
  listAnalyticsBuckets(input: ListAnalyticsBucketsInput): Promise<IcebergCatalog[]>
  findAnalyticsBucketByName(input: FindAnalyticsBucketByNameInput): Promise<IcebergCatalog>
}
