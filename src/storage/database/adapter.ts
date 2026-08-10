import type { TenantConnection, TransactionOptions } from '@internal/database'
import { DBMigration } from '@internal/database/migrations'
import { ObjectMetadata } from '../backend'
import { Bucket, IcebergCatalog, Obj, S3MultipartUpload, S3PartUpload } from '../schemas'

export type VersioningStatus = NonNullable<Bucket['versioning_status']>

export interface SearchObjectOption {
  search?: string
  sortBy?: {
    column?: string
    order?: string
  }
  limit?: number
  offset?: number
  noncurrentVersions?: 'exclude' | 'include' | 'only'
  deleteMarkers?: 'exclude' | 'include' | 'only'
  exactMatch?: boolean
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

export interface DatabaseOptions<TNX> {
  tenantId: string
  reqId?: string
  sbReqId?: string
  latestMigration?: keyof typeof DBMigration
  host: string
  tnx?: TNX
  parentTnx?: TNX
  parentConnection?: TenantConnection
}

export interface ListBucketOptions {
  limit?: number
  offset?: number
  sortColumn?: string
  sortOrder?: string
  search?: string
}

export interface ScannerS3Key {
  key: string
  size: number
}

export interface Database {
  tenantHost: string
  tenantId: string
  reqId?: string
  sbReqId?: string
  role?: string
  latestMigration?: keyof typeof DBMigration
  connection: TenantConnection

  tenant(): { ref: string; host: string }

  hasMigration(migration: keyof typeof DBMigration): Promise<boolean>

  asSuperUser(): Database

  withTransaction<T>(
    fn: (db: Database) => Promise<T>,
    transactionOptions?: TransactionOptions
  ): Promise<T>

  testPermission<T>(fn: (db: Database) => T | Promise<T>): Promise<Awaited<T>>

  createBucket(
    data: Pick<
      Bucket,
      | 'id'
      | 'name'
      | 'public'
      | 'owner'
      | 'file_size_limit'
      | 'allowed_mime_types'
      | 'versioning_status'
    >
  ): Promise<Pick<Bucket, 'id'>>

  createAnalyticsBucket(data: Pick<Bucket, 'name'>): Promise<IcebergCatalog>

  findBucketById<Filters extends FindBucketFilters = FindObjectFilters>(
    bucketId: string,
    columns: string,
    filters?: Filters
  ): Promise<Filters['dontErrorOnEmpty'] extends true ? Bucket | undefined : Bucket>

  countObjectsInBucket(bucketId: string, limit?: number): Promise<number>

  deleteBucket(bucketId: string | string[]): Promise<number>

  listObjects(
    bucketId: string,
    columns: string,
    limit: number,
    before?: Date,
    nextToken?: string,
    nextVersion?: string
  ): Promise<Obj[]>

  listObjectsV2(
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
        afterVersion?: string
      }
      noncurrentVersions?: 'exclude' | 'include' | 'only'
      deleteMarkers?: 'exclude' | 'include' | 'only'
      exactMatch?: boolean
    }
  ): Promise<Obj[]>

  listMultipartUploads(
    bucketId: string,
    options?: {
      prefix?: string
      deltimeter?: string
      nextUploadToken?: string
      nextUploadKeyToken?: string
      maxKeys?: number
    }
  ): Promise<S3MultipartUpload[]>

  listBuckets(columns: string, options?: ListBucketOptions): Promise<Bucket[]>
  mustLockObject(bucketId: string, objectName: string, version?: string): Promise<boolean>

  waitObjectLock(
    bucketId: string,
    objectName: string,
    version?: string,
    opts?: { timeout?: number }
  ): Promise<boolean>

  updateBucket(
    bucketId: string,
    fields: Pick<Bucket, 'public' | 'file_size_limit' | 'allowed_mime_types' | 'versioning_status'>
  ): Promise<{ previous: Pick<Bucket, 'public'> } | void>

  /*
   * Three write primitives for storage.objects, one per bucket-versioning
   * mechanic. None of these know about versioning_status - deciding which
   * one applies to a given bucket is the caller's job (see
   * upsertObjectForVersioningStatus in @storage/uploader), not something the
   * database layer should be picking on the caller's behalf. id is preserved
   * across every transition (archived, revived, or inserted) except the very
   * first insert of a brand new key.
   *
   * data.is_delete_marker (default false, on the two that accept it) doubles
   * these as the write path for DELETE too: a delete marker is just a row
   * inserted/revived through this exact same archive-or-revive machinery,
   * with empty content and is_delete_marker = true instead of real bytes -
   * not a separate operation. Callers creating a delete marker pass
   * metadata/user_metadata as null (S3 delete markers carry no content) and
   * are responsible for generating a fresh version themselves, same as any
   * other write.
   */

  /**
   * For an 'ENABLED' bucket: always archive whatever's currently there (real
   * version or null version) and insert a new row with is_versioned = true.
   */
  insertObjectAndArchive(
    data: Pick<
      Obj,
      'name' | 'owner' | 'bucket_id' | 'metadata' | 'version' | 'user_metadata' | 'is_delete_marker'
    >
  ): Promise<Obj>

  /**
   * For a 'DISABLED' bucket (or a tenant that hasn't migrated at all - it
   * never references archived_at/is_versioned, so it's schema-agnostic): a
   * bucket that's never been ENABLED has at most one row for the key, ever,
   * and it's always current - plain update-in-place, or insert if the key is
   * brand new. No archiving, no lookup beyond "does a row exist". Never
   * creates a delete marker - DELETE on a never-versioned key is a real hard
   * delete, a different operation entirely (see Database.deleteObject).
   */
  upsertObject(
    data: Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata' | 'version' | 'user_metadata'>
  ): Promise<Obj>

  /**
   * For a 'SUSPENDED' bucket: there is at most one "null version"
   * (is_versioned = false) row per key, EVER - matching S3, where the null
   * version is a single slot that gets reused/revived rather than a second
   * one ever being inserted. Same upsert semantics as upsertObject above
   * (update in place, or insert if none exists yet), just scoped to the
   * null-version row instead of the current row. That row can be archived
   * (non-current) if a later 'ENABLED' write superseded it, so it has to be
   * found and un-archived (archived_at -> NULL) rather than assumed to
   * already be current; whatever else was current gets archived, unless that
   * WAS the null-version row already (nothing else to archive then). If no
   * null-version row exists yet for this key, archive whatever's current (if
   * any) and insert a new one.
   */
  upsertNullObject(
    data: Pick<
      Obj,
      'name' | 'owner' | 'bucket_id' | 'metadata' | 'version' | 'user_metadata' | 'is_delete_marker'
    >
  ): Promise<Obj>

  updateObject(
    bucketId: string,
    name: string,
    data: Pick<Obj, 'owner' | 'metadata' | 'version' | 'name' | 'bucket_id' | 'user_metadata'>
  ): Promise<Obj>
  createObject(
    data: Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata' | 'version' | 'user_metadata'>
  ): Promise<Obj>

  deleteObject(bucketId: string, objectName: string, version?: string): Promise<Obj | undefined>

  deleteObjects(bucketId: string, objectNames: string[], by: keyof Obj): Promise<Obj[]>

  deleteObjectVersions(
    bucketId: string,
    objectNames: { name: string; version: string }[]
  ): Promise<Obj[]>

  updateObjectMetadata(bucketId: string, objectName: string, metadata: ObjectMetadata): Promise<Obj>

  updateObjectOwner(bucketId: string, objectName: string, owner?: string): Promise<Obj>

  findObjects(bucketId: string, objectNames: string[], columns: string): Promise<Obj[]>

  findObjectVersions(
    bucketId: string,
    objectNames: { name: string; version: string }[],
    columns: string
  ): Promise<Obj[]>

  findObject<Filters extends FindObjectFilters = FindObjectFilters>(
    bucketId: string,
    objectName: string,
    columns: string,
    filters?: Filters
  ): Promise<Filters['dontErrorOnEmpty'] extends true ? Obj | undefined : Obj>

  findObjectVersion<Filters extends FindObjectFilters = FindObjectFilters>(
    bucketId: string,
    objectName: string,
    version: string,
    columns: string,
    filters?: Filters
  ): Promise<Filters['dontErrorOnEmpty'] extends true ? Obj | undefined : Obj>

  searchObjects(bucketId: string, prefix: string, options: SearchObjectOption): Promise<Obj[]>

  healthcheck(): Promise<void>

  destroyConnection(): void

  createMultipartUpload(
    uploadId: string,
    bucketId: string,
    objectName: string,
    version: string,
    signature: string,
    owner?: string,
    userMetadata?: Record<string, string | null>,
    metadata?: Partial<ObjectMetadata>
  ): Promise<S3MultipartUpload>

  findMultipartUpload(
    uploadId: string,
    columns: string,
    options?: { forUpdate?: boolean }
  ): Promise<S3MultipartUpload>

  updateMultipartUploadProgress(
    uploadId: string,
    progress: number,
    signature: string
  ): Promise<void>

  deleteMultipartUpload(uploadId: string): Promise<void>

  insertUploadPart(part: S3PartUpload): Promise<S3PartUpload>

  listParts(
    uploadId: string,
    options: { afterPart?: string; maxParts: number }
  ): Promise<S3PartUpload[]>

  deleteAnalyticsBucket(id: string, opts?: { soft: boolean }): Promise<IcebergCatalog>
  listAnalyticsBuckets(
    columns: string,
    options: ListBucketOptions | undefined
  ): Promise<IcebergCatalog[]>
  findAnalyticsBucketByName(name: string): Promise<IcebergCatalog>

  createS3KeysTempTable(tableName: string): Promise<void>
  dropS3KeysTempTable(tableName: string): Promise<void>
  listS3KeysFromTempTable(
    tableName: string,
    nextItem: string,
    limit: number
  ): Promise<ScannerS3Key[]>
  findS3KeysInTempTable(tableName: string, keys: string[]): Promise<Pick<ScannerS3Key, 'key'>[]>
  insertS3KeysIntoTempTable(tableName: string, keys: ScannerS3Key[]): Promise<void>
}
