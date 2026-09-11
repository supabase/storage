import type { TenantConnection, TransactionOptions } from '@internal/database'
import { DBMigration } from '@internal/database/migrations'
import { ObjectMetadata } from '../backend'
import {
  Bucket,
  BucketLifecycleConfiguration,
  BucketVersioningStatus,
  IcebergCatalog,
  LifecycleBucket,
  Obj,
  ObjectListEntry,
  ObjectListingFilterMode,
  S3MultipartUpload,
  S3PartUpload,
} from '../schemas'

export interface SearchObjectOption {
  search?: string
  sortBy?: {
    column?: string
    order?: string
  }
  limit?: number
  offset?: number
  noncurrentVersions?: ObjectListingFilterMode
  deleteMarkers?: ObjectListingFilterMode
  exactMatch?: boolean
}

export interface FindBucketFilters {
  isPublic?: boolean
  forUpdate?: boolean
  forShare?: boolean
  dontErrorOnEmpty?: boolean
}

export interface ObjectTargets {
  /** Names whose current row is targeted. */
  names: string[]
  /** Exact (name, version) rows that are targeted. */
  versions: { name: string; version: string }[]
}

export interface VersioningStatusHint {
  /**
   * Versioning status of the bucket as read under its shared status lock earlier
   * in the same transaction. When given, the write skips its own status lock and
   * read; the row stays locked until the transaction ends.
   */
  versioningStatus?: BucketVersioningStatus
}

export interface UpsertObjectOptions extends VersioningStatusHint {
  /**
   * Authorization probe: runs the write as a status-independent
   * `INSERT ... ON CONFLICT (current row) DO UPDATE` inside the caller's
   * rolled-back transaction, so RLS policies are exercised without the bucket
   * status lock, without archiving anything and without ever raising a unique
   * violation against a concurrent writer.
   */
  probe?: boolean
}

/**
 * The row a write replaced in place: the DISABLED current row, or the
 * SUSPENDED null-version row (current or archived). Its previous bytes are
 * unreferenced once the write commits and must be removed from the backend.
 */
export interface ReplacedRow {
  id: string
  /** `null` for a legacy row written before uploads carried a version. */
  version: string | null
  isDeleteMarker: boolean
}

export type WrittenObject = Obj & { replaced?: ReplacedRow }

export interface DeleteMarkerOptions {
  /** The principal deleting; recorded as the owner of any delete marker written. */
  owner?: string
}

/**
 * The backend bytes a write made unreferenced, if any: the content of the row
 * it replaced in place. Delete markers own no bytes and are skipped.
 */
export function replacedContent(written: WrittenObject): { version: string | null } | undefined {
  const replaced = written.replaced
  if (!replaced || replaced.isDeleteMarker || replaced.version === written.version) {
    return undefined
  }
  return { version: replaced.version }
}

export interface ObjectLockKey {
  bucketId: string
  objectName: string
  version?: string
}

export interface FindObjectFilters {
  forUpdate?: boolean
  forShare?: boolean
  forKeyShare?: boolean
  noWait?: boolean
  dontErrorOnEmpty?: boolean
  excludeDeleteMarkers?: boolean
  includeNoncurrent?: boolean
  isVersioned?: boolean
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

  findBucketById<Filters extends FindBucketFilters = FindBucketFilters>(
    bucketId: string,
    columns: string,
    filters?: Filters
  ): Promise<Filters['dontErrorOnEmpty'] extends true ? Bucket | undefined : Bucket>

  /**
   * Finds several buckets in one statement, in ascending id order (which is
   * also the lock order for forShare/forUpdate reads).
   */
  findBucketsById(
    bucketIds: string[],
    columns: string,
    filters?: FindBucketFilters
  ): Promise<Bucket[]>

  findLifecycleBucket(bucketId: string): Promise<LifecycleBucket>

  putLifecycleConfiguration(
    bucketId: string,
    configuration: BucketLifecycleConfiguration
  ): Promise<LifecycleBucket>

  deleteLifecycleConfiguration(bucketId: string): Promise<LifecycleBucket>

  countObjectsInBucket(bucketId: string, limit?: number): Promise<number>

  deleteBucket(bucketId: string | string[]): Promise<number>

  listObjects(
    bucketId: string,
    columns: string,
    limit: number,
    before?: Date,
    nextToken?: string,
    nextTokenVersion?: string | null,
    filters?: {
      noncurrentVersions?: ObjectListingFilterMode
      deleteMarkers?: ObjectListingFilterMode
    }
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
        afterArchivedAt?: string
      }
      noncurrentVersions?: ObjectListingFilterMode
      deleteMarkers?: ObjectListingFilterMode
      exactMatch?: boolean
    }
  ): Promise<ObjectListEntry[]>

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

  /**
   * Acquires every advisory lock in one round trip, in a deterministic order.
   */
  waitObjectLocks(keys: ObjectLockKey[], opts?: { timeout?: number }): Promise<boolean>

  updateBucket(
    bucketId: string,
    fields: Pick<Bucket, 'public' | 'file_size_limit' | 'allowed_mime_types' | 'versioning_status'>
  ): Promise<{ previous: Pick<Bucket, 'public'> } | void>

  upsertObject(
    data: Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata' | 'version' | 'user_metadata'>,
    options?: UpsertObjectOptions
  ): Promise<WrittenObject>

  updateObject(
    bucketId: string,
    name: string,
    data: Pick<Obj, 'owner' | 'metadata' | 'version' | 'name' | 'bucket_id' | 'user_metadata'>,
    currentVersion?: string
  ): Promise<Obj>
  createObject(
    data: Pick<Obj, 'name' | 'owner' | 'bucket_id' | 'metadata' | 'version' | 'user_metadata'>
  ): Promise<Obj>

  /**
   * `owner` is the principal performing the delete. A delete without a
   * version on a versioned bucket writes a delete marker, and that marker is
   * a row of its own: it carries the owner like an uploaded row does, so
   * owner-scoped policies keep applying to it.
   */
  deleteObject(
    bucketId: string,
    objectName: string,
    version?: string | null,
    options?: DeleteMarkerOptions & { skipPromotion?: boolean } & VersioningStatusHint
  ): Promise<WrittenObject | undefined>

  deleteObjects(
    bucketId: string,
    objectNames: string[],
    by: keyof Obj,
    options?: DeleteMarkerOptions & { skipDeleteMarkers?: boolean } & VersioningStatusHint
  ): Promise<WrittenObject[]>

  deleteObjectVersions(
    bucketId: string,
    objectNames: { name: string; version: string }[],
    options?: { skipPromotion?: boolean }
  ): Promise<Obj[]>

  updateObjectOwner(bucketId: string, objectName: string, owner?: string): Promise<Obj>

  findObjects(
    bucketId: string,
    objectNames: string[],
    columns: string,
    filters?: FindObjectFilters
  ): Promise<Obj[]>

  /**
   * Current rows for the given names plus the exact rows for the given
   * (name, version) pairs, in one statement ordered by (name, version).
   */
  findObjectTargets(
    bucketId: string,
    targets: ObjectTargets,
    columns?: string,
    filters?: FindObjectFilters
  ): Promise<Obj[]>

  findObjectVersions(
    bucketId: string,
    objectNames: { name: string; version: string }[],
    columns?: string,
    filters?: FindObjectFilters
  ): Promise<Obj[]>

  findObject<Filters extends FindObjectFilters = FindObjectFilters>(
    bucketId: string,
    objectName: string,
    columns: string,
    filters?: Filters,
    version?: string | null
  ): Promise<Filters['dontErrorOnEmpty'] extends true ? Obj | undefined : Obj>

  searchObjects(
    bucketId: string,
    prefix: string,
    options: SearchObjectOption
  ): Promise<ObjectListEntry[]>

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
