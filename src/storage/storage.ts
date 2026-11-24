import { createLruCache } from '@internal/cache'
import { tenantHasFeature } from '@internal/database'
import { ERRORS, StorageBackendError } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import { StorageObjectLocator } from '@storage/locator'
import { InfoRenderer } from '@storage/renderer/info'
import { getConfig } from '../config'
import { StorageBackendAdapter } from './backend'
import { Database, FindBucketFilters, ListBucketOptions } from './database'
import {
  BucketCreatedEvent,
  BucketDeleted,
  getStorageQueue,
  ObjectAdminDeleteAllBefore,
  PurgeCdnCache,
} from './events'
import { wakeLifecycleTenant } from './lifecycle/control-plane'
import { assertTenantLifecycleApiEnabled } from './lifecycle/guards'
import {
  BucketType,
  getFileSizeLimit,
  isValidBucketName,
  isValidKey,
  mustBeNotReservedBucketName,
  mustBeValidBucketName,
  parseFileSizeToBytes,
} from './limits'
import { ObjectStorage } from './object'
import { AssetRenderer, HeadRenderer, ImageRenderer } from './renderer'
import type { Bucket, BucketLifecycleConfiguration, Obj } from './schemas'
import { normalizeAllowedMimeTypes } from './validators/mime-type'
import { assertVersioningEnabled } from './versioning/guards'

const { emptyBucketMax } = getConfig()

function assertNever(value: never): never {
  throw new Error(`Unexpected renderer type: ${String(value)}`)
}

export interface BucketObjectLookup {
  bucket: Bucket | undefined
  findObject(): Promise<Obj>
}

// The public flag of the buckets findBucketAndObject last saw (by tenant and id), to
// decide whether to read the object together with the bucket, and with which scope.
// The least recently used are dropped first.
const seenBuckets = createLruCache<string, boolean>({ max: 10_000 })

function rememberBucket(key: string, bucket: Bucket | undefined): void {
  if (bucket) {
    seenBuckets.set(key, Boolean(bucket.public))
  } else {
    seenBuckets.delete(key)
  }
}

/**
 * Storage
 * interacts with the storage backend of choice and the database
 * to provide a rich management API for any folders and files operations
 */
export class Storage {
  constructor(
    public readonly backend: StorageBackendAdapter,
    public readonly db: Database,
    public readonly location: StorageObjectLocator
  ) {}

  /**
   * Access object related functionality on a specific bucket
   * @param bucketId
   */
  from(bucketId: string) {
    mustBeValidBucketName(bucketId)

    return new ObjectStorage(this.backend, this.db, this.location, bucketId)
  }

  /**
   * Impersonate any subsequent chained operations
   * as superUser bypassing RLS rules
   */
  asSuperUser() {
    return new Storage(this.backend, this.db.asSuperUser(), this.location)
  }

  /**
   * Creates a renderer type
   * @param type
   */
  renderer(type: 'asset' | 'head' | 'image' | 'info') {
    switch (type) {
      case 'asset':
        return new AssetRenderer(this.backend)
      case 'head':
        return new HeadRenderer()
      case 'image':
        return new ImageRenderer(this.backend)
      case 'info':
        return new InfoRenderer()
      default:
        return assertNever(type)
    }
  }

  /**
   * Find a bucket by id
   * @param id
   * @param columns
   * @param filters
   */
  findBucket(id: string, columns = 'id', filters?: FindBucketFilters) {
    return this.db.findBucketById(id, columns, filters)
  }

  /**
   * The reads behind serving an object: `bucket` is the bucket's id and public flag, read
   * as the super user (undefined when there is none), and `findObject()` returns the object
   * as ObjectStorage.findObject would (excluding delete markers, NoSuchKey when missing),
   * read as the super user when the bucket is public or `objectAsSuperUser` is set and with
   * this storage's scope otherwise. Callers check the bucket before calling findObject();
   * `publicBucketOnly` says that they reject a bucket that is not public.
   *
   * That is what asSuperUser().findBucket() followed by findObject() gives, but where the
   * database allows it both reads share one round trip. The object is then read before the
   * caller checks the bucket, so they share it only for a bucket seen before that existed
   * and, with `publicBucketOnly`, was public: the object is read only where the regular
   * reads would read it too, unless the bucket changed since. The same public flag picks
   * the object read's scope; a read done with the wrong scope is never used where the
   * regular one could differ, and the object is read again instead.
   */
  async findBucketAndObject(
    bucketId: string,
    objectName: string,
    objectColumns: string,
    options: { version?: string; objectAsSuperUser?: boolean; publicBucketOnly?: boolean } = {}
  ): Promise<BucketObjectLookup> {
    const forceSuperUser = options.objectAsSuperUser === true
    const seenKey = `${this.db.tenantId}/${bucketId}`
    const seenPublic = seenBuckets.get(seenKey)
    const readAsSuperUser = forceSuperUser || seenPublic === true

    // Invalid names fail in from() and findObject() only after the bucket is checked.
    const batch =
      seenPublic !== undefined &&
      (seenPublic || !options.publicBucketOnly) &&
      isValidBucketName(bucketId) &&
      isValidKey(objectName)
    const batched = batch
      ? await this.db.findBucketAndObject({
          bucketId,
          bucketColumns: 'id,public',
          objectName,
          objectColumns,
          objectFilters: { excludeDeleteMarkers: true },
          version: options.version,
          objectAsSuperUser: readAsSuperUser,
        })
      : undefined

    const bucket = batched
      ? batched.bucket
      : await this.asSuperUser().findBucket(bucketId, 'id,public', { dontErrorOnEmpty: true })
    rememberBucket(seenKey, bucket)

    return {
      bucket,
      findObject: async () => {
        const asSuperUser = forceSuperUser || Boolean(bucket?.public)

        if (batched?.bucket) {
          if (asSuperUser === readAsSuperUser) {
            if (!batched.object) {
              throw ERRORS.NoSuchKey(objectName)
            }
            return batched.object
          }

          // Read with this storage's scope although the bucket is public. RLS only
          // filters rows, so a row it let through is the row the super user reads; a
          // missing one may just be hidden.
          if (asSuperUser && batched.object) {
            return batched.object
          }
        }

        // A row read as the super user is never used where this storage's scope applies.
        const storage = asSuperUser ? this.asSuperUser() : this
        return storage
          .from(bucketId)
          .findObject(objectName, objectColumns, undefined, options.version)
      },
    }
  }

  async getBucketLifecycle(bucketId: string) {
    await assertTenantLifecycleApiEnabled(bucketId, this.db)
    const bucket = await this.db.findLifecycleBucket(bucketId)
    return bucket.lifecycle_configuration
  }

  async putBucketLifecycle(id: string, config: BucketLifecycleConfiguration) {
    await assertTenantLifecycleApiEnabled(id, this.db)
    const bucket = await this.db.putLifecycleConfiguration(id, config)
    await wakeLifecycleTenant(this.db)
    return bucket.lifecycle_configuration
  }

  async deleteBucketLifecycle(bucketId: string) {
    await assertTenantLifecycleApiEnabled(bucketId, this.db)
    await this.db.deleteLifecycleConfiguration(bucketId)
    await wakeLifecycleTenant(this.db)
  }

  /**
   * List buckets
   * @param columns
   * @param options
   */
  listBuckets(columns = 'id', options?: ListBucketOptions) {
    return this.db.listBuckets(columns, options)
  }

  listAnalyticsBuckets(columns = 'name', options?: ListBucketOptions) {
    return this.db.listAnalyticsBuckets(columns, options)
  }

  /**
   * Creates a bucket
   * @param data
   */
  async createBucket(
    data: Omit<
      Parameters<Database['createBucket']>[0],
      'file_size_limit' | 'allowed_mime_types'
    > & {
      fileSizeLimit?: number | string | null
      allowedMimeTypes?: null | string[]
      type?: BucketType
    }
  ) {
    // prevent creation with leading or trailing whitespace
    if (data.name.trim().length !== data.name.length) {
      throw ERRORS.InvalidBucketName(data.name)
    }

    mustBeValidBucketName(data.name)
    mustBeNotReservedBucketName(data.name)

    if (data.versioning_status !== undefined && data.versioning_status !== 'DISABLED') {
      await assertVersioningEnabled(this, data.id ?? data.name, data.type ?? 'STANDARD')
    }

    if (data.type === 'ANALYTICS') {
      if (
        !(await this.db.hasMigration('iceberg-catalog-flag-on-buckets')) ||
        !(await tenantHasFeature(this.db.tenantId, 'icebergCatalog'))
      ) {
        throw ERRORS.FeatureNotEnabled(
          'iceberg_catalog',
          'Iceberg buckets are not enabled for this tenant'
        )
      }

      const icebergBucketData = data as Parameters<Database['createAnalyticsBucket']>[0]
      return this.createIcebergBucket(icebergBucketData)
    }

    const bucketData = { ...data } as Parameters<Database['createBucket']>[0] & {
      fileSizeLimit?: unknown
      allowedMimeTypes?: unknown
    }
    delete bucketData.fileSizeLimit
    delete bucketData.allowedMimeTypes

    if (typeof data.fileSizeLimit === 'number' || typeof data.fileSizeLimit === 'string') {
      bucketData.file_size_limit = await this.parseMaxSizeLimit(data.fileSizeLimit)
    }

    if (data.fileSizeLimit === null) {
      bucketData.file_size_limit = null
    }

    bucketData.allowed_mime_types = data.allowedMimeTypes
      ? normalizeAllowedMimeTypes(data.allowedMimeTypes)
      : data.allowedMimeTypes

    return this.db.createBucket(bucketData)
  }

  async createIcebergBucket(data: Parameters<Database['createAnalyticsBucket']>[0]) {
    return this.db.withTransaction(async (db) => {
      const result = await db.createAnalyticsBucket(data)

      // v1 invokeOrSend order: run the handler inline first; on failure, enqueue instead
      // (unless the failure is a StorageBackendError, which the caller must see).
      const event = new BucketCreatedEvent({
        bucketId: result.id,
        bucketName: result.name,
        type: 'ANALYTICS',
        tenant: {
          ref: db.tenantId,
          host: db.tenantHost,
        },
        sbReqId: db.sbReqId,
      })
      try {
        await getStorageQueue().invoke(event)
      } catch (error) {
        if (error instanceof StorageBackendError) {
          throw error
        }

        logSchema.error(logger, 'Failed to invoke BucketCreatedEvent handler', {
          project: db.tenantId,
          type: 'event',
          error,
          sbReqId: db.sbReqId,
        })
        await getStorageQueue().produce(event)
      }

      return result
    })
  }

  /**
   * Updates a bucket
   * @param id
   * @param data
   */
  async updateBucket(
    id: string,
    data: Omit<
      Parameters<Database['updateBucket']>[1],
      'file_size_limit' | 'allowed_mime_types'
    > & {
      fileSizeLimit?: number | string | null
      allowedMimeTypes?: null | string[]
    }
  ) {
    mustBeValidBucketName(id)
    if (!Object.values(data).some((v) => typeof v !== 'undefined')) {
      throw ERRORS.NoContentProvided()
    }

    if (data.versioning_status !== undefined && data.versioning_status !== 'DISABLED') {
      const bucket = await this.findBucket(id, 'type')
      await assertVersioningEnabled(this, id, bucket.type ?? 'STANDARD')
    }

    const bucketData = { ...data } as Parameters<Database['updateBucket']>[1] & {
      fileSizeLimit?: unknown
      allowedMimeTypes?: unknown
    }
    delete bucketData.fileSizeLimit
    delete bucketData.allowedMimeTypes

    if (typeof data.fileSizeLimit === 'number' || typeof data.fileSizeLimit === 'string') {
      bucketData.file_size_limit = await this.parseMaxSizeLimit(data.fileSizeLimit)
    }

    if (data.fileSizeLimit === null) {
      bucketData.file_size_limit = null
    }

    bucketData.allowed_mime_types = data.allowedMimeTypes
      ? normalizeAllowedMimeTypes(data.allowedMimeTypes)
      : data.allowedMimeTypes

    const wakeLifecycle =
      data.versioning_status === 'ENABLED' &&
      (await this.db.hasMigration('lifecycle-writer-coordination'))
    const result = await this.db.updateBucket(id, bucketData)
    if (wakeLifecycle) await wakeLifecycleTenant(this.db)

    // purge cache if a bucket is changing from public to private
    if (data.public === false && result?.previous.public === true) {
      await this.purgeBucketCache(id)
    }
  }

  /**
   * Delete a specific bucket if empty
   * @param id
   */
  async deleteBucket(id: string) {
    const deleted = await this.db.withTransaction(async (db) => {
      await db.asSuperUser().findBucketById(id, 'id', {
        forUpdate: true,
      })

      const countObjects = await db.asSuperUser().countObjectsInBucket(id, 1)

      if (countObjects && countObjects > 0) {
        throw ERRORS.BucketNotEmpty(id)
      }

      const deleted = await db.deleteBucket(id)

      if (!deleted) {
        throw ERRORS.NoSuchBucket(id)
      }

      return deleted
    })

    await this.purgeBucketCache(id)

    return deleted
  }

  private async purgeBucketCache(bucketId: string) {
    try {
      await PurgeCdnCache.send({
        tenant: {
          ref: this.db.tenantId,
          host: this.db.tenantHost,
        },
        sbReqId: this.db.sbReqId,
        purgeOptions: {
          type: 'bucket',
          bucket: bucketId,
          tenant: this.db.tenantId,
        },
      })
    } catch (error) {
      logSchema.error(logger, 'Failed to purge bucket cache', {
        type: 'cdn',
        project: this.db.tenantId,
        sbReqId: this.db.sbReqId,
        error,
      })
    }
  }

  async deleteIcebergBucket(name: string) {
    if (
      !(await this.db.hasMigration('iceberg-catalog-flag-on-buckets')) ||
      !(await tenantHasFeature(this.db.tenantId, 'icebergCatalog'))
    ) {
      throw ERRORS.FeatureNotEnabled(
        'iceberg_catalog',
        'Iceberg buckets are not enabled for this tenant'
      )
    }

    const catalog = await this.db.findAnalyticsBucketByName(name)

    await BucketDeleted.invoke({
      bucketId: catalog.id,
      type: 'ANALYTICS',
      tenant: {
        ref: this.db.tenantId,
        host: this.db.tenantHost,
      },
      sbReqId: this.db.sbReqId,
    })
  }

  /**
   * Deletes all files in a bucket
   * @param bucketId
   * @param before limit to files before the specified time (defaults to now)
   */
  async emptyBucket(bucketId: string, before: Date = new Date()) {
    await this.findBucket(bucketId, 'name')

    const count = await this.db.countObjectsInBucket(bucketId, emptyBucketMax + 1)
    if (count > emptyBucketMax) {
      throw ERRORS.UnableToEmptyBucket(
        bucketId,
        'Unable to empty the bucket because it contains too many objects'
      )
    }

    const objects = await this.db.listObjects(bucketId, 'id', 1, before)
    if (!objects || objects.length < 1) {
      // the bucket is already empty
      return
    }

    // use queue to recursively delete all objects created before the specified time
    await ObjectAdminDeleteAllBefore.send({
      before: before.toISOString(),
      bucketId,
      tenant: this.db.tenant(),
      reqId: this.db.reqId,
      sbReqId: this.db.sbReqId,
    })
  }

  healthcheck() {
    return this.db.asSuperUser().healthcheck()
  }

  protected async parseMaxSizeLimit(maxFileLimit: number | string) {
    if (typeof maxFileLimit === 'string') {
      maxFileLimit = parseFileSizeToBytes(maxFileLimit)
    }

    const globalMaxLimit = await getFileSizeLimit(this.db.tenantId)

    if (maxFileLimit > globalMaxLimit) {
      throw ERRORS.EntityTooLarge()
    }

    return maxFileLimit
  }
}
