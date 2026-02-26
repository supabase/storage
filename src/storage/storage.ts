import { StorageBackendAdapter } from './backend'
import { Database, FindBucketFilters, ListBucketOptions } from './database'
import { ERRORS, StorageBackendError } from '@internal/errors'
import { AssetRenderer, HeadRenderer, ImageRenderer } from './renderer'
import {
  BucketType,
  getFileSizeLimit,
  mustBeNotReservedBucketName,
  mustBeValidBucketName,
  parseFileSizeToBytes,
} from './limits'
import { getConfig } from '../config'
import { ObjectStorage } from './object'
import { InfoRenderer } from '@storage/renderer/info'
import { StorageObjectLocator } from '@storage/locator'
import { BucketCreatedEvent, BucketDeleted } from '@storage/events'
import { tenantHasMigrations } from '@internal/database/migrations'
import { tenantHasFeature } from '@internal/database'
import { ObjectAdminDeleteAllBefore } from './events'
import { logger, logSchema } from '@internal/monitoring'

const { emptyBucketMax } = getConfig()

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
    }

    throw new Error(`renderer of type "${type}" not supported`)
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

    if (data.type === 'ANALYTICS') {
      if (
        !(await tenantHasMigrations(this.db.tenantId, 'iceberg-catalog-flag-on-buckets')) ||
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

    const bucketData: Parameters<Database['createBucket']>[0] = data

    if (typeof data.fileSizeLimit === 'number' || typeof data.fileSizeLimit === 'string') {
      bucketData.file_size_limit = await this.parseMaxSizeLimit(data.fileSizeLimit)
    }

    if (data.fileSizeLimit === null) {
      bucketData.file_size_limit = null
    }

    if (data.allowedMimeTypes) {
      this.validateMimeType(data.allowedMimeTypes)
    }
    bucketData.allowed_mime_types = data.allowedMimeTypes

    return this.db.createBucket(bucketData)
  }

  async createIcebergBucket(data: Parameters<Database['createAnalyticsBucket']>[0]) {
    return this.db.withTransaction(async (db) => {
      const result = await db.createAnalyticsBucket(data)

      await BucketCreatedEvent.invokeOrSend(
        {
          bucketId: result.id,
          bucketName: result.name,
          type: 'ANALYTICS',
          tenant: {
            ref: db.tenantId,
            host: db.tenantHost,
          },
        },
        {
          sendWhenError: (error) => {
            if (error instanceof StorageBackendError) {
              return false
            }

            logSchema.error(logger, 'Failed to invoke BucketCreatedEvent handler', {
              project: db.tenantId,
              type: 'event',
              error,
            })
            return true
          },
        }
      )

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

    const bucketData: Parameters<Database['updateBucket']>[1] = data

    if (typeof data.fileSizeLimit === 'number' || typeof data.fileSizeLimit === 'string') {
      bucketData.file_size_limit = await this.parseMaxSizeLimit(data.fileSizeLimit)
    }

    if (data.fileSizeLimit === null) {
      bucketData.file_size_limit = null
    }

    if (data.allowedMimeTypes) {
      this.validateMimeType(data.allowedMimeTypes)
    }
    bucketData.allowed_mime_types = data.allowedMimeTypes

    return this.db.updateBucket(id, bucketData)
  }

  /**
   * Delete a specific bucket if empty
   * @param id
   */
  async deleteBucket(id: string) {
    return this.db.withTransaction(async (db) => {
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
  }

  async deleteIcebergBucket(name: string) {
    if (
      !(await tenantHasMigrations(this.db.tenantId, 'iceberg-catalog-flag-on-buckets')) ||
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

    const objects = await this.db.listObjects(bucketId, 'id, name', 1, before)
    if (!objects || objects.length < 1) {
      // the bucket is already empty
      return
    }

    // ensure delete permissions
    await this.db.testPermission((db) => {
      return db.deleteObject(bucketId, objects[0].id!)
    })

    // use queue to recursively delete all objects created before the specified time
    await ObjectAdminDeleteAllBefore.send({
      before,
      bucketId,
      tenant: this.db.tenant(),
      reqId: this.db.reqId,
    })
  }

  validateMimeType(mimeType: string[]) {
    for (const type of mimeType) {
      if (type.length > 1000) {
        throw ERRORS.InvalidMimeType(type)
      }

      if (
        !type.match(/^([a-zA-Z0-9\-+.]+)\/([a-zA-Z0-9\-+.]+)(;\s*charset=[a-zA-Z0-9\-]+)?$|\*$/)
      ) {
        throw ERRORS.InvalidMimeType(type)
      }
    }
    return true
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
