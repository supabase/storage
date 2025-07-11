import { StorageBackendAdapter } from './backend'
import { Database, FindBucketFilters, ListBucketOptions } from './database'
import { ERRORS } from '@internal/errors'
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
import { logger, logSchema } from '@internal/monitoring'
import { StorageObjectLocator } from '@storage/locator'
import { BucketCreatedEvent, BucketDeleted } from '@storage/events'
import { tenantHasMigrations } from '@internal/database/migrations'
import { tenantHasFeature } from '@internal/database'

const { requestUrlLengthLimit } = getConfig()

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

      const icebergBucketData = data as Parameters<Database['createIcebergBucket']>[0]
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

  async createIcebergBucket(data: Parameters<Database['createIcebergBucket']>[0]) {
    return this.db.withTransaction(async (db) => {
      const result = await db.createIcebergBucket(data)

      await BucketCreatedEvent.invoke({
        bucketId: result.id,
        type: 'ANALYTICS',
        tenant: {
          ref: db.tenantId,
          host: db.tenantHost,
        },
      })

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
   * Counts objects in a bucket
   * @param id
   */
  countObjects(id: string) {
    return this.db.countObjectsInBucket(id)
  }

  /**
   * Delete a specific bucket if empty
   * @param id
   * @param type
   */
  async deleteBucket(id: string, type: BucketType = 'STANDARD') {
    if (type === 'ANALYTICS') {
      return this.deleteIcebergBucket(id)
    }

    return this.db.withTransaction(async (db) => {
      await db.asSuperUser().findBucketById(id, 'id', {
        forUpdate: true,
      })

      const countObjects = await db.asSuperUser().countObjectsInBucket(id)

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

  async deleteIcebergBucket(id: string) {
    if (
      !(await tenantHasMigrations(this.db.tenantId, 'iceberg-catalog-flag-on-buckets')) ||
      !(await tenantHasFeature(this.db.tenantId, 'icebergCatalog'))
    ) {
      throw ERRORS.FeatureNotEnabled(
        'iceberg_catalog',
        'Iceberg buckets are not enabled for this tenant'
      )
    }

    return this.db.withTransaction(async (db) => {
      const deleted = await db.deleteAnalyticsBucket(id)

      await BucketDeleted.invoke({
        bucketId: id,
        type: 'ANALYTICS',
        tenant: {
          ref: db.tenantId,
          host: db.tenantHost,
        },
      })
      return deleted
    })
  }

  /**
   * Deletes all files in a bucket
   * @param bucketId
   */
  async emptyBucket(bucketId: string) {
    await this.findBucket(bucketId, 'name')

    while (true) {
      const objects = await this.db.listObjects(
        bucketId,
        'id, name',
        Math.floor(requestUrlLengthLimit / (36 + 3))
      )

      if (!(objects && objects.length > 0)) {
        break
      }

      const deleted = await this.db.deleteObjects(
        bucketId,
        objects.map(({ id }) => id!),
        'id'
      )

      if (deleted && deleted.length > 0) {
        const params = deleted.reduce((all, { name, version }) => {
          const fileName = this.location.getKeyLocation({
            tenantId: this.db.tenantId,
            bucketId,
            objectName: name,
            version,
          })
          all.push(fileName)
          all.push(fileName + '.info')
          return all
        }, [] as string[])
        // delete files from s3 asynchronously
        this.backend.deleteObjects(this.location.getRootLocation(), params).catch((e) => {
          logSchema.error(logger, 'Failed to delete objects from s3', { type: 's3', error: e })
        })
      }

      if (deleted?.length !== objects.length) {
        const deletedNames = new Set(deleted?.map(({ name }) => name))
        const remainingNames = objects
          .filter(({ name }) => !deletedNames.has(name))
          .map(({ name }) => name)

        throw ERRORS.AccessDenied(
          `Cannot delete: ${remainingNames.join(
            ' ,'
          )}, you may have SELECT but not DELETE permissions`
        )
      }
    }
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
