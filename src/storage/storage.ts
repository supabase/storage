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

export interface FindBucketInput {
  bucketId: string
  columns?: string
  filters?: FindBucketFilters
  signal?: AbortSignal
}

export interface ListBucketsInput {
  columns?: string
  options?: ListBucketOptions
  signal?: AbortSignal
}

export interface UpdateBucketInput {
  bucketId: string
  data: Omit<
    Parameters<Database['updateBucket']>[0]['fields'],
    'file_size_limit' | 'allowed_mime_types'
  > & {
    fileSizeLimit?: number | string | null
    allowedMimeTypes?: null | string[]
  }
  signal?: AbortSignal
}

export interface DeleteBucketInput {
  bucketId: string
  signal?: AbortSignal
}

export interface DeleteIcebergBucketInput {
  name: string
  signal?: AbortSignal
}

export interface EmptyBucketInput {
  bucketId: string
  before?: Date
  signal?: AbortSignal
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
    }

    throw new Error(`renderer of type "${type}" not supported`)
  }

  /**
   * Find a bucket by id
   * @param input
   */
  findBucket(input: FindBucketInput) {
    const { bucketId, columns = 'id', filters, signal } = input
    return this.db.findBucketById({ bucketId, columns, filters, signal })
  }

  /**
   * List buckets
   * @param input
   */
  listBuckets(input: ListBucketsInput = {}) {
    const { columns = 'id', options, signal } = input
    return this.db.listBuckets({ columns, options, signal })
  }

  listAnalyticsBuckets(input: ListBucketsInput = {}) {
    const { columns = 'name', options, signal } = input
    return this.db.listAnalyticsBuckets({ columns, options, signal })
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
    const { signal } = data
    return this.db.withTransaction(
      async (db) => {
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
                error: error,
              })
              return true
            },
          }
        )

        return result
      },
      { signal }
    )
  }

  /**
   * Updates a bucket
   * @param input
   */
  async updateBucket(input: UpdateBucketInput) {
    const { bucketId, data, signal } = input
    mustBeValidBucketName(bucketId)

    const bucketData: Parameters<Database['updateBucket']>[0]['fields'] = data

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

    return this.db.updateBucket({ bucketId, fields: bucketData, signal })
  }

  /**
   * Delete a specific bucket if empty
   * @param input
   */
  async deleteBucket(input: DeleteBucketInput) {
    const { bucketId, signal } = input
    return this.db.withTransaction(
      async (db) => {
        await db
          .asSuperUser()
          .findBucketById({ bucketId, columns: 'id', filters: { forUpdate: true } })

        const countObjects = await db.asSuperUser().countObjectsInBucket({ bucketId, limit: 1 })

        if (countObjects && countObjects > 0) {
          throw ERRORS.BucketNotEmpty(bucketId)
        }

        const deleted = await db.deleteBucket({ bucketId })

        if (!deleted) {
          throw ERRORS.NoSuchBucket(bucketId)
        }

        return deleted
      },
      { signal }
    )
  }

  async deleteIcebergBucket(input: DeleteIcebergBucketInput) {
    const { name, signal } = input
    if (
      !(await tenantHasMigrations(this.db.tenantId, 'iceberg-catalog-flag-on-buckets')) ||
      !(await tenantHasFeature(this.db.tenantId, 'icebergCatalog'))
    ) {
      throw ERRORS.FeatureNotEnabled(
        'iceberg_catalog',
        'Iceberg buckets are not enabled for this tenant'
      )
    }

    const catalog = await this.db.findAnalyticsBucketByName({ name, signal })

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
   * @param input
   */
  async emptyBucket(input: EmptyBucketInput) {
    const { bucketId, before = new Date(), signal } = input
    await this.findBucket({ bucketId, columns: 'name', signal })

    const count = await this.db.countObjectsInBucket({
      bucketId,
      limit: emptyBucketMax + 1,
      signal,
    })
    if (count > emptyBucketMax) {
      throw ERRORS.UnableToEmptyBucket(
        bucketId,
        'Unable to empty the bucket because it contains too many objects'
      )
    }

    const objects = await this.db.listObjects({
      bucketId,
      columns: 'id, name',
      limit: 1,
      before,
      signal,
    })
    if (!objects || objects.length < 1) {
      // the bucket is already empty
      return
    }

    // ensure delete permissions
    await this.db.testPermission(
      (db) => {
        return db.deleteObject({ bucketId, objectName: objects[0].id! })
      },
      { signal }
    )

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
