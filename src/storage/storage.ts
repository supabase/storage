import { StorageBackendAdapter } from './backend'
import { Database, FindBucketFilters } from './database'
import { StorageBackendError } from './errors'
import { AssetRenderer, HeadRenderer, ImageRenderer } from './renderer'
import { getFileSizeLimit, mustBeValidBucketName, parseFileSizeToBytes } from './limits'
import { Uploader } from './uploader'
import { getConfig } from '../config'
import { ObjectStorage } from './object'

const { urlLengthLimit, globalS3Bucket } = getConfig()

/**
 * Storage
 * interacts with the storage backend of choice and the database
 * to provide a rich management API for any folders and files operations
 */
export class Storage {
  constructor(private readonly backend: StorageBackendAdapter, private readonly db: Database) {}

  /**
   * Creates an object storage operations for a specific bucket
   * @param bucketId
   */
  from(bucketId: string) {
    mustBeValidBucketName(bucketId, 'The bucketId name contains invalid characters')

    return new ObjectStorage(this.backend, this.db, bucketId)
  }

  /**
   * Impersonate any subsequent chained operations
   * as superUser bypassing RLS rules
   */
  asSuperUser() {
    return new Storage(this.backend, this.db.asSuperUser())
  }

  /**
   * Creates a renderer type
   * @param type
   */
  renderer(type: 'asset' | 'head' | 'image') {
    switch (type) {
      case 'asset':
        return new AssetRenderer(this.backend)
      case 'head':
        return new HeadRenderer(this.backend)
      case 'image':
        return new ImageRenderer(this.backend)
    }

    throw new Error(`renderer of type "${type}" not supported`)
  }

  /**
   * Creates an uploader instance
   */
  uploader() {
    return new Uploader(this.backend)
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
   */
  listBuckets(columns = 'id') {
    return this.db.listBuckets(columns)
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
    }
  ) {
    mustBeValidBucketName(data.name, 'Bucket name invalid')

    const bucketData: Parameters<Database['createBucket']>[0] = data

    if (data.fileSizeLimit) {
      bucketData.file_size_limit = await this.validateMaxSizeLimit(data.fileSizeLimit)
    }

    bucketData.allowed_mime_types = data.allowedMimeTypes

    return this.db.createBucket(bucketData)
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
    mustBeValidBucketName(id, 'Bucket name invalid')

    const bucketData: Parameters<Database['updateBucket']>[1] = data

    if (data.fileSizeLimit) {
      bucketData.file_size_limit = await this.validateMaxSizeLimit(data.fileSizeLimit)
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
   */
  async deleteBucket(id: string) {
    const countObjects = await this.db.asSuperUser().countObjectsInBucket(id)

    if (countObjects && countObjects > 0) {
      throw new StorageBackendError(
        'Storage not empty',
        400,
        'Storage must be empty before you can delete it'
      )
    }

    return this.db.deleteBucket(id)
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
        Math.floor(urlLengthLimit / (36 + 3))
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
        const params = deleted.map(({ name }) => {
          return `${this.db.tenantId}/${bucketId}/${name}`
        })
        // delete files from s3 asynchronously
        this.backend.deleteObjects(globalS3Bucket, params)
      }

      if (deleted?.length !== objects.length) {
        const deletedNames = new Set(deleted?.map(({ name }) => name))
        const remainingNames = objects
          .filter(({ name }) => !deletedNames.has(name))
          .map(({ name }) => name)

        throw new StorageBackendError(
          'Cannot delete',
          400,
          `Cannot delete: ${remainingNames.join(
            ' ,'
          )}, you may have SELECT but not DELETE permissions`
        )
      }
    }
  }

  protected async validateMaxSizeLimit(maxFileLimit: number | string) {
    if (typeof maxFileLimit === 'string') {
      maxFileLimit = parseFileSizeToBytes(maxFileLimit)
    }

    const globalMaxLimit = await getFileSizeLimit(this.db.tenantId)

    if (maxFileLimit > globalMaxLimit) {
      throw new StorageBackendError(
        'max_file_size',
        422,
        'the requested max_file_size exceed the global limit'
      )
    }

    return maxFileLimit
  }
}
