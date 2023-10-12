import { StorageBackendAdapter, withOptionalVersion } from './backend'
import { Database, FindBucketFilters } from './database'
import { StorageBackendError } from './errors'
import { AssetRenderer, HeadRenderer, ImageRenderer } from './renderer'
import { getFileSizeLimit, mustBeValidBucketName, parseFileSizeToBytes } from './limits'
import { getConfig } from '../config'
import { ObjectStorage } from './object'

const { urlLengthLimit, globalS3Bucket } = getConfig()

/**
 * Storage
 * interacts with the storage backend of choice and the database
 * to provide a rich management API for any folders and files operations
 */
export class Storage {
  constructor(public readonly backend: StorageBackendAdapter, public readonly db: Database) {}

  /**
   * Access object related functionality on a specific bucket
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
   */
  async deleteBucket(id: string) {
    return this.db.withTransaction(async (db) => {
      await db.asSuperUser().findBucketById(id, 'id', {
        forUpdate: true,
      })

      const countObjects = await db.asSuperUser().countObjectsInBucket(id)

      if (countObjects && countObjects > 0) {
        throw new StorageBackendError(
          'Storage not empty',
          400,
          'Storage must be empty before you can delete it'
        )
      }

      const deleted = await db.deleteBucket(id)

      if (!deleted) {
        throw new StorageBackendError('not_found', 404, 'Bucket Not Found')
      }

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
        const params = deleted.reduce((all, { name, version }) => {
          const fileName = withOptionalVersion(`${this.db.tenantId}/${bucketId}/${name}`, version)
          all.push(fileName)
          all.push(fileName + '.info')
          return all
        }, [] as string[])
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

  validateMimeType(mimeType: string[]) {
    for (const type of mimeType) {
      if (type.length > 1000) {
        throw new StorageBackendError(
          'invalid_mime_type',
          422,
          `the requested mime type "${type}" is invalid`
        )
      }

      if (!type.match(/^[a-zA-Z0-9\-\+]+\/([a-zA-Z0-9\-\+\.]+$)|\*$/)) {
        throw new StorageBackendError(
          'invalid_mime_type',
          422,
          `the requested mime type "${type} is invalid`
        )
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
      throw new StorageBackendError(
        'max_file_size',
        422,
        'the requested max_file_size exceed the global limit'
      )
    }

    return maxFileLimit
  }
}
