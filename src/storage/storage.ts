import { S3Backend, StorageBackendAdapter, withOptionalVersion } from './backend'
import { Database, FindBucketFilters } from './database'
import { StorageBackendError } from './errors'
import { AssetRenderer, HeadRenderer, ImageRenderer } from './renderer'
import { getFileSizeLimit, mustBeValidBucketName, parseFileSizeToBytes } from './limits'
import { getConfig } from '../config'
import { ObjectStorage } from './object'
import { Bucket } from './schemas'
import { logger } from '../monitoring'

const { urlLengthLimit } = getConfig()

/**
 * Storage
 * interacts with the storage backend of choice and the database
 * to provide a rich management API for any folders and files operations
 */
export class Storage {
  constructor(public readonly backend: StorageBackendAdapter, public readonly db: Database) {}

  /**
   * Access object related functionality on a specific bucket
   * @param bucket
   */
  from(bucket: Bucket) {
    return new ObjectStorage(this.backend, this.db, bucket)
  }

  async fromBucketId(bucketId: string) {
    mustBeValidBucketName(bucketId, 'The bucketId name contains invalid characters')
    const bucket = await this.db.asSuperUser().findBucketById(bucketId, '*')
    return this.from(bucket)
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
      credentialId?: string
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

    if (this.backend instanceof S3Backend && data.credentialId) {
      const backend = this.backend as S3Backend
      bucketData.credential_id = data.credentialId

      return this.db.withTransaction(async (db) => {
        await backend.createBucketIfDoesntExists(data.name)
        return db.createBucket(bucketData)
      })
    }

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
      credentialId?: string
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

    if (this.backend instanceof S3Backend && data.credentialId) {
      const backend = this.backend as S3Backend
      bucketData.credential_id = data.credentialId

      return this.db.withTransaction(async (db) => {
        const bucket = await db.findBucketById(id, 'id,name,credential_id', {
          forUpdate: true,
        })

        if (!bucket.credential_id) {
          throw new StorageBackendError(
            'update_credential_error',
            400,
            'cannot add credentials to an existing bucket'
          )
        }

        await backend.createBucketIfDoesntExists(bucket.name)
        return db.updateBucket(id, bucketData)
      })
    }

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
    const bucket = await this.findBucket(bucketId, '*')
    const objectStore = this.from(bucket)

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
          // TODO: fix this
          const path = objectStore.computeObjectPath(name)
          const fileName = withOptionalVersion(path, version)
          all.push(fileName)
          all.push(fileName + '.info')
          return all
        }, [] as string[])
        // delete files from s3 asynchronously
        this.backend.deleteObjects(params).catch((err) => {
          logger.error(err, 'Error deleting objects from s3')
        })
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

  /**
   * List credentials
   */
  listCredentials() {
    return this.db.listCredentials()
  }

  /**
   * Create credential for external access
   * @param credential
   */
  createCredential(credential: Parameters<Database['createCredential']>[0]) {
    return this.db.createCredential(credential)
  }

  /**
   * Delete credential
   * @param credentialId
   */
  deleteCredential(credentialId: string) {
    return this.db.deleteCredential(credentialId)
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
