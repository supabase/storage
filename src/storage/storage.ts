import { GenericStorageBackend } from './backend'
import { Database, FindBucketFilters } from './database'
import { StorageBackendError } from './errors'
import { ImageRenderer, AssetRenderer, HeadRenderer } from './renderer'
import { mustBeValidKey } from './limits'
import { Uploader } from './uploader'
import { getConfig } from '../config'
import { ObjectStorage } from './object'

const { urlLengthLimit, globalS3Bucket } = getConfig()


export class Storage {
  constructor(
    private readonly backend: GenericStorageBackend,
    private readonly db: Database,
    private readonly id?: string
  ) {}

  from(bucketId: string) {
    mustBeValidKey(bucketId, 'The bucketId name contains invalid characters')

    return new ObjectStorage(this.backend, this.db, bucketId)
  }

  asSuperUser() {
    return new Storage(this.backend, this.db.asSuperUser(), this.id)
  }

  uploader() {
    return new Uploader(this.backend)
  }

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

  findBucket(id: string, columns = 'id', filters?: FindBucketFilters) {
    return this.db.findBucketById(id, columns, filters)
  }

  listBuckets(columns = 'id') {
    return this.db.listBuckets(columns)
  }

  createBucket(data: Parameters<Database['createBucket']>[0]) {
    return this.db.createBucket(data)
  }

  updateBucket(id: string, isPublic: boolean | undefined) {
    return this.db.updateBucket(id, isPublic)
  }

  countObjects(id: string) {
    return this.db.countObjectsInBucket(id)
  }

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

  async emptyBucket(bucketId: string) {
    const bucket = await this.findBucket(bucketId, 'name')

    while (true) {
      const objects = await this.db.listObjects(
        bucketId,
        'id, name',
        Math.floor(urlLengthLimit / (36 + 3))
      )

      if (!(objects && objects.length > 0)) {
        break
      }

      const deleted = await this.db.deleteBucket(objects.map(({ id }) => id!))

      if (deleted && deleted.length > 0) {
        const params = deleted.map(({ name }) => {
          return `${this.db.tenantId}/${bucket.name}/${name}`
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
}
