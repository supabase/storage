import { StorageBackendAdapter, ObjectMetadata } from './backend'
import { Database, SearchObjectOption } from './database'
import { mustBeValidKey } from './limits'
import { getJwtSecret, signJWT } from '../auth'
import { getConfig } from '../config'
import { FastifyRequest } from 'fastify'
import { Uploader } from './uploader'
import {
  ObjectCreatedCopyEvent,
  ObjectCreatedMove,
  ObjectCreatedPostEvent,
  ObjectCreatedPutEvent,
  ObjectRemoved,
  ObjectRemovedMove,
  ObjectUpdatedMetadata,
} from '../queue'

export interface UploadObjectOptions {
  objectName: string
  owner?: string
  isUpsert?: boolean
}

const { urlLengthLimit, globalS3Bucket } = getConfig()

/**
 * ObjectStorage
 * interact with remote objects and database state
 */
export class ObjectStorage {
  constructor(
    private readonly backend: StorageBackendAdapter,
    private readonly db: Database,
    private readonly bucketId: string
  ) {}

  /**
   * Upload an new object to a storage
   * @param request
   * @param options
   */
  async uploadNewObject(request: FastifyRequest, options: UploadObjectOptions) {
    const bucket = await this.db
      .asSuperUser()
      .findBucketById(this.bucketId, 'id, max_file_size_kb, allowed_mime_types')

    await this.createObject(
      {
        name: options.objectName,
        owner: options.owner,
      },
      options.isUpsert
    )

    const path = `${this.bucketId}/${options.objectName}`
    const s3Key = `${this.db.tenantId}/${path}`

    try {
      const uploader = new Uploader(this.backend)
      const objectMetadata = await uploader.upload(request, {
        key: s3Key,
        maxFileSizeKb: bucket.max_file_size_kb,
        allowedMimeTypes: bucket.allowed_mime_types,
      })

      await this.db
        .asSuperUser()
        .updateObjectMetadata(this.bucketId, options.objectName, objectMetadata)

      const event = options.isUpsert ? ObjectCreatedPutEvent : ObjectCreatedPostEvent
      await event.sendWebhook({
        tenant: this.db.tenant(),
        name: options.objectName,
        bucketId: this.bucketId,
        metadata: objectMetadata,
      })

      return { objectMetadata, path }
    } catch (e) {
      // undo operations as super user
      await this.db.asSuperUser().deleteObject(this.bucketId, options.objectName)
      throw e
    }
  }

  /**
   * Upload overriding an existing object
   * @param request
   * @param options
   */
  async uploadOverridingObject(
    request: FastifyRequest,
    options: Omit<UploadObjectOptions, 'isUpsert'>
  ) {
    const bucket = await this.db
      .asSuperUser()
      .findBucketById(this.bucketId, 'id, max_file_size_kb, allowed_mime_types')

    await this.updateObjectOwner(options.objectName, options.owner)

    const path = `${this.bucketId}/${options.objectName}`
    const s3Key = `${this.db.tenantId}/${path}`

    try {
      const uploader = new Uploader(this.backend)
      const objectMetadata = await uploader.upload(request, {
        key: s3Key,
        maxFileSizeKb: bucket.max_file_size_kb,
        allowedMimeTypes: bucket.allowed_mime_types,
      })

      await this.db
        .asSuperUser()
        .updateObjectMetadata(this.bucketId, options.objectName, objectMetadata)

      await ObjectCreatedPutEvent.sendWebhook({
        tenant: this.db.tenant(),
        name: options.objectName,
        bucketId: this.bucketId,
        metadata: objectMetadata,
      })

      return { objectMetadata, path }
    } catch (e) {
      // @todo tricky to handle since we need to undo the s3 upload
      throw e
    }
  }

  /**
   * Creates an object record
   * @param data object data
   * @param isUpsert specify if it is an upsert operation (default: false)
   */
  async createObject(
    data: Omit<Parameters<Database['upsertObject']>[0], 'bucket_id'>,
    isUpsert = false
  ) {
    mustBeValidKey(data.name, 'The object name contains invalid characters')

    if (isUpsert) {
      await this.upsertObject({
        name: data.name,
        owner: data.owner,
      })
    } else {
      await this.db.createObject({
        bucket_id: this.bucketId,
        ...data,
      })
    }

    return null
  }

  /**
   * Upsert an object record
   * @param data object data
   */
  upsertObject(data: Omit<Parameters<Database['upsertObject']>[0], 'bucket_id'>) {
    mustBeValidKey(data.name, 'The object name contains invalid characters')

    return this.db.upsertObject({
      bucket_id: this.bucketId,
      ...data,
    })
  }

  /**
   * Deletes an object from the remote storage
   * and the database
   * @param objectName
   */
  async deleteObject(objectName: string) {
    await this.db.deleteObject(this.bucketId, objectName)
    const s3Key = `${this.db.tenantId}/${this.bucketId}/${objectName}`

    const result = await this.backend.deleteObject(globalS3Bucket, s3Key)

    await ObjectRemoved.sendWebhook({
      tenant: this.db.tenant(),
      name: objectName,
      bucketId: this.bucketId,
    })

    return result
  }

  /**
   * Deletes multiple objects from the remote storage
   * and the database
   * @param prefixes
   */
  async deleteObjects(prefixes: string[]) {
    let results: { name: string }[] = []

    for (let i = 0; i < prefixes.length; ) {
      const prefixesSubset = []
      let urlParamLength = 0

      for (; i < prefixes.length && urlParamLength < urlLengthLimit; i++) {
        const prefix = prefixes[i]
        prefixesSubset.push(prefix)
        urlParamLength += encodeURIComponent(prefix).length + 9 // length of '%22%2C%22'
      }

      const data = await this.db.deleteObjects(this.bucketId, prefixesSubset)

      if (data.length > 0) {
        results = results.concat(data)

        await Promise.allSettled(
          data.map((object) =>
            ObjectRemoved.sendWebhook({
              tenant: this.db.tenant(),
              name: object.name,
              bucketId: this.bucketId,
            })
          )
        )

        // if successfully deleted, delete from s3 too
        const prefixesToDelete = data.map(
          ({ name }) => `${this.db.tenantId}/${this.bucketId}/${name}`
        )

        await this.backend.deleteObjects(globalS3Bucket, prefixesToDelete)
      }
    }

    return results
  }

  /**
   * Updates object metadata in the database
   * @param objectName
   * @param metadata
   */
  async updateObjectMetadata(objectName: string, metadata: ObjectMetadata) {
    mustBeValidKey(objectName, 'The object name contains invalid characters')

    const result = await this.db.updateObjectMetadata(this.bucketId, objectName, metadata)

    await ObjectUpdatedMetadata.sendWebhook({
      tenant: this.db.tenant(),
      name: objectName,
      bucketId: this.bucketId,
      metadata,
    })

    return result
  }

  /**
   * Updates the owner of an object in the database
   * @param objectName
   * @param owner
   */
  updateObjectOwner(objectName: string, owner?: string) {
    return this.db.updateObjectOwner(this.bucketId, objectName, owner)
  }

  /**
   * Finds an object by name
   * @param objectName
   * @param columns
   */
  async findObject(objectName: string, columns = 'id') {
    mustBeValidKey(objectName, 'The object name contains invalid characters')

    return this.db.findObject(this.bucketId, objectName, columns)
  }

  /**
   * Find multiple objects by name
   * @param objectNames
   * @param columns
   */
  async findObjects(objectNames: string[], columns = 'id') {
    return this.db.findObjects(this.bucketId, objectNames, columns)
  }

  /**
   * Copies an existing remote object to a given location
   * @param sourceKey
   * @param destinationKey
   * @param owner
   */
  async copyObject(sourceKey: string, destinationKey: string, owner?: string) {
    mustBeValidKey(destinationKey, 'The destination object name contains invalid characters')

    const bucketId = this.bucketId
    const originObject = await this.db.findObject(this.bucketId, sourceKey, 'bucket_id, metadata')

    const newObject = Object.assign({}, originObject, {
      name: destinationKey,
      owner,
    })

    const destObject = await this.createObject(newObject)

    const s3SourceKey = `${this.db.tenantId}/${bucketId}/${sourceKey}`
    const s3DestinationKey = `${this.db.tenantId}/${bucketId}/${destinationKey}`

    const copyResult = await this.backend.copyObject(globalS3Bucket, s3SourceKey, s3DestinationKey)
    const metadata = await this.backend.headObject(globalS3Bucket, s3DestinationKey)

    await this.db.asSuperUser().updateObjectMetadata(bucketId, destinationKey, metadata)

    await ObjectCreatedCopyEvent.sendWebhook({
      tenant: this.db.tenant(),
      name: destinationKey,
      bucketId: this.bucketId,
      metadata,
    })

    return {
      destObject,
      httpStatusCode: copyResult.httpStatusCode,
    }
  }

  /**
   * Moves an existing remote object to a given location
   * @param sourceObjectName
   * @param destinationObjectName
   */
  async moveObject(sourceObjectName: string, destinationObjectName: string) {
    mustBeValidKey(destinationObjectName, 'The destination object name contains invalid characters')

    if (sourceObjectName === destinationObjectName) {
      return
    }

    await this.db.updateObjectName(this.bucketId, sourceObjectName, destinationObjectName)

    const s3SourceKey = `${this.db.tenantId}/${this.bucketId}/${sourceObjectName}`
    const s3DestinationKey = `${this.db.tenantId}/${this.bucketId}/${destinationObjectName}`

    // @todo what happens if one of these fail?
    await this.backend.copyObject(globalS3Bucket, s3SourceKey, s3DestinationKey)
    await this.backend.deleteObject(globalS3Bucket, s3SourceKey)

    const metadata = await this.backend.headObject(globalS3Bucket, s3DestinationKey)
    await this.db.asSuperUser().updateObjectMetadata(this.bucketId, destinationObjectName, metadata)

    const movedObject = await this.db.findObject(
      this.bucketId,
      destinationObjectName,
      'bucket_id, metadata'
    )

    await Promise.allSettled([
      ObjectRemovedMove.sendWebhook({
        tenant: this.db.tenant(),
        name: sourceObjectName,
        bucketId: this.bucketId,
      }),
      ObjectCreatedMove.sendWebhook({
        tenant: this.db.tenant(),
        name: destinationObjectName,
        bucketId: this.bucketId,
        metadata: movedObject.metadata as ObjectMetadata,
        oldObject: {
          name: sourceObjectName,
          bucketId: this.bucketId,
        },
      }),
    ])
  }

  /**
   * Search objects by prefix
   * @param prefix
   * @param options
   */
  async searchObjects(prefix: string, options: SearchObjectOption) {
    if (prefix.length > 0 && !prefix.endsWith('/')) {
      // assuming prefix is always a folder
      prefix = `${prefix}/`
    }

    return this.db.searchObjects(this.bucketId, prefix, options)
  }

  /**
   * Generates a signed url for accessing an object securely
   * @param objectName
   * @param url
   * @param expiresIn seconds
   * @param metadata
   */
  async signObjectUrl(
    objectName: string,
    url: string,
    expiresIn: number,
    metadata?: Record<string, string>
  ) {
    await this.findObject(objectName)

    metadata = Object.keys(metadata || {}).reduce((all, key) => {
      if (!all[key]) {
        delete all[key]
      }
      return all
    }, metadata || {})

    const urlParts = url.split('/')
    const urlToSign = decodeURI(urlParts.splice(3).join('/'))
    const jwtSecret = await getJwtSecret(this.db.tenantId)
    const token = await signJWT({ url: urlToSign, ...metadata }, jwtSecret, expiresIn)

    let urlPath = 'object'

    if (metadata?.transformations) {
      urlPath = 'render/image'
    }

    // @todo parse the url properly
    return `/${urlPath}/sign/${urlToSign}?token=${token}`
  }

  /**
   * Generates multiple signed urls
   * @param paths
   * @param expiresIn
   */
  async signObjectUrls(paths: string[], expiresIn: number) {
    let results: { name: string }[] = []

    for (let i = 0; i < paths.length; ) {
      const pathsSubset = []
      let urlParamLength = 0

      for (; i < paths.length && urlParamLength < urlLengthLimit; i++) {
        const path = paths[i]
        pathsSubset.push(path)
        urlParamLength += encodeURIComponent(path).length + 9 // length of '%22%2C%22'
      }

      const objects = await this.findObjects(pathsSubset)
      results = results.concat(objects)
    }

    const nameSet = new Set(results.map(({ name }) => name))

    const jwtSecret = await getJwtSecret(this.db.tenantId)

    return Promise.all(
      paths.map(async (path) => {
        let error = null
        let signedURL = null
        if (nameSet.has(path)) {
          const urlToSign = `${this.bucketId}/${path}`
          const token = await signJWT({ url: urlToSign }, jwtSecret, expiresIn)
          signedURL = `/object/sign/${urlToSign}?token=${token}`
        } else {
          error = 'Either the object does not exist or you do not have access to it'
        }
        return {
          error,
          path,
          signedURL,
        }
      })
    )
  }
}
