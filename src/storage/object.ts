import { StorageBackendAdapter, ObjectMetadata } from './backend'
import { Database, FindObjectFilters, SearchObjectOption } from './database'
import { mustBeValidKey } from './limits'
import { getJwtSecret, signJWT } from '../auth'
import { getConfig } from '../config'
import { FastifyRequest } from 'fastify'
import { Uploader } from './uploader'
import {
  ObjectAdminDelete,
  ObjectCreatedCopyEvent,
  ObjectCreatedMove,
  ObjectCreatedPostEvent,
  ObjectCreatedPutEvent,
  ObjectRemoved,
  ObjectRemovedMove,
  ObjectUpdatedMetadata,
} from '../queue'
import { randomUUID } from 'crypto'
import { StorageBackendError } from './errors'

export interface UploadObjectOptions {
  objectName: string
  owner?: string
  isUpsert?: boolean
  version?: string
}

const { urlLengthLimit, globalS3Bucket } = getConfig()

/**
 * ObjectStorage
 * interact with remote objects and database state
 */
export class ObjectStorage {
  protected readonly uploader: Uploader

  constructor(
    private readonly backend: StorageBackendAdapter,
    private readonly db: Database,
    private readonly bucketId: string
  ) {
    this.uploader = new Uploader(backend, db)
  }

  /**
   * Impersonate any subsequent chained operations
   * as superUser bypassing RLS rules
   */
  asSuperUser() {
    return new ObjectStorage(this.backend, this.db.asSuperUser(), this.bucketId)
  }

  /**
   * Upload a new object to a storage
   * @param request
   * @param options
   */
  async uploadNewObject(request: FastifyRequest, options: UploadObjectOptions) {
    mustBeValidKey(options.objectName, 'The object name contains invalid characters')

    const path = `${this.bucketId}/${options.objectName}`

    const bucket = await this.db
      .asSuperUser()
      .findBucketById(this.bucketId, 'id, file_size_limit, allowed_mime_types')

    const uploader = new Uploader(this.backend, this.db)

    const { obj, isNew, metadata } = await uploader.upload(request, {
      ...options,
      bucketId: this.bucketId,
      fileSizeLimit: bucket.file_size_limit,
      allowedMimeTypes: bucket.allowed_mime_types,
    })

    if (!obj) {
      return { objectMetadata: metadata, path }
    }

    const event = options.isUpsert && !isNew ? ObjectCreatedPutEvent : ObjectCreatedPostEvent
    await event.sendWebhook({
      tenant: this.db.tenant(),
      name: options.objectName,
      bucketId: this.bucketId,
      metadata,
    })

    return { objectMetadata: metadata, path }
  }

  public async uploadOverridingObject(request: FastifyRequest, options: UploadObjectOptions) {
    mustBeValidKey(options.objectName, 'The object name contains invalid characters')

    const version = randomUUID()
    const path = `${this.bucketId}/${options.objectName}`

    const bucket = await this.db
      .asSuperUser()
      .findBucketById(this.bucketId, 'id, file_size_limit, allowed_mime_types')

    await this.db.testPermission((db) => {
      return db.updateObject(this.bucketId, options.objectName, {
        name: options.objectName,
        owner: options.owner,
        version: version,
      })
    })

    try {
      const uploader = new Uploader(this.backend, this.db)

      const { obj, metadata } = await uploader.upload(request, {
        ...options,
        bucketId: this.bucketId,
        fileSizeLimit: bucket.file_size_limit,
        allowedMimeTypes: bucket.allowed_mime_types,
        isUpsert: true,
      })

      if (!obj) {
        return { objectMetadata: metadata, path }
      }

      await Promise.allSettled([
        ObjectCreatedPutEvent.sendWebhook({
          tenant: this.db.tenant(),
          name: options.objectName,
          bucketId: this.bucketId,
          metadata,
        }),
      ])

      return { objectMetadata: metadata, path }
    } catch (e) {
      await ObjectAdminDelete.send({
        name: options.objectName,
        bucketId: this.bucketId,
        tenant: this.db.tenant(),
        version,
      })
      throw e
    }
  }

  /**
   * Deletes an object from the remote storage
   * and the database
   * @param objectName
   */
  async deleteObject(objectName: string) {
    const s3Key = `${this.db.tenantId}/${this.bucketId}/${objectName}`

    await this.db.withTransaction(async (db) => {
      const obj = await db.asSuperUser().findObject(this.bucketId, objectName, 'id,version', {
        forUpdate: true,
      })

      const deleted = await db.deleteObject(this.bucketId, objectName)

      if (!deleted) {
        throw new StorageBackendError('not_found', 404, 'Object Not Found')
      }

      await this.backend.deleteObjects(globalS3Bucket, [
        `${s3Key}/${obj.version}`,
        `${s3Key}/${obj.version}.info`,
      ])
    })

    await ObjectRemoved.sendWebhook({
      tenant: this.db.tenant(),
      name: objectName,
      bucketId: this.bucketId,
    })
  }

  /**
   * Deletes multiple objects from the remote storage
   * and the database
   * @param prefixes
   */
  async deleteObjects(prefixes: string[]) {
    let results: { name: string }[] = []

    for (let i = 0; i < prefixes.length; ) {
      const prefixesSubset: string[] = []
      let urlParamLength = 0

      for (; i < prefixes.length && urlParamLength < urlLengthLimit; i++) {
        const prefix = prefixes[i]
        prefixesSubset.push(prefix)
        urlParamLength += encodeURIComponent(prefix).length + 9 // length of '%22%2C%22'
      }

      await this.db.withTransaction(async (db) => {
        // todo: for update
        const data = await db.deleteObjects(this.bucketId, prefixesSubset, 'name')

        if (data.length > 0) {
          results = results.concat(data)

          // if successfully deleted, delete from s3 too
          const prefixesToDelete = data.map(({ name }) => `${db.tenantId}/${this.bucketId}/${name}`)

          await this.backend.deleteObjects(globalS3Bucket, prefixesToDelete)

          await Promise.allSettled(
            data.map((object) =>
              ObjectRemoved.sendWebhook({
                tenant: db.tenant(),
                name: object.name,
                bucketId: this.bucketId,
              })
            )
          )
        }
      })
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
   * @param filters
   */
  async findObject(objectName: string, columns = 'id', filters?: FindObjectFilters) {
    mustBeValidKey(objectName, 'The object name contains invalid characters')

    return this.db.findObject(this.bucketId, objectName, columns, filters)
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

    if (sourceKey === destinationKey) {
      return {
        destObject: undefined,
        httpStatusCode: 200,
      }
    }

    const newVersion = randomUUID()
    const bucketId = this.bucketId
    const s3SourceKey = `${this.db.tenantId}/${bucketId}/${sourceKey}`
    const s3DestinationKey = `${this.db.tenantId}/${bucketId}/${destinationKey}`

    await this.db.testPermission(async (db) => {
      await Promise.all([
        db.findObject(bucketId, sourceKey, 'id'),
        db.updateObject(bucketId, sourceKey, {
          name: sourceKey,
          version: newVersion,
        }),
      ])
    })

    try {
      const { originObject } = await this.db.withTransaction(async (db) => {
        const originObject = await db.findObject(
          this.bucketId,
          sourceKey,
          'bucket_id,metadata,version',
          {
            forShare: true,
          }
        )

        await new Uploader(this.backend, db).canUpload({
          bucketId: this.bucketId,
          objectName: destinationKey,
          isUpsert: false,
        })

        return { originObject }
      })

      const copyResult = await this.backend.copyObject(
        globalS3Bucket,
        s3SourceKey,
        originObject.version,
        s3DestinationKey,
        newVersion
      )

      const metadata = await this.backend.headObject(globalS3Bucket, s3DestinationKey, newVersion)

      const destObject = this.db.asSuperUser().createObject({
        ...originObject,
        name: destinationKey,
        owner,
        metadata,
      })

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
    } catch (e) {
      await ObjectAdminDelete.send({
        name: destinationKey,
        bucketId: this.bucketId,
        tenant: this.db.tenant(),
        version: newVersion,
      })
      throw e
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

    const newVersion = randomUUID()
    const s3SourceKey = `${this.db.tenantId}/${this.bucketId}/${sourceObjectName}`
    const s3DestinationKey = `${this.db.tenantId}/${this.bucketId}/${destinationObjectName}`

    await this.db.testPermission((db) => {
      return Promise.all([
        db.findObject(this.bucketId, sourceObjectName, 'id'),
        db.createObject({
          name: destinationObjectName,
          bucket_id: this.bucketId,
          version: newVersion,
        }),
      ])
    })

    const sourceObj = await this.db.asSuperUser().findObject(this.bucketId, sourceObjectName, 'id')

    await this.backend.copyObject(
      globalS3Bucket,
      s3SourceKey,
      sourceObj.version,
      s3DestinationKey,
      newVersion
    )

    const metadata = await this.backend.headObject(globalS3Bucket, s3DestinationKey, newVersion)

    await this.db.createObject({
      name: destinationObjectName,
      version: newVersion,
      bucket_id: this.bucketId,
      owner: sourceObj.owner,
      metadata,
    })

    await Promise.all([
      ObjectAdminDelete.send({
        name: sourceObjectName,
        bucketId: this.bucketId,
        tenant: this.db.tenant(),
        version: sourceObj.version,
      }),
      ObjectRemovedMove.sendWebhook({
        tenant: this.db.tenant(),
        name: sourceObjectName,
        bucketId: this.bucketId,
      }),
      ObjectCreatedMove.sendWebhook({
        tenant: this.db.tenant(),
        name: destinationObjectName,
        bucketId: this.bucketId,
        metadata: metadata,
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
    metadata?: Record<string, string | object | undefined>
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

      const objects = await this.findObjects(pathsSubset, 'name')
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
