import { StorageBackendAdapter, ObjectMetadata, withOptionalVersion } from './backend'
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

    const { metadata, obj } = await this.uploader.upload(request, {
      ...options,
      bucketId: this.bucketId,
      fileSizeLimit: bucket.file_size_limit,
      allowedMimeTypes: bucket.allowed_mime_types,
    })

    return { objectMetadata: metadata, path, id: obj.id }
  }

  public async uploadOverridingObject(request: FastifyRequest, options: UploadObjectOptions) {
    mustBeValidKey(options.objectName, 'The object name contains invalid characters')

    const path = `${this.bucketId}/${options.objectName}`

    const bucket = await this.db
      .asSuperUser()
      .findBucketById(this.bucketId, 'id, file_size_limit, allowed_mime_types')

    await this.db.testPermission((db) => {
      return db.updateObject(this.bucketId, options.objectName, {
        name: options.objectName,
        owner: options.owner,
        version: '1',
      })
    })

    const { metadata, obj } = await this.uploader.upload(request, {
      ...options,
      bucketId: this.bucketId,
      fileSizeLimit: bucket.file_size_limit,
      allowedMimeTypes: bucket.allowed_mime_types,
      isUpsert: true,
    })

    return { objectMetadata: metadata, path, id: obj.id }
  }

  /**
   * Deletes an object from the remote storage
   * and the database
   * @param objectName
   */
  async deleteObject(objectName: string) {
    await this.db.withTransaction(async (db) => {
      const obj = await db.asSuperUser().findObject(this.bucketId, objectName, 'id,version', {
        forUpdate: true,
      })

      const deleted = await db.deleteObject(this.bucketId, objectName)

      if (!deleted) {
        throw new StorageBackendError('not_found', 404, 'Object Not Found')
      }

      await ObjectAdminDelete.send({
        tenant: this.db.tenant(),
        name: objectName,
        bucketId: this.bucketId,
        version: obj.version,
        reqId: this.db.reqId,
      })
    })

    await ObjectRemoved.sendWebhook({
      tenant: this.db.tenant(),
      name: objectName,
      bucketId: this.bucketId,
      reqId: this.db.reqId,
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
        const data = await db.deleteObjects(this.bucketId, prefixesSubset, 'name')

        if (data.length > 0) {
          results = results.concat(data)

          // if successfully deleted, delete from s3 too
          // todo: consider moving this to a queue
          const prefixesToDelete = data.reduce((all, { name, version }) => {
            all.push(withOptionalVersion(`${db.tenantId}/${this.bucketId}/${name}`, version))

            if (version) {
              all.push(
                withOptionalVersion(`${db.tenantId}/${this.bucketId}/${name}`, version) + '.info'
              )
            }
            return all
          }, [] as string[])

          await this.backend.deleteObjects(globalS3Bucket, prefixesToDelete)

          await Promise.allSettled(
            data.map((object) =>
              ObjectRemoved.sendWebhook({
                tenant: db.tenant(),
                name: object.name,
                bucketId: this.bucketId,
                reqId: this.db.reqId,
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
      reqId: this.db.reqId,
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

    try {
      // We check if the user has permission to copy the object to the destination key
      const originObject = await this.db.findObject(
        this.bucketId,
        sourceKey,
        'bucket_id,metadata,version'
      )

      await this.uploader.canUpload({
        bucketId: this.bucketId,
        objectName: destinationKey,
        owner,
        isUpsert: false,
      })

      const copyResult = await this.backend.copyObject(
        globalS3Bucket,
        s3SourceKey,
        originObject.version,
        s3DestinationKey,
        newVersion
      )

      const metadata = await this.backend.headObject(globalS3Bucket, s3DestinationKey, newVersion)

      const destObject = await this.db.createObject({
        ...originObject,
        name: destinationKey,
        owner,
        metadata,
        version: newVersion,
      })

      await ObjectCreatedCopyEvent.sendWebhook({
        tenant: this.db.tenant(),
        name: destinationKey,
        bucketId: this.bucketId,
        metadata,
        reqId: this.db.reqId,
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
        reqId: this.db.reqId,
      })
      throw e
    }
  }

  /**
   * Moves an existing remote object to a given location
   * @param sourceObjectName
   * @param destinationObjectName
   * @param owner
   */
  async moveObject(sourceObjectName: string, destinationObjectName: string, owner?: string) {
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
        db.updateObject(this.bucketId, sourceObjectName, {
          name: sourceObjectName,
          version: '1',
          owner,
        }),
        // We also check if we can create the destination object
        // before starting the move
        db.asSuperUser().createObject({
          name: destinationObjectName,
          version: newVersion,
          bucket_id: this.bucketId,
          owner,
        }),
      ])
    })

    const sourceObj = await this.db
      .asSuperUser()
      .findObject(this.bucketId, sourceObjectName, 'id, version')

    try {
      await this.backend.copyObject(
        globalS3Bucket,
        s3SourceKey,
        sourceObj.version,
        s3DestinationKey,
        newVersion
      )

      const metadata = await this.backend.headObject(globalS3Bucket, s3DestinationKey, newVersion)

      await this.db.asSuperUser().withTransaction(async (db) => {
        await db.createObject({
          name: destinationObjectName,
          version: newVersion,
          bucket_id: this.bucketId,
          owner: sourceObj.owner,
          metadata,
        })

        await db.deleteObject(this.bucketId, sourceObjectName, sourceObj.version)
        await ObjectAdminDelete.send({
          name: sourceObjectName,
          bucketId: this.bucketId,
          tenant: this.db.tenant(),
          version: sourceObj.version,
          reqId: this.db.reqId,
        })

        await Promise.allSettled([
          ObjectRemovedMove.sendWebhook({
            tenant: this.db.tenant(),
            name: sourceObjectName,
            bucketId: this.bucketId,
            reqId: this.db.reqId,
          }),
          ObjectCreatedMove.sendWebhook({
            tenant: this.db.tenant(),
            name: destinationObjectName,
            bucketId: this.bucketId,
            metadata: metadata,
            oldObject: {
              name: sourceObjectName,
              bucketId: this.bucketId,
              reqId: this.db.reqId,
            },
            reqId: this.db.reqId,
          }),
        ])
      })
    } catch (e) {
      await ObjectAdminDelete.send({
        name: destinationObjectName,
        bucketId: this.bucketId,
        tenant: this.db.tenant(),
        version: newVersion,
        reqId: this.db.reqId,
      })
      throw e
    }
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

  /**
   * Generates a signed url for uploading an object
   * @param objectName
   * @param url
   * @param expiresIn seconds
   * @param owner
   */
  async signUploadObjectUrl(objectName: string, url: string, expiresIn: number, owner?: string) {
    // check as super user if the object already exists
    const found = await this.asSuperUser().findObject(objectName, 'id', {
      dontErrorOnEmpty: true,
    })

    if (found) {
      throw new StorageBackendError('Duplicate', 409, 'The resource already exists')
    }

    // check if user has INSERT permissions
    await this.db.testPermission((db) => {
      return db.createObject({
        bucket_id: this.bucketId,
        name: objectName,
        owner,
        metadata: {},
      })
    })

    const urlParts = url.split('/')
    const urlToSign = decodeURI(urlParts.splice(4).join('/'))
    const jwtSecret = await getJwtSecret(this.db.tenantId)
    const token = await signJWT({ owner, url: urlToSign }, jwtSecret, expiresIn)

    return `/object/upload/sign/${urlToSign}?token=${token}`
  }
}
