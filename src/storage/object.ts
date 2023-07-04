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
import { Bucket } from './schemas'

export interface UploadObjectOptions {
  objectName: string
  owner: string | undefined
  isUpsert?: boolean
  version?: string
}

const { urlLengthLimit } = getConfig()

/**
 * ObjectStorage
 * interact with remote objects and database state
 */
export class ObjectStorage {
  protected readonly uploader: Uploader

  constructor(
    private readonly backend: StorageBackendAdapter,
    private readonly db: Database,
    private readonly bucket: Bucket
  ) {
    this.uploader = new Uploader(backend, db)
  }

  /**
   * Impersonate any subsequent chained operations
   * as superUser bypassing RLS rules
   */
  asSuperUser() {
    return new ObjectStorage(this.backend, this.db.asSuperUser(), this.bucket)
  }

  computeObjectPath(objectName: string) {
    if (this.bucket.credential_id) {
      return objectName
    }
    return `${this.bucket.id}/${objectName}`
  }

  /**
   * Upload a new object to a storage
   * @param request
   * @param options
   */
  async uploadNewObject(request: FastifyRequest, options: UploadObjectOptions) {
    mustBeValidKey(options.objectName, 'The object name contains invalid characters')

    const path = this.computeObjectPath(options.objectName)

    const { metadata, obj } = await this.uploader.upload(request, {
      ...options,
      uploadPath: path,
      bucketId: this.bucket.id,
      fileSizeLimit: this.bucket.file_size_limit,
      allowedMimeTypes: this.bucket.allowed_mime_types,
    })

    return { objectMetadata: metadata, path, id: obj.id }
  }

  public async uploadOverridingObject(request: FastifyRequest, options: UploadObjectOptions) {
    mustBeValidKey(options.objectName, 'The object name contains invalid characters')

    const path = this.computeObjectPath(options.objectName)

    await this.db.testPermission((db) => {
      return db.updateObject(this.bucket.id, options.objectName, {
        name: options.objectName,
        owner: options.owner,
        version: '1',
      })
    })

    const { metadata, obj } = await this.uploader.upload(request, {
      ...options,
      uploadPath: path,
      bucketId: this.bucket.id,
      fileSizeLimit: this.bucket.file_size_limit,
      allowedMimeTypes: this.bucket.allowed_mime_types,
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
      const obj = await db.asSuperUser().findObject(this.bucket.id, objectName, 'id,version', {
        forUpdate: true,
      })

      const deleted = await db.deleteObject(this.bucket.id, objectName)

      if (!deleted) {
        throw new StorageBackendError('not_found', 404, 'Object Not Found')
      }

      await ObjectAdminDelete.send({
        tenant: this.db.tenant(),
        name: objectName,
        bucketId: this.bucket.id,
        version: obj.version,
      })
    })

    await ObjectRemoved.sendWebhook({
      tenant: this.db.tenant(),
      name: objectName,
      bucketId: this.bucket.id,
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
        const data = await db.deleteObjects(this.bucket.id, prefixesSubset, 'name')

        if (data.length > 0) {
          results = results.concat(data)

          // if successfully deleted, delete from s3 too
          // todo: consider moving this to a queue
          const prefixesToDelete = data.reduce((all, { name, version }) => {
            const path = this.computeObjectPath(name)
            all.push(withOptionalVersion(path, version))

            if (version) {
              all.push(withOptionalVersion(path, version) + '.info')
            }
            return all
          }, [] as string[])

          await this.backend.deleteObjects(prefixesToDelete)

          await Promise.allSettled(
            data.map((object) =>
              ObjectRemoved.sendWebhook({
                tenant: db.tenant(),
                name: object.name,
                bucketId: this.bucket.id,
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

    const result = await this.db.updateObjectMetadata(this.bucket.id, objectName, metadata)

    await ObjectUpdatedMetadata.sendWebhook({
      tenant: this.db.tenant(),
      name: objectName,
      bucketId: this.bucket.id,
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
    return this.db.updateObjectOwner(this.bucket.id, objectName, owner)
  }

  /**
   * Finds an object by name
   * @param objectName
   * @param columns
   * @param filters
   */
  async findObject(objectName: string, columns = 'id', filters?: FindObjectFilters) {
    mustBeValidKey(objectName, 'The object name contains invalid characters')

    return this.db.findObject(this.bucket.id, objectName, columns, filters)
  }

  /**
   * Find multiple objects by name
   * @param objectNames
   * @param columns
   */
  async findObjects(objectNames: string[], columns = 'id') {
    return this.db.findObjects(this.bucket.id, objectNames, columns)
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
    const s3SourceKey = this.computeObjectPath(sourceKey)
    const s3DestinationKey = this.computeObjectPath(destinationKey)

    try {
      // We check if the user has permission to copy the object to the destination key
      const originObject = await this.db.findObject(
        this.bucket.id,
        sourceKey,
        'bucket_id,metadata,version'
      )

      await this.uploader.canUpload({
        bucketId: this.bucket.id,
        objectName: destinationKey,
        owner,
        isUpsert: false,
      })

      const copyResult = await this.backend.copyObject(
        s3SourceKey,
        originObject.version,
        s3DestinationKey,
        newVersion
      )

      const metadata = await this.backend.headObject(s3DestinationKey, newVersion)

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
        bucketId: this.bucket.id,
        metadata,
      })

      return {
        destObject,
        httpStatusCode: copyResult.httpStatusCode,
      }
    } catch (e) {
      await ObjectAdminDelete.send({
        name: destinationKey,
        bucketId: this.bucket.id,
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
   * @param owner
   */
  async moveObject(sourceObjectName: string, destinationObjectName: string, owner?: string) {
    mustBeValidKey(destinationObjectName, 'The destination object name contains invalid characters')

    if (sourceObjectName === destinationObjectName) {
      return
    }

    const newVersion = randomUUID()
    const s3SourceKey = this.computeObjectPath(sourceObjectName)
    const s3DestinationKey = this.computeObjectPath(destinationObjectName)

    await this.db.testPermission((db) => {
      return Promise.all([
        db.findObject(this.bucket.id, sourceObjectName, 'id'),
        db.updateObject(this.bucket.id, sourceObjectName, {
          name: sourceObjectName,
          version: '1',
          owner,
        }),
        // We also check if we can create the destination object
        // before starting the move
        db.asSuperUser().createObject({
          name: destinationObjectName,
          version: newVersion,
          bucket_id: this.bucket.id,
          owner,
        }),
      ])
    })

    const sourceObj = await this.db
      .asSuperUser()
      .findObject(this.bucket.id, sourceObjectName, 'id, version')

    try {
      await this.backend.copyObject(s3SourceKey, sourceObj.version, s3DestinationKey, newVersion)

      const metadata = await this.backend.headObject(s3DestinationKey, newVersion)

      await this.db.asSuperUser().withTransaction(async (db) => {
        await db.createObject({
          name: destinationObjectName,
          version: newVersion,
          bucket_id: this.bucket.id,
          owner: sourceObj.owner,
          metadata,
        })

        await db.deleteObject(this.bucket.id, sourceObjectName, sourceObj.version)

        await Promise.all([
          ObjectAdminDelete.send({
            name: sourceObjectName,
            bucketId: this.bucket.id,
            tenant: this.db.tenant(),
            version: sourceObj.version,
          }),
          ObjectRemovedMove.sendWebhook({
            tenant: this.db.tenant(),
            name: sourceObjectName,
            bucketId: this.bucket.id,
          }),
          ObjectCreatedMove.sendWebhook({
            tenant: this.db.tenant(),
            name: destinationObjectName,
            bucketId: this.bucket.id,
            metadata: metadata,
            oldObject: {
              name: sourceObjectName,
              bucketId: this.bucket.id,
            },
          }),
        ])
      })
    } catch (e) {
      await ObjectAdminDelete.send({
        name: destinationObjectName,
        bucketId: this.bucket.id,
        tenant: this.db.tenant(),
        version: newVersion,
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

    return this.db.searchObjects(this.bucket.id, prefix, options)
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
          const urlToSign = `${this.bucket}/${path}`
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
        bucket_id: this.bucket.id,
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
