import { randomUUID } from 'node:crypto'
import {
  isDownloadScopedToken,
  isUploadScopedToken,
  SIGNED_URL_SCOPE_DOWNLOAD,
  SIGNED_URL_SCOPE_UPLOAD,
  SignedToken,
  SignedUploadToken,
  SignedUrlScope,
  signJWT,
  verifyJWT,
} from '@internal/auth'
import { getJwtSecret } from '@internal/database'
import { ERRORS } from '@internal/errors'
import { StorageObjectLocator } from '@storage/locator'
import { Obj } from '@storage/schemas'
import { FastifyRequest } from 'fastify/types/request'
import { ObjectMetadata, StorageBackendAdapter } from './backend'
import { Database, FindObjectFilters, SearchObjectOption } from './database'
import {
  ObjectAdminDelete,
  ObjectCreatedCopyEvent,
  ObjectCreatedMove,
  ObjectRemoved,
  ObjectRemovedMove,
  ObjectUpdatedMetadata,
} from './events'
import {
  MAX_OBJECTS_PER_DELETE_BATCH,
  MAX_OBJECTS_PER_LOOKUP_BATCH,
  mustBeValidKey,
} from './limits'
import { CanUploadMetadata, fileUploadFromRequest, Uploader, UploadRequest } from './uploader'

interface CopyObjectParams {
  sourceKey: string
  destinationBucket: string
  destinationKey: string
  owner?: string
  copyMetadata?: boolean
  upsert?: boolean
  uploadType: 'standard' | 's3' | 'resumable'
  metadata?: {
    cacheControl?: string
    mimetype?: string
  }
  userMetadata?: Record<string, unknown>
  conditions?: {
    ifMatch?: string
    ifNoneMatch?: string
    ifModifiedSince?: Date
    ifUnmodifiedSince?: Date
  }
}
export interface ListObjectsV2Result {
  folders: Obj[]
  objects: Obj[]
  hasNext: boolean
  nextCursor?: string
  nextCursorKey?: string
}

/**
 * ObjectStorage
 * interact with remote objects and database state
 */
export class ObjectStorage {
  protected readonly uploader: Uploader

  constructor(
    private readonly backend: StorageBackendAdapter,
    private readonly db: Database,
    private readonly location: StorageObjectLocator,
    private readonly bucketId: string
  ) {
    this.uploader = new Uploader(backend, db, location)
  }

  /**
   * Impersonate any subsequent chained operations
   * as superUser bypassing RLS rules
   */
  asSuperUser() {
    return new ObjectStorage(this.backend, this.db.asSuperUser(), this.location, this.bucketId)
  }

  async uploadFromRequest(
    request: FastifyRequest,
    file: {
      objectName: string
      owner?: string
      isUpsert: boolean
      signal?: AbortSignal
    }
  ) {
    const bucket = await this.db
      .asSuperUser()
      .findBucketById(this.bucketId, 'id, file_size_limit, allowed_mime_types')

    const uploadRequest = await fileUploadFromRequest(request, {
      objectName: file.objectName,
      fileSizeLimit: bucket.file_size_limit,
      allowedMimeTypes: bucket.allowed_mime_types || [],
    })

    return this.uploadNewObject({
      file: uploadRequest,
      objectName: file.objectName,
      owner: file.owner,
      isUpsert: Boolean(file.isUpsert),
      signal: file.signal,
      userMetadata: uploadRequest.userMetadata,
    })
  }

  /**
   * Upload a new object to a storage
   * @param request
   */
  async uploadNewObject(request: Omit<UploadRequest, 'bucketId' | 'uploadType'>) {
    mustBeValidKey(request.objectName)

    const path = `${this.bucketId}/${request.objectName}`

    const { metadata, obj } = await this.uploader.upload({
      ...request,
      bucketId: this.bucketId,
      uploadType: 'standard',
    })

    return { objectMetadata: metadata, path, id: obj.id }
  }

  /**
   * Deletes an object from the remote storage
   * and the database
   * @param objectName
   */
  async deleteObject(objectName: string) {
    const obj = await this.db.withTransaction(async (db) => {
      const obj = await db
        .asSuperUser()
        .findObject(this.bucketId, objectName, 'id,version,metadata', {
          forUpdate: true,
        })

      const deleted = await db.deleteObject(this.bucketId, objectName)

      if (!deleted) {
        throw ERRORS.AccessDenied('Access denied')
      }

      await this.backend.deleteObject(
        this.location.getRootLocation(),
        this.location.getKeyLocation({
          tenantId: this.db.tenantId,
          bucketId: this.bucketId,
          objectName,
        }),
        obj.version
      )

      return obj
    })

    await ObjectRemoved.sendWebhook({
      tenant: this.db.tenant(),
      name: objectName,
      version: obj.version,
      bucketId: this.bucketId,
      reqId: this.db.reqId,
      sbReqId: this.db.sbReqId,
      metadata: obj.metadata,
    })
  }

  /**
   * Deletes multiple objects from the remote storage
   * and the database
   * @param prefixes
   */
  async deleteObjects(prefixes: string[]) {
    const results: { name: string }[] = []

    for (let i = 0; i < prefixes.length; i += MAX_OBJECTS_PER_DELETE_BATCH) {
      const prefixesSubset = prefixes.slice(i, i + MAX_OBJECTS_PER_DELETE_BATCH)

      await this.db.withTransaction(async (db) => {
        const data = await db.deleteObjects(this.bucketId, prefixesSubset, 'name')

        if (data.length > 0) {
          results.push(...data)

          // if successfully deleted, delete from s3 too
          // todo: consider moving this to a queue
          const prefixesToDelete = data.reduce((all, { name, version }) => {
            const location = this.location.getKeyLocation({
              tenantId: db.tenantId,
              bucketId: this.bucketId,
              objectName: name,
              version,
            })

            all.push(location)

            if (version) {
              all.push(`${location}.info`)
            }
            return all
          }, [] as string[])

          await this.backend.deleteObjects(this.location.getRootLocation(), prefixesToDelete)

          await Promise.allSettled(
            data.map((object) =>
              ObjectRemoved.sendWebhook({
                tenant: db.tenant(),
                name: object.name,
                bucketId: this.bucketId,
                reqId: this.db.reqId,
                sbReqId: this.db.sbReqId,
                version: object.version,
                metadata: object.metadata,
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
    mustBeValidKey(objectName)

    const result = await this.db.updateObjectMetadata(this.bucketId, objectName, metadata)

    await ObjectUpdatedMetadata.sendWebhook({
      tenant: this.db.tenant(),
      name: objectName,
      version: result.version,
      bucketId: this.bucketId,
      metadata,
      reqId: this.db.reqId,
      sbReqId: this.db.sbReqId,
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
    mustBeValidKey(objectName)

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
   * @param destinationBucket
   * @param destinationKey
   * @param owner
   * @param conditions
   * @param copyMetadata
   * @param upsert
   * @param fileMetadata
   * @param userMetadata
   */
  async copyObject({
    sourceKey,
    destinationBucket,
    destinationKey,
    owner,
    conditions,
    copyMetadata,
    upsert,
    uploadType,
    metadata: fileMetadata,
    userMetadata,
  }: CopyObjectParams) {
    mustBeValidKey(destinationKey)

    const newVersion = randomUUID()
    const s3SourceKey = this.location.getKeyLocation({
      tenantId: this.db.tenantId,
      bucketId: this.bucketId,
      objectName: sourceKey,
    })
    const s3DestinationKey = this.location.getKeyLocation({
      tenantId: this.db.tenantId,
      bucketId: destinationBucket,
      objectName: destinationKey,
    })

    // We check if the user has permission to copy the object to the destination key
    const originObject = await this.db.findObject(
      this.bucketId,
      sourceKey,
      'bucket_id,metadata,user_metadata,version'
    )

    const baseMetadata = originObject.metadata || {}
    const destinationMetadata = copyMetadata
      ? baseMetadata
      : {
          ...baseMetadata,
          ...(fileMetadata || {}),
        }

    const destinationUserMetadata = copyMetadata ? originObject.user_metadata : userMetadata

    await this.uploader.canUpload({
      bucketId: destinationBucket,
      objectName: destinationKey,
      owner,
      isUpsert: upsert,
      userMetadata: destinationUserMetadata || undefined,
      metadata: destinationMetadata,
    })

    try {
      const copyResult = await this.backend.copyObject(
        this.location.getRootLocation(),
        s3SourceKey,
        originObject.version,
        s3DestinationKey,
        newVersion,
        destinationMetadata,
        conditions
      )

      const metadata = await this.backend.headObject(
        this.location.getRootLocation(),
        s3DestinationKey,
        newVersion
      )

      const destinationObject = await this.db.asSuperUser().withTransaction(async (db) => {
        await db.waitObjectLock(destinationBucket, destinationKey, undefined, {
          timeout: 3000,
        })

        const existingDestObject = await db.findObject(
          destinationBucket,
          destinationKey,
          'id,name,metadata,version,bucket_id',
          {
            dontErrorOnEmpty: true,
            forUpdate: true,
          }
        )

        const destinationObject = await db.upsertObject({
          ...originObject,
          bucket_id: destinationBucket,
          name: destinationKey,
          owner,
          metadata: {
            ...destinationMetadata,
            lastModified: copyResult.lastModified,
            eTag: copyResult.eTag,
          },
          user_metadata: destinationUserMetadata,
          version: newVersion,
        })

        if (existingDestObject) {
          await ObjectAdminDelete.send({
            name: existingDestObject.name,
            bucketId: existingDestObject.bucket_id ?? destinationBucket,
            tenant: this.db.tenant(),
            version: existingDestObject.version,
            reqId: this.db.reqId,
            sbReqId: this.db.sbReqId,
          })
        }

        return destinationObject
      })

      await ObjectCreatedCopyEvent.sendWebhook({
        tenant: this.db.tenant(),
        name: destinationKey,
        version: newVersion,
        bucketId: destinationBucket,
        metadata,
        uploadType,
        reqId: this.db.reqId,
        sbReqId: this.db.sbReqId,
      })

      return {
        destObject: destinationObject,
        httpStatusCode: copyResult.httpStatusCode,
        eTag: copyResult.eTag,
        lastModified: copyResult.lastModified,
      }
    } catch (e) {
      await ObjectAdminDelete.send({
        name: destinationKey,
        bucketId: destinationBucket,
        tenant: this.db.tenant(),
        version: newVersion,
        reqId: this.db.reqId,
        sbReqId: this.db.sbReqId,
      })
      throw e
    }
  }

  /**
   * Moves an existing remote object to a given location
   * @param sourceObjectName
   * @param destinationBucket
   * @param destinationObjectName
   * @param owner
   */
  async moveObject(
    sourceObjectName: string,
    destinationBucket: string,
    destinationObjectName: string,
    uploadType: 'standard' | 's3' | 'resumable',
    owner?: string
  ) {
    mustBeValidKey(destinationObjectName)

    const newVersion = randomUUID()
    const s3SourceKey = this.location.getKeyLocation({
      tenantId: this.db.tenantId,
      bucketId: this.bucketId,
      objectName: sourceObjectName,
    })

    const s3DestinationKey = this.location.getKeyLocation({
      tenantId: this.db.tenantId,
      bucketId: destinationBucket,
      objectName: destinationObjectName,
    })

    await this.db.testPermission((db) => {
      return Promise.all([
        db.findObject(this.bucketId, sourceObjectName, 'id'),
        db.updateObject(this.bucketId, sourceObjectName, {
          name: destinationObjectName,
          version: newVersion,
          bucket_id: destinationBucket,
          owner,
        }),
      ])
    })

    const sourceObj = await this.db
      .asSuperUser()
      .findObject(this.bucketId, sourceObjectName, 'id, version,user_metadata')

    if (s3SourceKey === s3DestinationKey) {
      return {
        destObject: sourceObj,
      }
    }

    try {
      await this.backend.copyObject(
        this.location.getRootLocation(),
        s3SourceKey,
        sourceObj.version,
        s3DestinationKey,
        newVersion
      )

      const metadata = await this.backend.headObject(
        this.location.getRootLocation(),
        s3DestinationKey,
        newVersion
      )

      return this.db.asSuperUser().withTransaction(async (db) => {
        await db.waitObjectLock(this.bucketId, destinationObjectName, undefined, {
          timeout: 5000,
        })

        const sourceObject = await db.findObject(
          this.bucketId,
          sourceObjectName,
          'id,version,metadata,user_metadata',
          {
            forUpdate: true,
            dontErrorOnEmpty: false,
          }
        )

        await db.updateObject(this.bucketId, sourceObjectName, {
          name: destinationObjectName,
          bucket_id: destinationBucket,
          version: newVersion,
          owner,
          metadata,
          user_metadata: sourceObj.user_metadata,
        })

        await ObjectAdminDelete.send({
          name: sourceObjectName,
          bucketId: this.bucketId,
          tenant: this.db.tenant(),
          version: sourceObj.version,
          reqId: this.db.reqId,
          sbReqId: this.db.sbReqId,
        })

        await Promise.allSettled([
          ObjectRemovedMove.sendWebhook({
            tenant: this.db.tenant(),
            name: sourceObjectName,
            bucketId: this.bucketId,
            reqId: this.db.reqId,
            sbReqId: this.db.sbReqId,
            version: sourceObject.version,
            metadata: sourceObject.metadata,
          }),
          ObjectCreatedMove.sendWebhook({
            tenant: this.db.tenant(),
            name: destinationObjectName,
            version: newVersion,
            bucketId: destinationBucket,
            metadata,
            uploadType,
            oldObject: {
              name: sourceObjectName,
              bucketId: this.bucketId,
              reqId: this.db.reqId,
              version: sourceObject.version,
            },
            reqId: this.db.reqId,
            sbReqId: this.db.sbReqId,
          }),
        ])

        return {
          destObject: {
            id: sourceObject.id,
            name: destinationObjectName,
            bucket_id: destinationBucket,
            version: newVersion,
            owner,
            metadata,
          },
        }
      })
    } catch (e) {
      await ObjectAdminDelete.send({
        name: destinationObjectName,
        bucketId: destinationBucket,
        tenant: this.db.tenant(),
        version: newVersion,
        reqId: this.db.reqId,
        sbReqId: this.db.sbReqId,
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

  async listObjectsV2(options?: {
    prefix?: string
    delimiter?: string
    cursor?: string
    startAfter?: string
    maxKeys?: number
    encodingType?: 'url'
    sortBy?: {
      column: 'name' | 'created_at' | 'updated_at'
      order?: string
    }
  }): Promise<ListObjectsV2Result> {
    const limit = Math.min(options?.maxKeys || 1000, 1000)
    const prefix = options?.prefix || ''
    const delimiter = options?.delimiter

    const cursor = options?.cursor ? decodeContinuationToken(options.cursor) : undefined
    let searchResult = await this.db.listObjectsV2(this.bucketId, {
      prefix: options?.prefix,
      delimiter: options?.delimiter,
      maxKeys: limit + 1,
      nextToken: cursor?.startAfter,
      startAfter: cursor?.startAfter || options?.startAfter,
      sortBy: {
        order: cursor?.sortOrder || options?.sortBy?.order,
        column: cursor?.sortColumn || options?.sortBy?.column,
        after: cursor?.sortColumnAfter,
      },
    })

    let prevPrefix = ''

    if (delimiter) {
      const delimitedResults: Obj[] = []
      for (const object of searchResult) {
        let idx = object.name.replace(prefix, '').indexOf(delimiter)

        if (idx >= 0) {
          idx = prefix.length + idx + delimiter.length
          const currPrefix = object.name.substring(0, idx)
          if (currPrefix === prevPrefix) {
            continue
          }
          prevPrefix = currPrefix
          delimitedResults.push({
            id: null,
            name: currPrefix,
            bucket_id: object.bucket_id,
          })
          continue
        }

        delimitedResults.push(object)
      }
      searchResult = delimitedResults
    }

    const isTruncated = searchResult.length > limit
    const resultCount = isTruncated ? limit : searchResult.length

    const folders: Obj[] = []
    const objects: Obj[] = []
    for (let index = 0; index < resultCount; index++) {
      const obj = searchResult[index]
      const target = obj.id === null ? folders : objects
      const name = obj.id === null && !obj.name.endsWith('/') ? obj.name + '/' : obj.name
      target.push({
        ...obj,
        name: options?.encodingType === 'url' ? encodeURIComponent(name) : name,
      })
    }

    let nextContinuationToken: string | undefined
    let nextCursorKey: string | undefined

    if (isTruncated) {
      const lastObject = searchResult[resultCount - 1]
      const sortColumn = (cursor?.sortColumn || options?.sortBy?.column) as
        | 'name'
        | 'created_at'
        | 'updated_at'
        | undefined

      nextContinuationToken = encodeContinuationToken({
        startAfter: lastObject.name,
        sortOrder: cursor?.sortOrder || options?.sortBy?.order,
        sortColumn,
        sortColumnAfter:
          sortColumn && sortColumn !== 'name' && lastObject[sortColumn]
            ? new Date(lastObject[sortColumn] || '').toISOString()
            : undefined,
      })
      nextCursorKey = lastObject.name
    }

    return {
      hasNext: isTruncated,
      nextCursor: nextContinuationToken,
      nextCursorKey,
      folders,
      objects,
    }
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

    metadata = metadata || {}
    for (const key in metadata) {
      if (!Object.prototype.hasOwnProperty.call(metadata, key)) {
        continue
      }

      if (!metadata[key]) {
        delete metadata[key]
      }
    }

    // security-in-depth: as signObjectUrl could be used as a signing oracle,
    // make sure it's never able to specify a role JWT claim, nor the claims that
    // identify an upload token (upsert/owner) — otherwise a download token could
    // be crafted to satisfy the upload-endpoint's legacy compatibility check.
    delete metadata['role']
    delete metadata['upsert']
    delete metadata['owner']

    const urlParts = url.split('/')
    const urlToSign = decodeURI(urlParts.splice(3).join('/'))
    const { urlSigningKey } = await getJwtSecret(this.db.tenantId)
    // `url` and `scope` are spread last so attacker-controlled metadata can never
    // override the intended object path or the token scope (token-forgery defense).
    const token = await signJWT(
      { ...metadata, url: urlToSign, scope: SIGNED_URL_SCOPE_DOWNLOAD },
      urlSigningKey,
      expiresIn
    )

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
    let results: { name: string }[]

    if (paths.length <= MAX_OBJECTS_PER_LOOKUP_BATCH) {
      results = await this.findObjects(paths, 'name')
    } else {
      results = []

      for (let i = 0; i < paths.length; i += MAX_OBJECTS_PER_LOOKUP_BATCH) {
        const pathsSubset = paths.slice(i, i + MAX_OBJECTS_PER_LOOKUP_BATCH)

        const objects = await this.findObjects(pathsSubset, 'name')
        results.push(...objects)
      }
    }

    const nameSet = new Set<string>()
    for (const { name } of results) {
      nameSet.add(name)
    }

    const { urlSigningKey } = await getJwtSecret(this.db.tenantId)

    return Promise.all(
      paths.map(async (path) => {
        let error = null
        let signedURL = null
        if (nameSet.has(path)) {
          const urlToSign = `${this.bucketId}/${path}`
          const token = await signJWT(
            { url: urlToSign, scope: SIGNED_URL_SCOPE_DOWNLOAD },
            urlSigningKey,
            expiresIn
          )
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
   * @param options
   */
  async signUploadObjectUrl(
    objectName: string,
    url: string,
    expiresIn: number,
    owner?: string,
    options?: {
      upsert?: boolean
      userMetadata?: Record<string, unknown>
      metadata?: CanUploadMetadata
    }
  ) {
    // check if user has INSERT permissions
    await this.uploader.canUpload({
      bucketId: this.bucketId,
      objectName,
      owner,
      isUpsert: options?.upsert ?? false,
      userMetadata: options?.userMetadata,
      metadata: options?.metadata,
    })

    const { urlSigningKey } = await getJwtSecret(this.db.tenantId)
    const token = await signJWT(
      { owner, url, upsert: Boolean(options?.upsert), scope: SIGNED_URL_SCOPE_UPLOAD },
      urlSigningKey,
      expiresIn
    )

    return { url: `/object/upload/sign/${url}?token=${token}`, token }
  }

  /**
   * Verify a signed-URL token for a specific object, enforcing that it was issued
   * for the requested action. This is the single place that validates a signed
   * token: signature, scope, object-path binding, and expiry.
   * @param token
   * @param objectName
   * @param scope the action the token must be authorized for (download or upload)
   */
  async verifyObjectSignature<Scope extends SignedUrlScope>(
    token: string,
    objectName: string,
    scope: Scope
  ): Promise<Scope extends typeof SIGNED_URL_SCOPE_UPLOAD ? SignedUploadToken : SignedToken> {
    const { secret: jwtSecret, jwks } = await getJwtSecret(this.db.tenantId)

    let payload: SignedToken | SignedUploadToken
    try {
      payload = await verifyJWT<SignedToken | SignedUploadToken>(token, jwtSecret, jwks)
    } catch (e) {
      const err = e as Error
      throw ERRORS.InvalidJWT(err)
    }

    const hasValidScope =
      scope === SIGNED_URL_SCOPE_UPLOAD
        ? isUploadScopedToken(payload)
        : isDownloadScopedToken(payload)
    if (!hasValidScope) {
      throw ERRORS.InvalidSignature(`Token is not scoped for ${scope}`)
    }

    if (payload.url !== `${this.bucketId}/${objectName}`) {
      throw ERRORS.InvalidSignature()
    }

    if (payload.exp * 1000 < Date.now()) {
      throw ERRORS.ExpiredSignature()
    }

    // the scope check above guarantees the payload matches the requested scope;
    // TS can't correlate the runtime value with the conditional return type.
    return payload as Scope extends typeof SIGNED_URL_SCOPE_UPLOAD ? SignedUploadToken : SignedToken
  }
}

interface ContinuationToken {
  startAfter: string
  sortOrder?: string // 'asc' | 'desc'
  sortColumn?: string
  sortColumnAfter?: string
}

const CONTINUATION_TOKEN_PART_MAP: Record<string, keyof ContinuationToken> = {
  l: 'startAfter',
  o: 'sortOrder',
  c: 'sortColumn',
  a: 'sortColumnAfter',
}

function encodeContinuationToken(tokenInfo: ContinuationToken) {
  let result = ''
  for (const [k, v] of Object.entries(CONTINUATION_TOKEN_PART_MAP)) {
    if (tokenInfo[v]) {
      result += `${k}:${tokenInfo[v]}\n`
    }
  }
  return Buffer.from(result.slice(0, -1)).toString('base64')
}

function decodeContinuationToken(token: string): ContinuationToken {
  const decodedParts = Buffer.from(token, 'base64').toString().split('\n')
  const result: ContinuationToken = {
    startAfter: '',
    sortOrder: 'asc',
  }
  for (const part of decodedParts) {
    const partMatch = part.match(/^(\S):(.*)/)
    if (!partMatch || partMatch.length !== 3 || !(partMatch[1] in CONTINUATION_TOKEN_PART_MAP)) {
      throw ERRORS.InvalidParameter('continuation token')
    }
    result[CONTINUATION_TOKEN_PART_MAP[partMatch[1]]] = partMatch[2]
  }
  return result
}
