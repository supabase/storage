import { randomUUID } from 'node:crypto'
import { SignedUploadToken, signJWT, verifyJWT } from '@internal/auth'
import { ERRORS } from '@internal/errors'
import { getJwtSecret } from '@internal/database'

import { ObjectMetadata, StorageBackendAdapter } from './backend'
import { Database, FindObjectFilters, SearchObjectOption } from './database'
import { mustBeValidKey } from './limits'
import { fileUploadFromRequest, Uploader, UploadRequest } from './uploader'
import { getConfig } from '../config'
import {
  ObjectAdminDelete,
  ObjectCreatedCopyEvent,
  ObjectCreatedMove,
  ObjectRemoved,
  ObjectRemovedMove,
  ObjectUpdatedMetadata,
} from './events'
import { FastifyRequest } from 'fastify/types/request'
import { Obj } from '@storage/schemas'
import { StorageObjectLocator } from '@storage/locator'

const { requestUrlLengthLimit } = getConfig()

export interface CopyObjectParams {
  sourceKey: string
  destinationBucket: string
  destinationKey: string
  owner?: string
  copyMetadata?: boolean
  upsert?: boolean
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
  signal?: AbortSignal
}

export interface DeleteObjectInput {
  objectName: string
  signal?: AbortSignal
}

export interface DeleteObjectsInput {
  prefixes: string[]
  signal?: AbortSignal
}

export interface UpdateObjectMetadataInput {
  objectName: string
  metadata: ObjectMetadata
  signal?: AbortSignal
}

export interface UpdateObjectOwnerInput {
  objectName: string
  owner?: string
  signal?: AbortSignal
}

export interface FindObjectInput {
  objectName: string
  columns?: string
  filters?: FindObjectFilters
  signal?: AbortSignal
}

export interface FindObjectsInput {
  objectNames: string[]
  columns?: string
  signal?: AbortSignal
}

export interface MoveObjectInput {
  sourceObjectName: string
  destinationBucket: string
  destinationObjectName: string
  owner?: string
  signal?: AbortSignal
}

export interface SearchObjectsInput {
  prefix: string
  options: SearchObjectOption
  signal?: AbortSignal
}

export interface ListObjectsV2Input {
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
  signal?: AbortSignal
}

export interface SignObjectUrlInput {
  objectName: string
  url: string
  expiresIn: number
  metadata?: Record<string, string | object | undefined>
  signal?: AbortSignal
}

export interface SignObjectUrlsInput {
  paths: string[]
  expiresIn: number
  signal?: AbortSignal
}

export interface SignUploadObjectUrlInput {
  objectName: string
  url: string
  expiresIn: number
  owner?: string
  options?: { upsert?: boolean }
  signal?: AbortSignal
}

export interface VerifyObjectSignatureInput {
  token: string
  objectName: string
  signal?: AbortSignal
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
    const bucket = await this.db.asSuperUser().findBucketById({
      bucketId: this.bucketId,
      columns: 'id, file_size_limit, allowed_mime_types',
    })

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
   * @param input
   */
  async deleteObject(input: DeleteObjectInput) {
    const { objectName, signal } = input
    const obj = await this.db.withTransaction(async (db) => {
      const obj = await db.asSuperUser().findObject({
        bucketId: this.bucketId,
        objectName,
        columns: 'id,version',
        filters: { forUpdate: true },
      })

      const deleted = await db.deleteObject({ bucketId: this.bucketId, objectName })

      if (!deleted) {
        throw ERRORS.NoSuchKey(objectName)
      }

      await this.backend.remove({
        bucket: this.location.getRootLocation(),
        key: this.location.getKeyLocation({
          tenantId: this.db.tenantId,
          bucketId: this.bucketId,
          objectName,
        }),
        version: obj.version,
        signal,
      })

      return obj
    })

    await ObjectRemoved.sendWebhook({
      tenant: this.db.tenant(),
      name: objectName,
      version: obj.version,
      bucketId: this.bucketId,
      reqId: this.db.reqId,
      metadata: obj.metadata,
    })
  }

  /**
   * Deletes multiple objects from the remote storage
   * and the database
   * @param input
   */
  async deleteObjects(input: DeleteObjectsInput) {
    const { prefixes, signal } = input
    let results: { name: string }[] = []

    for (let i = 0; i < prefixes.length; ) {
      const prefixesSubset: string[] = []
      let urlParamLength = 0

      for (; i < prefixes.length && urlParamLength < requestUrlLengthLimit; i++) {
        const prefix = prefixes[i]
        prefixesSubset.push(prefix)
        urlParamLength += encodeURIComponent(prefix).length + 9 // length of '%22%2C%22'
      }

      await this.db.withTransaction(async (db) => {
        const data = await db.deleteObjects({
          bucketId: this.bucketId,
          objectNames: prefixesSubset,
          by: 'name',
        })

        if (data.length > 0) {
          results = results.concat(data)

          // if successfully deleted, delete from s3 too
          // todo: consider moving this to a queue
          const prefixesToDelete = data.reduce((all, { name, version }) => {
            all.push(
              this.location.getKeyLocation({
                tenantId: db.tenantId,
                bucketId: this.bucketId,
                objectName: name,
                version,
              })
            )

            if (version) {
              all.push(
                this.location.getKeyLocation({
                  tenantId: db.tenantId,
                  bucketId: this.bucketId,
                  objectName: name,
                  version,
                }) + '.info'
              )
            }
            return all
          }, [] as string[])

          await this.backend.removeMany({
            bucket: this.location.getRootLocation(),
            prefixes: prefixesToDelete,
            signal,
          })

          await Promise.allSettled(
            data.map((object) =>
              ObjectRemoved.sendWebhook({
                tenant: db.tenant(),
                name: object.name,
                bucketId: this.bucketId,
                reqId: this.db.reqId,
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
   * @param input
   */
  async updateObjectMetadata(input: UpdateObjectMetadataInput) {
    const { objectName, metadata } = input
    mustBeValidKey(objectName)

    const result = await this.db.updateObjectMetadata({
      bucketId: this.bucketId,
      objectName,
      metadata,
    })

    await ObjectUpdatedMetadata.sendWebhook({
      tenant: this.db.tenant(),
      name: objectName,
      version: result.version,
      bucketId: this.bucketId,
      metadata,
      reqId: this.db.reqId,
    })

    return result
  }

  /**
   * Updates the owner of an object in the database
   * @param input
   */
  updateObjectOwner(input: UpdateObjectOwnerInput) {
    const { objectName, owner } = input
    return this.db.updateObjectOwner({ bucketId: this.bucketId, objectName, owner })
  }

  /**
   * Finds an object by name
   * @param input
   */
  async findObject(input: FindObjectInput) {
    const { objectName, columns = 'id', filters } = input
    mustBeValidKey(objectName)

    return this.db.findObject({ bucketId: this.bucketId, objectName, columns, filters })
  }

  /**
   * Find multiple objects by name
   * @param input
   */
  async findObjects(input: FindObjectsInput) {
    const { objectNames, columns = 'id' } = input
    return this.db.findObjects({ bucketId: this.bucketId, objectNames, columns })
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
   * @param signal
   */
  async copyObject({
    sourceKey,
    destinationBucket,
    destinationKey,
    owner,
    conditions,
    copyMetadata,
    upsert,
    metadata: fileMetadata,
    userMetadata,
    signal,
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
    const originObject = await this.db.findObject({
      bucketId: this.bucketId,
      objectName: sourceKey,
      columns: 'bucket_id,metadata,user_metadata,version',
    })

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const baseMetadata = originObject.metadata || {}
    const destinationMetadata = copyMetadata
      ? baseMetadata
      : {
          ...baseMetadata,
          ...(fileMetadata || {}),
        }

    await this.uploader.canUpload({
      bucketId: destinationBucket,
      objectName: destinationKey,
      owner,
      isUpsert: upsert,
    })

    try {
      const copyResult = await this.backend.copy({
        bucket: this.location.getRootLocation(),
        source: s3SourceKey,
        version: originObject.version,
        destination: s3DestinationKey,
        destinationVersion: newVersion,
        metadata: destinationMetadata,
        conditions,
        signal,
      })

      const metadata = await this.backend.stats({
        bucket: this.location.getRootLocation(),
        key: s3DestinationKey,
        version: newVersion,
        signal,
      })

      const destinationObject = await this.db.asSuperUser().withTransaction(async (db) => {
        await db.waitObjectLock({
          bucketId: destinationBucket,
          objectName: destinationKey,
          timeout: 3000,
        })

        const existingDestObject = await db.findObject({
          bucketId: destinationBucket,
          objectName: destinationKey,
          columns: 'id,name,metadata,version,bucket_id',
          filters: {
            dontErrorOnEmpty: true,
            forUpdate: true,
          },
        })

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
          user_metadata: copyMetadata ? originObject.user_metadata : userMetadata,
          version: newVersion,
        })

        if (existingDestObject) {
          await ObjectAdminDelete.send({
            name: existingDestObject.name,
            bucketId: existingDestObject.bucket_id,
            tenant: this.db.tenant(),
            version: existingDestObject.version,
            reqId: this.db.reqId,
          })
        }

        return destinationObject
      })

      await ObjectCreatedCopyEvent.sendWebhook({
        tenant: this.db.tenant(),
        name: destinationKey,
        version: newVersion,
        bucketId: this.bucketId,
        metadata,
        reqId: this.db.reqId,
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
      })
      throw e
    }
  }

  /**
   * Moves an existing remote object to a given location
   * @param input
   */
  async moveObject(input: MoveObjectInput) {
    const { sourceObjectName, destinationBucket, destinationObjectName, owner, signal } = input
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

    await this.db.testPermission(
      (db) => {
        return Promise.all([
          db.findObject({ bucketId: this.bucketId, objectName: sourceObjectName, columns: 'id' }),
          db.updateObject({
            bucketId: this.bucketId,
            name: sourceObjectName,
            data: {
              name: destinationObjectName,
              version: newVersion,
              bucket_id: destinationBucket,
              owner,
            },
          }),
        ])
      },
      {
        signal,
      }
    )

    const sourceObj = await this.db.asSuperUser().findObject({
      bucketId: this.bucketId,
      objectName: sourceObjectName,
      columns: 'id, version,user_metadata',
    })

    if (s3SourceKey === s3DestinationKey) {
      return {
        destObject: sourceObj,
      }
    }

    try {
      await this.backend.copy({
        bucket: this.location.getRootLocation(),
        source: s3SourceKey,
        version: sourceObj.version,
        destination: s3DestinationKey,
        destinationVersion: newVersion,
        signal,
      })

      const metadata = await this.backend.stats({
        bucket: this.location.getRootLocation(),
        key: s3DestinationKey,
        version: newVersion,
        signal,
      })

      return this.db.asSuperUser().withTransaction(async (db) => {
        await db.waitObjectLock({
          bucketId: this.bucketId,
          objectName: destinationObjectName,
          timeout: 5000,
        })

        const sourceObject = await db.findObject({
          bucketId: this.bucketId,
          objectName: sourceObjectName,
          columns: 'id,version,metadata,user_metadata',
          filters: {
            forUpdate: true,
            dontErrorOnEmpty: false,
          },
        })

        await db.updateObject({
          bucketId: this.bucketId,
          name: sourceObjectName,
          data: {
            name: destinationObjectName,
            bucket_id: destinationBucket,
            version: newVersion,
            owner: owner,
            metadata,
            user_metadata: sourceObj.user_metadata,
          },
        })

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
            version: sourceObject.version,
            metadata: sourceObject.metadata,
          }),
          ObjectCreatedMove.sendWebhook({
            tenant: this.db.tenant(),
            name: destinationObjectName,
            version: newVersion,
            bucketId: this.bucketId,
            metadata: metadata,
            oldObject: {
              name: sourceObjectName,
              bucketId: this.bucketId,
              reqId: this.db.reqId,
              version: sourceObject.version,
            },
            reqId: this.db.reqId,
          }),
        ])

        return {
          destObject: {
            id: sourceObject.id,
            name: destinationObjectName,
            bucket_id: destinationBucket,
            version: newVersion,
            owner: owner,
            metadata,
          },
        }
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
  async searchObjects(input: SearchObjectsInput) {
    let { prefix } = input
    const { options } = input
    if (prefix.length > 0 && !prefix.endsWith('/')) {
      // assuming prefix is always a folder
      prefix = `${prefix}/`
    }

    return this.db.searchObjects({ bucketId: this.bucketId, prefix, options })
  }

  async listObjectsV2(input: ListObjectsV2Input = {}): Promise<ListObjectsV2Result> {
    const limit = Math.min(input.maxKeys || 1000, 1000)
    const prefix = input.prefix || ''
    const delimiter = input.delimiter

    const cursor = input.cursor ? decodeContinuationToken(input.cursor) : undefined
    let searchResult = await this.db.listObjectsV2({
      bucketId: this.bucketId,
      options: {
        prefix: input.prefix,
        delimiter: input.delimiter,
        maxKeys: limit + 1,
        nextToken: cursor?.startAfter,
        startAfter: cursor?.startAfter || input.startAfter,
        sortBy: {
          order: cursor?.sortOrder || input.sortBy?.order,
          column: cursor?.sortColumn || input.sortBy?.column,
          after: cursor?.sortColumnAfter,
        },
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

    let isTruncated = false

    if (searchResult.length > limit) {
      searchResult = searchResult.slice(0, limit)
      isTruncated = true
    }

    const folders: Obj[] = []
    const objects: Obj[] = []
    searchResult.forEach((obj) => {
      const target = obj.id === null ? folders : objects
      const name = obj.id === null && !obj.name.endsWith('/') ? obj.name + '/' : obj.name
      target.push({
        ...obj,
        name: input.encodingType === 'url' ? encodeURIComponent(name) : name,
      })
    })

    let nextContinuationToken: string | undefined
    let nextCursorKey: string | undefined

    if (isTruncated) {
      const sortColumn = (cursor?.sortColumn || input.sortBy?.column) as
        | 'name'
        | 'created_at'
        | 'updated_at'
        | undefined

      nextContinuationToken = encodeContinuationToken({
        startAfter: searchResult[searchResult.length - 1].name,
        sortOrder: cursor?.sortOrder || input.sortBy?.order,
        sortColumn,
        sortColumnAfter:
          sortColumn && sortColumn !== 'name' && searchResult[searchResult.length - 1][sortColumn]
            ? new Date(searchResult[searchResult.length - 1][sortColumn] || '').toISOString()
            : undefined,
      })
      nextCursorKey = searchResult[searchResult.length - 1].name
    }

    return {
      hasNext: isTruncated,
      nextCursor: nextContinuationToken,
      nextCursorKey,
      folders: folders,
      objects: objects,
    }
  }

  /**
   * Generates a signed url for accessing an object securely
   * @param objectName
   * @param url
   * @param expiresIn seconds
   * @param metadata
   */
  async signObjectUrl(input: SignObjectUrlInput) {
    const { objectName, url, expiresIn } = input
    let { metadata } = input
    await this.findObject({ objectName })

    metadata = Object.keys(metadata || {}).reduce((all, key) => {
      if (!all[key]) {
        delete all[key]
      }
      return all
    }, metadata || {})

    // security-in-depth: as signObjectUrl could be used as a signing oracle,
    // make sure it's never able to specify a role JWT claim
    delete metadata['role']

    const urlParts = url.split('/')
    const urlToSign = decodeURI(urlParts.splice(3).join('/'))
    const { urlSigningKey } = await getJwtSecret(this.db.tenantId)
    const token = await signJWT({ url: urlToSign, ...metadata }, urlSigningKey, expiresIn)

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
  async signObjectUrls(input: SignObjectUrlsInput) {
    const { paths, expiresIn } = input
    let results: { name: string }[] = []

    for (let i = 0; i < paths.length; ) {
      const pathsSubset = []
      let urlParamLength = 0

      for (; i < paths.length && urlParamLength < requestUrlLengthLimit; i++) {
        const path = paths[i]
        pathsSubset.push(path)
        urlParamLength += encodeURIComponent(path).length + 9 // length of '%22%2C%22'
      }

      const objects = await this.findObjects({ objectNames: pathsSubset, columns: 'name' })
      results = results.concat(objects)
    }

    const nameSet = new Set(results.map(({ name }) => name))

    const { urlSigningKey } = await getJwtSecret(this.db.tenantId)

    return Promise.all(
      paths.map(async (path) => {
        let error = null
        let signedURL = null
        if (nameSet.has(path)) {
          const urlToSign = `${this.bucketId}/${path}`
          const token = await signJWT({ url: urlToSign }, urlSigningKey, expiresIn)
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
  async signUploadObjectUrl(input: SignUploadObjectUrlInput) {
    const { objectName, url, expiresIn, owner, options } = input
    // check if user has INSERT permissions
    await this.uploader.canUpload({
      bucketId: this.bucketId,
      objectName,
      owner,
      isUpsert: options?.upsert ?? false,
    })

    const { urlSigningKey } = await getJwtSecret(this.db.tenantId)
    const token = await signJWT(
      { owner, url, upsert: Boolean(options?.upsert) },
      urlSigningKey,
      expiresIn
    )

    return { url: `/object/upload/sign/${url}?token=${token}`, token }
  }

  /**
   * Verify the signature for a specific object
   * @param token
   * @param objectName
   */
  async verifyObjectSignature(input: VerifyObjectSignatureInput) {
    const { token, objectName } = input
    const { secret: jwtSecret, jwks } = await getJwtSecret(this.db.tenantId)

    let payload: SignedUploadToken
    try {
      payload = (await verifyJWT(token, jwtSecret, jwks)) as SignedUploadToken
    } catch (e) {
      const err = e as Error
      throw ERRORS.InvalidJWT(err)
    }

    const { url, exp } = payload

    if (url !== `${this.bucketId}/${objectName}`) {
      throw ERRORS.InvalidSignature()
    }

    if (exp * 1000 < Date.now()) {
      throw ERRORS.ExpiredSignature()
    }

    return payload
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
      throw new Error('Invalid continuation token')
    }
    result[CONTINUATION_TOKEN_PART_MAP[partMatch[1]]] = partMatch[2]
  }
  return result
}
