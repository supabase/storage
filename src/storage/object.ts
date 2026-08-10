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
import {
  CanUploadMetadata,
  fileUploadFromRequest,
  toVersionId,
  Uploader,
  UploadRequest,
  upsertObjectForVersioningStatus,
} from './uploader'

export type DeleteObjectsEntry = string | { name: string; versionId: string }

interface CopyObjectParams {
  sourceKey: string
  sourceVersionId?: string
  destinationBucket: string
  destinationKey: string
  owner?: string
  copyMetadata?: boolean
  preserveUnspecifiedFileMetadata?: boolean
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
      .findBucketById(this.bucketId, 'id, file_size_limit, allowed_mime_types, versioning_status')

    const uploadRequest = await fileUploadFromRequest(request, {
      objectName: file.objectName,
      fileSizeLimit: bucket.file_size_limit,
      allowedMimeTypes: bucket.allowed_mime_types || [],
    })

    const result = await this.uploadNewObject({
      file: uploadRequest,
      objectName: file.objectName,
      owner: file.owner,
      isUpsert: Boolean(file.isUpsert),
      signal: file.signal,
      userMetadata: uploadRequest.userMetadata,
      versioningStatus: bucket.versioning_status ?? 'DISABLED',
    })

    // A bucket that's never had versioning enabled has no version identity to
    // report at all (matches real S3, which omits x-amz-version-id entirely
    // until versioning has been configured at least once) - "null" is only
    // meaningful once a bucket has been 'ENABLED'/'SUSPENDED'.
    return {
      ...result,
      versionId: bucket.versioning_status === 'DISABLED' ? undefined : result.versionId,
    }
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

    return { objectMetadata: metadata, path, id: obj.id, versionId: toVersionId(obj) }
  }

  /**
   * Deletes an object from the remote storage and the database.
   *
   * S3 semantics: a DELETE with a specific versionId permanently removes
   * that exact version (and its backend bytes) regardless of the bucket's
   * versioning_status - a row-identity operation, not a versioning-state
   * one. Without a versionId: on a bucket that's never had versioning
   * enabled, DELETE really does remove the content - the pre-versioning
   * behavior below, unchanged. On an 'ENABLED'/'SUSPENDED' bucket, DELETE
   * with no versionId never removes content at all - it always creates (or,
   * while suspended, revives/overwrites the single null-version slot with) a
   * delete marker instead, even if the key doesn't currently exist.
   * @param objectName
   * @param owner
   * @param versionId
   */
  async deleteObject(objectName: string, owner?: string, versionId?: string) {
    if (versionId) {
      const deleted = await this.deleteObjectVersion(objectName, versionId)
      return { versionId: toVersionId(deleted), isDeleteMarker: deleted.is_delete_marker }
    }

    const bucket = await this.db
      .asSuperUser()
      .findBucketById(this.bucketId, 'id, versioning_status')
    const versioningStatus = bucket.versioning_status ?? 'DISABLED'

    if (versioningStatus === 'ENABLED' || versioningStatus === 'SUSPENDED') {
      const marker = await this.deleteObjectWithMarker(objectName, versioningStatus, owner)
      return { versionId: toVersionId(marker), isDeleteMarker: true }
    }

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

    // A bucket that's never had versioning enabled has no version identity
    // to report at all - same reasoning as uploadFromRequest's VersionId.
    return undefined
  }

  /**
   * Creates a delete marker in place of removing content - this is exactly
   * one of the Database write primitives' archive-or-revive path (see
   * upsertObjectForVersioningStatus), with empty content and
   * is_delete_marker = true instead of real bytes, not a separate operation.
   * No backend bytes are touched: the previous version's content stays
   * exactly where it is, just no longer current.
   */
  private async deleteObjectWithMarker(
    objectName: string,
    versioningStatus: 'ENABLED' | 'SUSPENDED',
    owner?: string
  ) {
    const db = this.db.asSuperUser()

    return db.withTransaction(async (db) => {
      await db.waitObjectLock(this.bucketId, objectName, undefined, {
        timeout: 5000,
      })

      return upsertObjectForVersioningStatus(db, versioningStatus, {
        bucket_id: this.bucketId,
        name: objectName,
        owner,
        metadata: null,
        user_metadata: null,
        version: randomUUID(),
        is_delete_marker: true,
      })
    })
  }

  /**
   * Hard-deletes one specific version permanently, backend bytes included -
   * a row-identity operation independent of the bucket's versioning_status
   * (see Database.deleteObject for the "promote most-recent remaining
   * version to current, if the deleted row was current" behavior). A
   * delete marker has no backend content, so there's nothing to remove there.
   * Returns the raw deleted row - callers (singular deleteObject, bulk
   * deleteObjectVersions) decorate it however their response shape needs.
   */
  private async deleteObjectVersion(objectName: string, versionId: string) {
    return this.db.withTransaction(async (db) => {
      await db.waitObjectLock(this.bucketId, objectName, undefined, {
        timeout: 5000,
      })

      const deleted = await db.deleteObject(this.bucketId, objectName, versionId)

      if (!deleted) {
        throw ERRORS.NoSuchKey(objectName)
      }

      if (!deleted.is_delete_marker) {
        await this.backend.deleteObject(
          this.location.getRootLocation(),
          this.location.getKeyLocation({
            tenantId: this.db.tenantId,
            bucketId: this.bucketId,
            objectName,
          }),
          deleted.version
        )
      }

      return deleted
    })
  }

  /**
   * Deletes multiple objects from the remote storage and the database. Each
   * entry is either a plain name (soft-deletes on a versioned bucket, same
   * as deleteObject with no versionId) or a {name, versionId} pair (hard-
   * deletes that exact version, same as deleteObject with a versionId) -
   * mirrors real S3's own DeleteObjects, whose Delete.Objects entries are
   * {Key, VersionId?} for exactly the same reason: bulk "delete these named
   * files" and bulk "permanently delete these specific versions" are
   * different operations that just happen to share a batch endpoint.
   * @param entries
   * @param owner
   */
  async deleteObjects(entries: DeleteObjectsEntry[], owner?: string) {
    const normalized: { name: string; versionId?: string }[] = entries.map((entry) =>
      typeof entry === 'string' ? { name: entry } : entry
    )
    const versionedEntries = normalized.filter(
      (entry): entry is { name: string; versionId: string } => Boolean(entry.versionId)
    )
    const plainNames = normalized.filter((entry) => !entry.versionId).map((entry) => entry.name)

    // dontErrorOnEmpty - deleting from a bucket that doesn't exist has always
    // been a graceful no-op (the plain DELETE below just matches zero rows),
    // not an error - matches every other call site's tolerance for a missing
    // bucket on this path. Fetched once up front since both branches below
    // need it to dispatch to the right delete primitive.
    const bucket = await this.db
      .asSuperUser()
      .findBucketById(this.bucketId, 'id, versioning_status', {
        dontErrorOnEmpty: true,
      })
    const versioningStatus = bucket?.versioning_status ?? 'DISABLED'

    const results: Obj[] = []

    if (versionedEntries.length > 0) {
      results.push(...(await this.deleteObjectVersions(versionedEntries)))
    }

    if (plainNames.length > 0) {
      if (versioningStatus === 'ENABLED' || versioningStatus === 'SUSPENDED') {
        results.push(...(await this.deleteObjectsWithMarker(plainNames, versioningStatus, owner)))
      } else {
        results.push(...(await this.deleteObjectsHard(plainNames)))
      }
    }

    return results
  }

  /**
   * Hard-deletes every row for each of these names, regardless of
   * versioning_status - the pre-versioning bulk-delete behavior, used as-is
   * for a 'DISABLED' bucket (see deleteObjectsWithMarker for the versioned
   * equivalent).
   */
  private async deleteObjectsHard(prefixes: string[]) {
    const results: Obj[] = []

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
   * Bulk hard-delete of specific versions - same row-identity semantics as
   * the singular deleteObjectVersion (including promoting the next-most-
   * recent version to current if the deleted row was current), just applied
   * to many (name, versionId) pairs at once, batched the same
   * bounded-concurrency way as deleteObjectsWithMarker.
   */
  private async deleteObjectVersions(entries: { name: string; versionId: string }[]) {
    const results: Obj[] = []

    for (let i = 0; i < entries.length; i += MAX_OBJECTS_PER_DELETE_BATCH) {
      const batch = entries.slice(i, i + MAX_OBJECTS_PER_DELETE_BATCH)

      const deleted = await Promise.all(
        batch.map((entry) => this.deleteObjectVersion(entry.name, entry.versionId))
      )

      results.push(...deleted)
    }

    return results
  }

  /**
   * Bulk-delete on a versioned bucket - soft-delete each key by creating a
   * delete marker (same primitive as the singular deleteObject's no-versionId
   * path), instead of the hard-delete-every-row loop above. No backend bytes
   * are touched: a delete marker has no content, and the previous current
   * row's bytes stay exactly where they are.
   */
  private async deleteObjectsWithMarker(
    prefixes: string[],
    versioningStatus: 'ENABLED' | 'SUSPENDED',
    owner?: string
  ) {
    const results: Obj[] = []

    for (let i = 0; i < prefixes.length; i += MAX_OBJECTS_PER_DELETE_BATCH) {
      const prefixesSubset = prefixes.slice(i, i + MAX_OBJECTS_PER_DELETE_BATCH)

      const markers = await Promise.all(
        prefixesSubset.map((name) => this.deleteObjectWithMarker(name, versioningStatus, owner))
      )

      results.push(...markers)
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
   * Finds a specific version of an object by name and version id
   * @param objectName
   * @param version
   * @param columns
   * @param filters
   */
  async findObjectVersion(
    objectName: string,
    version: string,
    columns = 'id',
    filters?: FindObjectFilters
  ) {
    mustBeValidKey(objectName)

    return this.db.findObjectVersion(this.bucketId, objectName, version, columns, filters)
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
   * @param preserveUnspecifiedFileMetadata
   * @param upsert
   * @param fileMetadata
   * @param userMetadata
   */
  async copyObject({
    sourceKey,
    sourceVersionId,
    destinationBucket,
    destinationKey,
    owner,
    conditions,
    copyMetadata = true,
    preserveUnspecifiedFileMetadata,
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
    const originObject = sourceVersionId
      ? await this.db.findObjectVersion(
          this.bucketId,
          sourceKey,
          sourceVersionId,
          'bucket_id,metadata,user_metadata,version,is_delete_marker'
        )
      : await this.db.findObject(
          this.bucketId,
          sourceKey,
          'bucket_id,metadata,user_metadata,version,is_delete_marker'
        )

    // A delete marker has no content to copy - S3 treats it as if the key
    // doesn't exist for this version.
    if (originObject.is_delete_marker) {
      throw ERRORS.NoSuchKey(sourceKey)
    }

    const baseMetadata = originObject.metadata || {}
    const destinationMetadata = { ...baseMetadata }

    if (!copyMetadata) {
      if (!preserveUnspecifiedFileMetadata) {
        delete destinationMetadata.cacheControl
        delete destinationMetadata.mimetype
      }

      if (fileMetadata?.cacheControl !== undefined) {
        destinationMetadata.cacheControl = fileMetadata.cacheControl
      }
      if (fileMetadata?.mimetype !== undefined) {
        destinationMetadata.mimetype = fileMetadata.mimetype
      }
    }

    const destinationUserMetadata = copyMetadata ? originObject.user_metadata : userMetadata

    const destBucket = await this.db
      .asSuperUser()
      .findBucketById(destinationBucket, 'id, versioning_status')
    const versioningStatus = destBucket.versioning_status ?? 'DISABLED'

    await this.uploader.canUpload({
      bucketId: destinationBucket,
      objectName: destinationKey,
      owner,
      isUpsert: upsert,
      userMetadata: destinationUserMetadata || undefined,
      metadata: destinationMetadata,
      versioningStatus,
    })

    try {
      const copyResult = await this.backend.copyObject(
        this.location.getRootLocation(),
        s3SourceKey,
        originObject.version,
        s3DestinationKey,
        newVersion,
        destinationMetadata,
        conditions,
        { copyMetadata }
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
          'id,name,metadata,version,bucket_id,is_versioned',
          {
            dontErrorOnEmpty: true,
            forUpdate: true,
          }
        )

        const destinationObject = await upsertObjectForVersioningStatus(db, versioningStatus, {
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

        // upsertObject archives the previous destination row (keeping its backend
        // content referenced) whenever versioningStatus is 'ENABLED', or the
        // previous row was itself a real version - only an in-place overwrite
        // actually discards it, so only then is it safe to delete its backend bytes
        const wasArchived =
          Boolean(existingDestObject) &&
          (versioningStatus === 'ENABLED' || Boolean(existingDestObject?.is_versioned))

        if (existingDestObject && !wasArchived) {
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
    owner?: string,
    sourceVersionId?: string
  ) {
    // Moving a specific historical version can't reuse the in-place rename
    // below (it mutates the source row directly) - the source row's
    // identity must stay intact until the copy at the destination is
    // durable, so this is copy-then-hard-delete-the-source-version instead.
    // "Restore" is just this with destinationObjectName === sourceObjectName.
    if (sourceVersionId) {
      return this.moveObjectVersion(
        sourceObjectName,
        sourceVersionId,
        destinationBucket,
        destinationObjectName,
        uploadType,
        owner
      )
    }

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

    await this.db.testPermission(async (db) => {
      // Sequential, not Promise.all - updateObject renames the row in place,
      // so if it ran first, findObject's lookup by the old name would find
      // nothing (both run in the same transaction, so the rename is visible
      // to it immediately). Read-before-write, not concurrent.
      await db.findObject(this.bucketId, sourceObjectName, 'id')
      await db.updateObject(this.bucketId, sourceObjectName, {
        name: destinationObjectName,
        version: newVersion,
        bucket_id: destinationBucket,
        owner,
      })
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
   * Moves one specific historical version to a destination key: put the
   * version's content at the destination (via copyObject), then
   * hard-delete the source version's row - see moveObject for why this
   * can't reuse the in-place rename it does for a plain move.
   */
  private async moveObjectVersion(
    sourceObjectName: string,
    sourceVersionId: string,
    destinationBucket: string,
    destinationObjectName: string,
    uploadType: 'standard' | 's3' | 'resumable',
    owner?: string
  ) {
    const copyResult = await this.copyObject({
      sourceKey: sourceObjectName,
      sourceVersionId,
      destinationBucket,
      destinationKey: destinationObjectName,
      owner,
      uploadType,
      upsert: true,
      copyMetadata: true,
    })

    await this.deleteObject(sourceObjectName, owner, sourceVersionId)

    return { destObject: copyResult.destObject }
  }

  /**
   * Search objects by prefix
   * @param prefix
   * @param options
   */
  async searchObjects(prefix: string, options: SearchObjectOption) {
    if (!options.exactMatch && prefix.length > 0 && !prefix.endsWith('/')) {
      // assuming prefix is always a folder - exactMatch means the caller
      // wants this literal key, not a folder, so skip the normalization.
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
    noncurrentVersions?: 'exclude' | 'include' | 'only'
    deleteMarkers?: 'exclude' | 'include' | 'only'
    exactMatch?: boolean
  }): Promise<ListObjectsV2Result> {
    const limit = Math.min(options?.maxKeys || 1000, 1000)
    const prefix = options?.prefix || ''
    const delimiter = options?.delimiter
    const noncurrentVersions = options?.noncurrentVersions ?? 'exclude'
    const deleteMarkers = options?.deleteMarkers ?? 'exclude'
    // Only 'only'/'include' can ever produce >1 row for the same name - see
    // upsertObjectForVersioningStatus/the object-versioning migration for why
    // everything gated on this is otherwise a no-op.
    const multiRow = noncurrentVersions === 'only' || noncurrentVersions === 'include'

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
        afterVersion: cursor?.afterVersion,
      },
      noncurrentVersions,
      deleteMarkers,
      exactMatch: options?.exactMatch,
    })

    let prevPrefix = ''

    // exactMatch has no folders to collapse into - a single key can't be
    // split by the delimiter into a folder entry, it's returned as-is.
    if (delimiter && !options?.exactMatch) {
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
    const objects: ListObjectsV2Result['objects'] = []
    for (let index = 0; index < resultCount; index++) {
      const obj = searchResult[index]
      const name = obj.id === null && !obj.name.endsWith('/') ? obj.name + '/' : obj.name
      const encodedName = options?.encodingType === 'url' ? encodeURIComponent(name) : name

      if (obj.id === null) {
        folders.push({ ...obj, name: encodedName })
        continue
      }

      objects.push({
        ...obj,
        name: encodedName,
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

      // The tiebreak column pg.ts's multi-row seek predicates actually read is
      // always created_at, whether that's because it's the explicit sort
      // column (chronological order across the whole bucket) or because it's
      // the implicit within-name recency order (name-grouped order) - the
      // latter only matters once multiRow makes >1 row per name possible.
      const needsTiebreak = (sortColumn && sortColumn !== 'name') || multiRow
      const tiebreakColumn = sortColumn && sortColumn !== 'name' ? sortColumn : 'created_at'

      nextContinuationToken = encodeContinuationToken({
        startAfter: lastObject.name,
        sortOrder: cursor?.sortOrder || options?.sortBy?.order,
        sortColumn,
        sortColumnAfter:
          needsTiebreak && lastObject[tiebreakColumn]
            ? new Date(lastObject[tiebreakColumn] || '').toISOString()
            : undefined,
        afterVersion: multiRow ? lastObject.version : undefined,
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
    const bucket = await this.db
      .asSuperUser()
      .findBucketById(this.bucketId, 'id, versioning_status')

    // check if user has INSERT permissions
    await this.uploader.canUpload({
      bucketId: this.bucketId,
      objectName,
      owner,
      isUpsert: options?.upsert ?? false,
      userMetadata: options?.userMetadata,
      metadata: options?.metadata,
      versioningStatus: bucket.versioning_status ?? 'DISABLED',
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
  afterVersion?: string
}

const CONTINUATION_TOKEN_PART_MAP: Record<string, keyof ContinuationToken> = {
  l: 'startAfter',
  o: 'sortOrder',
  c: 'sortColumn',
  a: 'sortColumnAfter',
  v: 'afterVersion',
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
