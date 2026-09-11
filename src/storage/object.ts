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
import { ERRORS, ErrorCode, isStorageError, StorageBackendError } from '@internal/errors'
import { StorageObjectLocator } from '@storage/locator'
import {
  BucketVersioningStatus,
  OBJECT_LISTING_FILTER_MODES,
  Obj,
  ObjectListEntry,
  ObjectListingFilterMode,
} from '@storage/schemas'
import { FastifyRequest } from 'fastify/types/request'
import { StorageBackendAdapter } from './backend'
import {
  Database,
  DeleteMarkerOptions,
  FindObjectFilters,
  replacedContent,
  SearchObjectOption,
} from './database'
import {
  ObjectAdminDelete,
  ObjectCreatedCopyEvent,
  ObjectCreatedMove,
  ObjectRemoved,
  ObjectRemovedMove,
} from './events'
import {
  MAX_OBJECTS_PER_DELETE_BATCH,
  MAX_OBJECTS_PER_LOOKUP_BATCH,
  mustBeValidKey,
} from './limits'
import { CanUploadMetadata, fileUploadFromRequest, Uploader, UploadRequest } from './uploader'

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
export type DeleteObjectEntry = string | { path: string; versionId: string }

interface DeleteTargets {
  plainNames: string[]
  versionedEntries: { name: string; version: string }[]
}

interface LockedDeleteTargets {
  targets: DeleteTargets
  versioningStatus: BucketVersioningStatus
  /** Current row of each plain name that exists (possibly a delete marker). */
  plainObjects: Map<string, Obj>
  /** Exact rows of the version entries that exist. */
  versionedObjects: Obj[]
  missingPlainNames: string[]
}

interface AuthorizedDeleteTargets {
  plainNames: string[]
  versioned: { name: string; version: string }[]
}

interface AppliedDeletes {
  plain: Obj[]
  versioned: Obj[]
}

interface MoveTarget {
  sourceObjectName: string
  destinationBucket: string
  destinationObjectName: string
  newVersion: string
  owner?: string
}

interface MoveVersioningStatuses {
  source: BucketVersioningStatus
  destination: BucketVersioningStatus
}

type CopyResult = Awaited<ReturnType<StorageBackendAdapter['copyObject']>>

/** The object store no longer holds the bytes a copy was asked to read. */
function isMissingSourceError(error: unknown): boolean {
  if (error instanceof StorageBackendError) {
    return error.httpStatusCode === 404
  }
  return (error as { code?: string } | undefined)?.code === 'ENOENT'
}

function versionKey(name: string, version: string) {
  return `${name}\0${version}`
}

function toVersionTarget(object: Obj) {
  return { name: object.name, version: object.version as string }
}

function partitionDeleteEntries(entries: DeleteObjectEntry[]): DeleteTargets {
  const plainNames: string[] = []
  const versionedEntries: { name: string; version: string }[] = []
  const seenPlainNames = new Set<string>()
  const seenVersions = new Set<string>()

  for (const entry of entries) {
    if (typeof entry === 'string') {
      if (!seenPlainNames.has(entry)) {
        seenPlainNames.add(entry)
        plainNames.push(entry)
      }
    } else {
      const key = versionKey(entry.path, entry.versionId)
      if (!seenVersions.has(key)) {
        seenVersions.add(key)
        versionedEntries.push({ name: entry.path, version: entry.versionId })
      }
    }
  }

  return { plainNames, versionedEntries }
}

export interface ListObjectsV2Result {
  folders: ObjectListEntry[]
  objects: ObjectListEntry[]
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
   * Runs the RLS checks a move needs against the given (rolled back) database.
   * A versioned move is checked as an INSERT at the destination plus a DELETE
   * of the source, which is what the write path performs; an unversioned move
   * is checked as the in-place UPDATE it performs.
   */
  private async authorizeMove(
    db: Database,
    move: MoveTarget,
    sourceObject: Pick<Obj, 'version' | 'metadata' | 'user_metadata'>,
    statuses: MoveVersioningStatuses,
    isVersionedMove: boolean,
    metadata = sourceObject.metadata,
    userMetadata = sourceObject.user_metadata
  ) {
    if (!isVersionedMove) {
      return db.updateObject(
        this.bucketId,
        move.sourceObjectName,
        {
          name: move.destinationObjectName,
          version: move.newVersion,
          bucket_id: move.destinationBucket,
          owner: move.owner,
        },
        sourceObject.version ?? undefined
      )
    }

    // The source removal is probed first: a same-path move on a SUSPENDED
    // bucket rewrites the null-version row in place, so the destination write
    // would leave nothing under the source version.
    const authorizedDelete = await db.deleteObject(
      this.bucketId,
      move.sourceObjectName,
      sourceObject.version,
      { skipPromotion: true, versioningStatus: statuses.source, owner: move.owner }
    )
    if (!authorizedDelete) {
      // A policy filter and a row that vanished in the meantime both delete
      // nothing; only the former is an authorization failure.
      const stillThere = await db
        .asSuperUser()
        .findObject(
          this.bucketId,
          move.sourceObjectName,
          'id',
          { dontErrorOnEmpty: true },
          sourceObject.version ?? null
        )
      if (!stillThere) {
        throw ERRORS.NoSuchKey(move.sourceObjectName)
      }
      throw ERRORS.AccessDenied('Access denied')
    }

    await db.upsertObject(
      {
        bucket_id: move.destinationBucket,
        name: move.destinationObjectName,
        owner: move.owner,
        metadata,
        user_metadata: userMetadata,
        version: move.newVersion,
      },
      { versioningStatus: statuses.destination, probe: true }
    )
    return authorizedDelete
  }

  /**
   * Reads a bucket's versioning status under its shared lock, so the writes
   * that follow in the same transaction can pass it as a hint and a status
   * transition waits for them. Buckets on a schema without versioning are
   * DISABLED.
   */
  private async lockVersioningStatus(
    superUserDb: Database,
    bucketId: string
  ): Promise<BucketVersioningStatus> {
    if (!(await superUserDb.hasMigration('object-versioning-core'))) {
      return 'DISABLED'
    }
    const bucket = await superUserDb.findBucketById(bucketId, 'versioning_status', {
      forShare: true,
    })
    return bucket.versioning_status ?? 'DISABLED'
  }

  /**
   * Copies the source bytes to a new version. The source row is read without
   * a lock, so a concurrent hard delete or in-place overwrite can remove those
   * bytes first: the row is resolved again and the copy retried once with the
   * version it points at now; a source that is gone is reported as missing.
   */
  private async copySourceContent<T extends Pick<Obj, 'version'>>(params: {
    source: T
    resolveSource: () => Promise<T | undefined>
    sourceKey: string
    copy: (version: T['version']) => Promise<CopyResult>
  }): Promise<{ result: CopyResult; source: T }> {
    try {
      return { result: await params.copy(params.source.version), source: params.source }
    } catch (error) {
      if (!isMissingSourceError(error)) {
        throw error
      }
      const current = await params.resolveSource()
      if (!current || current.version === params.source.version) {
        throw ERRORS.NoSuchKey(params.sourceKey, error as Error)
      }
      try {
        return { result: await params.copy(current.version), source: current }
      } catch (retryError) {
        if (isMissingSourceError(retryError)) {
          throw ERRORS.NoSuchKey(params.sourceKey, retryError as Error)
        }
        throw retryError
      }
    }
  }

  /**
   * Reads the versioning status of the move's buckets in one statement.
   * With forShare the rows stay share-locked for the rest of the transaction,
   * so a status transition waits for the move and the writes can reuse the
   * status instead of locking again.
   */
  private async readMoveVersioningStatuses(
    superUserDb: Database,
    destinationBucket: string,
    filters: { forShare: boolean }
  ): Promise<MoveVersioningStatuses> {
    const buckets = await superUserDb.findBucketsById(
      [this.bucketId, destinationBucket],
      'id,versioning_status',
      filters
    )
    const statusOf = (bucketId: string): BucketVersioningStatus =>
      buckets.find((bucket) => bucket.id === bucketId)?.versioning_status ?? 'DISABLED'

    return { source: statusOf(this.bucketId), destination: statusOf(destinationBucket) }
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
   * @param versionId
   */
  async deleteObject(objectName: string, versionId?: string, options: DeleteMarkerOptions = {}) {
    const eventObject = await this.db.withTransaction((db) =>
      db.asSuperUser().withTransaction(async (superUserDb) => {
        const { obj, versioningStatus } = await this.lockObjectForDelete(
          superUserDb,
          objectName,
          versionId
        )

        const authorized = await db.testPermission((permissionDb) =>
          permissionDb.deleteObject(this.bucketId, objectName, obj?.version, {
            skipPromotion: true,
            versioningStatus,
            owner: options.owner,
          })
        )

        if (!authorized) {
          if (!obj) {
            throw ERRORS.NoSuchKey(objectName)
          }
          throw ERRORS.AccessDenied('Access denied')
        }

        const deleted = await superUserDb.deleteObject(this.bucketId, objectName, versionId, {
          versioningStatus,
          owner: options.owner,
        })

        if (!deleted) {
          throw ERRORS.AccessDenied('Access denied')
        }

        // A delete marker keeps every versioned row; the only bytes it frees
        // are those of the null-version row it replaced in place (SUSPENDED).
        // A hard delete frees the bytes of the row it removed.
        const isMarkerWrite = deleted.is_delete_marker && deleted.version !== obj?.version
        const freed = isMarkerWrite ? replacedContent(deleted) : obj

        if (freed) {
          await this.backend.deleteObject(
            this.location.getRootLocation(),
            this.location.getKeyLocation({
              tenantId: this.db.tenantId,
              bucketId: this.bucketId,
              objectName,
            }),
            freed.version ?? undefined
          )
        }

        return obj ?? deleted
      })
    )

    await ObjectRemoved.sendWebhook({
      tenant: this.db.tenant(),
      name: objectName,
      version: eventObject.version,
      bucketId: this.bucketId,
      reqId: this.db.reqId,
      sbReqId: this.db.sbReqId,
      metadata: eventObject.metadata,
    })
  }

  /**
   * Takes the advisory lock a delete needs and reads the row it acts on.
   *
   * Deleting the current row (no versionId, or the versionId of the current
   * version) locks the key: writing a delete marker or promoting the next
   * version changes what is current, and every writer serializes on that
   * lock. Deleting a non-current version locks only that version, since
   * nothing the current row depends on changes and other writes to the key
   * can proceed. The lock scope is decided from an unlocked read and
   * verified once the row is locked.
   */
  private async lockObjectForDelete(
    superUserDb: Database,
    objectName: string,
    versionId?: string
  ): Promise<{ obj: Obj | undefined; versioningStatus: BucketVersioningStatus }> {
    const lockAndRead = async (lockVersion?: string) => {
      await superUserDb.waitObjectLock(this.bucketId, objectName, lockVersion, { timeout: 5000 })
      const versioningStatus = await this.lockVersioningStatus(superUserDb, this.bucketId)
      const obj = await superUserDb.findObject(
        this.bucketId,
        objectName,
        'id,version,metadata,is_delete_marker,is_versioned,archived_at',
        { forUpdate: true, dontErrorOnEmpty: true },
        versionId
      )
      return { obj, versioningStatus }
    }

    if (versionId === undefined) {
      return lockAndRead()
    }

    const peeked = await superUserDb.findObject(
      this.bucketId,
      objectName,
      'archived_at',
      { dontErrorOnEmpty: true },
      versionId
    )
    if (!peeked) {
      throw ERRORS.NoSuchKey(objectName)
    }

    const isCurrent = !peeked.archived_at
    const locked = await lockAndRead(isCurrent ? undefined : versionId)
    if (!locked.obj) {
      throw ERRORS.NoSuchKey(objectName)
    }
    if (!locked.obj.archived_at !== isCurrent) {
      throw ERRORS.ResourceLocked(
        new Error('Object version changed state while acquiring its lock')
      )
    }

    return locked
  }

  /**
   * Deletes multiple objects from the remote storage
   * and the database. Each entry is either a bare path (delete whichever
   * row is currently at that path) or a {path, versionId} pair (delete that
   * exact version only).
   *
   * Each batch takes its locks in the same order as uploads and moves:
   * advisory locks on every key, then the bucket's shared status lock, then
   * row locks on the targeted rows.
   * @param entries
   */
  async deleteObjects(entries: DeleteObjectEntry[], options: DeleteMarkerOptions = {}) {
    const results: Obj[] = []

    for (let i = 0; i < entries.length; i += MAX_OBJECTS_PER_DELETE_BATCH) {
      const targets = partitionDeleteEntries(entries.slice(i, i + MAX_OBJECTS_PER_DELETE_BATCH))

      const deleted = await this.db.withTransaction((db) =>
        db.asSuperUser().withTransaction(async (superUserDb) => {
          const locked = await this.lockDeleteTargets(superUserDb, targets)
          const authorized = await this.authorizeDeleteTargets(db, locked, options)
          const applied = await this.applyDeleteTargets(superUserDb, locked, authorized, options)
          await this.cleanupDeletedObjects(superUserDb, locked, applied)
          return [...applied.plain, ...applied.versioned]
        })
      )

      results.push(...deleted)
    }

    return results
  }

  /**
   * Locks every targeted key and reads the rows the batch will act on: the
   * current row of each plain name and the exact row of each version entry.
   */
  private async lockDeleteTargets(
    superUserDb: Database,
    targets: DeleteTargets
  ): Promise<LockedDeleteTargets> {
    const names = [
      ...new Set([...targets.plainNames, ...targets.versionedEntries.map((entry) => entry.name)]),
    ]
    await superUserDb.waitObjectLocks(
      names.map((objectName) => ({ bucketId: this.bucketId, objectName })),
      { timeout: 5000 }
    )

    // Hold the bucket's shared status lock for the rest of the transaction so
    // the writes below can skip their own and a status transition has to wait.
    const bucket = (await superUserDb.hasMigration('object-versioning-core'))
      ? await superUserDb.findBucketById(this.bucketId, 'id,versioning_status', {
          forShare: true,
          dontErrorOnEmpty: true,
        })
      : undefined
    const versioningStatus: BucketVersioningStatus = bucket?.versioning_status ?? 'DISABLED'

    const rows = await superUserDb.findObjectTargets(
      this.bucketId,
      { names: targets.plainNames, versions: targets.versionedEntries },
      'name,version,metadata,is_delete_marker,is_versioned,archived_at',
      { forUpdate: true }
    )

    const plainNames = new Set(targets.plainNames)
    const versionKeys = new Set(
      targets.versionedEntries.map((entry) => versionKey(entry.name, entry.version))
    )
    const plainObjects = new Map<string, Obj>()
    const versionedObjects: Obj[] = []
    for (const row of rows) {
      if (!row.archived_at && plainNames.has(row.name)) {
        plainObjects.set(row.name, row)
      }
      if (row.version && versionKeys.has(versionKey(row.name, row.version))) {
        versionedObjects.push(row)
      }
    }

    return {
      targets,
      versioningStatus,
      plainObjects,
      versionedObjects,
      missingPlainNames: targets.plainNames.filter((name) => !plainObjects.has(name)),
    }
  }

  /**
   * Runs the RLS permission probes for the locked targets and returns the
   * subset the caller may delete.
   */
  private async authorizeDeleteTargets(
    db: Database,
    locked: LockedDeleteTargets,
    options: DeleteMarkerOptions
  ): Promise<AuthorizedDeleteTargets> {
    const { versioningStatus } = locked
    const existingPlainNames = [...locked.plainObjects.keys()]
    const authorizedPlainObjects =
      existingPlainNames.length > 0
        ? await db.testPermission((permissionDb) =>
            permissionDb.deleteObjects(this.bucketId, existingPlainNames, 'name', {
              skipDeleteMarkers: true,
            })
          )
        : []
    const authorizedPlainNames = new Set(authorizedPlainObjects.map((object) => object.name))

    if (versioningStatus !== 'DISABLED' && locked.missingPlainNames.length > 0) {
      for (const name of await this.authorizeDeleteMarkers(
        db,
        locked.missingPlainNames,
        versioningStatus,
        options
      )) {
        authorizedPlainNames.add(name)
      }
    }

    const authorizedVersionedObjects =
      locked.versionedObjects.length > 0
        ? await db.testPermission((permissionDb) =>
            permissionDb.deleteObjectVersions(
              this.bucketId,
              locked.versionedObjects.map(toVersionTarget),
              { skipPromotion: true }
            )
          )
        : []

    return {
      plainNames: locked.targets.plainNames.filter((name) => authorizedPlainNames.has(name)),
      versioned: authorizedVersionedObjects.map(toVersionTarget),
    }
  }

  /**
   * Deleting a missing key on a versioned bucket writes a delete marker, an
   * INSERT under RLS: a policy violation throws instead of filtering rows.
   * Probe all names at once and, only when that is rejected, each name on its
   * own so a rejection drops just that name instead of the whole batch.
   */
  private async authorizeDeleteMarkers(
    db: Database,
    names: string[],
    versioningStatus: BucketVersioningStatus,
    options: DeleteMarkerOptions
  ): Promise<string[]> {
    const probe = (targets: string[]) =>
      db.testPermission((permissionDb) =>
        permissionDb.deleteObjects(this.bucketId, targets, 'name', {
          versioningStatus,
          owner: options.owner,
        })
      )

    try {
      return (await probe(names)).map((marker) => marker.name)
    } catch (e) {
      if (!isStorageError(ErrorCode.AccessDenied, e)) {
        throw e
      }
    }

    if (names.length === 1) {
      return []
    }

    const authorized: string[] = []
    for (const name of names) {
      try {
        if ((await probe([name])).length > 0) {
          authorized.push(name)
        }
      } catch (e) {
        if (!isStorageError(ErrorCode.AccessDenied, e)) {
          throw e
        }
      }
    }
    return authorized
  }

  private async applyDeleteTargets(
    superUserDb: Database,
    locked: LockedDeleteTargets,
    authorized: AuthorizedDeleteTargets,
    options: DeleteMarkerOptions
  ): Promise<AppliedDeletes> {
    const plain =
      authorized.plainNames.length > 0
        ? await superUserDb.deleteObjects(this.bucketId, authorized.plainNames, 'name', {
            versioningStatus: locked.versioningStatus,
            owner: options.owner,
          })
        : []
    const versioned =
      authorized.versioned.length > 0
        ? await superUserDb.deleteObjectVersions(this.bucketId, authorized.versioned)
        : []

    return { plain, versioned }
  }

  /**
   * Removes the backend content that nothing preserves any more and emits the
   * removal webhooks. A delete marker hides the row that was current before
   * it, so that row is what gets reported and, unless versioning preserves
   * it, physically removed.
   */
  private async cleanupDeletedObjects(
    superUserDb: Database,
    locked: LockedDeleteTargets,
    applied: AppliedDeletes
  ) {
    if (applied.plain.length === 0 && applied.versioned.length === 0) {
      return
    }

    const replacedObject = (deleted: Obj) =>
      deleted.is_delete_marker ? locked.plainObjects.get(deleted.name) : undefined
    const eventObjects = [
      ...applied.plain.map((deleted) => replacedObject(deleted) ?? deleted),
      ...applied.versioned,
    ]

    // todo: consider moving this to a queue
    const freedContent: { name: string; version: string | null | undefined }[] = []
    for (const deleted of applied.plain) {
      if (!deleted.is_delete_marker) {
        freedContent.push({ name: deleted.name, version: deleted.version })
        continue
      }
      // A marker frees only the bytes of the null-version row it replaced in
      // place (SUSPENDED); versioned rows keep theirs.
      const replaced = replacedContent(deleted)
      if (replaced) {
        freedContent.push({ name: deleted.name, version: replaced.version })
      }
    }
    for (const deleted of applied.versioned) {
      freedContent.push({ name: deleted.name, version: deleted.version })
    }
    const prefixesToDelete = freedContent.flatMap(({ name, version }) => {
      const location = this.location.getKeyLocation({
        tenantId: superUserDb.tenantId,
        bucketId: this.bucketId,
        objectName: name,
        version: version ?? undefined,
      })

      return version ? [location, `${location}.info`] : [location]
    })

    if (prefixesToDelete.length > 0) {
      await this.backend.deleteObjects(this.location.getRootLocation(), prefixesToDelete)
    }

    await Promise.allSettled(
      eventObjects.map((object) =>
        ObjectRemoved.sendWebhook({
          tenant: superUserDb.tenant(),
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

  /**
   * Updates the owner of an object in the database
   * @param objectName
   * @param owner
   */
  updateObjectOwner(objectName: string, owner?: string) {
    return this.db.updateObjectOwner(this.bucketId, objectName, owner)
  }

  /**
   * Finds an object by name, optionally pinned to a specific version id
   * @param objectName
   * @param columns
   * @param filters
   * @param version
   */
  async findObject(
    objectName: string,
    columns = 'id',
    filters?: FindObjectFilters,
    version?: string
  ) {
    mustBeValidKey(objectName)

    return this.db.findObject(
      this.bucketId,
      objectName,
      columns,
      { ...filters, excludeDeleteMarkers: true },
      version
    )
  }

  /**
   * Find multiple objects by name
   * @param objectNames
   * @param columns
   */
  async findObjects(objectNames: string[], columns = 'id') {
    return this.db.findObjects(this.bucketId, objectNames, columns, {
      excludeDeleteMarkers: true,
    })
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
    const originObject = await this.findObject(
      sourceKey,
      'bucket_id,metadata,user_metadata,version',
      undefined,
      sourceVersionId
    )

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

    await this.uploader.canUpload({
      bucketId: destinationBucket,
      objectName: destinationKey,
      owner,
      isUpsert: upsert,
      userMetadata: destinationUserMetadata || undefined,
      metadata: destinationMetadata,
    })

    try {
      const { result: copyResult } = await this.copySourceContent({
        source: originObject,
        sourceKey,
        resolveSource: () =>
          this.db.findObject(
            this.bucketId,
            sourceKey,
            'bucket_id,metadata,user_metadata,version',
            { dontErrorOnEmpty: true, excludeDeleteMarkers: true },
            sourceVersionId
          ),
        copy: (version) =>
          this.backend.copyObject(
            this.location.getRootLocation(),
            s3SourceKey,
            version,
            s3DestinationKey,
            newVersion,
            destinationMetadata,
            conditions,
            { copyMetadata }
          ),
      })

      const metadata = await this.backend.headObject(
        this.location.getRootLocation(),
        s3DestinationKey,
        newVersion
      )

      const destinationObject = await this.db.withTransaction((scopedDb) =>
        scopedDb.asSuperUser().withTransaction(async (db) => {
          await db.waitObjectLock(destinationBucket, destinationKey, undefined, {
            timeout: 3000,
          })
          const versioningStatus = await this.lockVersioningStatus(db, destinationBucket)

          const existingDestObject = await db.findObject(
            destinationBucket,
            destinationKey,
            'id,name,metadata,version,bucket_id,is_delete_marker,is_versioned',
            {
              dontErrorOnEmpty: true,
              forUpdate: true,
            }
          )

          if (!upsert && existingDestObject && !existingDestObject.is_delete_marker) {
            throw ERRORS.KeyAlreadyExists(destinationKey)
          }

          await this.uploader.authorizeUpload(scopedDb, {
            bucketId: destinationBucket,
            objectName: destinationKey,
            owner,
            isUpsert: upsert,
            userMetadata: destinationUserMetadata ?? undefined,
            metadata: destinationMetadata,
            currentObjectIsDeleteMarker: existingDestObject?.is_delete_marker === true,
          })

          const destinationObject = await db.upsertObject(
            {
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
            },
            { versioningStatus }
          )

          // The write reports the row it replaced in place; its bytes are free.
          const replaced = replacedContent(destinationObject)
          if (replaced) {
            await ObjectAdminDelete.send({
              name: destinationKey,
              bucketId: destinationBucket,
              tenant: this.db.tenant(),
              version: replaced.version ?? undefined,
              reqId: this.db.reqId,
              sbReqId: this.db.sbReqId,
            })
          }

          return destinationObject
        })
      )

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
    mustBeValidKey(destinationObjectName)

    const move: MoveTarget = {
      sourceObjectName,
      destinationBucket,
      destinationObjectName,
      newVersion: randomUUID(),
      owner,
    }
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
    const isSamePath =
      this.bucketId === destinationBucket && sourceObjectName === destinationObjectName
    const isSamePathNoop = isSamePath && sourceVersionId === undefined
    const objectKeys = [
      { bucketId: this.bucketId, objectName: sourceObjectName },
      { bucketId: destinationBucket, objectName: destinationObjectName },
    ]

    // Authorize before copying backend data. This pass takes no locks: the
    // pass after the copy re-reads everything under the final locks and
    // rejects the move if the status snapshot or the source row changed.
    const statuses = await this.db.testPermission(async (db) => {
      const superUserDb = db.asSuperUser()
      const statuses = await this.readMoveVersioningStatuses(superUserDb, destinationBucket, {
        forShare: false,
      })
      const isVersionedMove = statuses.source !== 'DISABLED' || statuses.destination !== 'DISABLED'

      const sourceForPermCheck = await db.findObject(
        this.bucketId,
        sourceObjectName,
        'id, version, metadata, user_metadata',
        { excludeDeleteMarkers: true },
        sourceVersionId
      )

      const destinationForPermCheck = await superUserDb.findObject(
        destinationBucket,
        destinationObjectName,
        'is_delete_marker',
        { dontErrorOnEmpty: true }
      )
      if (destinationForPermCheck && !destinationForPermCheck.is_delete_marker && !isSamePath) {
        throw ERRORS.KeyAlreadyExists(destinationObjectName)
      }

      await this.authorizeMove(
        db,
        move,
        sourceForPermCheck,
        statuses,
        isVersionedMove && !isSamePathNoop
      )

      return statuses
    })

    let sourceObj = await this.db
      .asSuperUser()
      .findObject(
        this.bucketId,
        sourceObjectName,
        'id, version,user_metadata',
        { excludeDeleteMarkers: true },
        sourceVersionId
      )

    if (isSamePathNoop) {
      return {
        destObject: sourceObj,
      }
    }

    try {
      const copied = await this.copySourceContent({
        source: sourceObj,
        sourceKey: sourceObjectName,
        resolveSource: () =>
          this.db
            .asSuperUser()
            .findObject(
              this.bucketId,
              sourceObjectName,
              'id, version,user_metadata',
              { dontErrorOnEmpty: true, excludeDeleteMarkers: true },
              sourceVersionId
            ),
        copy: (version) =>
          this.backend.copyObject(
            this.location.getRootLocation(),
            s3SourceKey,
            version,
            s3DestinationKey,
            move.newVersion
          ),
      })
      sourceObj = copied.source

      const metadata = await this.backend.headObject(
        this.location.getRootLocation(),
        s3DestinationKey,
        move.newVersion
      )

      return this.db.withTransaction((db) =>
        db.asSuperUser().withTransaction(async (superUserDb) => {
          // Lock object keys before bucket rows, matching the write path's lock order.
          await superUserDb.waitObjectLocks(objectKeys, { timeout: 5000 })

          const lockedStatuses = await this.readMoveVersioningStatuses(
            superUserDb,
            destinationBucket,
            { forShare: true }
          )
          const lockedVersionedMove =
            lockedStatuses.source !== 'DISABLED' || lockedStatuses.destination !== 'DISABLED'

          // Revalidate the status snapshot used by the pre-copy authorization.
          if (
            lockedStatuses.source !== statuses.source ||
            lockedStatuses.destination !== statuses.destination
          ) {
            throw ERRORS.ResourceLocked(
              new Error('Bucket versioning status changed while preparing the move')
            )
          }

          const sourceObject = await superUserDb.findObject(
            this.bucketId,
            sourceObjectName,
            'id,version,metadata,user_metadata,is_versioned',
            {
              forUpdate: true,
              dontErrorOnEmpty: false,
              excludeDeleteMarkers: true,
            },
            sourceVersionId
          )

          if (sourceObject.id !== sourceObj.id || sourceObject.version !== sourceObj.version) {
            throw ERRORS.ResourceLocked(new Error('Source object changed while preparing the move'))
          }

          const existingDestObject = await superUserDb.findObject(
            destinationBucket,
            destinationObjectName,
            'name,bucket_id,version,is_delete_marker,is_versioned',
            {
              dontErrorOnEmpty: true,
              forUpdate: true,
            }
          )

          if (existingDestObject && !existingDestObject.is_delete_marker && !isSamePath) {
            throw ERRORS.KeyAlreadyExists(destinationObjectName)
          }

          await db.testPermission((permissionDb) =>
            this.authorizeMove(
              permissionDb,
              move,
              sourceObject,
              lockedStatuses,
              lockedVersionedMove,
              metadata,
              sourceObj.user_metadata
            )
          )

          let destObject: Obj
          let shouldDeleteSourceContent = true

          if (!lockedVersionedMove) {
            await superUserDb.updateObject(
              this.bucketId,
              sourceObjectName,
              {
                name: destinationObjectName,
                bucket_id: destinationBucket,
                version: move.newVersion,
                owner,
                metadata,
                user_metadata: sourceObj.user_metadata,
              },
              sourceVersionId
            )

            destObject = {
              ...sourceObject,
              name: destinationObjectName,
              bucket_id: destinationBucket,
              version: move.newVersion,
              owner,
              metadata,
            }
          } else {
            // Move is copy-then-delete, not a rename in place: the destination
            // write goes through upsertObject (same archiving as copyObject), and
            // the source removal goes through deleteObject (hard delete when
            // sourceVersionId is given, otherwise a delete-marker under
            // ENABLED/SUSPENDED, exactly like a regular delete).
            destObject = await superUserDb.upsertObject(
              {
                bucket_id: destinationBucket,
                name: destinationObjectName,
                owner,
                metadata,
                user_metadata: sourceObj.user_metadata,
                version: move.newVersion,
              },
              { versioningStatus: lockedStatuses.destination }
            )

            // The destination write reports the row it replaced in place.
            const replacedDestination = replacedContent(destObject)
            if (replacedDestination) {
              await ObjectAdminDelete.send({
                name: destinationObjectName,
                bucketId: destinationBucket,
                tenant: this.db.tenant(),
                version: replacedDestination.version ?? undefined,
                reqId: this.db.reqId,
                sbReqId: this.db.sbReqId,
              })
            }

            const deletedSource = await superUserDb.deleteObject(
              this.bucketId,
              sourceObjectName,
              sourceVersionId,
              { versioningStatus: lockedStatuses.source, owner }
            )

            const isMarkerWrite =
              deletedSource?.is_delete_marker && deletedSource.version !== sourceObject.version
            if (deletedSource && isMarkerWrite) {
              // A delete marker keeps every versioned row; it frees only the
              // bytes of the null-version row it replaced in place (SUSPENDED).
              shouldDeleteSourceContent = false
              const freed = replacedContent(deletedSource)
              if (freed) {
                await ObjectAdminDelete.send({
                  name: sourceObjectName,
                  bucketId: this.bucketId,
                  tenant: this.db.tenant(),
                  version: freed.version ?? undefined,
                  reqId: this.db.reqId,
                  sbReqId: this.db.sbReqId,
                })
              }
            }
          }

          if (shouldDeleteSourceContent) {
            await ObjectAdminDelete.send({
              name: sourceObjectName,
              bucketId: this.bucketId,
              tenant: this.db.tenant(),
              version: sourceObj.version,
              reqId: this.db.reqId,
              sbReqId: this.db.sbReqId,
            })
          }

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
              version: move.newVersion,
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

          return { destObject }
        })
      )
    } catch (e) {
      await ObjectAdminDelete.send({
        name: destinationObjectName,
        bucketId: destinationBucket,
        tenant: this.db.tenant(),
        version: move.newVersion,
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
    sortBy?: {
      column: 'name' | 'created_at' | 'updated_at'
      order?: string
    }
    noncurrentVersions?: ObjectListingFilterMode
    deleteMarkers?: ObjectListingFilterMode
    exactMatch?: boolean
    // Set by the S3-compatible route, which has no request field for
    // noncurrentVersions/deleteMarkers, this way we can reject a cursor that tries
    // to adds them in instead of silently trusting it.
    s3Compatible?: boolean
  }): Promise<ListObjectsV2Result> {
    const limit = Math.min(options?.maxKeys || 1000, 1000)
    const prefix = options?.prefix || ''
    const delimiter = options?.delimiter

    const cursor = options?.cursor
      ? decodeContinuationToken(
          options.cursor,
          options.s3Compatible ? S3_ALLOWED_CONTINUATION_TOKEN_KEYS : undefined,
          options.s3Compatible ? S3_ALLOWED_CONTINUATION_TOKEN_VALUES : undefined
        )
      : undefined

    const noncurrentVersions = resolveLockedListParam(
      'noncurrentVersions',
      cursor,
      options?.noncurrentVersions
    )
    const deleteMarkers = resolveLockedListParam('deleteMarkers', cursor, options?.deleteMarkers)
    const exactMatch =
      resolveLockedListParam('exactMatch', cursor, options?.exactMatch?.toString()) === 'true'
    if (
      exactMatch &&
      cursor &&
      options?.prefix !== undefined &&
      options.prefix !== cursor.startAfter
    ) {
      throw ERRORS.InvalidParameter('prefix', {
        message: `prefix must match the value used to obtain this continuation token (expected "${cursor.startAfter}")`,
      })
    }
    const multiRow = noncurrentVersions === 'only' || noncurrentVersions === 'include'
    let searchResult = await this.db.listObjectsV2(this.bucketId, {
      prefix: options?.prefix,
      delimiter: options?.delimiter,
      maxKeys: limit + 1,
      nextToken: cursor?.startAfter,
      startAfter: cursor?.startAfter || options?.startAfter,
      sortBy: {
        // Sort order keeps its existing behavior of silently preferring the cursor value.
        order: cursor?.sortOrder || options?.sortBy?.order,
        column: cursor?.sortColumn || options?.sortBy?.column,
        after: cursor?.sortColumnAfter,
        afterVersion: cursor?.afterVersion,
        afterArchivedAt: cursor?.afterArchivedAt,
      },
      noncurrentVersions,
      deleteMarkers,
      exactMatch,
    })

    let prevPrefix = ''

    // exactMatch has no folders to collapse into - a single key can't be
    // split by the delimiter into a folder entry, it's returned as-is.
    if (delimiter && !exactMatch) {
      const delimitedResults: ObjectListEntry[] = []
      for (const object of searchResult) {
        let idx = object.name.slice(prefix.length).indexOf(delimiter)

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
            bucket_id: this.bucketId,
            updated_at: object.updated_at,
            created_at: object.created_at,
            last_accessed_at: object.last_accessed_at,
          })
          continue
        }

        delimitedResults.push(object)
      }
      searchResult = delimitedResults
    }

    const isTruncated = searchResult.length > limit
    const resultCount = isTruncated ? limit : searchResult.length

    const folders: ObjectListEntry[] = []
    const objects: ObjectListEntry[] = []
    for (let index = 0; index < resultCount; index++) {
      const obj = searchResult[index]
      const target = obj.id === null ? folders : objects
      const name =
        obj.id === null && delimiter && !obj.name.endsWith(delimiter)
          ? obj.name + delimiter
          : obj.name
      target.push({
        ...obj,
        name,
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

      // Only an explicit non-name sortColumn needs a sortColumnAfter cursor -
      // the name-sort-with-multiRow case resumes via afterArchivedAt/afterVersion
      // below instead, since archived_at (not created_at) is the tiebreak
      // that actually governs version order for that case.
      const needsTiebreak = sortColumn && sortColumn !== 'name'

      nextContinuationToken = encodeContinuationToken({
        startAfter: lastObject.name,
        sortOrder: cursor?.sortOrder || options?.sortBy?.order,
        sortColumn,
        sortColumnAfter:
          needsTiebreak && lastObject[sortColumn]
            ? new Date(lastObject[sortColumn]).toISOString()
            : undefined,
        afterVersion: multiRow ? (lastObject.version ?? undefined) : undefined,
        afterArchivedAt: multiRow
          ? lastObject.archived_at
            ? new Date(lastObject.archived_at).toISOString()
            : 'infinity'
          : undefined,
        noncurrentVersions,
        deleteMarkers,
        exactMatch: exactMatch ? 'true' : undefined,
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
    metadata?: Record<string, string | object | undefined>,
    versionId?: string
  ) {
    await this.findObject(objectName, 'id', undefined, versionId)

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
    // `url`, `scope`, and `versionId` are spread last so attacker-controlled
    // metadata can never override the intended object path, token scope, or
    // pinned version (token-forgery defense)
    const token = await signJWT(
      {
        ...metadata,
        url: urlToSign,
        scope: SIGNED_URL_SCOPE_DOWNLOAD,
        ...(versionId ? { versionId } : {}),
      },
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
  afterVersion?: string
  afterArchivedAt?: string
  noncurrentVersions?: string
  deleteMarkers?: string
  exactMatch?: string // 'true' | 'false'
}

const S3_CONTINUATION_TOKEN_PART_MAP = {
  l: 'startAfter',
  // Accept legacy o:asc tokens. TODO: remove after old tokens expire.
  o: 'sortOrder',
} satisfies Record<string, keyof ContinuationToken>

const CONTINUATION_TOKEN_PART_MAP: Record<string, keyof ContinuationToken> = {
  ...S3_CONTINUATION_TOKEN_PART_MAP,
  c: 'sortColumn',
  a: 'sortColumnAfter',
  v: 'afterVersion',
  r: 'afterArchivedAt',
  n: 'noncurrentVersions',
  d: 'deleteMarkers',
  e: 'exactMatch',
}

const CONTINUATION_TOKEN_DEFAULTS = {
  // Keep default-valued fields out of newly issued tokens so an older pod can
  // decode tokens produced during a rolling deployment.
  sortOrder: 'asc',
  sortColumn: 'name',
  noncurrentVersions: 'exclude',
  deleteMarkers: 'exclude',
  exactMatch: 'false',
} satisfies Partial<Record<keyof ContinuationToken, string>>

// Sort options silently prefer cursor values instead of rejecting changed request values.
type StrictListParam = Exclude<keyof typeof CONTINUATION_TOKEN_DEFAULTS, 'sortOrder' | 'sortColumn'>

const isDefaultTokenParam = (
  key: keyof ContinuationToken
): key is keyof typeof CONTINUATION_TOKEN_DEFAULTS => key in CONTINUATION_TOKEN_DEFAULTS

/**
 * Locks noncurrentVersions/deleteMarkers/exactMatch to whatever a continuation
 * token already carries, since afterVersion/afterArchivedAt only mean "resume
 * mid-key" under the mode that produced them, and an exact-match listing must
 * not widen into a prefix scan once the key's versions are exhausted. The
 * cursor is undefined on the first page because no token exists yet; decoded
 * cursors restore omitted defaults before reaching this helper. `requested`
 * must be read before any default is applied, or an omitted filter becomes
 * indistinguishable from an explicitly resent default.
 *
 * @param name the locked option, also used in the thrown error's message
 * @param cursor the decoded continuation token, if any
 * @param requested the raw value from the caller's request, read before any default is applied
 */
function resolveLockedListParam<T extends string>(
  name: StrictListParam,
  cursor: ContinuationToken | undefined,
  requested: T | undefined
): T {
  const stored = cursor?.[name]
  if (stored === undefined) {
    return (requested ?? CONTINUATION_TOKEN_DEFAULTS[name]) as T
  }

  if (requested !== undefined && requested !== stored) {
    throw ERRORS.InvalidParameter(name, {
      message: `${name} must match the value used to obtain this continuation token (expected "${stored}")`,
    })
  }

  return stored as T
}

function encodeContinuationToken(tokenInfo: ContinuationToken) {
  let result = ''
  for (const [k, v] of Object.entries(CONTINUATION_TOKEN_PART_MAP)) {
    const value = tokenInfo[v]
    if (value && !(isDefaultTokenParam(v) && value === CONTINUATION_TOKEN_DEFAULTS[v])) {
      result += `${k}:${value}\n`
    }
  }
  return Buffer.from(result.slice(0, -1)).toString('base64')
}

const CONTINUATION_TOKEN_TRI_STATE_VALUES: ReadonlySet<string> = new Set(
  OBJECT_LISTING_FILTER_MODES
)
const CONTINUATION_TOKEN_BOOLEAN_VALUES: ReadonlySet<string> = new Set(['true', 'false'])
const CONTINUATION_TOKEN_SORT_ORDER_VALUES: ReadonlySet<string> = new Set(['asc', 'desc'])
const CONTINUATION_TOKEN_SORT_COLUMN_VALUES: ReadonlySet<string> = new Set([
  'name',
  'created_at',
  'updated_at',
])
const CONTINUATION_TOKEN_ALLOWED_VALUES: Partial<Record<string, ReadonlySet<string>>> = {
  o: CONTINUATION_TOKEN_SORT_ORDER_VALUES,
  c: CONTINUATION_TOKEN_SORT_COLUMN_VALUES,
  n: CONTINUATION_TOKEN_TRI_STATE_VALUES,
  d: CONTINUATION_TOKEN_TRI_STATE_VALUES,
  e: CONTINUATION_TOKEN_BOOLEAN_VALUES,
}

const S3_ALLOWED_CONTINUATION_TOKEN_KEYS = new Set(Object.keys(S3_CONTINUATION_TOKEN_PART_MAP))
const S3_ALLOWED_CONTINUATION_TOKEN_VALUES: Partial<Record<string, ReadonlySet<string>>> = {
  o: new Set(['asc']),
}

function decodeContinuationToken(
  token: string,
  allowedKeys?: ReadonlySet<string>,
  routeAllowedValues?: Partial<Record<string, ReadonlySet<string>>>
): ContinuationToken {
  const decodedParts = Buffer.from(token, 'base64').toString().split('\n')
  const result: ContinuationToken = {
    ...CONTINUATION_TOKEN_DEFAULTS,
    startAfter: '',
  }
  for (const part of decodedParts) {
    const partMatch = part.match(/^(\S):(.*)/)
    if (!partMatch || partMatch.length !== 3 || !(partMatch[1] in CONTINUATION_TOKEN_PART_MAP)) {
      throw ERRORS.InvalidParameter('continuation token')
    }
    if (allowedKeys && !allowedKeys.has(partMatch[1])) {
      throw ERRORS.InvalidParameter('continuation token')
    }
    const routeValues = routeAllowedValues?.[partMatch[1]]
    if (routeValues && !routeValues.has(partMatch[2])) {
      throw ERRORS.InvalidParameter('continuation token')
    }
    const allowedValues = CONTINUATION_TOKEN_ALLOWED_VALUES[partMatch[1]]
    if (allowedValues && !allowedValues.has(partMatch[2])) {
      throw ERRORS.InvalidParameter('continuation token')
    }
    result[CONTINUATION_TOKEN_PART_MAP[partMatch[1]]] = partMatch[2]
  }
  return result
}
