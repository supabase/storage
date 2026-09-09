import { ERRORS, ErrorCode } from '@internal/errors'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { StorageBackendAdapter } from './backend'
import { Database } from './database'
import { ObjectRemoved } from './events'
import {
  MAX_KEYS_PER_S3_DELETE,
  MAX_OBJECTS_PER_DELETE_BATCH,
  MAX_OBJECTS_PER_REQUEST,
} from './limits'
import { StorageObjectLocator } from './locator'
import { ObjectStorage } from './object'
import type { Obj } from './schemas'

function createObjectStorage({
  findObject = vi.fn().mockResolvedValue({
    id: 'object-id',
    version: 'version-1',
  }),
  deleteObject = vi.fn().mockResolvedValue({
    name: 'private/file.txt',
    version: 'version-1',
  }),
  superUserDeleteObject = vi.fn().mockResolvedValue({
    name: 'private/file.txt',
    version: 'version-1',
  }),
}: {
  findObject?: ReturnType<typeof vi.fn>
  deleteObject?: ReturnType<typeof vi.fn>
  superUserDeleteObject?: ReturnType<typeof vi.fn>
} = {}) {
  const backend = {
    deleteObject: vi.fn(),
  } as unknown as StorageBackendAdapter
  const superUserDb = {
    waitObjectLock: vi.fn().mockResolvedValue(true),
    findObject,
    deleteObject: superUserDeleteObject,
  }
  const permissionDb = { deleteObject }
  const scopedDb = {
    asSuperUser: vi.fn(() => superUserDb),
    testPermission: vi.fn((fn) => fn(permissionDb)),
  }
  const db = {
    tenantId: 'tenant-id',
    reqId: 'req-id',
    sbReqId: 'sb-req-id',
    tenant: vi.fn(() => ({ ref: 'tenant-id' })),
    withTransaction: vi.fn((fn) => fn(scopedDb)),
  } as unknown as Database
  const location = {
    getRootLocation: vi.fn(() => 'root-bucket'),
    getKeyLocation: vi.fn(() => 'tenant-id/bucket/private/file.txt'),
  } as unknown as StorageObjectLocator
  const storage = new ObjectStorage(backend, db, location, 'bucket')

  return {
    backend,
    deleteObject,
    findObject,
    location,
    storage,
    waitObjectLock: superUserDb.waitObjectLock,
  }
}

describe('ObjectStorage.deleteObject', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('throws AccessDenied when the object exists but scoped delete is blocked by RLS', async () => {
    const { backend, deleteObject, findObject, storage } = createObjectStorage({
      deleteObject: vi.fn().mockResolvedValue(undefined),
    })

    await expect(storage.deleteObject('private/file.txt')).rejects.toMatchObject({
      code: ErrorCode.AccessDenied,
      httpStatusCode: 403,
      message: 'Access denied',
    })

    expect(findObject).toHaveBeenCalledWith(
      'bucket',
      'private/file.txt',
      'id,version,metadata,is_delete_marker,is_versioned',
      {
        forUpdate: true,
        dontErrorOnEmpty: true,
      },
      undefined
    )
    expect(deleteObject).toHaveBeenCalledWith('bucket', 'private/file.txt', 'version-1', {
      skipPromotion: true,
    })
    expect(backend.deleteObject).not.toHaveBeenCalled()
  })

  it('keeps true missing objects as NoSuchKey before attempting scoped delete', async () => {
    const { backend, deleteObject, storage } = createObjectStorage({
      findObject: vi.fn().mockRejectedValue(ERRORS.NoSuchKey('missing.txt')),
    })

    await expect(storage.deleteObject('missing.txt')).rejects.toMatchObject({
      code: ErrorCode.NoSuchKey,
      httpStatusCode: 404,
    })

    expect(deleteObject).not.toHaveBeenCalled()
    expect(backend.deleteObject).not.toHaveBeenCalled()
  })

  it('deletes and emits the explicitly removed version', async () => {
    const sendWebhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
    const { backend, storage } = createObjectStorage()

    await storage.deleteObject('private/file.txt', 'version-1')

    expect(backend.deleteObject).toHaveBeenCalledWith(
      'root-bucket',
      'tenant-id/bucket/private/file.txt',
      'version-1'
    )
    expect(sendWebhook).toHaveBeenCalledWith(
      expect.objectContaining({
        name: 'private/file.txt',
        version: 'version-1',
      })
    )
  })

  it('authorizes legacy rows with a null version as an unversioned delete', async () => {
    const deleteObject = vi.fn().mockResolvedValue({
      name: 'private/legacy.txt',
      version: null,
    })
    const { storage } = createObjectStorage({
      findObject: vi.fn().mockResolvedValue({
        id: 'legacy-object-id',
        version: null,
      }),
      deleteObject,
    })

    await storage.deleteObject('private/legacy.txt')

    expect(deleteObject).toHaveBeenCalledWith('bucket', 'private/legacy.txt', null, {
      skipPromotion: true,
    })
  })

  it('allows a versioned delete to create a marker for an absent key', async () => {
    const marker = {
      name: 'missing.txt',
      version: 'marker-version',
      metadata: null,
      is_delete_marker: true,
      is_versioned: true,
    }
    const deleteObject = vi.fn().mockResolvedValue(marker)
    const superUserDeleteObject = vi.fn().mockResolvedValue(marker)
    const sendWebhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
    const { backend, storage } = createObjectStorage({
      findObject: vi.fn().mockResolvedValue(undefined),
      deleteObject,
      superUserDeleteObject,
    })

    await storage.deleteObject('missing.txt')

    expect(deleteObject).toHaveBeenCalledWith('bucket', 'missing.txt', undefined, {
      skipPromotion: true,
    })
    expect(superUserDeleteObject).toHaveBeenCalledWith('bucket', 'missing.txt', undefined)
    expect(backend.deleteObject).not.toHaveBeenCalled()
    expect(sendWebhook).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'missing.txt', version: 'marker-version' })
    )
  })

  it('locks an absent key before authorizing its delete marker', async () => {
    const { deleteObject, findObject, storage, waitObjectLock } = createObjectStorage({
      findObject: vi.fn().mockResolvedValue(undefined),
    })

    await storage.deleteObject('missing.txt')

    expect(waitObjectLock).toHaveBeenCalledWith('bucket', 'missing.txt', undefined, {
      timeout: 5000,
    })
    expect(waitObjectLock.mock.invocationCallOrder[0]).toBeLessThan(
      findObject.mock.invocationCallOrder[0]
    )
    expect(findObject.mock.invocationCallOrder[0]).toBeLessThan(
      deleteObject.mock.invocationCallOrder[0]
    )
  })
})

describe('ObjectStorage.deleteObjects', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  const TARGET_COLUMNS = 'name,version,metadata,is_delete_marker,is_versioned,archived_at'

  type TargetRows = Obj[] | ((targets: { names: string[]; versions: unknown[] }) => Obj[])

  function createBulkDeleteFixture({
    versioningStatus = 'ENABLED',
    rows = [],
    permissionDeleteObjects = vi.fn().mockResolvedValue([]),
    permissionDeleteObjectVersions = vi.fn().mockResolvedValue([]),
    superUserDeleteObjects = vi.fn().mockResolvedValue([]),
    superUserDeleteObjectVersions = vi.fn().mockResolvedValue([]),
  }: {
    versioningStatus?: string
    rows?: TargetRows
    permissionDeleteObjects?: ReturnType<typeof vi.fn>
    permissionDeleteObjectVersions?: ReturnType<typeof vi.fn>
    superUserDeleteObjects?: ReturnType<typeof vi.fn>
    superUserDeleteObjectVersions?: ReturnType<typeof vi.fn>
  } = {}) {
    const backend = { deleteObjects: vi.fn() } as unknown as StorageBackendAdapter
    const permissionDb = {
      deleteObjects: permissionDeleteObjects,
      deleteObjectVersions: permissionDeleteObjectVersions,
    }
    const superUserDb = {
      tenantId: 'tenant-id',
      tenant: vi.fn(() => ({ ref: 'tenant-id', host: 'localhost' })),
      hasMigration: vi.fn().mockResolvedValue(true),
      waitObjectLocks: vi.fn().mockResolvedValue(true),
      findBucketById: vi
        .fn()
        .mockResolvedValue({ id: 'bucket', versioning_status: versioningStatus }),
      findObjectTargets: vi.fn(
        async (_bucketId: string, targets: { names: string[]; versions: unknown[] }) =>
          typeof rows === 'function' ? rows(targets) : rows
      ),
      deleteObjects: superUserDeleteObjects,
      deleteObjectVersions: superUserDeleteObjectVersions,
      withTransaction: vi.fn((fn: (db: unknown) => unknown) => fn(superUserDb)),
    }
    const testPermission = vi.fn((fn: (db: unknown) => unknown) => fn(permissionDb))
    const scopedDb = {
      tenantId: 'tenant-id',
      tenant: vi.fn(() => ({ ref: 'tenant-id' })),
      testPermission,
      asSuperUser: vi.fn(() => superUserDb),
    }
    const db = {
      tenantId: 'tenant-id',
      reqId: 'req-id',
      sbReqId: 'sb-req-id',
      withTransaction: vi.fn((fn: (db: unknown) => unknown) => fn(scopedDb)),
    } as unknown as Database
    const location = {
      getRootLocation: vi.fn(() => 'root-bucket'),
      getKeyLocation: vi.fn(({ tenantId, bucketId, objectName, version }) =>
        [tenantId, bucketId, objectName, version].filter(Boolean).join('/')
      ),
    } as unknown as StorageObjectLocator
    const storage = new ObjectStorage(backend, db, location, 'bucket')

    return { backend, permissionDb, storage, superUserDb, testPermission }
  }

  it('keeps versioned-object backend deletes within the S3 key limit', async () => {
    const sendWebhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
    const rowsFor = (names: string[]) =>
      names.map((name) => ({ name, version: `version-${name}`, metadata: {} }) as Obj)
    const deleteObjects = vi.fn((_bucketId: string, names: string[]) => rowsFor(names))
    const { backend, storage, superUserDb, testPermission } = createBulkDeleteFixture({
      rows: (targets) => rowsFor(targets.names),
      permissionDeleteObjects: deleteObjects,
      superUserDeleteObjects: deleteObjects,
    })
    const objectNames = [...Array(MAX_OBJECTS_PER_REQUEST).keys()].map((i) => `object-${i}`)
    const batches = Math.ceil(MAX_OBJECTS_PER_REQUEST / MAX_OBJECTS_PER_DELETE_BATCH)

    const results = await storage.deleteObjects(objectNames)

    expect(results).toHaveLength(MAX_OBJECTS_PER_REQUEST)
    expect(superUserDb.waitObjectLocks).toHaveBeenCalledTimes(batches)
    for (const [keys] of superUserDb.waitObjectLocks.mock.calls) {
      expect(keys).toHaveLength(MAX_OBJECTS_PER_DELETE_BATCH)
    }
    expect(superUserDb.findObjectTargets).toHaveBeenCalledTimes(batches)
    expect(superUserDb.findObjectTargets).toHaveBeenCalledWith(
      'bucket',
      { names: expect.any(Array), versions: [] },
      TARGET_COLUMNS,
      { forUpdate: true }
    )
    expect(superUserDb.findObjectTargets.mock.invocationCallOrder[0]).toBeLessThan(
      testPermission.mock.invocationCallOrder[0]
    )
    expect(deleteObjects).toHaveBeenCalledTimes(2 * batches)
    expect(backend.deleteObjects).toHaveBeenCalledTimes(batches)
    for (const [, keys] of vi.mocked(backend.deleteObjects).mock.calls) {
      expect(keys).toHaveLength(MAX_KEYS_PER_S3_DELETE)
    }
    expect(sendWebhook).toHaveBeenCalledTimes(MAX_OBJECTS_PER_REQUEST)
  })

  it('takes the advisory locks, then the bucket status lock, then the row locks', async () => {
    vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
    const { storage, superUserDb } = createBulkDeleteFixture()

    await storage.deleteObjects(['b.txt', { path: 'a.txt', versionId: 'v1' }, 'a.txt', 'b.txt'])

    expect(superUserDb.waitObjectLocks).toHaveBeenCalledWith(
      [
        { bucketId: 'bucket', objectName: 'b.txt' },
        { bucketId: 'bucket', objectName: 'a.txt' },
      ],
      { timeout: 5000 }
    )
    expect(superUserDb.findBucketById).toHaveBeenCalledWith('bucket', 'id,versioning_status', {
      forShare: true,
      dontErrorOnEmpty: true,
    })
    expect(superUserDb.findObjectTargets).toHaveBeenCalledWith(
      'bucket',
      { names: ['b.txt', 'a.txt'], versions: [{ name: 'a.txt', version: 'v1' }] },
      TARGET_COLUMNS,
      { forUpdate: true }
    )
    const [locks, status, targets] = [
      superUserDb.waitObjectLocks,
      superUserDb.findBucketById,
      superUserDb.findObjectTargets,
    ].map((mock) => mock.mock.invocationCallOrder[0])
    expect(locks).toBeLessThan(status)
    expect(status).toBeLessThan(targets)
  })

  it('emits the hidden content version rather than the newly written delete marker', async () => {
    const sendWebhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
    const original = {
      name: 'private/file.txt',
      version: 'content-version',
      metadata: { size: 4 },
      is_versioned: true,
      archived_at: null,
    } as Obj
    const marker = {
      name: original.name,
      version: 'marker-version',
      metadata: null,
      is_delete_marker: true,
      is_versioned: true,
    } as Obj
    const { storage } = createBulkDeleteFixture({
      rows: [original],
      permissionDeleteObjects: vi.fn().mockResolvedValue([original]),
      superUserDeleteObjects: vi.fn().mockResolvedValue([marker]),
    })

    await expect(storage.deleteObjects([original.name])).resolves.toEqual([marker])
    expect(sendWebhook).toHaveBeenCalledWith(
      expect.objectContaining({
        name: original.name,
        version: original.version,
        metadata: original.metadata,
      })
    )
  })

  it('authorizes and writes delete markers for missing names in a bulk delete', async () => {
    vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
    const existing = {
      name: 'existing.txt',
      version: 'content-version',
      metadata: { size: 4 },
      is_versioned: true,
      archived_at: null,
    } as Obj
    const markers = ['existing.txt', 'missing.txt'].map(
      (name) =>
        ({
          name,
          version: `marker-${name}`,
          metadata: null,
          is_delete_marker: true,
          is_versioned: true,
        }) as Obj
    )
    const permissionDeleteObjects = vi
      .fn()
      .mockResolvedValueOnce([existing])
      .mockResolvedValueOnce([markers[1]])
    const superUserDeleteObjects = vi.fn().mockResolvedValue(markers)
    const { storage } = createBulkDeleteFixture({
      rows: [existing],
      permissionDeleteObjects,
      superUserDeleteObjects,
    })

    await expect(storage.deleteObjects(['existing.txt', 'missing.txt'])).resolves.toEqual(markers)

    expect(permissionDeleteObjects).toHaveBeenNthCalledWith(1, 'bucket', ['existing.txt'], 'name', {
      skipDeleteMarkers: true,
    })
    expect(permissionDeleteObjects).toHaveBeenNthCalledWith(2, 'bucket', ['missing.txt'], 'name', {
      versioningStatus: 'ENABLED',
    })
    expect(superUserDeleteObjects).toHaveBeenCalledWith(
      'bucket',
      ['existing.txt', 'missing.txt'],
      'name',
      { versioningStatus: 'ENABLED' }
    )
  })

  it('drops only the missing name whose delete-marker probe is rejected by RLS', async () => {
    vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
    const existing = {
      name: 'existing.txt',
      version: 'content-version',
      metadata: { size: 4 },
      archived_at: null,
    } as Obj
    const allowedMarker = {
      name: 'allowed.txt',
      version: 'marker-allowed',
      is_delete_marker: true,
    } as Obj
    const rlsError = () => ERRORS.AccessDenied('new row violates row-level security policy')
    const permissionDeleteObjects = vi
      .fn()
      .mockResolvedValueOnce([existing])
      .mockRejectedValueOnce(rlsError())
      .mockRejectedValueOnce(rlsError())
      .mockResolvedValueOnce([allowedMarker])
    const superUserDeleteObjects = vi.fn().mockResolvedValue([existing, allowedMarker])
    const { storage } = createBulkDeleteFixture({
      rows: [existing],
      permissionDeleteObjects,
      superUserDeleteObjects,
    })

    await expect(
      storage.deleteObjects(['existing.txt', 'denied.txt', 'allowed.txt'])
    ).resolves.toEqual([existing, allowedMarker])

    expect(permissionDeleteObjects).toHaveBeenCalledTimes(4)
    expect(permissionDeleteObjects).toHaveBeenNthCalledWith(
      2,
      'bucket',
      ['denied.txt', 'allowed.txt'],
      'name',
      { versioningStatus: 'ENABLED' }
    )
    expect(permissionDeleteObjects).toHaveBeenNthCalledWith(3, 'bucket', ['denied.txt'], 'name', {
      versioningStatus: 'ENABLED',
    })
    expect(permissionDeleteObjects).toHaveBeenNthCalledWith(4, 'bucket', ['allowed.txt'], 'name', {
      versioningStatus: 'ENABLED',
    })
    expect(superUserDeleteObjects).toHaveBeenCalledWith(
      'bucket',
      ['existing.txt', 'allowed.txt'],
      'name',
      { versioningStatus: 'ENABLED' }
    )
  })

  it('authorizes all missing names with one probe when none is rejected', async () => {
    vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
    const markers = ['a.txt', 'b.txt'].map(
      (name) => ({ name, version: `marker-${name}`, is_delete_marker: true }) as Obj
    )
    const permissionDeleteObjects = vi.fn().mockResolvedValue(markers)
    const superUserDeleteObjects = vi.fn().mockResolvedValue(markers)
    const { storage } = createBulkDeleteFixture({ permissionDeleteObjects, superUserDeleteObjects })

    await expect(storage.deleteObjects(['a.txt', 'b.txt'])).resolves.toEqual(markers)

    expect(permissionDeleteObjects).toHaveBeenCalledTimes(1)
    expect(permissionDeleteObjects).toHaveBeenCalledWith('bucket', ['a.txt', 'b.txt'], 'name', {
      versioningStatus: 'ENABLED',
    })
  })

  it('rethrows non-RLS failures from a delete-marker probe', async () => {
    const { storage } = createBulkDeleteFixture({
      permissionDeleteObjects: vi.fn().mockRejectedValue(new Error('connection lost')),
    })

    await expect(storage.deleteObjects(['missing.txt'])).rejects.toThrow('connection lost')
  })

  it('skips delete-marker probes for missing names on a DISABLED bucket', async () => {
    vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
    const existing = { name: 'existing.txt', version: 'v1', metadata: {}, archived_at: null } as Obj
    const permissionDeleteObjects = vi.fn().mockResolvedValue([existing])
    const superUserDeleteObjects = vi.fn().mockResolvedValue([existing])
    const { storage } = createBulkDeleteFixture({
      versioningStatus: 'DISABLED',
      rows: [existing],
      permissionDeleteObjects,
      superUserDeleteObjects,
    })

    await expect(storage.deleteObjects(['existing.txt', 'missing.txt'])).resolves.toEqual([
      existing,
    ])
    expect(permissionDeleteObjects).toHaveBeenCalledTimes(1)
    expect(superUserDeleteObjects).toHaveBeenCalledWith('bucket', ['existing.txt'], 'name', {
      versioningStatus: 'DISABLED',
    })
  })

  it('hard-deletes exact versions and removes their backend content', async () => {
    vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
    const archived = {
      name: 'a.txt',
      version: 'v1',
      metadata: {},
      archived_at: '2026-01-01T00:00:00Z',
    } as Obj
    const current = { name: 'a.txt', version: 'v2', metadata: {}, archived_at: null } as Obj
    const permissionDeleteObjectVersions = vi.fn().mockResolvedValue([archived])
    const superUserDeleteObjectVersions = vi.fn().mockResolvedValue([archived])
    const { backend, storage, superUserDb } = createBulkDeleteFixture({
      rows: [archived, current],
      permissionDeleteObjectVersions,
      superUserDeleteObjectVersions,
    })

    await expect(storage.deleteObjects([{ path: 'a.txt', versionId: 'v1' }])).resolves.toEqual([
      archived,
    ])

    expect(superUserDb.deleteObjects).not.toHaveBeenCalled()
    expect(permissionDeleteObjectVersions).toHaveBeenCalledWith(
      'bucket',
      [{ name: 'a.txt', version: 'v1' }],
      { skipPromotion: true }
    )
    expect(superUserDeleteObjectVersions).toHaveBeenCalledWith('bucket', [
      { name: 'a.txt', version: 'v1' },
    ])
    expect(backend.deleteObjects).toHaveBeenCalledWith('root-bucket', [
      'tenant-id/bucket/a.txt/v1',
      'tenant-id/bucket/a.txt/v1.info',
    ])
  })
})
