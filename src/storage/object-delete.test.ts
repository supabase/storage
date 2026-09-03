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
})

describe('ObjectStorage.deleteObjects', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('keeps versioned-object backend deletes within the S3 key limit', async () => {
    const sendWebhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
    const backend = {
      deleteObjects: vi.fn(),
    } as unknown as StorageBackendAdapter
    const scopedDb = {
      tenantId: 'tenant-id',
      tenant: vi.fn(() => ({ ref: 'tenant-id' })),
      deleteObjects: vi.fn((_bucketId: string, names: string[]) =>
        names.map((name) => ({
          name,
          version: `version-${name}`,
          metadata: {},
        }))
      ),
    }
    const permissionDb = {
      deleteObjects: scopedDb.deleteObjects,
      deleteObjectVersions: vi.fn().mockResolvedValue([]),
    }
    const superUserDb = {
      waitObjectLock: vi.fn().mockResolvedValue(true),
      findObjects: vi.fn((_bucketId: string, names: string[]) =>
        names.map((name) => ({
          name,
          version: `version-${name}`,
          metadata: {},
        }))
      ),
      findObjectVersions: vi.fn().mockResolvedValue([]),
      deleteObjects: scopedDb.deleteObjects,
      deleteObjectVersions: vi.fn().mockResolvedValue([]),
    }
    const testPermission = vi.fn((fn) => fn(permissionDb))
    Object.assign(scopedDb, {
      testPermission,
      asSuperUser: vi.fn(() => superUserDb),
    })
    const db = {
      tenantId: 'tenant-id',
      reqId: 'req-id',
      sbReqId: 'sb-req-id',
      withTransaction: vi.fn((fn) => fn(scopedDb)),
    } as unknown as Database
    const location = {
      getRootLocation: vi.fn(() => 'root-bucket'),
      getKeyLocation: vi.fn(({ tenantId, bucketId, objectName, version }) =>
        [tenantId, bucketId, objectName, version].filter(Boolean).join('/')
      ),
    } as unknown as StorageObjectLocator
    const storage = new ObjectStorage(backend, db, location, 'bucket')
    const objectNames = [...Array(MAX_OBJECTS_PER_REQUEST).keys()].map((i) => `object-${i}`)

    const results = await storage.deleteObjects(objectNames)

    expect(results).toHaveLength(MAX_OBJECTS_PER_REQUEST)
    expect(superUserDb.findObjects).toHaveBeenCalledTimes(
      Math.ceil(MAX_OBJECTS_PER_REQUEST / MAX_OBJECTS_PER_DELETE_BATCH)
    )
    expect(superUserDb.findObjects).toHaveBeenCalledWith(
      'bucket',
      expect.any(Array),
      'name,version,metadata,is_delete_marker,is_versioned',
      { forUpdate: true }
    )
    expect(superUserDb.findObjects.mock.invocationCallOrder[0]).toBeLessThan(
      testPermission.mock.invocationCallOrder[0]
    )
    expect(superUserDb.waitObjectLock).toHaveBeenCalledTimes(MAX_OBJECTS_PER_REQUEST)
    expect(scopedDb.deleteObjects).toHaveBeenCalledTimes(
      2 * Math.ceil(MAX_OBJECTS_PER_REQUEST / MAX_OBJECTS_PER_DELETE_BATCH)
    )
    expect(backend.deleteObjects).toHaveBeenCalledTimes(
      Math.ceil(MAX_OBJECTS_PER_REQUEST / MAX_OBJECTS_PER_DELETE_BATCH)
    )
    for (const [, keys] of vi.mocked(backend.deleteObjects).mock.calls) {
      expect(keys).toHaveLength(MAX_KEYS_PER_S3_DELETE)
    }
    expect(sendWebhook).toHaveBeenCalledTimes(MAX_OBJECTS_PER_REQUEST)
  })

  it('emits the hidden content version rather than the newly written delete marker', async () => {
    const sendWebhook = vi.spyOn(ObjectRemoved, 'sendWebhook').mockResolvedValue(undefined)
    const original = {
      name: 'private/file.txt',
      version: 'content-version',
      metadata: { size: 4 },
      is_versioned: true,
    }
    const marker = {
      name: original.name,
      version: 'marker-version',
      metadata: null,
      is_delete_marker: true,
      is_versioned: true,
    }
    const permissionDb = {
      deleteObjects: vi.fn().mockResolvedValue([original]),
      deleteObjectVersions: vi.fn().mockResolvedValue([]),
    }
    const superUserDb = {
      waitObjectLock: vi.fn().mockResolvedValue(true),
      findObjects: vi.fn().mockResolvedValue([original]),
      findObjectVersions: vi.fn().mockResolvedValue([]),
      deleteObjects: vi.fn().mockResolvedValue([marker]),
      deleteObjectVersions: vi.fn().mockResolvedValue([]),
    }
    const scopedDb = {
      tenantId: 'tenant-id',
      tenant: vi.fn(() => ({ ref: 'tenant-id' })),
      testPermission: vi.fn((fn) => fn(permissionDb)),
      asSuperUser: vi.fn(() => superUserDb),
    }
    const db = {
      tenantId: 'tenant-id',
      reqId: 'req-id',
      sbReqId: 'sb-req-id',
      withTransaction: vi.fn((fn) => fn(scopedDb)),
    } as unknown as Database
    const storage = new ObjectStorage(
      { deleteObjects: vi.fn() } as unknown as StorageBackendAdapter,
      db,
      {
        getRootLocation: vi.fn(() => 'root-bucket'),
        getKeyLocation: vi.fn(() => 'object-key'),
      } as unknown as StorageObjectLocator,
      'bucket'
    )

    await expect(storage.deleteObjects([original.name])).resolves.toEqual([marker])
    expect(sendWebhook).toHaveBeenCalledWith(
      expect.objectContaining({
        name: original.name,
        version: original.version,
        metadata: original.metadata,
      })
    )
  })
})
