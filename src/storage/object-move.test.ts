import { ErrorCode } from '@internal/errors'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { StorageBackendAdapter } from './backend'
import { Database } from './database'
import { ObjectAdminDelete } from './events'
import { StorageObjectLocator } from './locator'
import { ObjectStorage } from './object'

function createVersionedMove(
  deleteResult: object | undefined,
  sourceVersion: string | null = 'source-version'
) {
  const stopAfterAuthorization = new Error('stop after authorization')
  vi.spyOn(ObjectAdminDelete, 'send').mockResolvedValue(undefined)
  const permissionObject = {
    id: 'source-id',
    version: sourceVersion,
    metadata: { size: 4 },
    user_metadata: null,
  }
  const permissionDb = {
    waitObjectLock: vi.fn().mockResolvedValue(true),
    findObject: vi.fn().mockResolvedValue(permissionObject),
    upsertObject: vi.fn().mockResolvedValue({ id: 'destination-id' }),
    deleteObject: vi.fn().mockResolvedValue(deleteResult),
    updateObject: vi.fn(),
    asSuperUser: vi.fn(),
  }
  const superUserDb = {
    findBucketById: vi.fn().mockResolvedValue({ versioning_status: 'ENABLED' }),
    findObject: vi.fn(async (_bucketId: string, objectName: string, columns: string) =>
      columns === 'is_delete_marker'
        ? objectName === 'existing.txt'
          ? permissionObject
          : undefined
        : permissionObject
    ),
  }
  permissionDb.asSuperUser.mockReturnValue(superUserDb)
  const db = {
    tenantId: 'tenant-id',
    tenant: vi.fn(() => ({ ref: 'tenant-id' })),
    asSuperUser: vi.fn(() => superUserDb),
    testPermission: vi.fn((fn) => fn(permissionDb)),
  } as unknown as Database
  const location = {
    getRootLocation: vi.fn(() => 'root-bucket'),
    getKeyLocation: vi.fn(
      ({ bucketId, objectName }: { bucketId: string; objectName: string }) =>
        `${bucketId}/${objectName}`
    ),
  } as unknown as StorageObjectLocator
  const backend = {
    copyObject: vi.fn().mockRejectedValue(stopAfterAuthorization),
  } as unknown as StorageBackendAdapter
  const storage = new ObjectStorage(backend, db, location, 'source-bucket')
  return { permissionDb, storage, stopAfterAuthorization }
}

describe('ObjectStorage.moveObject versioned authorization', () => {
  afterEach(() => vi.restoreAllMocks())

  it('checks destination INSERT and source DELETE instead of source UPDATE', async () => {
    const { permissionDb, storage, stopAfterAuthorization } = createVersionedMove({
      id: 'source-id',
    })

    await expect(
      storage.moveObject('file.txt', 'destination-bucket', 'new.txt', 'standard', 'owner-id')
    ).rejects.toBe(stopAfterAuthorization)

    expect(permissionDb.upsertObject).toHaveBeenCalledOnce()
    expect(permissionDb.deleteObject).toHaveBeenCalledWith(
      'source-bucket',
      'file.txt',
      'source-version',
      { skipPromotion: true }
    )
    expect(permissionDb.updateObject).not.toHaveBeenCalled()
  })

  it('rejects the move when source DELETE is denied by RLS', async () => {
    const { storage } = createVersionedMove(undefined)

    await expect(
      storage.moveObject('file.txt', 'destination-bucket', 'new.txt', 'standard', 'owner-id')
    ).rejects.toMatchObject({ code: ErrorCode.AccessDenied })
  })

  it('targets a legacy null-version row in the source DELETE permission check', async () => {
    const { permissionDb, storage, stopAfterAuthorization } = createVersionedMove(
      { id: 'source-id' },
      null
    )

    await expect(
      storage.moveObject('file.txt', 'destination-bucket', 'new.txt', 'standard', 'owner-id')
    ).rejects.toBe(stopAfterAuthorization)

    expect(permissionDb.deleteObject).toHaveBeenCalledWith('source-bucket', 'file.txt', null, {
      skipPromotion: true,
    })
  })

  it('uses the existing update permission check for a same-path move without a version', async () => {
    const { permissionDb, storage } = createVersionedMove({ id: 'source-id' })

    await storage.moveObject('file.txt', 'source-bucket', 'file.txt', 'standard', 'owner-id')

    expect(permissionDb.updateObject).toHaveBeenCalledOnce()
    expect(permissionDb.upsertObject).not.toHaveBeenCalled()
    expect(permissionDb.deleteObject).not.toHaveBeenCalled()
  })

  it('rejects a versioned move when a distinct destination already exists', async () => {
    const { storage } = createVersionedMove({ id: 'source-id' })

    await expect(
      storage.moveObject('source.txt', 'destination-bucket', 'existing.txt', 'standard', 'owner-id')
    ).rejects.toMatchObject({ code: ErrorCode.KeyAlreadyExists })
  })

  it('rejects execution when ENABLED changes to SUSPENDED after authorization', async () => {
    const sourceObject = {
      id: 'source-id',
      version: 'source-version',
      metadata: { size: 4 },
      user_metadata: null,
    }
    const permissionDb = {
      waitObjectLock: vi.fn().mockResolvedValue(true),
      findObject: vi.fn().mockResolvedValue(sourceObject),
      upsertObject: vi.fn().mockResolvedValue({ id: 'destination-id' }),
      deleteObject: vi.fn().mockResolvedValue(sourceObject),
      asSuperUser: vi.fn(),
    }
    const preflightSuperUserDb = {
      findBucketById: vi.fn().mockResolvedValue({ versioning_status: 'ENABLED' }),
      findObject: vi.fn().mockResolvedValue(undefined),
    }
    permissionDb.asSuperUser.mockReturnValue(preflightSuperUserDb)
    const lockedSuperUserDb = {
      findBucketById: vi.fn(async (bucketId: string) => ({
        versioning_status: bucketId === 'source-bucket' ? 'ENABLED' : 'SUSPENDED',
      })),
      waitObjectLock: vi.fn(),
    }
    const lockedDb = {
      asSuperUser: vi.fn(() => lockedSuperUserDb),
    }
    const superUserDb = {
      findBucketById: vi.fn().mockResolvedValue({ versioning_status: 'ENABLED' }),
      findObject: vi.fn().mockResolvedValue(sourceObject),
    }
    const db = {
      tenantId: 'tenant-id',
      asSuperUser: vi.fn(() => superUserDb),
      testPermission: vi.fn((fn) => fn(permissionDb)),
      withTransaction: vi.fn((fn) => fn(lockedDb)),
    } as unknown as Database
    const backend = {
      copyObject: vi.fn().mockResolvedValue(undefined),
      headObject: vi.fn().mockResolvedValue({ size: 4 }),
    } as unknown as StorageBackendAdapter
    const location = {
      getRootLocation: vi.fn(() => 'root-bucket'),
      getKeyLocation: vi.fn(({ bucketId, objectName }) => `${bucketId}/${objectName}`),
    } as unknown as StorageObjectLocator
    const storage = new ObjectStorage(backend, db, location, 'source-bucket')

    await expect(
      storage.moveObject('source.txt', 'destination-bucket', 'destination.txt', 'standard')
    ).rejects.toMatchObject({ code: ErrorCode.ResourceLocked })
    expect(lockedSuperUserDb.findBucketById.mock.calls).toEqual([
      ['destination-bucket', 'versioning_status', { forShare: true }],
      ['source-bucket', 'versioning_status', { forShare: true }],
    ])
    expect(lockedSuperUserDb.waitObjectLock).toHaveBeenCalledTimes(2)
  })
})
