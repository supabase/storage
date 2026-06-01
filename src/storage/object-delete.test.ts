import { ERRORS, ErrorCode } from '@internal/errors'
import { describe, expect, it, vi } from 'vitest'
import { StorageBackendAdapter } from './backend'
import { Database } from './database'
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
}: {
  findObject?: ReturnType<typeof vi.fn>
  deleteObject?: ReturnType<typeof vi.fn>
} = {}) {
  const backend = {
    deleteObject: vi.fn(),
  } as unknown as StorageBackendAdapter
  const superUserDb = {
    findObject,
  }
  const scopedDb = {
    asSuperUser: vi.fn(() => superUserDb),
    deleteObject,
  }
  const db = {
    tenantId: 'tenant-id',
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
  it('throws AccessDenied when the object exists but scoped delete is blocked by RLS', async () => {
    const { backend, deleteObject, findObject, storage } = createObjectStorage({
      deleteObject: vi.fn().mockResolvedValue(undefined),
    })

    await expect(storage.deleteObject('private/file.txt')).rejects.toMatchObject({
      code: ErrorCode.AccessDenied,
      httpStatusCode: 403,
      message: 'Access denied',
    })

    expect(findObject).toHaveBeenCalledWith('bucket', 'private/file.txt', 'id,version,metadata', {
      forUpdate: true,
    })
    expect(deleteObject).toHaveBeenCalledWith('bucket', 'private/file.txt')
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
})
