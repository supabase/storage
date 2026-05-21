import { ERRORS, ErrorCode } from '@internal/errors'
import { describe, expect, it, vi } from 'vitest'
import { StorageBackendAdapter } from './backend'
import { Database } from './database'
import { StorageObjectLocator } from './locator'
import { ObjectStorage } from './object'

function createObjectStorage({
  deleteObjectWithLock = vi.fn().mockResolvedValue({
    version: 'version-1',
  }),
}: {
  deleteObjectWithLock?: ReturnType<typeof vi.fn>
} = {}) {
  const backend = {
    deleteObject: vi.fn(),
  } as unknown as StorageBackendAdapter
  const db = {
    tenantId: 'tenant-id',
    deleteObjectWithLock,
  } as unknown as Database
  const location = {
    getRootLocation: vi.fn(() => 'root-bucket'),
    getKeyLocation: vi.fn(() => 'tenant-id/bucket/private/file.txt'),
  } as unknown as StorageObjectLocator
  const storage = new ObjectStorage(backend, db, location, 'bucket')

  return {
    backend,
    deleteObjectWithLock,
    location,
    storage,
  }
}

describe('ObjectStorage.deleteObject', () => {
  it('throws AccessDenied when the object exists but scoped delete is blocked by RLS', async () => {
    const { backend, deleteObjectWithLock, storage } = createObjectStorage({
      deleteObjectWithLock: vi.fn().mockRejectedValue(ERRORS.AccessDenied('Access denied')),
    })

    await expect(storage.deleteObject('private/file.txt')).rejects.toMatchObject({
      code: ErrorCode.AccessDenied,
      httpStatusCode: 403,
      message: 'Access denied',
    })

    expect(deleteObjectWithLock).toHaveBeenCalledWith('bucket', 'private/file.txt')
    expect(backend.deleteObject).not.toHaveBeenCalled()
  })

  it('keeps true missing objects as NoSuchKey before attempting scoped delete', async () => {
    const { backend, deleteObjectWithLock, storage } = createObjectStorage({
      deleteObjectWithLock: vi.fn().mockRejectedValue(ERRORS.NoSuchKey('missing.txt')),
    })

    await expect(storage.deleteObject('missing.txt')).rejects.toMatchObject({
      code: ErrorCode.NoSuchKey,
      httpStatusCode: 404,
    })

    expect(deleteObjectWithLock).toHaveBeenCalledWith('bucket', 'missing.txt')
    expect(backend.deleteObject).not.toHaveBeenCalled()
  })
})
