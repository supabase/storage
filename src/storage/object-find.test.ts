import { ERRORS, ErrorCode } from '@internal/errors'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { StorageBackendAdapter } from './backend'
import { Database } from './database'
import { StorageObjectLocator } from './locator'
import { ObjectStorage } from './object'

function createStorage({ migrated, row }: { migrated: boolean; row?: Record<string, unknown> }) {
  const findObject = vi.fn(
    async (
      _bucketId,
      objectName,
      _columns,
      filters?: { dontErrorOnEmpty?: boolean; excludeDeleteMarkers?: boolean }
    ) => {
      if (filters?.excludeDeleteMarkers && row?.is_delete_marker) {
        if (filters.dontErrorOnEmpty) {
          return undefined
        }
        throw ERRORS.NoSuchKey(objectName)
      }
      return row
    }
  )
  const db = {
    tenantId: 'tenant-id',
    hasMigration: vi.fn().mockResolvedValue(migrated),
    findObject,
  } as unknown as Database
  const storage = new ObjectStorage(
    {} as StorageBackendAdapter,
    db,
    {} as StorageObjectLocator,
    'bucket-id'
  )
  return { findObject, storage }
}

describe('ObjectStorage.findObject', () => {
  afterEach(() => vi.restoreAllMocks())

  it('keeps the requested columns when excluding current delete markers', async () => {
    const { findObject, storage } = createStorage({ migrated: false, row: { id: 'object-id' } })

    await expect(storage.findObject('file.txt', 'id')).resolves.toEqual({ id: 'object-id' })
    expect(findObject).toHaveBeenCalledWith(
      'bucket-id',
      'file.txt',
      'id',
      { excludeDeleteMarkers: true },
      undefined
    )
  })

  it('treats a current delete marker as a missing object', async () => {
    const { findObject, storage } = createStorage({
      migrated: true,
      row: { id: 'marker-id', is_delete_marker: true },
    })

    await expect(storage.findObject('file.txt', 'id')).rejects.toMatchObject({
      code: ErrorCode.NoSuchKey,
    })
    expect(findObject).toHaveBeenCalledWith(
      'bucket-id',
      'file.txt',
      'id',
      { excludeDeleteMarkers: true },
      undefined
    )
  })

  it('returns undefined for a current delete marker when dontErrorOnEmpty is set', async () => {
    const { storage } = createStorage({
      migrated: true,
      row: { id: 'marker-id', is_delete_marker: true },
    })

    await expect(
      storage.findObject('file.txt', 'id', { dontErrorOnEmpty: true })
    ).resolves.toBeUndefined()
  })

  it('treats an explicitly requested delete-marker version as a missing object', async () => {
    const { findObject, storage } = createStorage({
      migrated: true,
      row: { id: 'marker-id', is_delete_marker: true },
    })

    await expect(
      storage.findObject('file.txt', 'id', undefined, 'marker-version')
    ).rejects.toMatchObject({ code: ErrorCode.NoSuchKey })
    expect(findObject).toHaveBeenCalledWith(
      'bucket-id',
      'file.txt',
      'id',
      { excludeDeleteMarkers: true },
      'marker-version'
    )
  })
})
