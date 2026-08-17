import { afterEach, describe, expect, it, vi } from 'vitest'
import { StorageBackendAdapter } from './backend'
import { Database } from './database'
import { ObjectAdminDeleteAllBefore } from './events'
import { StorageObjectLocator } from './locator'
import type { BucketLifecycleConfiguration } from './schemas'
import { Storage } from './storage'

function createStorage(dbOverrides: Partial<Database> = {}) {
  const deleteObject = vi.fn()
  const testPermission = vi.fn()
  const db = {
    tenantId: 'tenant-id',
    reqId: 'req-id',
    sbReqId: 'sb-req-id',
    tenant: vi.fn(() => ({ ref: 'tenant-id', host: 'localhost' })),
    findBucketById: vi.fn().mockResolvedValue({ name: 'bucket' }),
    testPermission,
    countObjectsInBucket: vi.fn().mockResolvedValue(1),
    listObjects: vi.fn().mockResolvedValue([{ id: 'object-id', name: 'visible.txt' }]),
    deleteObject,
    ...dbOverrides,
  } as unknown as Database
  const storage = new Storage({} as StorageBackendAdapter, db, {} as StorageObjectLocator)

  return {
    db,
    deleteObject,
    storage,
    testPermission,
  }
}

describe('Storage.emptyBucket', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('enqueues deletion without probing object or bucket permissions', async () => {
    const send = vi.spyOn(ObjectAdminDeleteAllBefore, 'send').mockResolvedValue(undefined)
    const { deleteObject, storage, testPermission } = createStorage()

    await storage.emptyBucket('bucket')

    expect(testPermission).not.toHaveBeenCalled()
    expect(deleteObject).not.toHaveBeenCalled()
    expect(send).toHaveBeenCalledOnce()
  })

  it('does not enqueue when the bucket is already empty', async () => {
    const send = vi.spyOn(ObjectAdminDeleteAllBefore, 'send').mockResolvedValue(undefined)
    const { storage } = createStorage({
      listObjects: vi.fn().mockResolvedValue([]),
    })

    await storage.emptyBucket('bucket')

    expect(send).not.toHaveBeenCalled()
  })
})

describe('Storage bucket lifecycle configuration', () => {
  const configuration: BucketLifecycleConfiguration = {
    rules: [
      {
        id: 'expire-history',
        status: 'Enabled',
        filter: {},
        noncurrentVersionExpiration: { noncurrentDays: 30 },
      },
    ],
  }

  it('delegates persistence while exposing only the lifecycle configuration', async () => {
    const findLifecycleBucket = vi.fn().mockResolvedValue({
      id: 'bucket',
      name: 'bucket',
      type: 'STANDARD',
      lifecycle_configuration: configuration,
      lifecycle_configuration_generation: 'generation',
    })
    const putLifecycleConfiguration = vi.fn().mockResolvedValue({
      changed: true,
      bucket: {
        id: 'bucket',
        name: 'bucket',
        type: 'STANDARD',
        lifecycle_configuration: configuration,
        lifecycle_configuration_generation: 'generation',
      },
    })
    const deleteLifecycleConfiguration = vi.fn().mockResolvedValue({
      changed: true,
      bucket: {
        id: 'bucket',
        name: 'bucket',
        type: 'STANDARD',
        lifecycle_configuration: null,
        lifecycle_configuration_generation: null,
      },
    })
    const { storage } = createStorage({
      deleteLifecycleConfiguration,
      findLifecycleBucket,
      putLifecycleConfiguration,
    })

    await expect(storage.getBucketLifecycle('bucket')).resolves.toBe(configuration)
    await expect(storage.putBucketLifecycle('bucket', configuration)).resolves.toBe(configuration)
    await expect(storage.deleteBucketLifecycle('bucket')).resolves.toBeUndefined()

    expect(findLifecycleBucket).toHaveBeenCalledWith('bucket')
    expect(putLifecycleConfiguration).toHaveBeenCalledWith('bucket', configuration)
    expect(deleteLifecycleConfiguration).toHaveBeenCalledWith('bucket')
  })
})
