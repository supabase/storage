import { setQueueForTesting } from '@internal/queue'
import { describe, expect, it, vi } from 'vitest'
import { StorageBackendAdapter } from './backend'
import { Database } from './database'
import { ObjectAdminDeleteAllBefore } from './events'
import { StorageObjectLocator } from './locator'
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

function stubQueue() {
  const produce = vi.fn().mockResolvedValue(undefined)
  setQueueForTesting({ produce })
  return produce
}

describe('Storage.emptyBucket', () => {
  it('enqueues deletion without probing object or bucket permissions', async () => {
    const produce = stubQueue()
    const { deleteObject, storage, testPermission } = createStorage()

    await storage.emptyBucket('bucket')

    expect(testPermission).not.toHaveBeenCalled()
    expect(deleteObject).not.toHaveBeenCalled()
    expect(produce).toHaveBeenCalledOnce()
    expect(produce.mock.calls[0][0]).toBeInstanceOf(ObjectAdminDeleteAllBefore)
  })

  it('does not enqueue when the bucket is already empty', async () => {
    const produce = stubQueue()
    const { storage } = createStorage({
      listObjects: vi.fn().mockResolvedValue([]),
    })

    await storage.emptyBucket('bucket')

    expect(produce).not.toHaveBeenCalled()
  })
})

describe('Storage.findBucketAndObject', () => {
  let tenant = 0
  const privateBucket = { id: 'bucket', public: false } as never
  const publicBucket = { id: 'bucket', public: true } as never

  // `seen` primes the remembered buckets with a first request that reads the bucket alone.
  async function createLookup(
    batched: Awaited<ReturnType<Database['findBucketAndObject']>>,
    seen?: { public: boolean }
  ) {
    const tenantId = `lookup-tenant-${++tenant}`
    const superUserDb = {
      tenantId,
      findBucketById: vi.fn().mockResolvedValue({ id: 'bucket', public: seen?.public ?? false }),
      findObject: vi.fn().mockResolvedValue({ id: 'read-as-super-user' }),
    } as unknown as Database
    const db = {
      tenantId,
      findBucketAndObject: vi.fn().mockResolvedValue(batched),
      findObject: vi.fn().mockResolvedValue({ id: 'read-with-rls' }),
      asSuperUser: () => superUserDb,
    } as unknown as Database
    Object.assign(superUserDb, { asSuperUser: () => superUserDb })
    const storage = new Storage({} as StorageBackendAdapter, db, {} as StorageObjectLocator)

    if (seen) {
      await storage.findBucketAndObject('bucket', 'a.txt', 'id')
      vi.mocked(superUserDb.findBucketById).mockClear()
    }

    return { db, storage, superUserDb }
  }

  it('reads a bucket it has not seen alone, and then the object', async () => {
    const { db, storage, superUserDb } = await createLookup({
      bucket: privateBucket,
      object: { id: 'batched' } as never,
    })

    const lookup = await storage.findBucketAndObject('bucket', 'a.txt', 'id')

    expect(db.findBucketAndObject).not.toHaveBeenCalled()
    expect(superUserDb.findBucketById).toHaveBeenCalledWith('bucket', 'id,public', {
      dontErrorOnEmpty: true,
    })
    expect(lookup.bucket).toEqual({ id: 'bucket', public: false })
    await expect(lookup.findObject()).resolves.toEqual({ id: 'read-with-rls' })

    // Once the bucket is known to exist, both reads share a round trip.
    const next = await storage.findBucketAndObject('bucket', 'a.txt', 'id')
    expect(db.findBucketAndObject).toHaveBeenCalledTimes(1)
    await expect(next.findObject()).resolves.toEqual({ id: 'batched' })
  })

  it('uses the object read with RLS for a private bucket', async () => {
    const { db, storage, superUserDb } = await createLookup(
      { bucket: privateBucket, object: { id: 'batched' } as never },
      { public: false }
    )

    const lookup = await storage.findBucketAndObject('bucket', 'a.txt', 'id', { version: 'v1' })

    expect(lookup.bucket).toEqual({ id: 'bucket', public: false })
    await expect(lookup.findObject()).resolves.toEqual({ id: 'batched' })
    expect(db.findBucketAndObject).toHaveBeenCalledWith({
      bucketId: 'bucket',
      bucketColumns: 'id,public',
      objectName: 'a.txt',
      objectColumns: 'id',
      objectFilters: { excludeDeleteMarkers: true },
      version: 'v1',
      objectAsSuperUser: false,
    })
    expect(db.findObject).not.toHaveBeenCalled()
    expect(superUserDb.findBucketById).not.toHaveBeenCalled()
  })

  it('reports a missing object of a private bucket without reading it again', async () => {
    const { db, storage } = await createLookup({ bucket: privateBucket }, { public: false })

    const lookup = await storage.findBucketAndObject('bucket', 'a.txt', 'id')

    await expect(lookup.findObject()).rejects.toMatchObject({ code: 'NoSuchKey' })
    expect(db.findObject).not.toHaveBeenCalled()
  })

  it('uses a row RLS let through for a public bucket, and reads a hidden one as the super user', async () => {
    const found = await createLookup(
      { bucket: publicBucket, object: { id: 'batched' } as never },
      { public: false }
    )
    const hidden = await createLookup({ bucket: publicBucket }, { public: false })

    const foundLookup = await found.storage.findBucketAndObject('bucket', 'a.txt', 'id')
    const hiddenLookup = await hidden.storage.findBucketAndObject('bucket', 'a.txt', 'id')

    await expect(foundLookup.findObject()).resolves.toEqual({ id: 'batched' })
    expect(found.superUserDb.findObject).not.toHaveBeenCalled()
    await expect(hiddenLookup.findObject()).resolves.toEqual({ id: 'read-as-super-user' })
    expect(hidden.superUserDb.findObject).toHaveBeenCalledWith(
      'bucket',
      'a.txt',
      'id',
      { excludeDeleteMarkers: true },
      undefined
    )
  })

  it('reads the object of a bucket last seen public as the super user', async () => {
    const { db, storage, superUserDb } = await createLookup(
      { bucket: publicBucket },
      { public: true }
    )

    const lookup = await storage.findBucketAndObject('bucket', 'a.txt', 'id')

    expect(vi.mocked(db.findBucketAndObject).mock.calls[0][0]).toMatchObject({
      objectAsSuperUser: true,
    })
    await expect(lookup.findObject()).rejects.toMatchObject({ code: 'NoSuchKey' })
    expect(superUserDb.findObject).not.toHaveBeenCalled()
  })

  it('never uses an object read as the super user once the bucket turned private', async () => {
    const { db, storage, superUserDb } = await createLookup(
      { bucket: privateBucket, object: { id: 'batched-as-super-user' } as never },
      { public: true }
    )

    const lookup = await storage.findBucketAndObject('bucket', 'a.txt', 'id')

    expect(vi.mocked(db.findBucketAndObject).mock.calls[0][0]).toMatchObject({
      objectAsSuperUser: true,
    })
    await expect(lookup.findObject()).resolves.toEqual({ id: 'read-with-rls' })
    expect(superUserDb.findObject).not.toHaveBeenCalled()
  })

  it('reads the object as the super user when asked to', async () => {
    const { db, storage } = await createLookup(
      { bucket: privateBucket, object: { id: 'batched' } as never },
      { public: false }
    )

    const lookup = await storage.findBucketAndObject('bucket', 'a.txt', 'id', {
      objectAsSuperUser: true,
    })

    expect(vi.mocked(db.findBucketAndObject).mock.calls[0][0]).toMatchObject({
      objectAsSuperUser: true,
    })
    await expect(lookup.findObject()).resolves.toEqual({ id: 'batched' })
  })

  it('reads the object with the bucket for publicBucketOnly only if the bucket was seen public', async () => {
    const seenPrivate = await createLookup(
      { bucket: privateBucket, object: { id: 'batched' } as never },
      { public: false }
    )
    const seenPublic = await createLookup(
      { bucket: publicBucket, object: { id: 'batched' } as never },
      { public: true }
    )

    const privateLookup = await seenPrivate.storage.findBucketAndObject('bucket', 'a.txt', 'id', {
      publicBucketOnly: true,
    })
    const publicLookup = await seenPublic.storage.findBucketAndObject('bucket', 'a.txt', 'id', {
      publicBucketOnly: true,
    })

    // The caller rejects the private bucket: its object is not read.
    expect(seenPrivate.db.findBucketAndObject).not.toHaveBeenCalled()
    expect(seenPrivate.superUserDb.findBucketById).toHaveBeenCalledTimes(1)
    expect(privateLookup.bucket).toEqual({ id: 'bucket', public: false })
    expect(seenPrivate.db.findObject).not.toHaveBeenCalled()

    expect(seenPublic.db.findBucketAndObject).toHaveBeenCalledTimes(1)
    await expect(publicLookup.findObject()).resolves.toEqual({ id: 'batched' })
  })

  it('forgets a bucket that is gone', async () => {
    const { db, storage, superUserDb } = await createLookup(
      { bucket: undefined },
      { public: false }
    )

    const lookup = await storage.findBucketAndObject('bucket', 'a.txt', 'id')
    expect(lookup.bucket).toBeUndefined()

    vi.mocked(superUserDb.findBucketById).mockResolvedValue(undefined)
    await storage.findBucketAndObject('bucket', 'a.txt', 'id')
    expect(db.findBucketAndObject).toHaveBeenCalledTimes(1)
    expect(superUserDb.findBucketById).toHaveBeenCalledTimes(1)
  })

  it('takes the regular reads when the database cannot batch them', async () => {
    const { db, storage, superUserDb } = await createLookup(undefined, { public: false })

    const lookup = await storage.findBucketAndObject('bucket', 'a.txt', 'id')

    expect(db.findBucketAndObject).toHaveBeenCalledTimes(1)
    expect(superUserDb.findBucketById).toHaveBeenCalledWith('bucket', 'id,public', {
      dontErrorOnEmpty: true,
    })
    await expect(lookup.findObject()).resolves.toEqual({ id: 'read-with-rls' })
    expect(db.findObject).toHaveBeenCalledTimes(1)
  })

  it('checks the bucket before rejecting an invalid key, as the regular reads do', async () => {
    const { db, storage, superUserDb } = await createLookup(
      { bucket: privateBucket },
      { public: false }
    )

    const lookup = await storage.findBucketAndObject('bucket', '', 'id')

    expect(db.findBucketAndObject).not.toHaveBeenCalled()
    expect(superUserDb.findBucketById).toHaveBeenCalledTimes(1)
    await expect(lookup.findObject()).rejects.toMatchObject({ code: 'InvalidKey' })
  })

  it('drops the least recently seen buckets first', async () => {
    const { db, storage, superUserDb } = await createLookup(
      { bucket: privateBucket, object: { id: 'batched' } as never },
      { public: false }
    )

    // 10,001 buckets, one more than are remembered; `bucket` is seen again halfway.
    for (let i = 0; i < 10_000; i++) {
      if (i === 5_000) {
        await storage.findBucketAndObject('bucket', 'a.txt', 'id')
      }
      await storage.findBucketAndObject(`other-${i}`, 'a.txt', 'id')
    }
    vi.mocked(db.findBucketAndObject).mockClear()
    vi.mocked(superUserDb.findBucketById).mockClear()

    await storage.findBucketAndObject('bucket', 'a.txt', 'id')
    await storage.findBucketAndObject('other-0', 'a.txt', 'id')

    expect(vi.mocked(db.findBucketAndObject).mock.calls.map(([query]) => query.bucketId)).toEqual([
      'bucket',
    ])
    expect(superUserDb.findBucketById).toHaveBeenCalledWith('other-0', 'id,public', {
      dontErrorOnEmpty: true,
    })
  })
})
