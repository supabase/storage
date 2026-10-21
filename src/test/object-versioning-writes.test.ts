import { randomUUID } from 'node:crypto'
import { replacedContent } from '../storage/database/adapter'
import { ObjectAdminDelete, ObjectPromoted, ObjectRemoved } from '../storage/events'
import { useStorage, withDeleteEnabled } from './utils/storage'

describe('object versioning - version-aware writes', () => {
  const tHelper = useStorage()
  let bucketId: string
  let objectName: string

  beforeEach(async () => {
    bucketId = `versioning-writes-${randomUUID()}`
    objectName = `key-${randomUUID()}.txt`
  })

  afterEach(async () => {
    await withDeleteEnabled(tHelper.database.connection, async (transaction) => {
      await transaction.query('DELETE FROM storage.objects WHERE bucket_id = $1', [bucketId])
      await transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
    })
  })

  async function allRowsFor(name: string) {
    const result = await tHelper.database.connection.query<{
      version: string
      archived_at: string | null
      is_delete_marker: boolean
      is_versioned: boolean
    }>(
      `SELECT version, archived_at, is_delete_marker, is_versioned
       FROM storage.objects WHERE bucket_id = $1 AND name = $2
       ORDER BY created_at`,
      [bucketId, name]
    )
    return result.rows
  }

  it('allows independent writes in the same bucket to hold compatible status locks', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    const firstWriteReady = Promise.withResolvers<void>()
    const releaseFirstWrite = Promise.withResolvers<void>()
    const firstWrite = tHelper.database.withTransaction(async (db) => {
      await db.upsertObject({
        bucket_id: bucketId,
        name: `${objectName}-first`,
        metadata: null,
        user_metadata: null,
        version: 'v1',
      })
      firstWriteReady.resolve()
      await releaseFirstWrite.promise
    })

    await firstWriteReady.promise
    const secondWrite = tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: `${objectName}-second`,
      metadata: null,
      user_metadata: null,
      version: 'v1',
    })

    try {
      await expect(
        Promise.race([
          secondWrite,
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('independent write was bucket-blocked')), 2000)
          ),
        ])
      ).resolves.toMatchObject({ name: `${objectName}-second` })
    } finally {
      releaseFirstWrite.resolve()
      await firstWrite
    }
  })

  it('makes a status transition wait for a write using the prior status', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    const writeReady = Promise.withResolvers<void>()
    const releaseWrite = Promise.withResolvers<void>()
    const write = tHelper.database.withTransaction(async (db) => {
      await db.upsertObject({
        bucket_id: bucketId,
        name: objectName,
        metadata: null,
        user_metadata: null,
        version: 'v1',
      })
      writeReady.resolve()
      await releaseWrite.promise
    })

    await writeReady.promise
    let transitionFinished = false
    const transition = tHelper.database
      .updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
      .then(() => {
        transitionFinished = true
      })

    await new Promise((resolve) => setTimeout(resolve, 100))
    expect(transitionFinished).toBe(false)

    releaseWrite.resolve()
    await Promise.all([write, transition])
    await expect(
      tHelper.database.findBucketById(bucketId, 'versioning_status')
    ).resolves.toMatchObject({ versioning_status: 'SUSPENDED' })
  })

  it('does not deadlock when a move-style write meets an in-flight object write', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    const objectLocked = Promise.withResolvers<void>()
    const allowUploadWrite = Promise.withResolvers<void>()

    const upload = tHelper.database.withTransaction(async (db) => {
      await db.waitObjectLock(bucketId, objectName)
      objectLocked.resolve()
      await allowUploadWrite.promise
      await db.upsertObject({
        bucket_id: bucketId,
        name: objectName,
        metadata: null,
        user_metadata: null,
        version: 'v1',
      })
    })

    await objectLocked.promise
    const move = tHelper.database.withTransaction(async (db) => {
      await db.waitObjectLock(bucketId, objectName)
      await db.findBucketById(bucketId, 'versioning_status', { forShare: true })
    })

    allowUploadWrite.resolve()
    await expect(
      Promise.race([
        Promise.all([upload, move]),
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('upload and move deadlocked')), 5000)
        ),
      ])
    ).resolves.toBeDefined()
  })

  it('empty-bucket accounting includes current, archived, and delete-marker rows', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    const write = (version: string) =>
      tHelper.database.upsertObject({
        bucket_id: bucketId,
        name: objectName,
        owner: undefined,
        metadata: { size: 4 },
        user_metadata: null,
        version,
      })

    await write(randomUUID())
    await write(randomUUID())
    await tHelper.database.deleteObject(bucketId, objectName)

    await expect(tHelper.database.countObjectsInBucket(bucketId)).resolves.toBe(3)
    await expect(tHelper.database.listObjects(bucketId, 'id', 10)).resolves.toHaveLength(3)
  })

  it('DISABLED: second upsert overwrites the same row, no history', async () => {
    await tHelper.database.createBucket({ id: bucketId, name: bucketId })

    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 1 },
      user_metadata: null,
      version: 'v1',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 2 },
      user_metadata: null,
      version: 'v2',
    })

    const rows = await allRowsFor(objectName)
    expect(rows).toHaveLength(1)
    expect(rows[0]).toMatchObject({ version: 'v2', archived_at: null, is_versioned: false })
  })

  it('ENABLED: second upsert archives the old row and inserts a new current one', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })

    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 1 },
      user_metadata: null,
      version: 'v1',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 2 },
      user_metadata: null,
      version: 'v2',
    })

    const rows = await allRowsFor(objectName)
    expect(rows).toHaveLength(2)
    const current = rows.find((r) => r.archived_at === null)
    const archived = rows.find((r) => r.archived_at !== null)
    expect(current).toMatchObject({ version: 'v2', is_versioned: true })
    expect(archived).toMatchObject({ version: 'v1', is_versioned: true })
  })

  it('ENABLED: each version gets its own row id (id is still the real PK)', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })

    const first = await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v1',
    })
    const second = await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v2',
    })

    expect(second.id).not.toBe(first.id)
  })

  it('SUSPENDED: second upsert overwrites the null-version row in place, no history', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })

    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 1 },
      user_metadata: null,
      version: 'v1',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 2 },
      user_metadata: null,
      version: 'v2',
    })

    const rows = await allRowsFor(objectName)
    expect(rows).toHaveLength(1)
    expect(rows[0]).toMatchObject({ version: 'v2', archived_at: null, is_versioned: false })
  })

  it('ENABLED -> SUSPENDED: replaces an enabled current version with a current null version', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 1 },
      user_metadata: null,
      version: 'enabled-v1',
    })

    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 2 },
      user_metadata: null,
      version: 'null-v1',
    })

    const rows = await allRowsFor(objectName)
    expect(rows).toHaveLength(2)
    expect(rows.find((row) => row.archived_at === null)).toMatchObject({
      version: 'null-v1',
      is_versioned: false,
    })
    expect(rows.find((row) => row.version === 'enabled-v1')?.archived_at).not.toBeNull()
  })

  it('DISABLED -> ENABLED -> SUSPENDED: reuses the historical null-version row as current', async () => {
    await tHelper.database.createBucket({ id: bucketId, name: bucketId })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 0 },
      user_metadata: null,
      version: 'disabled-v1',
    })
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 1 },
      user_metadata: null,
      version: 'enabled-v1',
    })
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 2 },
      user_metadata: null,
      version: 'null-v2',
    })

    const rows = await allRowsFor(objectName)
    expect(rows).toHaveLength(2)
    expect(rows.find((row) => row.archived_at === null)).toMatchObject({
      version: 'null-v2',
      is_versioned: false,
    })
    expect(rows.some((row) => row.version === 'disabled-v1')).toBe(false)
    expect(rows.find((row) => row.version === 'enabled-v1')?.archived_at).not.toBeNull()
  })

  it("DISABLED -> ENABLED -> SUSPENDED: replacedContent reports the reused row's own metadata, not the row that was current beforehand", async () => {
    await tHelper.database.createBucket({ id: bucketId, name: bucketId })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 100 },
      user_metadata: null,
      version: 'disabled-v1',
    })
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 200 },
      user_metadata: null,
      version: 'enabled-v1',
    })
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    const written = await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 300 },
      user_metadata: null,
      version: 'null-v2',
    })

    expect(replacedContent(written)).toMatchObject({
      version: 'disabled-v1',
      metadata: { size: 100 },
    })
  })

  it('DISABLED: delete without a versionId physically removes the row', async () => {
    await tHelper.database.createBucket({ id: bucketId, name: bucketId })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v1',
    })

    const { deleted } = await tHelper.database.deleteObject(bucketId, objectName)
    expect(deleted).toMatchObject({ version: 'v1' })
    expect(await allRowsFor(objectName)).toHaveLength(0)
  })

  it('ENABLED: delete without a versionId writes a delete-marker, preserving history', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 1 },
      user_metadata: null,
      version: 'v1',
    })

    const { deleted: marker } = await tHelper.database.deleteObject(bucketId, objectName)
    expect(marker).toMatchObject({ is_delete_marker: true, is_versioned: true })
    expect(marker!.version).not.toBe('v1')

    const rows = await allRowsFor(objectName)
    expect(rows).toHaveLength(2)
    const original = rows.find((r) => r.version === 'v1')
    expect(original?.archived_at).not.toBeNull()

    // v1's content is still fetchable by version, it was never destroyed
    const stillThere = await tHelper.database.findObject(bucketId, objectName, 'version', {}, 'v1')
    expect(stillThere.version).toBe('v1')
  })

  it('SUSPENDED: delete without a versionId overwrites the null-version row', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 1 },
      user_metadata: null,
      version: 'v1',
    })

    const { deleted: marker } = await tHelper.database.deleteObject(bucketId, objectName)
    expect(marker).toMatchObject({ is_delete_marker: true, is_versioned: false })

    const rows = await allRowsFor(objectName)
    expect(rows).toHaveLength(1)
    expect(rows[0].version).toBe(marker!.version)
  })

  it("SUSPENDED: delete without a versionId frees the overwritten null-version row's bytes", async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 70 },
      user_metadata: null,
      version: 'v1',
    })

    const removedSpy = vi.spyOn(ObjectRemoved, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).deleteObject(objectName)

      // The marker's own write frees the null-version row's bytes it replaced,
      // which here is the same row the event otherwise describes.
      expect(removedSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          bytesChanged: -70,
          replacedObject: expect.objectContaining({
            metadata: expect.objectContaining({ size: 70 }),
          }),
        })
      )
    } finally {
      removedSpy.mockRestore()
    }
  })

  it('reports the row actually freed, not the one the event otherwise describes, when a SUSPENDED marker reuses a stale null-version slot', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 70 },
      user_metadata: null,
      version: 'stale-null-version',
    })

    await tHelper.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 20 },
      user_metadata: null,
      version: 'enabled-current',
    })

    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })

    const removedSpy = vi.spyOn(ObjectRemoved, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).deleteObject(objectName)

      expect(removedSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          name: objectName,
          bucketId,
          // The event's own identity is the row actually archived from current...
          metadata: expect.objectContaining({ size: 20 }),
          // ...but the bytes physically freed came from the stale row the
          // marker's insert reused, reported separately via replacedObject.
          bytesChanged: -70,
          replacedObject: expect.objectContaining({
            metadata: expect.objectContaining({ size: 70 }),
          }),
        })
      )
    } finally {
      removedSpy.mockRestore()
    }
  })

  it.each([
    'ENABLED',
    'SUSPENDED',
  ] as const)('%s: deleting an absent key creates a current delete marker', async (status) => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    if (status === 'SUSPENDED') {
      await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    }

    const removedSpy = vi.spyOn(ObjectRemoved, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).deleteObject(objectName)

      // Nothing existed to free: the marker's own write frees no bytes, and
      // there's no other row to report.
      expect(removedSpy).toHaveBeenCalledWith(expect.objectContaining({ bytesChanged: 0 }))
    } finally {
      removedSpy.mockRestore()
    }

    await expect(
      tHelper.database.findObject(bucketId, objectName, 'is_delete_marker,is_versioned,archived_at')
    ).resolves.toMatchObject({
      is_delete_marker: true,
      is_versioned: status === 'ENABLED',
      archived_at: null,
    })
  })

  it.each([
    ['single', (bucket: string, name: string) => tHelper.database.deleteObject(bucket, name, 'v3')],
    [
      'bulk',
      (bucket: string, name: string) =>
        tHelper.database.deleteObjectVersions(bucket, [{ name, version: 'v3' }]),
    ],
  ] as const)('%s hard delete of the current version promotes the next remaining version when its first candidate is deleted concurrently', async (_mode, deleteCurrent) => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    for (const version of ['v1', 'v2', 'v3']) {
      await tHelper.database.upsertObject({
        bucket_id: bucketId,
        name: objectName,
        metadata: null,
        user_metadata: null,
        version,
      })
    }

    const candidateLocked = Promise.withResolvers<void>()
    const releaseCandidate = Promise.withResolvers<void>()
    // Hold the row lock on v2, the candidate promotion would pick first, then
    // hard-delete it before releasing.
    const candidateDeleter = tHelper.database.withTransaction(async (db) => {
      await db.findObject(bucketId, objectName, 'id', { forUpdate: true }, 'v2')
      candidateLocked.resolve()
      await releaseCandidate.promise
      await db.deleteObject(bucketId, objectName, 'v2', { skipPromotion: true })
    })

    await candidateLocked.promise
    const currentDelete = deleteCurrent(bucketId, objectName)
    // Give the current-version delete time to block inside its promotion.
    await new Promise((resolve) => setTimeout(resolve, 300))
    releaseCandidate.resolve()
    const [, result] = await Promise.all([candidateDeleter, currentDelete])

    expect([result.promoted].flat()).toEqual([expect.objectContaining({ version: 'v1' })])

    expect(await allRowsFor(objectName)).toEqual([
      expect.objectContaining({ version: 'v1', archived_at: null }),
    ])
  })

  it('deleting a non-current version does not wait for the key lock', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    const sizes = { v1: 40, v2: 60 }
    for (const version of ['v1', 'v2'] as const) {
      await tHelper.database.upsertObject({
        bucket_id: bucketId,
        name: objectName,
        metadata: { size: sizes[version] },
        user_metadata: null,
        version,
      })
    }

    const keyLocked = Promise.withResolvers<void>()
    const releaseKey = Promise.withResolvers<void>()
    const keyHolder = tHelper.database.withTransaction(async (db) => {
      await db.waitObjectLock(bucketId, objectName)
      keyLocked.resolve()
      await releaseKey.promise
    })
    await keyLocked.promise

    const removedSpy = vi.spyOn(ObjectRemoved, 'sendWebhook')
    try {
      await expect(
        Promise.race([
          tHelper.storage.from(bucketId).deleteObject(objectName, 'v1'),
          new Promise((_, reject) =>
            setTimeout(() => reject(new Error('non-current version delete was key-blocked')), 2000)
          ),
        ])
      ).resolves.toBeUndefined()

      // v1's own bytes are hard-deleted, regardless of it being noncurrent;
      // there's no other row reused in place to report.
      expect(removedSpy).toHaveBeenCalledWith(expect.objectContaining({ bytesChanged: -40 }))
    } finally {
      releaseKey.resolve()
      await keyHolder
      removedSpy.mockRestore()
    }

    expect(await allRowsFor(objectName)).toEqual([
      expect.objectContaining({ version: 'v2', archived_at: null }),
    ])
  })

  it('deleting the current version waits for the key lock', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    const sizes = { v1: 40, v2: 60 }
    for (const version of ['v1', 'v2'] as const) {
      await tHelper.database.upsertObject({
        bucket_id: bucketId,
        name: objectName,
        metadata: { size: sizes[version] },
        user_metadata: null,
        version,
      })
    }

    const keyLocked = Promise.withResolvers<void>()
    const releaseKey = Promise.withResolvers<void>()
    const keyHolder = tHelper.database.withTransaction(async (db) => {
      await db.waitObjectLock(bucketId, objectName)
      keyLocked.resolve()
      await releaseKey.promise
    })
    await keyLocked.promise

    const removedSpy = vi.spyOn(ObjectRemoved, 'sendWebhook')
    let deleteFinished = false
    const currentDelete = tHelper.storage
      .from(bucketId)
      .deleteObject(objectName, 'v2')
      .then(() => {
        deleteFinished = true
      })
    await new Promise((resolve) => setTimeout(resolve, 300))
    expect(deleteFinished).toBe(false)

    releaseKey.resolve()
    try {
      await Promise.all([keyHolder, currentDelete])
      // v2 was the current version, hard-deleted just the same.
      expect(removedSpy).toHaveBeenCalledWith(expect.objectContaining({ bytesChanged: -60 }))
    } finally {
      removedSpy.mockRestore()
    }
    expect(await allRowsFor(objectName)).toEqual([
      expect.objectContaining({ version: 'v1', archived_at: null }),
    ])
  })

  it('serializes concurrent deletes of the same absent key', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })

    await Promise.all([
      tHelper.storage.from(bucketId).deleteObject(objectName),
      tHelper.storage.from(bucketId).deleteObject(objectName),
    ])

    const rows = await allRowsFor(objectName)
    expect(rows).toHaveLength(2)
    expect(rows.every((row) => row.is_delete_marker && row.is_versioned)).toBe(true)
    expect(rows.filter((row) => row.archived_at === null)).toHaveLength(1)
  })

  it.each([
    'ENABLED',
    'SUSPENDED',
  ] as const)('%s: writes delete markers for existing and missing keys in a bulk delete', async (status) => {
    const missingName = `${objectName}-missing`
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 80 },
      user_metadata: null,
      version: randomUUID(),
    })
    if (status === 'SUSPENDED') {
      await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    }

    const removedSpy = vi.spyOn(ObjectRemoved, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).deleteObjects([objectName, missingName])

      // Both keys only ever get archived behind a marker here, never freed,
      // and neither write reused another row's slot.
      expect(removedSpy).toHaveBeenCalledTimes(2)
      expect(removedSpy).toHaveBeenCalledWith(expect.objectContaining({ bytesChanged: 0 }))
    } finally {
      removedSpy.mockRestore()
    }

    const [existingRows, missingRows] = await Promise.all([
      allRowsFor(objectName),
      allRowsFor(missingName),
    ])
    expect(existingRows).toHaveLength(2)
    expect(existingRows.find((row) => row.archived_at === null)).toMatchObject({
      archived_at: null,
      is_delete_marker: true,
      is_versioned: status === 'ENABLED',
    })
    expect(missingRows).toEqual([
      expect.objectContaining({
        archived_at: null,
        is_delete_marker: true,
        is_versioned: status === 'ENABLED',
      }),
    ])
  })

  it("SUSPENDED: bulk delete without versionIds frees the overwritten null-version row's bytes", async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 70 },
      user_metadata: null,
      version: 'v1',
    })

    const removedSpy = vi.spyOn(ObjectRemoved, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).deleteObjects([objectName])

      // The marker's own write frees the null-version row's bytes it replaced,
      // which here is the same row the event otherwise describes.
      expect(removedSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          name: objectName,
          bytesChanged: -70,
          replacedObject: expect.objectContaining({
            metadata: expect.objectContaining({ size: 70 }),
          }),
        })
      )
    } finally {
      removedSpy.mockRestore()
    }
  })

  it('reports the row actually freed, not the one the event otherwise describes, for a bulk delete that reuses a stale null-version slot', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 70 },
      user_metadata: null,
      version: 'stale-null-version',
    })

    await tHelper.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 20 },
      user_metadata: null,
      version: 'enabled-current',
    })

    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })

    const removedSpy = vi.spyOn(ObjectRemoved, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).deleteObjects([objectName])

      expect(removedSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          name: objectName,
          bucketId,
          // The event's own identity is the row actually archived from current...
          metadata: expect.objectContaining({ size: 20 }),
          // ...but the bytes physically freed came from the stale row the
          // marker's insert reused, reported separately via replacedObject.
          bytesChanged: -70,
          replacedObject: expect.objectContaining({
            metadata: expect.objectContaining({ size: 70 }),
          }),
        })
      )
    } finally {
      removedSpy.mockRestore()
    }
  })

  it.each([
    'ENABLED',
    'SUSPENDED',
  ] as const)('%s: a normal upload can recreate a key hidden by a delete marker', async (status) => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v1',
    })
    await tHelper.database.deleteObject(bucketId, objectName)
    if (status === 'SUSPENDED') {
      await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    }

    await tHelper.uploader.canUpload({
      bucketId,
      objectName,
      owner: undefined,
      isUpsert: false,
      userMetadata: undefined,
      metadata: { mimetype: 'text/plain', contentLength: 4 },
    })
    await tHelper.uploader.completeUpload({
      version: 'restored-v2',
      bucketId,
      objectName,
      owner: undefined,
      objectMetadata: {
        eTag: 'etag',
        mimetype: 'text/plain',
        cacheControl: 'no-cache',
        lastModified: new Date(),
        contentLength: 4,
        httpStatusCode: 200,
        size: 4,
      },
      uploadType: 'standard',
      isUpsert: false,
      userMetadata: undefined,
    })

    await expect(
      tHelper.database.findObject(bucketId, objectName, 'version,is_delete_marker,is_versioned')
    ).resolves.toMatchObject({
      version: 'restored-v2',
      is_delete_marker: false,
      is_versioned: status === 'ENABLED',
    })
  })

  it('ENABLED -> SUSPENDED: delete preserves the enabled version behind a null delete marker', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 1 },
      user_metadata: null,
      version: 'enabled-v1',
    })
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })

    const { deleted: marker } = await tHelper.database.deleteObject(bucketId, objectName)

    const rows = await allRowsFor(objectName)
    expect(rows).toHaveLength(2)
    expect(rows.find((row) => row.archived_at === null)).toMatchObject({
      version: marker!.version,
      is_delete_marker: true,
      is_versioned: false,
    })
    expect(rows.find((row) => row.version === 'enabled-v1')).toMatchObject({
      is_delete_marker: false,
      is_versioned: true,
    })
  })

  it('delete with a versionId is always a real physical removal, in any mode', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v1',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v2',
    })
    expect(await allRowsFor(objectName)).toHaveLength(2)

    const { deleted } = await tHelper.database.deleteObject(bucketId, objectName, 'v1')
    expect(deleted).toMatchObject({ version: 'v1' })

    const rows = await allRowsFor(objectName)
    expect(rows).toHaveLength(1)
    expect(rows[0].version).toBe('v2')
  })

  it('deleting the current version promotes the newest remaining version', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v1',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v2',
    })

    const { deleted, promoted } = await tHelper.database.deleteObject(bucketId, objectName, 'v2')

    expect(deleted).toMatchObject({ version: 'v2' })
    expect(promoted).toMatchObject({ version: 'v1' })

    const rows = await allRowsFor(objectName)
    expect(rows).toHaveLength(1)
    expect(rows[0]).toMatchObject({ version: 'v1', archived_at: null })
    await expect(
      tHelper.database.findObject(bucketId, objectName, 'version')
    ).resolves.toMatchObject({ version: 'v1' })
  })

  it('deleting a current delete marker promotes the previous content version', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v1',
    })
    const { deleted: marker } = await tHelper.database.deleteObject(bucketId, objectName)

    const { deleted, promoted } = await tHelper.database.deleteObject(
      bucketId,
      objectName,
      marker!.version
    )

    expect(deleted).toMatchObject({ version: marker!.version })
    expect(promoted).toMatchObject({ version: 'v1' })

    const rows = await allRowsFor(objectName)
    expect(rows).toHaveLength(1)
    expect(rows[0]).toMatchObject({
      version: 'v1',
      archived_at: null,
      is_delete_marker: false,
    })
    await expect(
      tHelper.database.findObject(bucketId, objectName, 'version,is_delete_marker')
    ).resolves.toMatchObject({ version: 'v1', is_delete_marker: false })
  })

  it('single delete: promoting an older version emits ObjectPromoted, not just ObjectRemoved', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 1 },
      user_metadata: null,
      version: 'v1',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 2 },
      user_metadata: null,
      version: 'v2',
    })

    const removedSpy = vi.spyOn(ObjectRemoved, 'sendWebhook')
    const promotedSpy = vi.spyOn(ObjectPromoted, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).deleteObject(objectName, 'v2')

      // v2 (hard-deleted) and v1 (silently promoted) are two different rows
      // the promotion must not be folded into the removal event
      expect(removedSpy).toHaveBeenCalledWith(
        expect.objectContaining({ name: objectName, version: 'v2', bytesChanged: -2 })
      )
      expect(promotedSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          name: objectName,
          bucketId,
          version: 'v1',
          metadata: expect.objectContaining({ size: 1 }),
          bytesChanged: 0,
        })
      )
    } finally {
      removedSpy.mockRestore()
      promotedSpy.mockRestore()
    }
  })

  it('single delete: hard-deleting a current delete marker frees no bytes and still emits ObjectPromoted', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 1 },
      user_metadata: null,
      version: 'v1',
    })
    await tHelper.storage.from(bucketId).deleteObject(objectName)
    const marker = (await allRowsFor(objectName)).find(
      (r) => r.is_delete_marker && r.archived_at === null
    )
    expect(marker).toBeDefined()

    const removedSpy = vi.spyOn(ObjectRemoved, 'sendWebhook')
    const promotedSpy = vi.spyOn(ObjectPromoted, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).deleteObject(objectName, marker!.version)

      // Hard-deleting the marker itself frees no bytes (markers own none),
      // and the content it was hiding silently becomes current again.
      expect(removedSpy).toHaveBeenCalledWith(
        expect.objectContaining({ name: objectName, version: marker!.version, bytesChanged: 0 })
      )
      expect(removedSpy.mock.calls[0][0]).not.toHaveProperty('replacedObject')
      expect(promotedSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          name: objectName,
          bucketId,
          version: 'v1',
          metadata: expect.objectContaining({ size: 1 }),
          bytesChanged: 0,
        })
      )
    } finally {
      removedSpy.mockRestore()
      promotedSpy.mockRestore()
    }
  })

  it('single delete: nothing left to promote does not emit ObjectPromoted', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 1 },
      user_metadata: null,
      version: 'v1',
    })

    const promotedSpy = vi.spyOn(ObjectPromoted, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).deleteObject(objectName, 'v1')
      expect(promotedSpy).not.toHaveBeenCalled()
    } finally {
      promotedSpy.mockRestore()
    }
  })

  it('bulk delete: promoting an older version emits ObjectPromoted for the promoted row', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 1 },
      user_metadata: null,
      version: 'v1',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 2 },
      user_metadata: null,
      version: 'v2',
    })

    const removedSpy = vi.spyOn(ObjectRemoved, 'sendWebhook')
    const promotedSpy = vi.spyOn(ObjectPromoted, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).deleteObjects([{ path: objectName, versionId: 'v2' }])

      expect(removedSpy).toHaveBeenCalledWith(
        expect.objectContaining({ name: objectName, version: 'v2', bytesChanged: -2 })
      )
      expect(promotedSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          name: objectName,
          bucketId,
          version: 'v1',
          metadata: expect.objectContaining({ size: 1 }),
          bytesChanged: 0,
        })
      )
    } finally {
      removedSpy.mockRestore()
      promotedSpy.mockRestore()
    }
  })

  it('bulk delete: nothing left to promote does not emit ObjectPromoted', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 1 },
      user_metadata: null,
      version: 'v1',
    })

    const promotedSpy = vi.spyOn(ObjectPromoted, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).deleteObjects([{ path: objectName, versionId: 'v1' }])
      expect(promotedSpy).not.toHaveBeenCalled()
    } finally {
      promotedSpy.mockRestore()
    }
  })

  it("DISABLED: bulk delete without versionIds frees the deleted row's bytes", async () => {
    await tHelper.database.createBucket({ id: bucketId, name: bucketId })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 42 },
      user_metadata: null,
      version: 'v1',
    })

    const removedSpy = vi.spyOn(ObjectRemoved, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).deleteObjects([objectName])

      // DISABLED never archives - the row is physically gone, so its own
      // bytes are freed outright, with nothing netted via replacedObject.
      expect(removedSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          name: objectName,
          bucketId,
          bytesChanged: -42,
        })
      )
      expect(removedSpy.mock.calls[0][0]).not.toHaveProperty('replacedObject')
    } finally {
      removedSpy.mockRestore()
    }
  })

  it('bulk delete: a mixed batch of a plain name and an explicit version each report their own bytes', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    const plainName = `${objectName}-plain`
    const versionedName = `${objectName}-versioned`
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: plainName,
      metadata: { size: 5 },
      user_metadata: null,
      version: 'plain-v1',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: versionedName,
      metadata: { size: 9 },
      user_metadata: null,
      version: 'versioned-v1',
    })

    const removedSpy = vi.spyOn(ObjectRemoved, 'sendWebhook')
    try {
      await tHelper.storage
        .from(bucketId)
        .deleteObjects([plainName, { path: versionedName, versionId: 'versioned-v1' }])

      // A plain-name entry archives the current version behind a fresh delete-marker (no bytes freed yet)
      // while an explicit versionId hard-deletes that exact row (its own bytes freed outright)
      expect(removedSpy).toHaveBeenCalledWith(
        expect.objectContaining({ name: plainName, bucketId, bytesChanged: 0 })
      )
      expect(removedSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          name: versionedName,
          bucketId,
          version: 'versioned-v1',
          bytesChanged: -9,
        })
      )
    } finally {
      removedSpy.mockRestore()
    }
  })

  it('bulk delete: a same-name batch deleting both the current and an older version promotes nothing', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 1 },
      user_metadata: null,
      version: 'v1',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: { size: 2 },
      user_metadata: null,
      version: 'v2',
    })

    const removedSpy = vi.spyOn(ObjectRemoved, 'sendWebhook')
    const promotedSpy = vi.spyOn(ObjectPromoted, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).deleteObjects([
        { path: objectName, versionId: 'v1' },
        { path: objectName, versionId: 'v2' },
      ])

      // Both versions for this key are gone, so there's nothing left behind either one to promote
      // v1 being in the same batch as the deleted current version (v2) must not make it look like a promotion
      expect(removedSpy).toHaveBeenCalledWith(
        expect.objectContaining({ name: objectName, version: 'v1', bytesChanged: -1 })
      )
      expect(removedSpy).toHaveBeenCalledWith(
        expect.objectContaining({ name: objectName, version: 'v2', bytesChanged: -2 })
      )
      expect(promotedSpy).not.toHaveBeenCalled()
    } finally {
      removedSpy.mockRestore()
      promotedSpy.mockRestore()
    }

    expect(await allRowsFor(objectName)).toHaveLength(0)
  })

  it('excludes current delete markers at the object-storage boundary', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v1',
    })
    await tHelper.database.deleteObject(bucketId, objectName)

    await expect(tHelper.database.findObjects(bucketId, [objectName], 'name')).resolves.toEqual([
      { name: objectName },
    ])
    await expect(tHelper.storage.from(bucketId).findObjects([objectName], 'name')).resolves.toEqual(
      []
    )
  })

  it('owner updates affect only the current version', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v1',
      owner: 'old-owner',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v2',
      owner: 'old-owner',
    })

    await tHelper.database.updateObjectOwner(bucketId, objectName, 'new-owner')

    const owners = await tHelper.database.connection.query<{
      version: string
      owner_id: string | null
    }>('SELECT version, owner_id FROM storage.objects WHERE bucket_id = $1 AND name = $2', [
      bucketId,
      objectName,
    ])
    expect(owners.rows.find((row) => row.version === 'v1')?.owner_id).toBe('old-owner')
    expect(owners.rows.find((row) => row.version === 'v2')?.owner_id).toBe('new-owner')
  })

  it('ENABLED: bulk delete without versionIds writes batched delete-markers', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    const nameA = `${objectName}-a`
    const nameB = `${objectName}-b`
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: nameA,
      metadata: null,
      user_metadata: null,
      version: 'a1',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: nameB,
      metadata: null,
      user_metadata: null,
      version: 'b1',
    })

    const markers = await tHelper.database.deleteObjects(bucketId, [nameA, nameB], 'name')
    expect(markers).toHaveLength(2)
    expect(markers.every((m) => m.is_delete_marker && m.is_versioned)).toBe(true)

    expect(await allRowsFor(nameA)).toHaveLength(2)
    expect(await allRowsFor(nameB)).toHaveLength(2)
  })

  it('SUSPENDED: bulk delete without versionIds overwrites null-version rows', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    const nameA = `${objectName}-a`
    const nameB = `${objectName}-b`
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: nameA,
      metadata: null,
      user_metadata: null,
      version: 'a1',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: nameB,
      metadata: null,
      user_metadata: null,
      version: 'b1',
    })

    const markers = await tHelper.database.deleteObjects(bucketId, [nameA, nameB], 'name')
    expect(markers).toHaveLength(2)
    expect(markers.every((m) => m.is_delete_marker && !m.is_versioned)).toBe(true)

    expect(await allRowsFor(nameA)).toHaveLength(1)
    expect(await allRowsFor(nameB)).toHaveLength(1)
  })

  it('bulk delete deduplicates repeated names in enabled and suspended modes', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v1',
    })

    const enabledMarkers = await tHelper.database.deleteObjects(
      bucketId,
      [objectName, objectName],
      'name'
    )
    expect(enabledMarkers).toHaveLength(1)

    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    const suspendedMarkers = await tHelper.database.deleteObjects(
      bucketId,
      [objectName, objectName],
      'name'
    )
    expect(suspendedMarkers).toHaveLength(1)
    expect((await allRowsFor(objectName)).filter((row) => row.archived_at === null)).toHaveLength(1)
  })

  it('admin by:"id" bulk delete always hard-deletes, regardless of versioning status', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v1',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v2',
    })

    const archived = (
      await tHelper.database.connection.query<{ id: string }>(
        `SELECT id FROM storage.objects
       WHERE bucket_id = $1 AND name = $2 AND archived_at IS NOT NULL`,
        [bucketId, objectName]
      )
    ).rows[0]

    const deleted = await tHelper.database.deleteObjects(bucketId, [archived.id], 'id')
    expect(deleted).toHaveLength(1)
    expect(deleted[0]).toMatchObject({ version: 'v1', is_delete_marker: false })
    expect(await allRowsFor(objectName)).toHaveLength(1)
    expect((await allRowsFor(objectName))[0]).toMatchObject({ version: 'v2', archived_at: null })
  })

  it('ENABLED: archives versions in write order even when an older transaction writes last', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    const write = (version: string, db = tHelper.database) =>
      db.upsertObject({
        bucket_id: bucketId,
        name: objectName,
        metadata: null,
        user_metadata: null,
        version,
      })
    await write('v1')

    // The late writer's transaction starts (and so pins now()) before the
    // early writer runs, but it archives v2 after v2 archived v1.
    const lateStarted = Promise.withResolvers<void>()
    const releaseLate = Promise.withResolvers<void>()
    const late = tHelper.database.withTransaction(async (db) => {
      await db.findBucketById(bucketId, 'id')
      lateStarted.resolve()
      await releaseLate.promise
      await write('v3', db)
    })
    await lateStarted.promise
    await new Promise((resolve) => setTimeout(resolve, 20))
    await write('v2')
    releaseLate.resolve()
    await late

    const archiveOrder = await tHelper.database.connection.query<{
      in_write_order: boolean | null
    }>(
      `SELECT
         max(archived_at) FILTER (WHERE version = 'v2') >
         max(archived_at) FILTER (WHERE version = 'v1') AS in_write_order
       FROM storage.objects
       WHERE bucket_id = $1 AND name = $2 AND archived_at IS NOT NULL`,
      [bucketId, objectName]
    )
    expect(archiveOrder.rows).toEqual([{ in_write_order: true }])

    // Promotion follows the same order: removing v3 must bring back v2, not v1.
    await tHelper.database.deleteObject(bucketId, objectName, 'v3')
    expect((await allRowsFor(objectName)).find((row) => row.archived_at === null)).toMatchObject({
      version: 'v2',
    })
  })

  it.each([
    'ENABLED',
    'SUSPENDED',
  ] as const)('%s: a delete marker records the principal that deleted', async (status) => {
    const owner = randomUUID()
    await tHelper.database.createBucket({ id: bucketId, name: bucketId })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v1',
      owner: randomUUID(),
    })
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
    if (status === 'SUSPENDED') {
      // The marker rewrites the not-versioned v1 row in place (ON CONFLICT DO
      // UPDATE) instead of inserting, so the owner must be updated as well.
      await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    }

    await tHelper.storage.from(bucketId).deleteObject(objectName, undefined, { owner })

    const marker = await tHelper.database.findObject(
      bucketId,
      objectName,
      'owner,owner_id,is_delete_marker'
    )
    expect(marker).toMatchObject({ is_delete_marker: true, owner, owner_id: owner })
  })

  it.each([
    'ENABLED',
    'SUSPENDED',
  ] as const)('%s: bulk delete markers record the principal, for existing and missing keys', async (status) => {
    const owner = randomUUID()
    const missingName = `${objectName}-missing`
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v1',
      owner: randomUUID(),
    })
    if (status === 'SUSPENDED') {
      await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    }

    await tHelper.storage.from(bucketId).deleteObjects([objectName, missingName], { owner })

    const markers = await tHelper.database.findObjects(
      bucketId,
      [objectName, missingName],
      'name,owner,owner_id,is_delete_marker'
    )
    expect(markers).toHaveLength(2)
    for (const marker of markers) {
      expect(marker).toMatchObject({ is_delete_marker: true, owner, owner_id: owner })
    }
  })

  it.each([
    'DISABLED',
    'ENABLED',
  ] as const)('%s: replaying the completion of a committed upload keeps the object', async (status) => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      ...(status === 'ENABLED' ? { versioning_status: 'ENABLED' } : {}),
    })
    const deleteSpy = vi.spyOn(ObjectAdminDelete, 'send').mockResolvedValue(undefined)
    const version = randomUUID()
    const complete = () =>
      tHelper.uploader.completeUpload({
        version,
        bucketId,
        objectName,
        owner: undefined,
        objectMetadata: {
          eTag: 'etag',
          mimetype: 'text/plain',
          cacheControl: 'no-cache',
          lastModified: new Date(),
          contentLength: 4,
          httpStatusCode: 200,
          size: 4,
        },
        uploadType: 'resumable',
        isUpsert: false,
        userMetadata: undefined,
      })

    await complete()
    // The retried final TUS PATCH runs the finish hook again with the same
    // version: a non-upsert upload must not fail as a duplicate key, and the
    // failure path must not schedule the removal of the live content.
    await expect(complete()).resolves.toMatchObject({ obj: { version }, isNew: false })

    expect(deleteSpy).not.toHaveBeenCalled()
    expect(await allRowsFor(objectName)).toEqual([
      expect.objectContaining({ version, archived_at: null, is_delete_marker: false }),
    ])
  })
})
