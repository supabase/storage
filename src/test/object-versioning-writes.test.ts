import { randomUUID } from 'node:crypto'
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

  it('DISABLED: delete without a versionId physically removes the row', async () => {
    await tHelper.database.createBucket({ id: bucketId, name: bucketId })
    await tHelper.database.upsertObject({
      bucket_id: bucketId,
      name: objectName,
      metadata: null,
      user_metadata: null,
      version: 'v1',
    })

    const deleted = await tHelper.database.deleteObject(bucketId, objectName)
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

    const marker = await tHelper.database.deleteObject(bucketId, objectName)
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

    const marker = await tHelper.database.deleteObject(bucketId, objectName)
    expect(marker).toMatchObject({ is_delete_marker: true, is_versioned: false })

    const rows = await allRowsFor(objectName)
    expect(rows).toHaveLength(1)
    expect(rows[0].version).toBe(marker!.version)
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

    await tHelper.storage.from(bucketId).deleteObject(objectName)

    await expect(
      tHelper.database.findObject(bucketId, objectName, 'is_delete_marker,is_versioned,archived_at')
    ).resolves.toMatchObject({
      is_delete_marker: true,
      is_versioned: status === 'ENABLED',
      archived_at: null,
    })
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

    const marker = await tHelper.database.deleteObject(bucketId, objectName)

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

    const deleted = await tHelper.database.deleteObject(bucketId, objectName, 'v1')
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

    await tHelper.database.deleteObject(bucketId, objectName, 'v2')

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
    const marker = await tHelper.database.deleteObject(bucketId, objectName)

    await tHelper.database.deleteObject(bucketId, objectName, marker!.version)

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
})
