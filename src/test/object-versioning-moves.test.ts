import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import { useStorage, withDeleteEnabled } from './utils/storage'

describe('object versioning - moveObject', () => {
  const tHelper = useStorage()
  let bucketId: string
  let sourceName: string
  let destName: string

  beforeEach(async () => {
    bucketId = `versioning-writes-${randomUUID()}`
    sourceName = `move-src-${randomUUID()}.txt`
    destName = `move-dst-${randomUUID()}.txt`
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })
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
    }>(
      `SELECT version, archived_at, is_delete_marker
       FROM storage.objects WHERE bucket_id = $1 AND name = $2
       ORDER BY created_at`,
      [bucketId, name]
    )
    return result.rows
  }

  async function uploadSource(isUpsert = false, content = 'test') {
    const upload = await tHelper.uploader.upload({
      bucketId,
      objectName: sourceName,
      uploadType: 'standard',
      isUpsert,
      file: {
        body: Readable.from(Buffer.from(content)),
        mimeType: 'text/plain',
        cacheControl: 'no-cache',
        isTruncated: () => false,
      },
    })
    return upload.obj.version as string
  }

  async function currentContent() {
    const current = await tHelper.database.connection.query<{ version: string }>(
      `SELECT version FROM storage.objects
       WHERE bucket_id = $1 AND name = $2 AND archived_at IS NULL`,
      [bucketId, sourceName]
    )
    const object = await tHelper.storage.backend.getObject(
      tHelper.storage.location.getRootLocation(),
      tHelper.storage.location.getKeyLocation({
        tenantId: tHelper.database.tenantId,
        bucketId,
        objectName: sourceName,
      }),
      current.rows[0].version
    )
    const chunks: Buffer[] = []
    for await (const chunk of object.body as AsyncIterable<Buffer | Uint8Array | string>) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
    }
    return Buffer.concat(chunks).toString()
  }

  it('serializes concurrent moves to one destination without deadlocking or overwriting it', async () => {
    const firstName = sourceName
    const secondName = destName
    const sharedDestination = `000-move-destination-${randomUUID()}.txt`
    await uploadSource(false, 'first')

    await tHelper.uploader.upload({
      bucketId,
      objectName: secondName,
      uploadType: 'standard',
      file: {
        body: Readable.from(Buffer.from('second')),
        mimeType: 'text/plain',
        cacheControl: 'no-cache',
        isTruncated: () => false,
      },
    })

    const originalCopyObject = tHelper.storage.backend.copyObject.bind(tHelper.storage.backend)
    const bothCopiesReady = Promise.withResolvers<void>()
    const releaseCopies = Promise.withResolvers<void>()
    let copyCount = 0
    const copySpy = vi
      .spyOn(tHelper.storage.backend, 'copyObject')
      .mockImplementation(async (...args) => {
        const result = await originalCopyObject(...args)
        copyCount += 1
        if (copyCount === 2) {
          bothCopiesReady.resolve()
        }
        await releaseCopies.promise
        return result
      })

    const moves = [
      tHelper.storage.from(bucketId).moveObject(firstName, bucketId, sharedDestination, 'standard'),
      tHelper.storage
        .from(bucketId)
        .moveObject(secondName, bucketId, sharedDestination, 'standard'),
    ]

    try {
      await bothCopiesReady.promise
      releaseCopies.resolve()
      const results = await Promise.race([
        Promise.allSettled(moves),
        new Promise<never>((_, reject) =>
          setTimeout(() => reject(new Error('concurrent moves deadlocked')), 5000)
        ),
      ])

      expect(results.filter((result) => result.status === 'fulfilled')).toHaveLength(1)
      const rejected = results.find((result) => result.status === 'rejected')
      expect(rejected).toMatchObject({
        reason: expect.objectContaining({ code: 'KeyAlreadyExists' }),
      })
    } finally {
      releaseCopies.resolve()
      await Promise.allSettled(moves)
      copySpy.mockRestore()
    }
  })

  it('without sourceVersionId, the source gets a delete-marker (regular delete semantics)', async () => {
    await uploadSource()

    await tHelper.storage
      .from(bucketId)
      .moveObject(sourceName, bucketId, destName, 'standard', undefined, undefined)

    const sourceRows = await allRowsFor(sourceName)
    expect(sourceRows).toHaveLength(2)
    expect(sourceRows.some((r) => r.is_delete_marker)).toBe(true)
    expect(sourceRows.some((r) => !r.is_delete_marker && r.archived_at !== null)).toBe(true)

    const destRows = await allRowsFor(destName)
    expect(destRows).toHaveLength(1)
  })

  it('with sourceVersionId, the source row is hard-deleted', async () => {
    const sourceVersion = await uploadSource()

    await tHelper.storage
      .from(bucketId)
      .moveObject(sourceName, bucketId, destName, 'standard', undefined, sourceVersion)

    expect(await allRowsFor(sourceName)).toHaveLength(0)
    expect(await allRowsFor(destName)).toHaveLength(1)
  })

  it('same-path move with sourceVersionId restores a noncurrent version to current', async () => {
    const noncurrentVersion = await uploadSource(false, 'historical content')
    const currentVersion = await uploadSource(true, 'current content')

    await tHelper.storage
      .from(bucketId)
      .moveObject(sourceName, bucketId, sourceName, 'standard', undefined, noncurrentVersion)

    const rows = await allRowsFor(sourceName)
    // The old current row is archived, the restored version's own row is
    // hard-deleted, and a brand new current row (fresh version id) holds the
    // restored content - so exactly 2 rows remain, neither of them the
    // version that was passed in.
    expect(rows).toHaveLength(2)
    expect(rows.some((r) => r.version === noncurrentVersion)).toBe(false)
    expect(rows.some((r) => r.version === currentVersion && r.archived_at !== null)).toBe(true)
    expect(rows.some((r) => r.version !== currentVersion && r.archived_at === null)).toBe(true)
    expect(await currentContent()).toBe('historical content')
  })

  it('same-path move with sourceVersionId revives an object whose current row is a delete marker', async () => {
    const contentVersion = await uploadSource(false, 'revived content')
    await tHelper.database.deleteObject(bucketId, sourceName)

    const beforeRows = await allRowsFor(sourceName)
    expect(beforeRows).toHaveLength(2)
    const deleteMarkerRow = beforeRows.find((r) => r.is_delete_marker)
    expect(deleteMarkerRow?.archived_at).toBeNull()

    await tHelper.storage
      .from(bucketId)
      .moveObject(sourceName, bucketId, sourceName, 'standard', undefined, contentVersion)

    const rows = await allRowsFor(sourceName)
    // The delete-marker row (whatever was current) is archived, the restored
    // content version's own row is hard-deleted, and a brand new current row
    // holds the restored content - no delete marker is current anymore.
    expect(rows).toHaveLength(2)
    expect(rows.some((r) => r.version === contentVersion)).toBe(false)
    expect(
      rows.some(
        (r) =>
          r.version === deleteMarkerRow?.version && r.is_delete_marker && r.archived_at !== null
      )
    ).toBe(true)
    expect(rows.some((r) => !r.is_delete_marker && r.archived_at === null)).toBe(true)
    expect(await currentContent()).toBe('revived content')
  })
})

describe('object versioning - disabled move compatibility', () => {
  const tHelper = useStorage()
  let bucketId: string

  beforeEach(async () => {
    bucketId = `versioning-disabled-move-${randomUUID()}`
    await tHelper.database.createBucket({ id: bucketId, name: bucketId })
  })

  afterEach(async () => {
    await withDeleteEnabled(tHelper.database.connection, async (transaction) => {
      await transaction.query('DELETE FROM storage.objects WHERE bucket_id = $1', [bucketId])
      await transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
    })
  })

  it('preserves the row id when versioning is disabled', async () => {
    const sourceName = `move-src-${randomUUID()}.txt`
    const destinationName = `move-dst-${randomUUID()}.txt`
    const upload = await tHelper.uploader.upload({
      bucketId,
      objectName: sourceName,
      uploadType: 'standard',
      file: {
        body: Readable.from(Buffer.from('test')),
        mimeType: 'text/plain',
        cacheControl: 'no-cache',
        isTruncated: () => false,
      },
    })

    const moved = await tHelper.storage
      .from(bucketId)
      .moveObject(sourceName, bucketId, destinationName, 'standard')

    expect(moved.destObject.id).toBe(upload.obj.id)
    await expect(
      tHelper.database.findObject(bucketId, destinationName, 'id')
    ).resolves.toMatchObject({ id: upload.obj.id })
  })
})
