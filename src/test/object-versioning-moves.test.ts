import { randomUUID } from 'node:crypto'
import { Readable } from 'node:stream'
import {
  ObjectAdminDelete,
  ObjectCreatedMove,
  ObjectPromotedMove,
  ObjectRemovedMove,
} from '../storage/events'
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

    const removedMoveSpy = vi.spyOn(ObjectRemovedMove, 'sendWebhook')
    const promotedSpy = vi.spyOn(ObjectPromotedMove, 'sendWebhook')
    try {
      await tHelper.storage
        .from(bucketId)
        .moveObject(sourceName, bucketId, destName, 'standard', undefined, sourceVersion)

      // The source's own content is hard-deleted, so its bytes are freed,
      // and there's no other row to report via replacedObject.
      expect(removedMoveSpy).toHaveBeenCalledWith(expect.objectContaining({ bytesChanged: -4 }))
      // Nothing else existed under this key to promote.
      expect(promotedSpy).not.toHaveBeenCalled()
    } finally {
      removedMoveSpy.mockRestore()
      promotedSpy.mockRestore()
    }

    expect(await allRowsFor(sourceName)).toHaveLength(0)
    expect(await allRowsFor(destName)).toHaveLength(1)
  })

  it('moving the current version by its explicit id promotes the older version at the source', async () => {
    const historicalVersion = await uploadSource(false, 'historical content')
    const currentVersion = await uploadSource(true, 'current content')

    const removedMoveSpy = vi.spyOn(ObjectRemovedMove, 'sendWebhook')
    const promotedSpy = vi.spyOn(ObjectPromotedMove, 'sendWebhook')
    try {
      await tHelper.storage
        .from(bucketId)
        .moveObject(sourceName, bucketId, destName, 'standard', undefined, currentVersion)

      // currentVersion is hard-deleted (moved away)
      // a different, untouched row silently becomes current in its place
      expect(removedMoveSpy).toHaveBeenCalledWith(
        expect.objectContaining({ name: sourceName, version: currentVersion })
      )
      expect(promotedSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          name: sourceName,
          bucketId,
          version: historicalVersion,
          metadata: expect.objectContaining({ size: 'historical content'.length }),
          bytesChanged: 0,
        })
      )
    } finally {
      removedMoveSpy.mockRestore()
      promotedSpy.mockRestore()
    }

    await expect(currentContent()).resolves.toBe('historical content')
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

  it('same-path restore on a SUSPENDED bucket nets the overwritten current row into ObjectCreated:Move', async () => {
    const historicalContent = 'historical content'
    const historicalVersion = await uploadSource(false, historicalContent)
    await uploadSource(true, 'enabled current content')

    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    const overwrittenContent = 'suspended null-version content'
    await uploadSource(true, overwrittenContent)

    const removedMoveSpy = vi.spyOn(ObjectRemovedMove, 'sendWebhook')
    const createdMoveSpy = vi.spyOn(ObjectCreatedMove, 'sendWebhook')
    try {
      await tHelper.storage
        .from(bucketId)
        .moveObject(sourceName, bucketId, sourceName, 'standard', undefined, historicalVersion)

      expect(removedMoveSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          name: sourceName,
          bucketId,
          bytesChanged: -historicalContent.length,
        })
      )
      expect(removedMoveSpy.mock.calls[0][0]).not.toHaveProperty('replacedObject')
      expect(createdMoveSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          name: sourceName,
          bucketId,
          metadata: expect.objectContaining({ size: historicalContent.length }),
          bytesChanged: historicalContent.length - overwrittenContent.length,
          replacedObject: expect.objectContaining({
            metadata: expect.objectContaining({ size: overwrittenContent.length }),
          }),
        })
      )

      // The net physical change across both events is just the destroyed
      // row's bytes: the historical row's content moved, not grew or shrank.
      const removedPayload = removedMoveSpy.mock.calls[0][0] as unknown as { bytesChanged: number }
      const createdPayload = createdMoveSpy.mock.calls[0][0] as unknown as { bytesChanged: number }
      expect(removedPayload.bytesChanged + createdPayload.bytesChanged).toBe(
        -overwrittenContent.length
      )
    } finally {
      removedMoveSpy.mockRestore()
      createdMoveSpy.mockRestore()
    }

    expect(await currentContent()).toBe('historical content')
  })

  it('reports the row actually freed, not the one the event otherwise describes, when a SUSPENDED marker reuses a stale null-version slot', async () => {
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    const staleContent = 'stale suspended content'
    await uploadSource(false, staleContent)

    await tHelper.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
    const currentContentStr = 'enabled content'
    await uploadSource(true, currentContentStr)

    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })

    const removedMoveSpy = vi.spyOn(ObjectRemovedMove, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).moveObject(sourceName, bucketId, destName, 'standard')

      expect(removedMoveSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          name: sourceName,
          bucketId,
          // The event's own identity is the row actually archived from current
          metadata: expect.objectContaining({ size: currentContentStr.length }),
          // ...but the bytes physically freed came from the stale row the
          // marker's insert reused, reported separately via replacedObject.
          bytesChanged: -staleContent.length,
          replacedObject: expect.objectContaining({
            metadata: expect.objectContaining({ size: staleContent.length }),
          }),
        })
      )
    } finally {
      removedMoveSpy.mockRestore()
    }
  })

  it('does not emit ObjectRemoved:Delete for an ordinary move that overwrites nothing, and reports the source as archived (not freed)', async () => {
    const content = 'source content'
    await uploadSource(false, content)

    const removedMoveSpy = vi.spyOn(ObjectRemovedMove, 'sendWebhook')
    const createdMoveSpy = vi.spyOn(ObjectCreatedMove, 'sendWebhook')
    try {
      await tHelper.storage.from(bucketId).moveObject(sourceName, bucketId, destName, 'standard')

      // ENABLED: the source is archived behind a delete-marker, not freed,
      // and no other row was reused in place.
      expect(removedMoveSpy).toHaveBeenCalledWith(expect.objectContaining({ bytesChanged: 0 }))
      // The destination always receives genuinely new bytes; nothing existed
      // there to replace.
      expect(createdMoveSpy).toHaveBeenCalledWith(
        expect.objectContaining({ bytesChanged: content.length })
      )
    } finally {
      removedMoveSpy.mockRestore()
      createdMoveSpy.mockRestore()
    }
  })

  it('same-path move by the current version id does not double free its own row (SUSPENDED)', async () => {
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    const content = 'suspended content'
    const currentVersion = await uploadSource(false, content)

    const removedMoveSpy = vi.spyOn(ObjectRemovedMove, 'sendWebhook')
    const createdMoveSpy = vi.spyOn(ObjectCreatedMove, 'sendWebhook')
    const adminDeleteSpy = vi.spyOn(ObjectAdminDelete, 'send')
    try {
      await tHelper.storage
        .from(bucketId)
        .moveObject(sourceName, bucketId, sourceName, 'standard', undefined, currentVersion)

      // The destination write landed on an already-deleted row - nothing was
      // left for it to replace, so the two events must net to zero real bytes.
      const removedPayload = removedMoveSpy.mock.calls[0][0] as unknown as { bytesChanged: number }
      const createdPayload = createdMoveSpy.mock.calls[0][0] as unknown as { bytesChanged: number }
      expect(removedPayload.bytesChanged + createdPayload.bytesChanged).toBe(0)
      expect(createdPayload).not.toHaveProperty('replacedObject')

      // The old row's backend bytes must be queued for cleanup exactly once.
      expect(adminDeleteSpy).toHaveBeenCalledWith(
        expect.objectContaining({ version: currentVersion })
      )
      const callsForVersion = adminDeleteSpy.mock.calls.filter(
        (call) => (call[0] as unknown as { version?: string }).version === currentVersion
      )
      expect(callsForVersion).toHaveLength(1)
    } finally {
      removedMoveSpy.mockRestore()
      createdMoveSpy.mockRestore()
      adminDeleteSpy.mockRestore()
    }
  })

  it('same-path move by the current version id does not promote older history either (ENABLED)', async () => {
    const historicalContent = 'historical content'
    await uploadSource(false, historicalContent)
    const content = 'current content'
    const currentVersion = await uploadSource(true, content)

    const removedMoveSpy = vi.spyOn(ObjectRemovedMove, 'sendWebhook')
    const createdMoveSpy = vi.spyOn(ObjectCreatedMove, 'sendWebhook')
    const promotedSpy = vi.spyOn(ObjectPromotedMove, 'sendWebhook')
    try {
      await tHelper.storage
        .from(bucketId)
        .moveObject(sourceName, bucketId, sourceName, 'standard', undefined, currentVersion)

      // The destination write always reclaims this key's current slot in the
      // same transaction, so the older history behind it is never promoted.
      // ENABLED archives rather than overwrites, so nothing is replaced and
      // the two events net to zero real bytes, same as the SUSPENDED case.
      expect(promotedSpy).not.toHaveBeenCalled()
      const removedPayload = removedMoveSpy.mock.calls[0][0] as unknown as { bytesChanged: number }
      const createdPayload = createdMoveSpy.mock.calls[0][0] as unknown as { bytesChanged: number }
      expect(removedPayload.bytesChanged + createdPayload.bytesChanged).toBe(0)
      expect(createdPayload).not.toHaveProperty('replacedObject')
    } finally {
      removedMoveSpy.mockRestore()
      createdMoveSpy.mockRestore()
      promotedSpy.mockRestore()
    }

    const rows = await allRowsFor(sourceName)
    expect(rows).toHaveLength(2)
    expect(rows.some((r) => r.version === currentVersion)).toBe(false)
    expect(rows.filter((r) => r.archived_at !== null)).toHaveLength(1)
    expect(rows.filter((r) => r.archived_at === null)).toHaveLength(1)
  })

  it('same-path move does not promote a row that the destination write immediately reclaims', async () => {
    await uploadSource(false, 'aaa')
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    await uploadSource(true, 'bb')
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })
    const v3 = await uploadSource(true, 'cccc')
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })

    const promotedSpy = vi.spyOn(ObjectPromotedMove, 'sendWebhook')
    try {
      await tHelper.storage
        .from(bucketId)
        .moveObject(sourceName, bucketId, sourceName, 'standard', undefined, v3)

      // The destination write reclaims this key's current slot in the same
      // transaction regardless of what the delete would otherwise promote.
      expect(promotedSpy).not.toHaveBeenCalled()
    } finally {
      promotedSpy.mockRestore()
    }
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
