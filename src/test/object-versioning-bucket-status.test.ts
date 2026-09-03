import { randomUUID } from 'node:crypto'
import { useStorage, withDeleteEnabled } from './utils/storage'

describe('object versioning - bucket status transitions', () => {
  const tHelper = useStorage()
  let bucketId: string

  beforeEach(() => {
    bucketId = `versioning-writes-${randomUUID()}`
  })

  afterEach(async () => {
    await withDeleteEnabled(tHelper.database.connection, async (transaction) => {
      await transaction.query('DELETE FROM storage.objects WHERE bucket_id = $1', [bucketId])
      await transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
    })
  })

  it('defaults to DISABLED and allows DISABLED -> ENABLED', async () => {
    await tHelper.database.createBucket({ id: bucketId, name: bucketId })

    const created = await tHelper.database.connection.query<{ versioning_status: string }>(
      'SELECT versioning_status FROM storage.buckets WHERE id = $1',
      [bucketId]
    )
    expect(created.rows[0].versioning_status).toBe('DISABLED')

    await tHelper.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })

    const updated = await tHelper.database.connection.query<{ versioning_status: string }>(
      'SELECT versioning_status FROM storage.buckets WHERE id = $1',
      [bucketId]
    )
    expect(updated.rows[0].versioning_status).toBe('ENABLED')
  })

  it('rejects SUSPENDED at creation time', async () => {
    await expect(
      tHelper.database.createBucket({
        id: bucketId,
        name: bucketId,
        versioning_status: 'SUSPENDED',
      })
    ).rejects.toMatchObject({ message: expect.stringContaining('never had it enabled') })
  })

  it('rejects DISABLED -> SUSPENDED', async () => {
    await tHelper.database.createBucket({ id: bucketId, name: bucketId })

    await expect(
      tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    ).rejects.toMatchObject({ message: expect.stringContaining('DISABLED to SUSPENDED') })
  })

  it('allows ENABLED <-> SUSPENDED both ways', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })

    await tHelper.database.updateBucket(bucketId, { versioning_status: 'SUSPENDED' })
    await tHelper.database.updateBucket(bucketId, { versioning_status: 'ENABLED' })

    const final = await tHelper.database.connection.query<{ versioning_status: string }>(
      'SELECT versioning_status FROM storage.buckets WHERE id = $1',
      [bucketId]
    )
    expect(final.rows[0].versioning_status).toBe('ENABLED')
  })

  it('rejects any transition back to DISABLED', async () => {
    await tHelper.database.createBucket({
      id: bucketId,
      name: bucketId,
      versioning_status: 'ENABLED',
    })

    await expect(
      tHelper.database.updateBucket(bucketId, { versioning_status: 'DISABLED' })
    ).rejects.toMatchObject({ message: expect.stringContaining('ENABLED to DISABLED') })
  })
})
