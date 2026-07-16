import { defineBucketColumns } from '@storage/database'
import { useStorage } from './utils/storage'

const BUCKET_ID_NAME_COLUMNS = defineBucketColumns('id', 'name')

describe('pg storage test helper', () => {
  const t = useStorage()

  it('creates and deletes an isolated bucket without dummy data', async () => {
    const bucketId = t.random.name('pg-helper')

    await t.storage.createBucket({
      id: bucketId,
      name: bucketId,
    })

    await expect(t.database.findBucketById(bucketId, BUCKET_ID_NAME_COLUMNS)).resolves.toEqual({
      id: bucketId,
      name: bucketId,
    })

    await expect(t.database.deleteBucket(bucketId)).resolves.toBe(1)
  })
})
