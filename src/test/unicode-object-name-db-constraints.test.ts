'use strict'

import { randomUUID } from 'node:crypto'
import { DatabaseError } from 'pg'
import { useStorage } from './utils/storage'

describe('Unicode object name database constraints', () => {
  const tHelper = useStorage()
  const testBucketName = `unicode-db-constraints-${Date.now()}`

  beforeAll(async () => {
    await tHelper.database.createBucket({
      id: testBucketName,
      name: testBucketName,
    })
  })

  const invalidKey = `invalid-\u000b-${randomUUID()}`

  it('rejects invalid object names at the storage.objects constraint', async () => {
    const db = tHelper.database.connection.pool.acquire()
    const tnx = await db.transaction()

    try {
      await expect(
        tnx.raw(
          'INSERT INTO storage.objects (bucket_id, name, owner, version) VALUES (?, ?, ?, ?)',
          [testBucketName, invalidKey, null, randomUUID()]
        )
      ).rejects.toMatchObject<Partial<DatabaseError>>({
        code: '23514',
        constraint: 'objects_name_check',
      })
    } finally {
      await tnx.rollback()
    }
  })

  it('rejects invalid multipart upload keys at the storage.s3_multipart_uploads constraint', async () => {
    const db = tHelper.database.connection.pool.acquire()
    const tnx = await db.transaction()

    try {
      await expect(
        tnx.raw(
          `INSERT INTO storage.s3_multipart_uploads
            (id, in_progress_size, upload_signature, bucket_id, key, version, owner_id)
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
          [randomUUID(), 0, 'sig', testBucketName, invalidKey, randomUUID(), null]
        )
      ).rejects.toMatchObject<Partial<DatabaseError>>({
        code: '23514',
        constraint: 's3_multipart_uploads_key_check',
      })
    } finally {
      await tnx.rollback()
    }
  })

  it('rejects invalid multipart part keys at the storage.s3_multipart_uploads_parts constraint', async () => {
    const db = tHelper.database.connection.pool.acquire()
    const tnx = await db.transaction()
    const uploadId = randomUUID()

    try {
      await tnx.raw(
        `INSERT INTO storage.s3_multipart_uploads
          (id, in_progress_size, upload_signature, bucket_id, key, version, owner_id)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [uploadId, 0, 'sig', testBucketName, `valid-${randomUUID()}.txt`, randomUUID(), null]
      )

      await expect(
        tnx.raw(
          `INSERT INTO storage.s3_multipart_uploads_parts
            (upload_id, size, part_number, bucket_id, key, etag, owner_id, version)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
          [uploadId, 1, 1, testBucketName, invalidKey, 'etag', null, randomUUID()]
        )
      ).rejects.toMatchObject<Partial<DatabaseError>>({
        code: '23514',
        constraint: 's3_multipart_uploads_parts_key_check',
      })
    } finally {
      await tnx.rollback()
    }
  })
})
