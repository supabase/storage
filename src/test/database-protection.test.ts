'use strict'

import { DatabaseError } from 'pg'
import { useStorage, withDeleteEnabled } from './utils/storage'

describe('Database Protection Triggers', () => {
  const tHelper = useStorage()
  const testBucketName = `test-db-protection-${Date.now()}`

  beforeAll(async () => {
    await tHelper.database.createBucket({
      id: testBucketName,
      name: testBucketName,
    })
  })

  afterAll(async () => {
    await tHelper.database.connection.dispose()
  })

  describe('Direct DELETE protection (migration 0050)', () => {
    it('should prevent direct DELETE on storage.buckets without storage.allow_delete_query', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const testBucket = `temp-bucket-${Date.now()}`

      // Create a test bucket
      await db.raw('INSERT INTO storage.buckets (id, name) VALUES (?, ?)', [testBucket, testBucket])

      // Attempt to delete without setting storage.allow_delete_query
      try {
        await db.raw('DELETE FROM storage.buckets WHERE id = ?', [testBucket])
        fail('Expected DELETE to be blocked by trigger')
      } catch (error) {
        const dbError = error as DatabaseError
        expect(dbError.code).toBe('42501') // PostgreSQL error code for insufficient privilege
        expect(dbError.message).toContain('Direct deletion from storage tables is not allowed')
      }

      // Verify bucket still exists
      const result = await db.raw('SELECT id FROM storage.buckets WHERE id = ?', [testBucket])
      expect(result.rows).toHaveLength(1)

      // Cleanup: delete with proper config
      await withDeleteEnabled(db, async (db) => {
        await db.raw('DELETE FROM storage.buckets WHERE id = ?', [testBucket])
      })
    })

    it('should prevent direct DELETE on storage.objects without storage.allow_delete_query', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const testObjectName = `test-object-${Date.now()}.txt`

      // Create a test object
      await db.raw(
        'INSERT INTO storage.objects (bucket_id, name, owner, version) VALUES (?, ?, ?, ?)',
        [testBucketName, testObjectName, null, '1']
      )

      // Attempt to delete without setting storage.allow_delete_query
      try {
        await db.raw('DELETE FROM storage.objects WHERE bucket_id = ? AND name = ?', [
          testBucketName,
          testObjectName,
        ])
        fail('Expected DELETE to be blocked by trigger')
      } catch (error) {
        const dbError = error as DatabaseError
        expect(dbError.code).toBe('42501')
        expect(dbError.message).toContain('Direct deletion from storage tables is not allowed')
      }

      // Verify object still exists
      const result = await db.raw(
        'SELECT name FROM storage.objects WHERE bucket_id = ? AND name = ?',
        [testBucketName, testObjectName]
      )
      expect(result.rows).toHaveLength(1)

      // Cleanup: delete with proper config
      await withDeleteEnabled(db, async (db) => {
        await db.raw('DELETE FROM storage.objects WHERE bucket_id = ? AND name = ?', [
          testBucketName,
          testObjectName,
        ])
      })
    })

    it('should allow DELETE on storage.buckets when storage.allow_delete_query is set', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const testBucket = `temp-bucket-allow-${Date.now()}`

      await withDeleteEnabled(db, async (db) => {
        // Create a test bucket
        await db.raw('INSERT INTO storage.buckets (id, name) VALUES (?, ?)', [
          testBucket,
          testBucket,
        ])

        // Delete with proper config should succeed
        await db.raw('DELETE FROM storage.buckets WHERE id = ?', [testBucket])

        // Verify bucket is deleted
        const result = await db.raw('SELECT id FROM storage.buckets WHERE id = ?', [testBucket])
        expect(result.rows).toHaveLength(0)
      })
    })

    it('should allow DELETE on storage.objects when storage.allow_delete_query is set', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const testObjectName = `test-object-allow-${Date.now()}.txt`

      await withDeleteEnabled(db, async (db) => {
        // Create a test object
        await db.raw(
          'INSERT INTO storage.objects (bucket_id, name, owner, version) VALUES (?, ?, ?, ?)',
          [testBucketName, testObjectName, null, '1']
        )

        // Delete with proper config should succeed
        await db.raw('DELETE FROM storage.objects WHERE bucket_id = ? AND name = ?', [
          testBucketName,
          testObjectName,
        ])

        // Verify object is deleted
        const result = await db.raw(
          'SELECT name FROM storage.objects WHERE bucket_id = ? AND name = ?',
          [testBucketName, testObjectName]
        )
        expect(result.rows).toHaveLength(0)
      })
    })
  })
})
