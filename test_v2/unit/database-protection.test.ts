import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { randomUUID } from 'node:crypto'
import { Knex } from 'knex'
import { DatabaseError } from 'pg'
import { disposeTestKnex, getTestKnex, withDeleteEnabled } from '@internal/testing/helpers'

// Database-level tests for the protect_delete trigger (migration 0055). These
// run directly against the postgres-superuser knex and skip the normal tenant
// routing so we can assert the trigger behavior deterministically. Everything
// is cleaned up in afterAll via withDeleteEnabled.

describe('Database Protection Triggers', () => {
  // Deferred until beforeAll so DATABASE_URL from .env has been loaded by the
  // setup.ts hook before we open the connection.
  let db: Knex
  const testBucketName = `v2_db_protection_${randomUUID().slice(0, 8)}`

  beforeAll(async () => {
    db = getTestKnex()
    await db.raw('INSERT INTO storage.buckets (id, name) VALUES (?, ?)', [
      testBucketName,
      testBucketName,
    ])
  })

  afterAll(async () => {
    await withDeleteEnabled(db, async (trx) => {
      await trx('storage.objects').where({ bucket_id: testBucketName }).del()
      await trx('storage.buckets').where({ id: testBucketName }).del()
    })
    await disposeTestKnex()
  })

  describe('Direct DELETE protection (migration 0055)', () => {
    it('blocks DELETE on storage.buckets without storage.allow_delete_query', async () => {
      const tempBucket = `v2_temp_bucket_${randomUUID().slice(0, 8)}`

      await db.raw('INSERT INTO storage.buckets (id, name) VALUES (?, ?)', [
        tempBucket,
        tempBucket,
      ])

      try {
        await db.raw('DELETE FROM storage.buckets WHERE id = ?', [tempBucket])
        throw new Error('Expected DELETE to be blocked by trigger')
      } catch (error) {
        const dbError = error as DatabaseError
        expect(dbError.code).toBe('42501')
        expect(dbError.message).toContain('Direct deletion from storage tables is not allowed')
      }

      const result = await db.raw('SELECT id FROM storage.buckets WHERE id = ?', [tempBucket])
      expect(result.rows).toHaveLength(1)

      await withDeleteEnabled(db, async (trx) => {
        await trx.raw('DELETE FROM storage.buckets WHERE id = ?', [tempBucket])
      })
    })

    it('blocks DELETE on storage.objects without storage.allow_delete_query', async () => {
      const testObjectName = `test-object-${randomUUID().slice(0, 8)}.txt`

      await db.raw(
        'INSERT INTO storage.objects (bucket_id, name, owner, version) VALUES (?, ?, ?, ?)',
        [testBucketName, testObjectName, null, '1']
      )

      try {
        await db.raw('DELETE FROM storage.objects WHERE bucket_id = ? AND name = ?', [
          testBucketName,
          testObjectName,
        ])
        throw new Error('Expected DELETE to be blocked by trigger')
      } catch (error) {
        const dbError = error as DatabaseError
        expect(dbError.code).toBe('42501')
        expect(dbError.message).toContain('Direct deletion from storage tables is not allowed')
      }

      const result = await db.raw(
        'SELECT name FROM storage.objects WHERE bucket_id = ? AND name = ?',
        [testBucketName, testObjectName]
      )
      expect(result.rows).toHaveLength(1)

      await withDeleteEnabled(db, async (trx) => {
        await trx.raw('DELETE FROM storage.objects WHERE bucket_id = ? AND name = ?', [
          testBucketName,
          testObjectName,
        ])
      })
    })

    it('allows DELETE on storage.buckets when storage.allow_delete_query is set', async () => {
      const tempBucket = `v2_allow_bucket_${randomUUID().slice(0, 8)}`

      await withDeleteEnabled(db, async (trx) => {
        await trx.raw('INSERT INTO storage.buckets (id, name) VALUES (?, ?)', [
          tempBucket,
          tempBucket,
        ])
        await trx.raw('DELETE FROM storage.buckets WHERE id = ?', [tempBucket])

        const result = await trx.raw('SELECT id FROM storage.buckets WHERE id = ?', [tempBucket])
        expect(result.rows).toHaveLength(0)
      })
    })

    it('allows DELETE on storage.objects when storage.allow_delete_query is set', async () => {
      const testObjectName = `test-object-allow-${randomUUID().slice(0, 8)}.txt`

      await withDeleteEnabled(db, async (trx) => {
        await trx.raw(
          'INSERT INTO storage.objects (bucket_id, name, owner, version) VALUES (?, ?, ?, ?)',
          [testBucketName, testObjectName, null, '1']
        )

        await trx.raw('DELETE FROM storage.objects WHERE bucket_id = ? AND name = ?', [
          testBucketName,
          testObjectName,
        ])

        const result = await trx.raw(
          'SELECT name FROM storage.objects WHERE bucket_id = ? AND name = ?',
          [testBucketName, testObjectName]
        )
        expect(result.rows).toHaveLength(0)
      })
    })
  })
})
