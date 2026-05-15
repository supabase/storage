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
        throw new Error('Expected DELETE to be blocked by trigger')
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
        throw new Error('Expected DELETE to be blocked by trigger')
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

  describe('Object updated_at trigger', () => {
    it('does not change updated_at when only the internal signature changes', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const testObjectName = `test-signature-updated-at-${Date.now()}.txt`
      const initialUpdatedAt = '2024-01-02T03:04:05.000Z'
      const signature = Buffer.from('a'.repeat(64), 'hex')

      try {
        await db.raw(
          `
          INSERT INTO storage.objects (bucket_id, name, owner, version, metadata, updated_at)
          VALUES (?, ?, ?, ?, ?::jsonb, ?)
          `,
          [testBucketName, testObjectName, null, '1', JSON.stringify({ size: 1 }), initialUpdatedAt]
        )

        await expect(
          tHelper.database.updateObjectSignature(testBucketName, testObjectName, '1', signature)
        ).resolves.toBe(true)

        const signed = await db.raw(
          'SELECT updated_at FROM storage.objects WHERE bucket_id = ? AND name = ?',
          [testBucketName, testObjectName]
        )
        expect(signed.rows[0].updated_at).toEqual(new Date(initialUpdatedAt))

        await db.raw(
          'UPDATE storage.objects SET metadata = ?::jsonb WHERE bucket_id = ? AND name = ?',
          [JSON.stringify({ size: 2 }), testBucketName, testObjectName]
        )

        const metadataUpdated = await db.raw(
          'SELECT updated_at FROM storage.objects WHERE bucket_id = ? AND name = ?',
          [testBucketName, testObjectName]
        )
        expect(metadataUpdated.rows[0].updated_at.getTime()).toBeGreaterThan(
          new Date(initialUpdatedAt).getTime()
        )
      } finally {
        await withDeleteEnabled(db, async (db) => {
          await db.raw('DELETE FROM storage.objects WHERE bucket_id = ? AND name = ?', [
            testBucketName,
            testObjectName,
          ])
        })
      }
    })

    it('allows authenticated users to clear signatures but not write them', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const testObjectName = `test-signature-guard-${Date.now()}.txt`
      const testInsertObjectName = `${testObjectName}.insert`
      const policyPrefix = `signature_guard_${Date.now()}`
      const selectPolicyName = `${policyPrefix}_select`
      const updatePolicyName = `${policyPrefix}_update`
      const insertPolicyName = `${policyPrefix}_insert`
      const bucketLiteral = `'${testBucketName.replace(/'/g, "''")}'`
      const signature = 'a'.repeat(64)

      try {
        await db.raw(
          `CREATE POLICY "${selectPolicyName}"
           ON storage.objects
           FOR SELECT
           TO authenticated
           USING (bucket_id = ${bucketLiteral})`
        )
        await db.raw(
          `CREATE POLICY "${updatePolicyName}"
           ON storage.objects
           FOR UPDATE
           TO authenticated
           USING (bucket_id = ${bucketLiteral})
           WITH CHECK (bucket_id = ${bucketLiteral})`
        )
        await db.raw(
          `CREATE POLICY "${insertPolicyName}"
           ON storage.objects
           FOR INSERT
           TO authenticated
           WITH CHECK (bucket_id = ${bucketLiteral})`
        )

        await db.raw(
          `
          INSERT INTO storage.objects (bucket_id, name, owner, version, metadata, signature)
          VALUES (?, ?, ?, ?, ?::jsonb, decode(?, 'hex'))
          `,
          [testBucketName, testObjectName, null, '1', JSON.stringify({ size: 1 }), signature]
        )

        await db.transaction(async (trx) => {
          await trx.raw('SET LOCAL ROLE authenticated')
          await trx.raw(
            'UPDATE storage.objects SET signature = NULL WHERE bucket_id = ? AND name = ?',
            [testBucketName, testObjectName]
          )
        })

        const cleared = await db.raw(
          'SELECT signature FROM storage.objects WHERE bucket_id = ? AND name = ?',
          [testBucketName, testObjectName]
        )
        expect(cleared.rows[0].signature).toBeNull()

        await expect(
          db.transaction(async (trx) => {
            await trx.raw('SET LOCAL ROLE authenticated')
            await trx.raw(
              `UPDATE storage.objects
               SET signature = decode(?, 'hex')
               WHERE bucket_id = ? AND name = ?`,
              [signature, testBucketName, testObjectName]
            )
          })
        ).rejects.toMatchObject({ code: '42501' })

        await expect(
          db.transaction(async (trx) => {
            await trx.raw('SET LOCAL ROLE authenticated')
            await trx.raw(
              `
              INSERT INTO storage.objects (bucket_id, name, owner, version, metadata, signature)
              VALUES (?, ?, ?, ?, ?::jsonb, decode(?, 'hex'))
              `,
              [
                testBucketName,
                testInsertObjectName,
                null,
                '1',
                JSON.stringify({ size: 1 }),
                signature,
              ]
            )
          })
        ).rejects.toMatchObject({ code: '42501' })
      } finally {
        await db.raw(`DROP POLICY IF EXISTS "${selectPolicyName}" ON storage.objects`)
        await db.raw(`DROP POLICY IF EXISTS "${updatePolicyName}" ON storage.objects`)
        await db.raw(`DROP POLICY IF EXISTS "${insertPolicyName}" ON storage.objects`)
        await withDeleteEnabled(db, async (db) => {
          await db.raw('DELETE FROM storage.objects WHERE bucket_id = ? AND name IN (?, ?)', [
            testBucketName,
            testObjectName,
            testInsertObjectName,
          ])
        })
      }
    })

    it('enforces signature write guards against the request-scoped role setting', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const testObjectName = `test-signature-effective-role-${Date.now()}.txt`
      const testInsertObjectName = `${testObjectName}.insert`
      const policyPrefix = `signature_effective_role_${Date.now()}`
      const selectPolicyName = `${policyPrefix}_select`
      const updatePolicyName = `${policyPrefix}_update`
      const insertPolicyName = `${policyPrefix}_insert`
      const bucketLiteral = `'${testBucketName.replace(/'/g, "''")}'`
      const signature = 'b'.repeat(64)

      try {
        await db.raw(
          `CREATE POLICY "${selectPolicyName}"
           ON storage.objects
           FOR SELECT
           TO authenticated
           USING (bucket_id = ${bucketLiteral})`
        )
        await db.raw(
          `CREATE POLICY "${updatePolicyName}"
           ON storage.objects
           FOR UPDATE
           TO authenticated
           USING (bucket_id = ${bucketLiteral})
           WITH CHECK (bucket_id = ${bucketLiteral})`
        )
        await db.raw(
          `CREATE POLICY "${insertPolicyName}"
           ON storage.objects
           FOR INSERT
           TO authenticated
           WITH CHECK (bucket_id = ${bucketLiteral})`
        )

        await db.raw(
          `
          INSERT INTO storage.objects (bucket_id, name, owner, version, metadata, signature)
          VALUES (?, ?, ?, ?, ?::jsonb, decode(?, 'hex'))
          `,
          [testBucketName, testObjectName, null, '1', JSON.stringify({ size: 1 }), signature]
        )

        await db.transaction(async (trx) => {
          await trx.raw(`SELECT set_config('role', 'authenticated', true)`)
          await trx.raw(
            'UPDATE storage.objects SET signature = NULL WHERE bucket_id = ? AND name = ?',
            [testBucketName, testObjectName]
          )
        })

        await expect(
          db.transaction(async (trx) => {
            await trx.raw(`SELECT set_config('role', 'authenticated', true)`)
            await trx.raw(
              `UPDATE storage.objects
               SET signature = decode(?, 'hex')
               WHERE bucket_id = ? AND name = ?`,
              [signature, testBucketName, testObjectName]
            )
          })
        ).rejects.toMatchObject({ code: '42501' })

        await expect(
          db.transaction(async (trx) => {
            await trx.raw(`SELECT set_config('role', 'authenticated', true)`)
            await trx.raw(
              `
              INSERT INTO storage.objects (bucket_id, name, owner, version, metadata, signature)
              VALUES (?, ?, ?, ?, ?::jsonb, decode(?, 'hex'))
              `,
              [
                testBucketName,
                testInsertObjectName,
                null,
                '1',
                JSON.stringify({ size: 1 }),
                signature,
              ]
            )
          })
        ).rejects.toMatchObject({ code: '42501' })
      } finally {
        await db.raw(`DROP POLICY IF EXISTS "${selectPolicyName}" ON storage.objects`)
        await db.raw(`DROP POLICY IF EXISTS "${updatePolicyName}" ON storage.objects`)
        await db.raw(`DROP POLICY IF EXISTS "${insertPolicyName}" ON storage.objects`)
        await withDeleteEnabled(db, async (db) => {
          await db.raw('DELETE FROM storage.objects WHERE bucket_id = ? AND name IN (?, ?)', [
            testBucketName,
            testObjectName,
            testInsertObjectName,
          ])
        })
      }
    })

    it('clears signatures on object content upsert but preserves them on metadata updates', async () => {
      const db = tHelper.database.connection.pool.acquire()
      const testObjectName = `test-signature-invalidation-${Date.now()}.txt`
      const signature = Buffer.from('a'.repeat(64), 'hex')
      const metadata = (size: number) => ({
        cacheControl: 'no-cache',
        contentLength: size,
        eTag: `etag-${size}`,
        mimetype: 'text/plain',
        size,
      })

      try {
        await tHelper.database.createObject({
          bucket_id: testBucketName,
          name: testObjectName,
          owner: undefined,
          version: 'v1',
          metadata: metadata(1),
          user_metadata: undefined,
        })

        await expect(
          tHelper.database.updateObjectSignature(testBucketName, testObjectName, 'v1', signature)
        ).resolves.toBe(true)

        await tHelper.database.updateObjectMetadata(testBucketName, testObjectName, metadata(2))

        const metadataUpdated = await db.raw(
          'SELECT signature FROM storage.objects WHERE bucket_id = ? AND name = ?',
          [testBucketName, testObjectName]
        )
        expect(metadataUpdated.rows[0].signature).toEqual(signature)

        await tHelper.database.upsertObject({
          bucket_id: testBucketName,
          name: testObjectName,
          owner: undefined,
          version: 'v2',
          metadata: metadata(3),
          user_metadata: undefined,
        })

        const contentUpdated = await db.raw(
          'SELECT signature FROM storage.objects WHERE bucket_id = ? AND name = ?',
          [testBucketName, testObjectName]
        )
        expect(contentUpdated.rows[0].signature).toBeNull()
      } finally {
        await withDeleteEnabled(db, async (db) => {
          await db.raw('DELETE FROM storage.objects WHERE bucket_id = ? AND name = ?', [
            testBucketName,
            testObjectName,
          ])
        })
      }
    })
  })
})
