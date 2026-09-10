import { Client } from 'pg'
import { getConfig } from '../../config'

/**
 * Explicit history fixtures for the dormant lifecycle engine. These serial suites
 * temporarily relax master's two rollout barriers without installing a writer.
 * They prove lifecycle behavior for supplied rows, not future writer integration.
 * Call after useStorage() so its migration hook runs first.
 */
export function useLifecycleVersioningFixtures() {
  let client: Client | undefined
  let originalIndex: string | undefined
  let originalConstraint: string | undefined
  let relaxed = false
  const bucketIds = new Set<string>()

  beforeAll(async () => {
    client = new Client({ connectionString: getConfig().databaseURL })
    await client.connect()
    const index = await client.query<{ definition: string }>(`
      SELECT pg_get_indexdef(indexrelid) AS definition
      FROM pg_catalog.pg_index
      WHERE indexrelid = to_regclass('storage.bucketid_objname')
        AND indisunique AND indisvalid
    `)
    const constraint = await client.query<{ definition: string }>(`
      SELECT pg_get_constraintdef(oid) AS definition
      FROM pg_catalog.pg_constraint
      WHERE conrelid = 'storage.buckets'::regclass
        AND conname = 'buckets_versioning_dark_check' AND convalidated
    `)
    originalIndex = index.rows[0]?.definition
    originalConstraint = constraint.rows[0]?.definition
    expect(originalIndex).toContain('UNIQUE INDEX bucketid_objname')
    expect(originalConstraint).toContain("versioning_status = 'DISABLED'::text")

    await client.query('BEGIN')
    try {
      await client.query("SET LOCAL lock_timeout = '10s'")
      await client.query('DROP INDEX storage.bucketid_objname')
      await client.query(
        'ALTER TABLE storage.buckets DROP CONSTRAINT buckets_versioning_dark_check'
      )
      await client.query('COMMIT')
      relaxed = true
    } catch (error) {
      await client.query('ROLLBACK')
      throw error
    }
  })

  afterAll(async () => {
    if (!client) return
    try {
      if (!relaxed || !originalIndex || !originalConstraint) return
      await client.query('BEGIN')
      try {
        await client.query("SET LOCAL lock_timeout = '10s'")
        await client.query("SET LOCAL storage.allow_delete_query = 'true'")
        // Retry scoped cleanup if an assertion or an afterEach hook failed.
        const ids = [...bucketIds]
        await client.query(
          'DELETE FROM storage.bucket_lifecycle_states WHERE bucket_id = ANY($1::text[])',
          [ids]
        )
        await client.query('DELETE FROM storage.objects WHERE bucket_id = ANY($1::text[])', [ids])
        await client.query('DELETE FROM storage.buckets WHERE id = ANY($1::text[])', [ids])
        await client.query(originalIndex)
        await client.query(
          `ALTER TABLE storage.buckets ADD CONSTRAINT buckets_versioning_dark_check ${originalConstraint}`
        )
        const restored = await client.query<{ index: string; constraint: string }>(`
          SELECT pg_get_indexdef('storage.bucketid_objname'::regclass) AS index,
                 (SELECT pg_get_constraintdef(oid)
                  FROM pg_catalog.pg_constraint
                  WHERE conrelid = 'storage.buckets'::regclass
                    AND conname = 'buckets_versioning_dark_check') AS constraint
        `)
        expect(restored.rows).toEqual([{ index: originalIndex, constraint: originalConstraint }])
        await client.query('COMMIT')
      } catch (error) {
        await client.query('ROLLBACK')
        throw error
      }
    } finally {
      await client.end()
    }
  })

  return {
    trackBucket(bucketId: string) {
      bucketIds.add(bucketId)
      return bucketId
    },
    async setStatus(bucketId: string, status: 'DISABLED' | 'ENABLED' | 'SUSPENDED') {
      if (!client || !relaxed) throw new Error('Lifecycle history fixtures are not initialized')
      // Deliberately no wake/invalidation hook: the real lifecycle API must be
      // called explicitly where a test needs scheduling or claim reconciliation.
      await client.query('UPDATE storage.buckets SET versioning_status = $2 WHERE id = $1', [
        bucketId,
        status,
      ])
    },
  }
}
