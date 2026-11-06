import { randomUUID } from 'node:crypto'
import type { DatabaseTransaction } from '@internal/database'
import { useStorage, withDeleteEnabled } from './storage'

/** Shared bucket setup, scoped cleanup and service-operation transactions. */
export function useLifecycleStorage(
  prefix: string,
  options: { operation?: () => string | undefined } = {}
) {
  const helper = useStorage({ operation: options.operation })
  const buckets = new Set<string>()
  let bucketId: string

  async function createBucket(type: 'STANDARD' | 'ANALYTICS' = 'STANDARD') {
    const id = `${prefix}-${randomUUID()}`
    buckets.add(id)
    await helper.database.createBucket({ id, name: id })
    if (type === 'ANALYTICS') {
      await helper.database.connection.query(
        "UPDATE storage.buckets SET type = 'ANALYTICS' WHERE id = $1",
        [id]
      )
    }
    return id
  }

  beforeEach(async () => {
    bucketId = await createBucket()
  })

  async function cleanupBuckets() {
    const ids = [...buckets]
    if (ids.length === 0) return

    await withDeleteEnabled(helper.database.connection, async (transaction) => {
      // Lock the buckets before deleting their objects: OrioleDB can otherwise
      // skip the bucket DELETE after a large batch of FK-referencing rows.
      await transaction.query(
        'SELECT id FROM storage.buckets WHERE id = ANY($1::text[]) ORDER BY id FOR UPDATE',
        [ids]
      )
      await transaction.query(
        'DELETE FROM storage.bucket_lifecycle_states WHERE bucket_id = ANY($1::text[])',
        [ids]
      )
      await transaction.query(
        'DELETE FROM storage.lifecycle_abandoned_artifacts WHERE bucket_id = ANY($1::text[])',
        [ids]
      )
      await transaction.query('DELETE FROM storage.objects WHERE bucket_id = ANY($1::text[])', [
        ids,
      ])
      await transaction.query('DELETE FROM storage.buckets WHERE id = ANY($1::text[])', [ids])
      const remaining = await transaction.query<{ id: string }>(
        'SELECT id FROM storage.buckets WHERE id = ANY($1::text[])',
        [ids]
      )
      if (remaining.rows.length > 0) {
        throw new Error(
          `Failed to clean up lifecycle test buckets: ${remaining.rows.map(({ id }) => id).join(', ')}`
        )
      }
    })
    buckets.clear()
  }

  afterEach(cleanupBuckets)

  return {
    helper,
    get bucketId() {
      return bucketId
    },
    createBucket,
    cleanupBuckets,
    async readState(id: string, columns: string) {
      const result = await helper.database.connection.query(
        `SELECT ${columns} FROM storage.bucket_lifecycle_states WHERE bucket_id = $1`,
        [id]
      )
      return result.rows
    },
    async setStatus(id: string, status: 'DISABLED' | 'ENABLED' | 'SUSPENDED') {
      if (status === 'SUSPENDED') {
        await helper.database.updateBucket(id, { versioning_status: 'ENABLED' })
      }
      await helper.database.updateBucket(id, { versioning_status: status })
    },
    async withServiceOperation<T>(
      operation: string,
      fn: (transaction: DatabaseTransaction) => Promise<T>
    ): Promise<T> {
      const transaction = await helper.database.connection.transaction()
      try {
        await helper.database.connection.setScope(transaction)
        await transaction.query("SELECT set_config('storage.operation', $1, true)", [operation])
        const result = await fn(transaction)
        await transaction.commit()
        return result
      } catch (error) {
        await transaction.rollback()
        throw error
      }
    },
  }
}
