import { randomUUID } from 'node:crypto'
import type { DatabaseTransaction } from '@internal/database'
import { useStorage, withDeleteEnabled } from './storage'

/** Shared bucket setup, scoped cleanup and service-operation transactions. */
export function useLifecycleStorage(prefix: string) {
  const helper = useStorage()
  const buckets = new Set<string>()
  let bucketId: string

  async function createBucket() {
    const id = `${prefix}-${randomUUID()}`
    buckets.add(id)
    await helper.database.createBucket({ id, name: id })
    return id
  }

  beforeEach(async () => {
    bucketId = await createBucket()
  })

  afterEach(async () => {
    const ids = [...buckets]
    await withDeleteEnabled(helper.database.connection, async (transaction) => {
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
    })
    buckets.clear()
  })

  return {
    helper,
    get bucketId() {
      return bucketId
    },
    createBucket,
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
