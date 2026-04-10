import { expect } from 'vitest'
import { Knex } from 'knex'

/**
 * "Snapshot-style" row assertions that ignore volatile fields by default.
 *
 * The aim is to make it cheap to assert "after this API call, the row in the
 * database has exactly this shape" without spelling out the volatile fields
 * (id, timestamps, version) every time. Tests can still pin a volatile field
 * to a concrete value when they care.
 *
 *   await snapshot.object({ bucketId: b.id, name: 'cat.png' }).matches({
 *     owner: user.id,
 *     metadata: { mimetype: 'image/png' },
 *   })
 */
export interface RowSnapshot {
  /** Read the row, then assert that it matches the expected shape. */
  matches(expected: Record<string, unknown>): Promise<void>
  /** Assert that NO row exists for this lookup. */
  notFound(): Promise<void>
}

const VOLATILE_OBJECT_FIELDS = new Set([
  'id',
  'version',
  'created_at',
  'updated_at',
  'last_accessed_at',
])

const VOLATILE_BUCKET_FIELDS = new Set(['created_at', 'updated_at'])

function stripVolatile(row: Record<string, unknown>, volatile: Set<string>): Record<string, unknown> {
  const out: Record<string, unknown> = {}
  for (const [k, v] of Object.entries(row)) {
    if (!volatile.has(k)) out[k] = v
  }
  return out
}

export class Snapshot {
  constructor(private readonly db: Knex) {}

  object(query: { bucketId: string; name: string }): RowSnapshot {
    return {
      matches: async (expected) => {
        const row = await this.db('storage.objects')
          .where({ bucket_id: query.bucketId, name: query.name })
          .first()
        if (!row) {
          throw new Error(
            `expected storage.objects row for { bucket_id: ${query.bucketId}, name: ${query.name} } — none found`
          )
        }
        expect(stripVolatile(row, VOLATILE_OBJECT_FIELDS)).toMatchObject(expected)
      },
      notFound: async () => {
        const row = await this.db('storage.objects')
          .where({ bucket_id: query.bucketId, name: query.name })
          .first()
        expect(row).toBeUndefined()
      },
    }
  }

  bucket(query: { id: string }): RowSnapshot {
    return {
      matches: async (expected) => {
        const row = await this.db('storage.buckets').where({ id: query.id }).first()
        if (!row) {
          throw new Error(`expected storage.buckets row for id=${query.id} — none found`)
        }
        expect(stripVolatile(row, VOLATILE_BUCKET_FIELDS)).toMatchObject(expected)
      },
      notFound: async () => {
        const row = await this.db('storage.buckets').where({ id: query.id }).first()
        expect(row).toBeUndefined()
      },
    }
  }
}
