import { Knex } from 'knex'
import { randomUUID } from 'node:crypto'
import { TestBucket } from './bucket'
import { CleanupRegistry } from './types'

export interface TestObject {
  id: string
  bucket_id: string
  name: string
  owner: string | null
  version: string | null
  metadata: Record<string, unknown> | null
  user_metadata: Record<string, unknown> | null
  created_at: string
  updated_at: string
  last_accessed_at: string
}

export interface ObjectOverrides {
  id?: string
  name?: string
  owner?: string | null
  metadata?: Record<string, unknown> | null
  userMetadata?: Record<string, unknown> | null
  version?: string
}

const DEFAULT_METADATA = {
  size: 1234,
  mimetype: 'image/png',
  eTag: '"abc"',
  cacheControl: 'no-cache',
  lastModified: 'Wed, 12 Oct 2022 11:17:02 GMT',
  contentLength: 1234,
  httpStatusCode: 200,
}

/**
 * Object factory. Tests that only care about row shape (e.g. "does GET /object
 * return this metadata") use `create()` to insert DB-only — no S3 side effect.
 *
 * Tests that want end-to-end upload semantics should instead go through the
 * HTTP API (via context.app) so the real uploader / storage backend runs.
 */
export class ObjectFactory {
  constructor(
    private readonly db: Knex,
    private readonly bucket: TestBucket,
    private readonly registry: CleanupRegistry
  ) {}

  async create(overrides: ObjectOverrides = {}): Promise<TestObject> {
    const id = overrides.id ?? randomUUID()
    const name = overrides.name ?? `obj_${id.slice(0, 8)}.png`
    const metadata = overrides.metadata === undefined ? DEFAULT_METADATA : overrides.metadata
    const userMetadata = overrides.userMetadata ?? null
    const version = overrides.version ?? randomUUID()

    const { rows } = await this.db.raw<{ rows: TestObject[] }>(
      `INSERT INTO storage.objects
         (id, bucket_id, name, owner, version, metadata, user_metadata,
          created_at, updated_at, last_accessed_at)
       VALUES (?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, now(), now(), now())
       RETURNING id, bucket_id, name, owner, version, metadata, user_metadata,
                 created_at, updated_at, last_accessed_at`,
      [
        id,
        this.bucket.id,
        name,
        overrides.owner ?? null,
        version,
        JSON.stringify(metadata),
        userMetadata ? JSON.stringify(userMetadata) : null,
      ]
    )

    const row = rows[0]
    this.registry.s3Keys.add(`${this.bucket.id}/${row.name}`)
    return row
  }

  /**
   * Insert several objects via batched INSERTs (fast path for list-heavy
   * tests). Postgres caps each bind message at 65_535 parameters, and each
   * row uses 7 params, so we chunk at 8_000 rows per statement to stay well
   * under the limit while still keeping round-trips minimal.
   */
  async createMany(
    count: number,
    generator: (index: number) => ObjectOverrides = () => ({})
  ): Promise<TestObject[]> {
    if (count <= 0) return []

    const rows = Array.from({ length: count }, (_, i) => {
      const o = generator(i + 1)
      const id = o.id ?? randomUUID()
      return {
        id,
        name: o.name ?? `obj_${i + 1}_${id.slice(0, 8)}.png`,
        owner: o.owner ?? null,
        version: o.version ?? randomUUID(),
        metadata: JSON.stringify(o.metadata === undefined ? DEFAULT_METADATA : o.metadata),
        user_metadata: o.userMetadata ? JSON.stringify(o.userMetadata) : null,
      }
    })

    const CHUNK = 8_000
    const inserted: TestObject[] = []

    for (let start = 0; start < rows.length; start += CHUNK) {
      const chunk = rows.slice(start, start + CHUNK)
      const values = chunk
        .map(() => '(?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, now(), now(), now())')
        .join(', ')
      const bindings = chunk.flatMap((r) => [
        r.id,
        this.bucket.id,
        r.name,
        r.owner,
        r.version,
        r.metadata,
        r.user_metadata,
      ])

      const { rows: insertedChunk } = await this.db.raw<{ rows: TestObject[] }>(
        `INSERT INTO storage.objects
           (id, bucket_id, name, owner, version, metadata, user_metadata,
            created_at, updated_at, last_accessed_at)
         VALUES ${values}
         RETURNING id, bucket_id, name, owner, version, metadata, user_metadata,
                   created_at, updated_at, last_accessed_at`,
        bindings
      )

      inserted.push(...insertedChunk)
    }

    for (const row of inserted) {
      this.registry.s3Keys.add(`${this.bucket.id}/${row.name}`)
    }
    return inserted
  }
}
