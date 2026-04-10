import { Knex } from 'knex'
import { uniqueName } from '../random'
import { CleanupRegistry } from './types'

export interface TestBucket {
  id: string
  name: string
  owner: string | null
  public: boolean
  file_size_limit: number | null
  allowed_mime_types: string[] | null
  type: 'STANDARD' | 'ANALYTICS'
  created_at: string
  updated_at: string
}

export interface BucketOverrides {
  id?: string
  name?: string
  owner?: string | null
  public?: boolean
  fileSizeLimit?: number | null
  allowedMimeTypes?: string[] | null
  type?: 'STANDARD' | 'ANALYTICS'
}

export class BucketFactory {
  constructor(
    private readonly db: Knex,
    private readonly prefix: string,
    private readonly registry: CleanupRegistry
  ) {}

  /**
   * Insert a storage.buckets row and return the full DB record.
   *
   * A test that just wants *some* bucket calls `create()` with no args and
   * gets a private bucket with a prefix-namespaced id. Overrides map 1:1 to
   * the route-level payload (`fileSizeLimit`, `allowedMimeTypes`) so the
   * same shape works everywhere.
   */
  async create(overrides: BucketOverrides = {}): Promise<TestBucket> {
    const id = overrides.id ?? overrides.name ?? uniqueName(this.prefix, 'bucket')
    const name = overrides.name ?? id

    const { rows } = await this.db.raw<{ rows: TestBucket[] }>(
      `INSERT INTO storage.buckets
         (id, name, owner, public, file_size_limit, allowed_mime_types, type, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?::text[], ?, now(), now())
       RETURNING id, name, owner, public, file_size_limit, allowed_mime_types, type, created_at, updated_at`,
      [
        id,
        name,
        overrides.owner ?? null,
        overrides.public ?? false,
        overrides.fileSizeLimit ?? null,
        overrides.allowedMimeTypes ? `{${overrides.allowedMimeTypes.map((m) => `"${m}"`).join(',')}}` : null,
        overrides.type ?? 'STANDARD',
      ]
    )

    this.registry.buckets.add(rows[0].id)
    return rows[0]
  }

  /** Shortcut: public bucket with no size/mime restrictions. */
  async public(overrides: Omit<BucketOverrides, 'public'> = {}): Promise<TestBucket> {
    return this.create({ ...overrides, public: true })
  }

  /** Shortcut: bucket with a max file size. */
  async withSizeLimit(
    fileSizeLimit: number,
    overrides: Omit<BucketOverrides, 'fileSizeLimit'> = {}
  ): Promise<TestBucket> {
    return this.create({ ...overrides, fileSizeLimit })
  }

  /** Shortcut: bucket with a mime allow-list. */
  async withMimeTypes(
    allowedMimeTypes: string[],
    overrides: Omit<BucketOverrides, 'allowedMimeTypes'> = {}
  ): Promise<TestBucket> {
    return this.create({ ...overrides, allowedMimeTypes })
  }

  /** Insert many buckets in one round-trip. */
  async createMany(
    count: number,
    generator: (index: number) => BucketOverrides = () => ({})
  ): Promise<TestBucket[]> {
    const out: TestBucket[] = []
    for (let i = 0; i < count; i++) {
      out.push(await this.create(generator(i + 1)))
    }
    return out
  }
}
