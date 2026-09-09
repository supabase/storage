import { randomUUID } from 'node:crypto'
import type { DatabaseTransaction } from '@internal/database'
import { runMigrationsOnTenant } from '@internal/database/migrations'
import { buildEvaluateNoncurrentLifecyclePageStatement } from '@storage/database/lifecycle'
import { getConfig } from '../config'
import { useStorage } from './utils/storage'

const { databaseURL, tenantId } = getConfig()

function findPlanNode(
  value: unknown,
  predicate: (node: Record<string, unknown>) => boolean
): Record<string, unknown> | undefined {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return undefined
  const node = value as Record<string, unknown>
  if (predicate(node)) return node
  if (!Array.isArray(node.Plans)) return undefined
  for (const child of node.Plans) {
    const match = findPlanNode(child, predicate)
    if (match) return match
  }
  return undefined
}

describe('noncurrent lifecycle schema foundation', () => {
  const tHelper = useStorage()
  let bucketId: string

  beforeEach(async () => {
    bucketId = `lifecycle-schema-${randomUUID()}`
    await tHelper.database.createBucket({ id: bucketId, name: bucketId })
  })

  afterEach(async () => {
    const transaction = await tHelper.database.connection.transaction()
    try {
      await transaction.query(`SELECT set_config('storage.allow_delete_query', 'true', true)`)
      await transaction.query('DELETE FROM storage.bucket_lifecycle_states WHERE bucket_id = $1', [
        bucketId,
      ])
      await transaction.query('DELETE FROM storage.objects WHERE bucket_id = $1', [bucketId])
      await transaction.query('DELETE FROM storage.buckets WHERE id = $1', [bucketId])
      await transaction.commit()
    } catch (error) {
      await transaction.rollback()
      throw error
    }
  })

  async function withServiceOperation<T>(
    operation: string,
    fn: (transaction: DatabaseTransaction) => Promise<T>
  ): Promise<T> {
    const transaction = await tHelper.database.connection.transaction()
    try {
      await tHelper.database.connection.setScope(transaction)
      await transaction.query(`SELECT set_config('storage.operation', $1, true)`, [operation])
      const result = await fn(transaction)
      await transaction.commit()
      return result
    } catch (error) {
      await transaction.rollback()
      throw error
    }
  }

  it('installs inert bucket defaults and preserves unique object row identity', async () => {
    const bucket = await tHelper.database.connection.query<{
      versioning_status: string
      lifecycle_configuration: unknown | null
      lifecycle_configuration_generation: string | null
      lifecycle_shard_epoch: number
      lifecycle_shard_count: number
    }>(
      `SELECT versioning_status,
              lifecycle_configuration,
              lifecycle_configuration_generation,
              lifecycle_shard_epoch,
              lifecycle_shard_count
       FROM storage.buckets
       WHERE id = $1`,
      [bucketId]
    )

    expect(bucket.rows[0]).toMatchObject({
      versioning_status: 'DISABLED',
      lifecycle_configuration: null,
      lifecycle_configuration_generation: null,
      lifecycle_shard_epoch: 1,
      lifecycle_shard_count: 1,
    })

    const constraints = await tHelper.database.connection.query<{
      conname: string
      convalidated: boolean
    }>(
      `SELECT conname, convalidated
       FROM pg_catalog.pg_constraint
       WHERE conrelid = 'storage.buckets'::regclass
         AND conname = ANY($1::text[])
       ORDER BY conname`,
      [
        [
          'buckets_lifecycle_shard_count_check',
          'buckets_lifecycle_shard_epoch_check',
          'buckets_lifecycle_standard_only_check',
        ],
      ]
    )
    expect(constraints.rows).toEqual([
      { conname: 'buckets_lifecycle_shard_count_check', convalidated: true },
      { conname: 'buckets_lifecycle_shard_epoch_check', convalidated: true },
      { conname: 'buckets_lifecycle_standard_only_check', convalidated: true },
    ])

    const primaryKey = await tHelper.database.connection.query<{ columns: string }>(`
      SELECT string_agg(attribute.attname, ',' ORDER BY key_column.ordinality) AS columns
      FROM pg_catalog.pg_constraint AS constraint_row
      CROSS JOIN LATERAL unnest(constraint_row.conkey)
        WITH ORDINALITY AS key_column(attnum, ordinality)
      JOIN pg_catalog.pg_attribute AS attribute
        ON attribute.attrelid = constraint_row.conrelid
       AND attribute.attnum = key_column.attnum
      WHERE constraint_row.conrelid = 'storage.objects'::regclass
        AND constraint_row.contype = 'p'
    `)
    expect(primaryKey.rows[0]?.columns).toBe('id')

    const indexes = await tHelper.database.connection.query<{
      indexname: string
      indexdef: string
      indisvalid: boolean
    }>(
      `
      SELECT indexes.indexname, indexes.indexdef, pg_index.indisvalid
      FROM pg_catalog.pg_indexes AS indexes
      JOIN pg_catalog.pg_class AS index_relation
        ON index_relation.relname = indexes.indexname
       AND index_relation.relnamespace = 'storage'::regnamespace
      JOIN pg_catalog.pg_index
        ON pg_index.indexrelid = index_relation.oid
      WHERE indexes.schemaname = 'storage'
        AND indexes.tablename = 'objects'
        AND indexes.indexname = ANY($1::text[])
      ORDER BY indexes.indexname
    `,
      [
        [
          'bucketid_objname',
          'idx_objects_current_version',
          'idx_objects_null_version',
          'objects_archived_due_idx',
          'objects_archived_order_uq',
          'objects_bucket_id_name_version_key',
          'objects_pkey',
        ],
      ]
    )

    expect(indexes.rows.map((row) => row.indexname)).toEqual([
      'bucketid_objname',
      'idx_objects_current_version',
      'idx_objects_null_version',
      'objects_archived_due_idx',
      'objects_archived_order_uq',
      'objects_bucket_id_name_version_key',
      'objects_pkey',
    ])
    expect(indexes.rows.every((row) => row.indisvalid)).toBe(true)
    expect(
      indexes.rows.find((row) => row.indexname === 'objects_archived_order_uq')?.indexdef
    ).toContain('(bucket_id, name COLLATE "C", archived_at) INCLUDE (version, is_delete_marker)')

    expect(
      indexes.rows.find((row) => row.indexname === 'objects_archived_due_idx')?.indexdef
    ).toContain('(bucket_id, archived_at, name COLLATE "C") INCLUDE (version, is_delete_marker)')

    const versionColumn = await tHelper.database.connection.query<{
      attnotnull: boolean
      default_value: string | null
    }>(`
      SELECT attribute.attnotnull,
             pg_get_expr(default_value.adbin, default_value.adrelid) AS default_value
      FROM pg_catalog.pg_attribute AS attribute
      LEFT JOIN pg_catalog.pg_attrdef AS default_value
        ON default_value.adrelid = attribute.attrelid
       AND default_value.adnum = attribute.attnum
      WHERE attribute.attrelid = 'storage.objects'::regclass
        AND attribute.attname = 'version'
    `)
    expect(versionColumn.rows).toEqual([{ attnotnull: false, default_value: null }])

    const incompatibleIndexes = await tHelper.database.connection.query<{ name: string | null }>(`
      SELECT unnest(ARRAY[
        to_regclass('storage.objects_key_version_uq')::text,
        to_regclass('storage.objects_one_current_per_key_uq')::text,
        to_regclass('storage.idx_name_bucket_level_unique')::text,
        to_regclass('storage.objects_bucket_id_level_idx')::text
      ]) AS name
    `)
    expect(incompatibleIndexes.rows.every((row) => row.name === null)).toBe(true)
  })

  it('retains the dark constraint and leaves writer ownership with the future versioning rollout', async () => {
    const dark = await tHelper.database.connection.query<{ definition: string }>(`
      SELECT pg_get_constraintdef(oid) AS definition
      FROM pg_catalog.pg_constraint
      WHERE conrelid = 'storage.buckets'::regclass
        AND conname = 'buckets_versioning_dark_check'
    `)
    expect(dark.rows[0]?.definition).toContain("versioning_status = 'DISABLED'::text")
    const removedObjects = await tHelper.database.connection.query<{
      legacy_lock: string | null
      legacy_writer: string | null
      legacy_trigger: boolean
      legacy_column: boolean
      legacy_constraint: boolean
    }>(`
      SELECT
        to_regprocedure('storage.bucket_versioning_lock_key(text)')::text AS legacy_lock,
        to_regprocedure('storage.reject_stale_versioning_writer()')::text AS legacy_writer,
        EXISTS (
          SELECT 1 FROM pg_catalog.pg_trigger
          WHERE tgrelid = 'storage.objects'::regclass
            AND tgname = 'protect_versioned_object_writes'
        ) AS legacy_trigger,
        EXISTS (
          SELECT 1 FROM pg_catalog.pg_attribute
          WHERE attrelid = 'storage.buckets'::regclass
            AND attname = 'versioning_enabled_at'
            AND NOT attisdropped
        ) AS legacy_column,
        EXISTS (
          SELECT 1 FROM pg_catalog.pg_constraint
          WHERE conrelid = 'storage.objects'::regclass
            AND conname = 'objects_version_identity_not_null'
        ) AS legacy_constraint
    `)

    expect(removedObjects.rows).toEqual([
      {
        legacy_lock: null,
        legacy_writer: null,
        legacy_trigger: false,
        legacy_column: false,
        legacy_constraint: false,
      },
    ])
  })

  it('protects lifecycle configuration and static topology', async () => {
    const generation = randomUUID()
    const policy = {
      rules: [
        {
          id: 'expire',
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 30 },
        },
      ],
    }
    await withServiceOperation('storage.s3.bucket.put_lifecycle', async (transaction) => {
      await transaction.query(
        `UPDATE storage.buckets
         SET lifecycle_configuration = $2,
             lifecycle_configuration_generation = $3
         WHERE id = $1`,
        [bucketId, policy, generation]
      )
    })

    await expect(
      withServiceOperation('storage.s3.bucket.put_lifecycle', async (transaction) => {
        await transaction.query(
          `UPDATE storage.buckets
           SET lifecycle_configuration_generation = $2
           WHERE id = $1`,
          [bucketId, randomUUID()]
        )
      })
    ).rejects.toMatchObject({ code: '22023' })

    await expect(
      withServiceOperation('storage.s3.bucket.put_lifecycle', async (transaction) => {
        await transaction.query(
          `UPDATE storage.buckets SET lifecycle_shard_count = 2 WHERE id = $1`,
          [bucketId]
        )
      })
    ).rejects.toMatchObject({ code: '0A000' })
  })

  it('keeps lifecycle and versioning controls inert on non-Standard buckets', async () => {
    await tHelper.database.connection.query(
      `UPDATE storage.buckets SET type = 'ANALYTICS' WHERE id = $1`,
      [bucketId]
    )

    await expect(
      tHelper.database.connection.query(
        `UPDATE storage.buckets SET versioning_status = 'ENABLED' WHERE id = $1`,
        [bucketId]
      )
    ).rejects.toMatchObject({
      code: '23514',
      constraint: 'buckets_versioning_dark_check',
    })

    await expect(
      withServiceOperation('storage.s3.bucket.put_lifecycle', async (transaction) => {
        await transaction.query(
          `UPDATE storage.buckets SET lifecycle_shard_count = 2 WHERE id = $1`,
          [bucketId]
        )
      })
    ).rejects.toMatchObject({ code: '0A000' })

    const constraint = await tHelper.database.connection.query<{ definition: string }>(`
      SELECT pg_get_constraintdef(oid) AS definition
      FROM pg_catalog.pg_constraint
      WHERE conrelid = 'storage.buckets'::regclass
        AND conname = 'buckets_lifecycle_standard_only_check'
    `)

    expect(constraint.rows[0]?.definition).toContain("type = 'STANDARD'::storage.buckettype")
    expect(constraint.rows[0]?.definition).toContain('lifecycle_shard_epoch = 1')
    expect(constraint.rows[0]?.definition).toContain('lifecycle_shard_count = 1')
  })

  it('uses the archived due index for explicit history fixtures', async () => {
    const objectName = 'history/object.txt'
    const version = randomUUID()
    const rowId = randomUUID()

    const transaction = await tHelper.database.connection.transaction()
    try {
      await transaction.query(
        `INSERT INTO storage.objects (id, bucket_id, name, version)
         VALUES ($1, $2, $3, $4)`,
        [rowId, bucketId, objectName, version]
      )
      await transaction.query(
        `UPDATE storage.objects
         SET archived_at = clock_timestamp()
         WHERE bucket_id = $1 AND name COLLATE "C" = $2 AND version = $3`,
        [bucketId, objectName, version]
      )
      // Keep most archived rows outside the due range.
      await transaction.query(
        `INSERT INTO storage.objects (bucket_id, name, version, archived_at)
         SELECT $1, 'future/' || n, gen_random_uuid()::text, '2099-01-01'::timestamptz
         FROM generate_series(1, 1000) AS n`,
        [bucketId]
      )
      await transaction.query('ANALYZE storage.objects')
      await transaction.query('SET LOCAL enable_seqscan = off')
      await transaction.query('SET LOCAL enable_bitmapscan = off')
      // Check that the index supplies the ordering. A one-row sort can otherwise
      // be cheaper after other tests change the shared table's index statistics.
      await transaction.query('SET LOCAL enable_sort = off')

      const evaluation = buildEvaluateNoncurrentLifecyclePageStatement({
        bucketId,
        snapshotAt: '2030-01-01T00:00:00.000Z',
        rules: [
          {
            cutoffAt: '2030-01-01T00:00:00.000Z',
            newerNoncurrentVersions: 1,
          },
        ],
        pageSize: 500,
      })
      const plan = await transaction.query<{ 'QUERY PLAN': unknown }>({
        text: `EXPLAIN (FORMAT JSON) ${evaluation.text}`,
        values: evaluation.values,
      })
      const explain = plan.rows[0]?.['QUERY PLAN']
      const root = Array.isArray(explain)
        ? (explain[0] as { Plan?: unknown } | undefined)?.Plan
        : undefined
      const rawPage = findPlanNode(root, (node) => node['Subplan Name'] === 'CTE raw_page')
      expect(rawPage).toBeDefined()
      expect(
        findPlanNode(
          rawPage,
          (node) =>
            node['Node Type'] === 'Index Only Scan' &&
            node['Index Name'] === 'objects_archived_due_idx'
        ),
        JSON.stringify(rawPage, null, 2)
      ).toBeDefined()
      await transaction.commit()
    } catch (error) {
      await transaction.rollback()
      throw error
    }
  })

  it('keeps shard state service-owned and fences bucket deletion while state exists', async () => {
    const generation = randomUUID()
    await withServiceOperation('storage.s3.bucket.put_lifecycle', async (transaction) => {
      await transaction.query(
        `UPDATE storage.buckets
         SET lifecycle_configuration = '{"rules": [{"id": "expire", "status": "Enabled", "filter": {}, "noncurrentVersionExpiration": {"noncurrentDays": 30}}]}'::jsonb,
             lifecycle_configuration_generation = $2
         WHERE id = $1`,
        [bucketId, generation]
      )
    })

    await tHelper.database.connection.query(
      `INSERT INTO storage.bucket_lifecycle_states (
         bucket_id,
         scan_kind,
         shard_id,
         shard_epoch,
         shard_count,
         configuration_generation,
         next_run_at
       ) VALUES ($1, 'NONCURRENT', 0, 1, 1, $2, now())`,
      [bucketId, generation]
    )

    const security = await tHelper.database.connection.query<{
      relrowsecurity: boolean
      anon_select: boolean
      authenticated_select: boolean
      service_select: boolean
    }>(`
      SELECT relation.relrowsecurity,
             has_table_privilege('anon', 'storage.bucket_lifecycle_states', 'SELECT') AS anon_select,
             has_table_privilege('authenticated', 'storage.bucket_lifecycle_states', 'SELECT') AS authenticated_select,
             has_table_privilege('service_role', 'storage.bucket_lifecycle_states', 'SELECT') AS service_select
      FROM pg_catalog.pg_class AS relation
      WHERE relation.oid = 'storage.bucket_lifecycle_states'::regclass
    `)

    expect(security.rows[0]).toEqual({
      relrowsecurity: true,
      anon_select: false,
      authenticated_select: false,
      service_select: true,
    })

    await expect(tHelper.database.deleteBucket(bucketId)).rejects.toMatchObject({
      code: 'ResourceReferenced',
    })
  })

  it('is a no-op when the registered tenant migrations run again', async () => {
    await expect(
      runMigrationsOnTenant({ databaseUrl: databaseURL!, tenantId, waitForLock: true })
    ).resolves.toBeUndefined()
    await expect(
      runMigrationsOnTenant({ databaseUrl: databaseURL!, tenantId, waitForLock: true })
    ).resolves.toBeUndefined()
  })
})
