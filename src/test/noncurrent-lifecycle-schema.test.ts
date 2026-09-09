import { randomUUID } from 'node:crypto'
import { buildEvaluateNoncurrentLifecyclePageStatement } from '@storage/database/lifecycle'
import { useLifecycleStorage } from './utils/lifecycle-versioning'

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
  const fixtures = useLifecycleStorage('lifecycle-schema')
  const tHelper = fixtures.helper
  const { withServiceOperation } = fixtures
  let bucketId: string

  beforeEach(() => {
    bucketId = fixtures.bucketId
  })

  it('installs inert bucket defaults and the archived-history indexes', async () => {
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
      [['buckets_lifecycle_shard_topology_check']]
    )
    expect(constraints.rows).toEqual([
      { conname: 'buckets_lifecycle_shard_topology_check', convalidated: true },
    ])

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
      [['objects_archived_due_idx', 'objects_archived_order_uq']]
    )

    expect(indexes.rows.map((row) => row.indexname)).toEqual([
      'objects_archived_due_idx',
      'objects_archived_order_uq',
    ])
    expect(indexes.rows.every((row) => row.indisvalid)).toBe(true)
    expect(
      indexes.rows.find((row) => row.indexname === 'objects_archived_order_uq')?.indexdef
    ).toContain('(bucket_id, name COLLATE "C", archived_at) WHERE')

    expect(
      indexes.rows.find((row) => row.indexname === 'objects_archived_due_idx')?.indexdef
    ).toContain('(bucket_id, archived_at, name COLLATE "C") INCLUDE (version, is_delete_marker)')
  })

  it.each([
    ['update epoch', 'UPDATE storage.buckets SET lifecycle_shard_epoch = 2 WHERE id = $1'],
    ['update count', 'UPDATE storage.buckets SET lifecycle_shard_count = 2 WHERE id = $1'],
    [
      'insert',
      `INSERT INTO storage.buckets (id, name, lifecycle_shard_count)
       VALUES ($1::text || '-topology', $1::text || '-topology', 2)`,
    ],
  ])('pins the shard topology on %s', async (_name, sql) => {
    await expect(
      withServiceOperation('storage.s3.bucket.put_lifecycle', async (transaction) => {
        await transaction.query(sql, [bucketId])
      })
    ).rejects.toMatchObject({
      code: '23514',
      constraint: 'buckets_lifecycle_shard_topology_check',
    })
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
        findPlanNode(rawPage, (node) => node['Index Name'] === 'objects_archived_due_idx'),
        JSON.stringify(rawPage, null, 2)
      ).toBeDefined()
      await transaction.commit()
    } catch (error) {
      await transaction.rollback()
      throw error
    }
  })

  it('keeps shard state service-owned', async () => {
    await tHelper.database.putLifecycleConfiguration(bucketId, {
      rules: [
        {
          id: 'expire',
          status: 'Enabled',
          filter: {},
          noncurrentVersionExpiration: { noncurrentDays: 30 },
        },
      ],
    })
    await expect(fixtures.readState(bucketId, '1')).resolves.toHaveLength(1)

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
  })
})
