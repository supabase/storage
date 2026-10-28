import { randomUUID } from 'node:crypto'
import { buildEvaluateNoncurrentLifecyclePageStatement } from '@storage/database/lifecycle'
import { Client } from 'pg'
import { getConfig } from '../config'
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

async function withRollback(fn: (client: Client) => Promise<void>): Promise<void> {
  const client = new Client({ connectionString: getConfig().databaseURL })
  await client.connect()
  try {
    await client.query('BEGIN')
    await fn(client)
  } finally {
    await client.query('ROLLBACK').catch(() => undefined)
    await client.end()
  }
}

describe('noncurrent lifecycle schema foundation', () => {
  const fixtures = useLifecycleStorage('lifecycle-schema')
  const tHelper = fixtures.helper
  let bucketId: string

  beforeEach(() => {
    bucketId = fixtures.bucketId
  })

  it('installs inert bucket defaults and the archived-history indexes', async () => {
    const bucket = await tHelper.database.connection.query<{
      versioning_status: string
      lifecycle_configuration: unknown | null
      lifecycle_configuration_generation: string | null
    }>(
      `SELECT versioning_status,
              lifecycle_configuration,
              lifecycle_configuration_generation
       FROM storage.buckets
       WHERE id = $1`,
      [bucketId]
    )

    expect(bucket.rows[0]).toMatchObject({
      versioning_status: 'DISABLED',
      lifecycle_configuration: null,
      lifecycle_configuration_generation: null,
    })

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
    ).toContain('CREATE UNIQUE INDEX objects_archived_order_uq')
    expect(
      indexes.rows.find((row) => row.indexname === 'objects_archived_order_uq')?.indexdef
    ).toContain('(bucket_id, name COLLATE "C", archived_at) WHERE')

    expect(
      indexes.rows.find((row) => row.indexname === 'objects_archived_due_idx')?.indexdef
    ).toContain(
      '(bucket_id, archived_at, name COLLATE "C") INCLUDE (id, version, is_delete_marker)'
    )
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
      const indexScan = findPlanNode(
        rawPage,
        (node) => node['Index Name'] === 'objects_archived_due_idx'
      )
      expect(indexScan, JSON.stringify(rawPage, null, 2)).toBeDefined()
      expect(['Index Only Scan', 'Custom Scan'], JSON.stringify(rawPage, null, 2)).toContain(
        indexScan?.['Node Type']
      )
      await transaction.commit()
    } catch (error) {
      await transaction.rollback()
      throw error
    }
  })

  it('enforces lifecycle state constraints and indexes', async () => {
    await withRollback(async (client) => {
      await client.query('INSERT INTO storage.bucket_lifecycle_states (bucket_id) VALUES ($1)', [
        bucketId,
      ])
      for (const [sql, code, constraint] of [
        ['INSERT INTO storage.bucket_lifecycle_states (bucket_id) VALUES ($1)', '23505', undefined],
        [
          'UPDATE storage.bucket_lifecycle_states SET failure_count = -1 WHERE bucket_id = $1',
          '23514',
          'bucket_lifecycle_failure_count_check',
        ],
        [
          "UPDATE storage.bucket_lifecycle_states SET continuation = '[]' WHERE bucket_id = $1",
          '23514',
          'bucket_lifecycle_continuation_object_check',
        ],
        [
          "UPDATE storage.bucket_lifecycle_states SET last_result = '[]' WHERE bucket_id = $1",
          '23514',
          'bucket_lifecycle_last_result_object_check',
        ],
        [
          'UPDATE storage.bucket_lifecycle_states SET continuation = \'{"batch":{"inFlight":{}}}\', next_run_at = NULL WHERE bucket_id = $1',
          '23514',
          'bucket_lifecycle_recovery_due_check',
        ],
      ] as const) {
        await client.query('SAVEPOINT invalid_state')
        await expect(client.query(sql, [bucketId])).rejects.toMatchObject({
          code,
          ...(constraint && { constraint }),
        })
        await client.query('ROLLBACK TO SAVEPOINT invalid_state')
      }
      await client.query(
        `UPDATE storage.bucket_lifecycle_states
         SET failure_count = 0, continuation = '{"batch":{"inFlight":{}}}',
             next_run_at = now(), last_result = '{}' WHERE bucket_id = $1`,
        [bucketId]
      )
      await client.query(
        `INSERT INTO storage.lifecycle_abandoned_artifacts (bucket_id, key, name)
         VALUES ($1, 'artifact', 'name')`,
        [bucketId]
      )
      await client.query(
        `INSERT INTO storage.lifecycle_abandoned_artifacts (bucket_id, key, name)
         VALUES ($1, 'other-artifact', 'name'), ($2, 'artifact', 'name')`,
        [bucketId, `${bucketId}-other`]
      )
      await client.query('SAVEPOINT duplicate_artifact')
      await expect(
        client.query(
          `INSERT INTO storage.lifecycle_abandoned_artifacts (bucket_id, key, name)
           VALUES ($1, 'artifact', 'other')`,
          [bucketId]
        )
      ).rejects.toMatchObject({ code: '23505' })
      await client.query('ROLLBACK TO SAVEPOINT duplicate_artifact')
      const indexes = await client.query<{ indexname: string; indexdef: string }>(
        `SELECT indexname, indexdef FROM pg_catalog.pg_indexes
         WHERE schemaname = 'storage' AND indexname IN
           ('bucket_lifecycle_states_due_idx', 'lifecycle_abandoned_artifacts_attempted_at_idx')
         ORDER BY indexname`
      )
      expect(indexes.rows).toEqual([
        {
          indexname: 'bucket_lifecycle_states_due_idx',
          indexdef:
            'CREATE INDEX bucket_lifecycle_states_due_idx ON storage.bucket_lifecycle_states USING btree (next_run_at, bucket_id) WHERE (next_run_at IS NOT NULL)',
        },
        {
          indexname: 'lifecycle_abandoned_artifacts_attempted_at_idx',
          indexdef:
            'CREATE INDEX lifecycle_abandoned_artifacts_attempted_at_idx ON storage.lifecycle_abandoned_artifacts USING btree (attempted_at)',
        },
      ])
    })
  })

  it('keeps bucket state service-owned', async () => {
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
    const permissions = await tHelper.database.connection.query(`
      SELECT has_function_privilege('anon', 'storage.reconcile_lifecycle_state(text,boolean)', 'EXECUTE') AS anon_execute,
             has_function_privilege('authenticated', 'storage.reconcile_lifecycle_state(text,boolean)', 'EXECUTE') AS authenticated_execute,
             has_function_privilege('service_role', 'storage.reconcile_lifecycle_state(text,boolean)', 'EXECUTE') AS service_execute
    `)
    expect(permissions.rows).toEqual([
      { anon_execute: false, authenticated_execute: false, service_execute: true },
    ])
    const probePermissions = await tHelper.database.connection.query(`
      SELECT has_function_privilege('anon', 'storage.delete_lifecycle_objects(uuid[])', 'EXECUTE') AS anon_execute,
             has_function_privilege('authenticated', 'storage.delete_lifecycle_objects(uuid[])', 'EXECUTE') AS authenticated_execute,
             has_function_privilege('service_role', 'storage.delete_lifecycle_objects(uuid[])', 'EXECUTE') AS service_execute
    `)
    expect(probePermissions.rows).toEqual(permissions.rows)
  })

  it('keeps abandoned artifacts service-owned', async () => {
    const security = await tHelper.database.connection.query(`
      SELECT relation.relrowsecurity,
             has_table_privilege('anon', 'storage.lifecycle_abandoned_artifacts', 'SELECT') AS anon_select,
             has_table_privilege('authenticated', 'storage.lifecycle_abandoned_artifacts', 'SELECT') AS authenticated_select,
             has_table_privilege('service_role', 'storage.lifecycle_abandoned_artifacts', 'SELECT') AS service_select
      FROM pg_catalog.pg_class AS relation
      WHERE relation.oid = 'storage.lifecycle_abandoned_artifacts'::regclass
    `)

    expect(security.rows).toEqual([
      {
        relrowsecurity: true,
        anon_select: false,
        authenticated_select: false,
        service_select: true,
      },
    ])
  })

  it.each(['unset', 'false'] as const)(
    'deletes lifecycle objects when the delete guard setting is %s',
    async (setting) => {
      await withRollback(async (client) => {
        if (setting === 'false') {
          await client.query("SELECT set_config('storage.allow_delete_query', 'false', true)")
        }
        const id = (
          await client.query<{ id: string }>(
            `INSERT INTO storage.objects (bucket_id, name, version, archived_at, is_versioned)
             VALUES ($1, 'guard/object', gen_random_uuid()::text, now(), true) RETURNING id`,
            [bucketId]
          )
        ).rows[0].id
        const deleted = await client.query<{ id: string }>(
          'SELECT * FROM storage.delete_lifecycle_objects($1::uuid[])',
          [[id]]
        )
        expect(deleted.rows.map((row) => row.id)).toEqual([id])
        expect(
          (await client.query('SELECT 1 FROM storage.objects WHERE id = $1', [id])).rowCount
        ).toBe(0)
        expect(
          (
            await client.query(
              "SELECT current_setting('storage.allow_delete_query', true) AS value"
            )
          ).rows
        ).toEqual([{ value: 'true' }])
      })
    }
  )

  it.each([
    { code: '42501', vetoAll: false },
    { code: '42501', vetoAll: true },
  ])(
    'keeps rows vetoed with $code (all: $vetoAll) while deleting other archived rows',
    async ({ code, vetoAll }) => {
      await withRollback(async (client) => {
        await client.query("SELECT set_config('storage.allow_delete_query', 'false', true)")
        const ids = (
          await client.query<{ id: string }>(
            `INSERT INTO storage.objects (bucket_id, name, version, archived_at, is_versioned)
             SELECT $1, 'veto/' || n, gen_random_uuid()::text, now(), true
             FROM generate_series(1, 2) AS n RETURNING id`,
            [bucketId]
          )
        ).rows.map((row) => row.id)
        await client.query(`CREATE FUNCTION public.lifecycle_veto_probe() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN
            IF ${vetoAll ? 'true' : "OLD.name = 'veto/1'"} THEN
              RAISE EXCEPTION 'protected' USING ERRCODE = '${code}';
            END IF;
            RETURN OLD;
          END $$`)
        await client.query(`CREATE TRIGGER lifecycle_veto_probe BEFORE DELETE ON storage.objects
          FOR EACH ROW EXECUTE FUNCTION public.lifecycle_veto_probe()`)

        const deleted = await client.query<{ id: string }>(
          'SELECT * FROM storage.delete_lifecycle_objects($1::uuid[])',
          [ids]
        )
        expect(deleted.rows.map((row) => row.id)).toEqual(vetoAll ? [] : [ids[1]])
        expect(
          (
            await client.query(
              'SELECT id FROM storage.objects WHERE id = ANY($1::uuid[]) ORDER BY id',
              [ids]
            )
          ).rows
        ).toEqual((vetoAll ? ids : [ids[0]]).sort().map((id) => ({ id })))
      })
    }
  )

  it('deletes only non-vetoed metadata and propagates non-veto errors', async () => {
    await withRollback(async (client) => {
      await client.query("SELECT set_config('storage.allow_delete_query', 'true', true)")
      const ids = (
        await client.query<{ id: string }>(
          `INSERT INTO storage.objects (bucket_id, name, version, archived_at, is_versioned)
           SELECT $1, 'probe/' || n, gen_random_uuid()::text, now(), true
           FROM generate_series(1, 3) AS n RETURNING id`,
          [bucketId]
        )
      ).rows.map((row) => row.id)
      const current = (
        await client.query<{ id: string }>(
          `INSERT INTO storage.objects (bucket_id, name, version, is_versioned)
           VALUES ($1, 'probe/current', gen_random_uuid()::text, true) RETURNING id`,
          [bucketId]
        )
      ).rows[0].id
      await client.query(
        `CREATE TABLE public.lifecycle_probe_refs (
          object_id uuid REFERENCES storage.objects(id) DEFERRABLE INITIALLY DEFERRED
        )`
      )
      await client.query('INSERT INTO public.lifecycle_probe_refs VALUES ($1)', [ids[1]])
      await client.query('CREATE SEQUENCE public.lifecycle_probe_calls')
      const codes = [
        '08006',
        '40001',
        '42P01',
        '42883',
        '53300',
        '55P03',
        '57P03',
        '58030',
        'XX001',
      ]
      // The Multigres pooler discards the transaction for connection/startup errors.
      for (const code of codes.filter(
        (value) => getConfig().databaseEngine !== 'multigres' || !['08006', '57P03'].includes(value)
      )) {
        // The first error is one-shot; the second occurs only after the FK forces fallback.
        for (const raiseAfter of [0, 3]) {
          await client.query('SAVEPOINT transient')
          await client.query("SELECT setval('public.lifecycle_probe_calls', 1, false)")
          await client.query(`CREATE FUNCTION public.lifecycle_probe_busy() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
              IF nextval('public.lifecycle_probe_calls') ${raiseAfter === 0 ? '= 1' : '> 3'} THEN
                RAISE EXCEPTION 'busy' USING ERRCODE = '${code}';
              END IF;
              RETURN OLD;
            END $$`)
          await client.query(`CREATE TRIGGER lifecycle_probe_busy BEFORE DELETE ON storage.objects
            FOR EACH ROW EXECUTE FUNCTION public.lifecycle_probe_busy()`)
          await expect(
            client.query('SELECT * FROM storage.delete_lifecycle_objects($1::uuid[])', [ids])
          ).rejects.toMatchObject({ code })
          await client.query('ROLLBACK TO SAVEPOINT transient')
        }
      }
      const currentOnly = await client.query(
        'SELECT * FROM storage.delete_lifecycle_objects($1::uuid[])',
        [[current]]
      )
      expect(currentOnly.rows).toEqual([])
      await client.query('SET CONSTRAINTS ALL DEFERRED')
      const deleted = await client.query<{ id: string }>(
        'SELECT * FROM storage.delete_lifecycle_objects($1::uuid[])',
        [[...ids, current]]
      )
      expect(deleted.rows.map((row) => row.id).sort()).toEqual([ids[0], ids[2]].sort())
      const remaining = await client.query(
        'SELECT id FROM storage.objects WHERE id = ANY($1::uuid[]) ORDER BY id',
        [[...ids, current]]
      )
      expect(remaining.rows).toEqual(
        [{ id: ids[1] }, { id: current }].sort((x, y) => x.id.localeCompare(y.id))
      )
    })
  })

  it.each(['bulk', 'fallback'] as const)(
    'returns marker and metadata values aligned with deleted ids in %s',
    async (mode) => {
      await withRollback(async (client) => {
        const inserted = await client.query<{
          id: string
          is_delete_marker: boolean
          metadata: unknown
        }>(
          `INSERT INTO storage.objects (bucket_id, name, version, archived_at, is_versioned, is_delete_marker, metadata)
           VALUES ($1, 'return/marker', gen_random_uuid()::text, now(), true, true, NULL),
                  ($1, 'return/veto', gen_random_uuid()::text, now(), true, false, '{"size":17}'),
                  ($1, 'return/object', gen_random_uuid()::text, now(), true, false, '{"size":43}'),
                  ($1, 'return/no-metadata', gen_random_uuid()::text, now(), true, false, NULL)
           RETURNING id, is_delete_marker, metadata`,
          [bucketId]
        )
        if (mode === 'fallback') {
          await client.query(
            'CREATE TABLE public.lifecycle_return_ref (id uuid REFERENCES storage.objects(id))'
          )
          await client.query('INSERT INTO lifecycle_return_ref VALUES ($1)', [inserted.rows[1].id])
        }
        const deleted = await client.query(
          'SELECT * FROM storage.delete_lifecycle_objects($1::uuid[]) ORDER BY id',
          [inserted.rows.map((row) => row.id)]
        )
        const expected = inserted.rows.filter((_, index) => mode === 'bulk' || index !== 1)
        expect(deleted.rows).toEqual(expected.sort((a, b) => a.id.localeCompare(b.id)))
      })
    }
  )

  it('propagates pending deferred violations before deleting lifecycle objects', async () => {
    await withRollback(async (client) => {
      await client.query("SELECT set_config('storage.allow_delete_query', 'true', true)")
      const id = (
        await client.query<{ id: string }>(
          `INSERT INTO storage.objects (bucket_id, name, version, archived_at, is_versioned)
           VALUES ($1, 'deferred/object', gen_random_uuid()::text, now(), true) RETURNING id`,
          [bucketId]
        )
      ).rows[0].id
      await client.query('CREATE TEMP TABLE lifecycle_deferred_parent (id integer PRIMARY KEY)')
      await client.query(`CREATE TEMP TABLE lifecycle_deferred_child (
        parent_id integer REFERENCES lifecycle_deferred_parent(id) DEFERRABLE INITIALLY DEFERRED
      )`)
      await client.query('INSERT INTO lifecycle_deferred_child VALUES (1)')
      await client.query('SAVEPOINT before_delete')
      await expect(
        client.query('SELECT * FROM storage.delete_lifecycle_objects($1::uuid[])', [[id]])
      ).rejects.toMatchObject({ code: '23503' })
      await client.query('ROLLBACK TO SAVEPOINT before_delete')
      expect(
        (await client.query('SELECT id FROM storage.objects WHERE id = $1', [id])).rows
      ).toEqual([{ id }])

      await client.query('INSERT INTO lifecycle_deferred_parent VALUES (1)')
      const deleted = await client.query(
        'SELECT * FROM storage.delete_lifecycle_objects($1::uuid[])',
        [[id]]
      )
      expect(deleted.rows.map((row) => row.id)).toEqual([id])
      await expect(
        client.query('INSERT INTO lifecycle_deferred_child VALUES (2)')
      ).rejects.toMatchObject({
        code: '23503',
      })
    })
  })

  it('runs user delete triggers with the caller search_path', async () => {
    await withRollback(async (client) => {
      await client.query("SELECT set_config('storage.allow_delete_query', 'true', true)")
      const id = (
        await client.query<{ id: string }>(
          `INSERT INTO storage.objects (bucket_id, name, version, archived_at, is_versioned)
           VALUES ($1, 'search-path/k', gen_random_uuid()::text, now(), true) RETURNING id`,
          [bucketId]
        )
      ).rows[0].id
      await client.query('CREATE TABLE public.lifecycle_probe_audit (id uuid)')
      await client.query(`CREATE FUNCTION public.lifecycle_probe_audit_fn() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN INSERT INTO lifecycle_probe_audit VALUES (OLD.id); RETURN OLD; END $$`)
      await client.query(`CREATE TRIGGER lifecycle_probe_audit AFTER DELETE ON storage.objects
        FOR EACH ROW EXECUTE FUNCTION public.lifecycle_probe_audit_fn()`)
      const deleted = await client.query(
        'SELECT * FROM storage.delete_lifecycle_objects($1::uuid[])',
        [[id]]
      )
      expect(deleted.rows.map((row) => row.id)).toEqual([id])
      const audit = await client.query('SELECT id FROM public.lifecycle_probe_audit')
      expect(audit.rows).toEqual([{ id }])
    })
  })

  it('deletes a full batch without overflowing the subtransaction cache', async (context) => {
    const client = new Client({ connectionString: getConfig().databaseURL })
    await client.connect()
    try {
      const server = await client.query<{ version: string }>(
        `SELECT current_setting('server_version_num') AS version`
      )
      if (Number(server.rows[0].version) < 160000) context.skip()
      await client.query('BEGIN')
      await client.query("SELECT set_config('storage.allow_delete_query', 'true', true)")
      const ids = (
        await client.query<{ id: string }>(
          `INSERT INTO storage.objects (bucket_id, name, version, archived_at, is_versioned)
           SELECT $1, 'batch/' || n, gen_random_uuid()::text, now(), true
           FROM generate_series(1, 100) AS n RETURNING id`,
          [bucketId]
        )
      ).rows.map((row) => row.id)
      const deleted = await client.query(
        'SELECT * FROM storage.delete_lifecycle_objects($1::uuid[])',
        [ids]
      )
      expect(deleted.rowCount).toBe(ids.length)
      const subxact = await client.query(
        `SELECT subxact.subxact_overflowed
         FROM pg_stat_get_backend_idset() AS backend,
              LATERAL pg_stat_get_backend_subxact(backend) AS subxact
         WHERE pg_stat_get_backend_pid(backend) = pg_backend_pid()`
      )
      expect(subxact.rows).toEqual([{ subxact_overflowed: false }])
    } finally {
      await client.query('ROLLBACK').catch(() => undefined)
      await client.end()
    }
  })

  it('takes the bucket row lock first so reconcile excludes arm', async () => {
    await tHelper.database.putLifecycleConfiguration(bucketId, {
      rules: [
        { status: 'Enabled', filter: {}, noncurrentVersionExpiration: { noncurrentDays: 1 } },
      ],
    })
    const owner = new Client({ connectionString: getConfig().databaseURL })
    const prober = new Client({ connectionString: getConfig().databaseURL })
    await Promise.all([owner.connect(), prober.connect()])
    try {
      await owner.query('BEGIN')
      await owner.query('SELECT storage.reconcile_lifecycle_state($1, true)', [bucketId])
      await prober.query('BEGIN')
      await expect(
        prober.query('SELECT 1 FROM storage.buckets WHERE id = $1 FOR SHARE NOWAIT', [bucketId])
      ).rejects.toMatchObject({ code: '55P03' })
    } finally {
      await prober.query('ROLLBACK').catch(() => undefined)
      await owner.query('ROLLBACK').catch(() => undefined)
      await Promise.all([owner.end(), prober.end()])
    }
  })

  it('allows a bucket foreign-key insert during versioning enable while excluding arm', async () => {
    await tHelper.database.putLifecycleConfiguration(bucketId, {
      rules: [
        { status: 'Enabled', filter: {}, noncurrentVersionExpiration: { noncurrentDays: 1 } },
      ],
    })
    const owner = new Client({ connectionString: getConfig().databaseURL })
    const prober = new Client({ connectionString: getConfig().databaseURL })
    await Promise.all([owner.connect(), prober.connect()])
    try {
      await owner.query('BEGIN')
      await owner.query("UPDATE storage.buckets SET versioning_status = 'ENABLED' WHERE id = $1", [
        bucketId,
      ])
      await prober.query('BEGIN')
      await prober.query("SET LOCAL lock_timeout = '2s'")
      await prober.query(
        "INSERT INTO storage.objects (bucket_id, name) VALUES ($1, 'concurrent-insert')",
        [bucketId]
      )
      await prober.query('SAVEPOINT before_arm_lock')
      await expect(
        prober.query('SELECT 1 FROM storage.buckets WHERE id = $1 FOR SHARE NOWAIT', [bucketId])
      ).rejects.toMatchObject({ code: '55P03' })
      await prober.query('ROLLBACK TO SAVEPOINT before_arm_lock')
    } finally {
      await prober.query('ROLLBACK').catch(() => undefined)
      await owner.query('ROLLBACK').catch(() => undefined)
      await Promise.all([owner.end(), prober.end()])
    }
  })
})
