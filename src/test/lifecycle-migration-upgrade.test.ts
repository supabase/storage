import { randomUUID } from 'node:crypto'
import { readdir, readFile } from 'node:fs/promises'
import { setTimeout } from 'node:timers/promises'
import { runMigrationsOnTenant } from '@internal/database/migrations'
import { Client } from 'pg'
import { getConfig } from '../config'

const rerunCases = [
  {
    file: '0076-noncurrent-lifecycle.sql',
    table: 'bucket_lifecycle_states',
    index: 'bucket_lifecycle_states_due_idx',
  },
  {
    file: '0079-lifecycle-writer-coordination.sql',
    table: 'lifecycle_abandoned_artifacts',
    index: 'lifecycle_abandoned_artifacts_attempted_at_idx',
  },
] as const

// Upgrade fixtures use disposable databases but Multigres pooler doesn't support creating them.
describe.skipIf(getConfig().databaseEngine === 'multigres')('lifecycle migration upgrades', () => {
  const databaseName = `lifecycle_upgrade_${randomUUID().replaceAll('-', '')}`
  const databaseUrl = new URL(getConfig().databaseURL)
  databaseUrl.pathname = `/${databaseName}`
  const admin = new Client({ connectionString: getConfig().databaseURL })
  let client: Client
  const policy = {
    rules: [{ status: 'Enabled', filter: {}, noncurrentVersionExpiration: { noncurrentDays: 1 } }],
  }

  beforeAll(async () => {
    await admin.connect()
  })

  beforeEach(async () => {
    await admin.query(`CREATE DATABASE ${databaseName}`)
    client = new Client({ connectionString: databaseUrl.toString() })
    await client.connect()
  })

  afterEach(async () => {
    await client.end()
    await admin.query(`DROP DATABASE IF EXISTS ${databaseName} WITH (FORCE)`)
  })

  afterAll(async () => {
    await admin.end()
  })

  it.each(['backfill', 'policy write', 'versioning enable'] as const)(
    'creates state for a legacy long-named bucket during %s',
    async (entryPoint) => {
      await runMigrationsOnTenant({
        databaseUrl: databaseUrl.toString(),
        ...(entryPoint === 'backfill'
          ? { upToMigration: 'noncurrent-lifecycle-archived-due-index' as const }
          : {}),
      })
      const name = 'x'.repeat(120)
      const hasPolicy = entryPoint !== 'policy write'
      await client.query('BEGIN')
      let updatedAt: Date
      try {
        // Model a bucket retained from before the name-length trigger in migration 37.
        await client.query('SET LOCAL session_replication_role = replica')
        const inserted = await client.query(
          `INSERT INTO storage.buckets (id, name, versioning_status,
             lifecycle_configuration, lifecycle_configuration_generation)
           VALUES ($1, $1, $2, $3, $4) RETURNING updated_at`,
          [
            name,
            entryPoint === 'versioning enable' ? 'DISABLED' : 'ENABLED',
            hasPolicy ? JSON.stringify(policy) : null,
            hasPolicy ? randomUUID() : null,
          ]
        )
        updatedAt = inserted.rows[0].updated_at
        await client.query('COMMIT')
      } catch (error) {
        await client.query('ROLLBACK')
        throw error
      }
      expect((await client.query('SELECT 1 FROM storage.bucket_lifecycle_states')).rows).toEqual([])

      if (entryPoint === 'backfill') {
        await runMigrationsOnTenant({ databaseUrl: databaseUrl.toString() })
      } else if (entryPoint === 'policy write') {
        await client.query('BEGIN')
        try {
          await client.query("SELECT set_config('role', $1, true)", [getConfig().dbServiceRole])
          await client.query(
            `UPDATE storage.buckets SET lifecycle_configuration = $2,
               lifecycle_configuration_generation = gen_random_uuid() WHERE id = $1`,
            [name, JSON.stringify(policy)]
          )
          await client.query('COMMIT')
        } catch (error) {
          await client.query('ROLLBACK')
          throw error
        }
      } else {
        await client.query(
          "UPDATE storage.buckets SET versioning_status = 'ENABLED' WHERE id = $1",
          [name]
        )
      }

      expect(
        (
          await client.query(
            'SELECT bucket_id, next_run_at <= clock_timestamp() AS due FROM storage.bucket_lifecycle_states'
          )
        ).rows
      ).toEqual([{ bucket_id: name, due: true }])
      expect(
        (await client.query('SELECT name, updated_at FROM storage.buckets WHERE id = $1', [name]))
          .rows
      ).toEqual([{ name, updated_at: updatedAt }])
    }
  )

  it('enrolls pre-execution policies and repairs dormant schedules without losing existing work', async () => {
    await runMigrationsOnTenant({
      databaseUrl: databaseUrl.toString(),
      upToMigration: 'unlock-object-versioning',
    })
    for (const [id, status, enabled] of [
      ['enabled', 'ENABLED', true],
      ['suspended', 'SUSPENDED', true],
      ['disabled', 'DISABLED', true],
      ['disabled-rule', 'ENABLED', false],
      ['stranded', 'ENABLED', true],
      ['claimed', 'ENABLED', true],
    ]) {
      await client.query(
        `INSERT INTO storage.buckets (id, name, versioning_status,
           lifecycle_configuration, lifecycle_configuration_generation)
         VALUES ($1, $1, $2, $3, gen_random_uuid())`,
        [
          id,
          status,
          JSON.stringify({
            rules: [{ ...policy.rules[0], status: enabled ? 'Enabled' : 'Disabled' }],
          }),
        ]
      )
    }
    await client.query("INSERT INTO storage.buckets (id, name) VALUES ('no-policy', 'no-policy')")
    await runMigrationsOnTenant({
      databaseUrl: databaseUrl.toString(),
      upToMigration: 'noncurrent-lifecycle-archived-due-index',
    })
    expect((await client.query('SELECT * FROM storage.bucket_lifecycle_states')).rows).toEqual([])
    await client.query(`INSERT INTO storage.bucket_lifecycle_states (
      bucket_id,
      next_run_at, continuation, claim_id, claim_until
    ) SELECT id,
      CASE WHEN id = 'claimed' THEN '2099-01-01'::timestamptz END,
      CASE WHEN id = 'claimed' THEN '{"continuationVersion":999,"preserve":true}'::jsonb END,
      CASE WHEN id = 'claimed' THEN gen_random_uuid() END,
      CASE WHEN id = 'claimed' THEN '2099-01-01'::timestamptz END
      FROM storage.buckets WHERE id IN ('stranded', 'claimed')`)
    const before = (
      await client.query(
        "SELECT * FROM storage.bucket_lifecycle_states WHERE bucket_id = 'claimed'"
      )
    ).rows
    await runMigrationsOnTenant({ databaseUrl: databaseUrl.toString() })
    expect(
      (
        await client.query(`SELECT bucket_id, next_run_at <= clock_timestamp() AS due
      FROM storage.bucket_lifecycle_states ORDER BY bucket_id`)
      ).rows
    ).toEqual([
      { bucket_id: 'claimed', due: false },
      { bucket_id: 'disabled', due: null },
      { bucket_id: 'disabled-rule', due: null },
      { bucket_id: 'enabled', due: true },
      { bucket_id: 'stranded', due: true },
      { bucket_id: 'suspended', due: true },
    ])
    const read = () =>
      client.query('SELECT * FROM storage.bucket_lifecycle_states ORDER BY bucket_id')
    expect(
      (
        await client.query(
          "SELECT * FROM storage.bucket_lifecycle_states WHERE bucket_id = 'claimed'"
        )
      ).rows
    ).toEqual(before)
    const first = (await read()).rows
    for (const file of (await readdir('migrations/tenant'))
      .filter((name) => name >= '0079')
      .sort()) {
      await client.query(await readFile(`migrations/tenant/${file}`, 'utf8'))
    }
    expect((await read()).rows).toEqual(first)
    await client.query(
      "UPDATE storage.buckets SET versioning_status = 'ENABLED' WHERE id = 'disabled'"
    )
    expect(
      (
        await client.query(
          "SELECT next_run_at <= clock_timestamp() AS due FROM storage.bucket_lifecycle_states WHERE bucket_id = 'disabled'"
        )
      ).rows
    ).toEqual([{ due: true }])
  })

  it('creates the central registry in public with a shadow storage schema', async () => {
    const insertTenant = await prepareCentralRegistry(client)
    await insertTenant('existing', true)
    await client.query('CREATE SCHEMA storage')
    await client.query('CREATE TABLE storage.tenants (id text PRIMARY KEY)')
    await client.query('SET search_path = storage, public, extensions')

    await applyEnrollment(client)
    await applyEnrollment(client)
    await insertTenant('later', true)

    expect(
      (await client.query('SELECT tenant_id FROM public.lifecycle_tenants ORDER BY tenant_id')).rows
    ).toEqual([{ tenant_id: 'existing' }, { tenant_id: 'later' }])
    expect(
      (
        await client.query(`SELECT referenced_namespace.nspname AS schema_name,
          referenced_table.relname AS table_name
        FROM pg_catalog.pg_constraint AS constraint_row
        JOIN pg_catalog.pg_class AS referenced_table ON referenced_table.oid = constraint_row.confrelid
        JOIN pg_catalog.pg_namespace AS referenced_namespace ON referenced_namespace.oid = referenced_table.relnamespace
        WHERE constraint_row.conrelid = 'public.lifecycle_tenants'::regclass
          AND constraint_row.contype = 'f'`)
      ).rows
    ).toEqual([{ schema_name: 'public', table_name: 'tenants' }])
    expect(
      (
        await client.query(`SELECT schemaname, indexname FROM pg_catalog.pg_indexes
          WHERE tablename = 'lifecycle_tenants'
            AND indexname IN ('lifecycle_tenants_due_idx', 'lifecycle_tenants_expired_claim_idx')
          ORDER BY indexname`)
      ).rows
    ).toEqual([
      { schemaname: 'public', indexname: 'lifecycle_tenants_due_idx' },
      { schemaname: 'public', indexname: 'lifecycle_tenants_expired_claim_idx' },
    ])
  })

  it('backfills the central registry and enrolls later feature activation and restored tenants', async () => {
    const insertTenant = await prepareCentralRegistry(client)
    await insertTenant('existing-enabled', true)
    await insertTenant('enable-later', false)
    await applyEnrollment(client)
    expect((await client.query('SELECT tenant_id FROM public.lifecycle_tenants')).rows).toEqual([
      { tenant_id: 'existing-enabled' },
    ])
    await client.query(
      "UPDATE public.lifecycle_tenants SET claim_id = gen_random_uuid(), claim_until = '2099-01-01'"
    )
    const before = (await client.query('SELECT * FROM public.lifecycle_tenants')).rows
    await applyEnrollment(client)
    expect((await client.query('SELECT * FROM public.lifecycle_tenants')).rows).toEqual(before)
    await client.query(
      "UPDATE public.tenants SET feature_object_versioning = true WHERE id = 'enable-later'"
    )
    await insertTenant('restored-enabled', true)
    await insertTenant('disabled', false)
    expect(
      (
        await client.query(`SELECT tenant_id, next_dispatch_at <= clock_timestamp() AS due
      FROM public.lifecycle_tenants ORDER BY tenant_id`)
      ).rows
    ).toEqual([
      { tenant_id: 'enable-later', due: true },
      { tenant_id: 'existing-enabled', due: true },
      { tenant_id: 'restored-enabled', due: true },
    ])
    await client.query(
      "UPDATE public.lifecycle_tenants SET next_dispatch_at = '2099-01-01' WHERE tenant_id = 'enable-later'"
    )
    await client.query(
      "UPDATE public.tenants SET migrations_version = migrations_version, feature_object_versioning = feature_object_versioning WHERE id = 'enable-later'"
    )
    const dispatchDue = async () =>
      (
        await client.query(
          "SELECT next_dispatch_at <= clock_timestamp() AS due FROM public.lifecycle_tenants WHERE tenant_id = 'enable-later'"
        )
      ).rows[0].due
    expect(await dispatchDue()).toBe(false)
    await client.query(
      "UPDATE public.tenants SET migrations_version = 'changed' WHERE id = 'enable-later'"
    )
    expect(await dispatchDue()).toBe(true)
  })

  it('enforces registry identity and failure count, indexes dispatch, and cascades tenant deletion', async () => {
    const insertTenant = await prepareCentralRegistry(client)
    await insertTenant('delete-me', true)
    await insertTenant('keep-me', true)
    await applyEnrollment(client)
    await expect(
      client.query("INSERT INTO public.lifecycle_tenants (tenant_id) VALUES ('delete-me')")
    ).rejects.toMatchObject({ code: '23505' })
    await expect(
      client.query(
        "UPDATE public.lifecycle_tenants SET failure_count = -1 WHERE tenant_id = 'delete-me'"
      )
    ).rejects.toMatchObject({ code: '23514' })
    await client.query(
      "UPDATE public.lifecycle_tenants SET failure_count = 0 WHERE tenant_id = 'delete-me'"
    )
    const indexes = await client.query<{ indexname: string; indexdef: string }>(
      `SELECT indexname, indexdef FROM pg_catalog.pg_indexes
       WHERE schemaname = 'public' AND tablename = 'lifecycle_tenants'
         AND indexname IN ('lifecycle_tenants_due_idx', 'lifecycle_tenants_expired_claim_idx')
       ORDER BY indexname`
    )
    expect(indexes.rows).toEqual([
      {
        indexname: 'lifecycle_tenants_due_idx',
        indexdef:
          'CREATE INDEX lifecycle_tenants_due_idx ON public.lifecycle_tenants USING btree (next_dispatch_at, tenant_id)',
      },
      {
        indexname: 'lifecycle_tenants_expired_claim_idx',
        indexdef:
          'CREATE INDEX lifecycle_tenants_expired_claim_idx ON public.lifecycle_tenants USING btree (claim_until, tenant_id) WHERE (claim_until IS NOT NULL)',
      },
    ])
    await client.query("DELETE FROM public.tenants WHERE id = 'delete-me'")
    expect((await client.query('SELECT tenant_id FROM public.lifecycle_tenants')).rows).toEqual([
      { tenant_id: 'keep-me' },
    ])
  })

  it('migrates without waiting on a writer that holds objects and then needs its bucket', async () => {
    await runMigrationsOnTenant({
      databaseUrl: databaseUrl.toString(),
      upToMigration: 'noncurrent-lifecycle-archived-due-index',
    })
    await client.query(
      "INSERT INTO storage.buckets (id, name, versioning_status) VALUES ('busy', 'busy', 'ENABLED')"
    )
    await client.query(
      `INSERT INTO storage.objects (id, bucket_id, name, version, archived_at, is_versioned)
       VALUES ($1, 'busy', 'history', $2, now(), true)`,
      [randomUUID(), randomUUID()]
    )
    const writer = new Client({ connectionString: databaseUrl.toString() })
    await writer.connect()
    try {
      await writer.query('BEGIN')
      await writer.query("UPDATE storage.objects SET metadata = '{}' WHERE bucket_id = 'busy'")
      await expect(
        runMigrationsOnTenant({ databaseUrl: databaseUrl.toString() })
      ).resolves.toBeUndefined()
      await expect(
        writer.query("SELECT 1 FROM storage.buckets WHERE id = 'busy' FOR SHARE")
      ).resolves.toMatchObject({ rowCount: 1 })
      await writer.query('COMMIT')
    } finally {
      await writer.query('ROLLBACK').catch(() => undefined)
      await writer.end()
    }
  })

  it('does not deadlock with a writer that locks a policy bucket and then updates it', async () => {
    await runMigrationsOnTenant({
      databaseUrl: databaseUrl.toString(),
      upToMigration: 'noncurrent-lifecycle-archived-due-index',
    })
    await client.query(
      `INSERT INTO storage.buckets (id, name, versioning_status,
         lifecycle_configuration, lifecycle_configuration_generation)
       VALUES ('locked', 'locked', 'ENABLED', $1, gen_random_uuid())`,
      [JSON.stringify(policy)]
    )
    const writer = new Client({ connectionString: databaseUrl.toString() })
    await writer.connect()
    try {
      await writer.query('BEGIN')
      await writer.query("SELECT 1 FROM storage.buckets WHERE id = 'locked' FOR UPDATE")
      const migration = runMigrationsOnTenant({ databaseUrl: databaseUrl.toString() })
      migration.catch(() => undefined)
      await waitUntilRows(
        client,
        "SELECT 1 FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'"
      )
      await expect(
        writer.query("UPDATE storage.buckets SET public = public WHERE id = 'locked'")
      ).resolves.toMatchObject({ rowCount: 1 })
      await writer.query('COMMIT')
      await expect(migration).resolves.toBeUndefined()
    } finally {
      await writer.query('ROLLBACK').catch(() => undefined)
      await writer.end()
    }
  })

  it('retries the bucket lock and lets queued writers proceed between attempts', async () => {
    await runMigrationsOnTenant({
      databaseUrl: databaseUrl.toString(),
      upToMigration: 'noncurrent-lifecycle-archived-due-index',
    })
    await client.query("INSERT INTO storage.buckets (id, name) VALUES ('held', 'held')")
    const writer = new Client({ connectionString: databaseUrl.toString() })
    const queuedWriter = new Client({ connectionString: databaseUrl.toString() })
    await Promise.all([writer.connect(), queuedWriter.connect()])
    try {
      await writer.query('BEGIN')
      await writer.query("INSERT INTO storage.objects (bucket_id, name) VALUES ('held', 'open')")
      let migrationFinished = false
      const migration = runMigrationsOnTenant({ databaseUrl: databaseUrl.toString() }).finally(
        () => {
          migrationFinished = true
        }
      )
      migration.catch(() => undefined)
      await waitUntilRows(
        client,
        `SELECT 1 FROM pg_catalog.pg_locks
         WHERE database = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
           AND relation = 'storage.buckets'::regclass AND mode = 'ExclusiveLock' AND NOT granted`
      )
      const queuedAt = performance.now()
      await expect(
        queuedWriter.query(
          "INSERT INTO storage.objects (bucket_id, name) VALUES ('held', 'queued')"
        )
      ).resolves.toMatchObject({ rowCount: 1 })
      expect(performance.now() - queuedAt).toBeLessThan(2_000)
      expect(migrationFinished).toBe(false)
      await writer.query('ROLLBACK')
      await expect(migration).resolves.toBeUndefined()
    } finally {
      await writer.query('ROLLBACK').catch(() => undefined)
      await Promise.all([writer.end(), queuedWriter.end()])
    }
  }, 20_000)

  it('propagates statement cancellation while waiting for the bucket lock', async () => {
    await runMigrationsOnTenant({
      databaseUrl: databaseUrl.toString(),
      upToMigration: 'noncurrent-lifecycle-archived-due-index',
    })
    await client.query("INSERT INTO storage.buckets (id, name) VALUES ('held', 'held')")
    const writer = new Client({ connectionString: databaseUrl.toString() })
    const migration = new Client({ connectionString: databaseUrl.toString() })
    await Promise.all([writer.connect(), migration.connect()])
    try {
      await writer.query('BEGIN')
      await writer.query("INSERT INTO storage.objects (bucket_id, name) VALUES ('held', 'open')")
      await migration.query('BEGIN')
      await migration.query("SET LOCAL statement_timeout = '250ms'")
      await expect(
        migration.query(
          await readFile('migrations/tenant/0079-lifecycle-writer-coordination.sql', 'utf8')
        )
      ).rejects.toMatchObject({ code: '57014' })
      await migration.query('ROLLBACK')
      await writer.query('ROLLBACK')
      await expect(
        runMigrationsOnTenant({ databaseUrl: databaseUrl.toString() })
      ).resolves.toBeUndefined()
    } finally {
      await migration.query('ROLLBACK').catch(() => undefined)
      await writer.query('ROLLBACK').catch(() => undefined)
      await Promise.all([migration.end(), writer.end()])
    }
  })

  it.each([
    { lockTimeout: '1s', statementTimeout: '1800ms' },
    { lockTimeout: '50ms', statementTimeout: '200ms' },
  ])(
    'gives up within the caller lock_timeout of $lockTimeout',
    async ({ lockTimeout, statementTimeout }) => {
      await runMigrationsOnTenant({
        databaseUrl: databaseUrl.toString(),
        upToMigration: 'noncurrent-lifecycle-archived-due-index',
      })
      await client.query("INSERT INTO storage.buckets (id, name) VALUES ('held', 'held')")
      const writer = new Client({ connectionString: databaseUrl.toString() })
      const migration = new Client({ connectionString: databaseUrl.toString() })
      await Promise.all([writer.connect(), migration.connect()])
      try {
        await writer.query('BEGIN')
        await writer.query("INSERT INTO storage.objects (bucket_id, name) VALUES ('held', 'open')")
        await migration.query('BEGIN')
        // A separate statement deadline catches either an unclamped retry sleep or
        // a lock attempt that ignores a caller timeout shorter than 250ms.
        await migration.query(
          "SELECT set_config('lock_timeout', $1, true), set_config('statement_timeout', $2, true)",
          [lockTimeout, statementTimeout]
        )
        await expect(
          migration.query(
            await readFile('migrations/tenant/0079-lifecycle-writer-coordination.sql', 'utf8')
          )
        ).rejects.toMatchObject({ code: '55P03' })
      } finally {
        await migration.query('ROLLBACK').catch(() => undefined)
        await writer.query('ROLLBACK').catch(() => undefined)
        await Promise.all([migration.end(), writer.end()])
      }
    }
  )

  it('reruns while a bucket reader holds its table lock, without upgrading the migration lock', async () => {
    await runMigrationsOnTenant({ databaseUrl: databaseUrl.toString() })
    await client.query("INSERT INTO storage.buckets (id, name) VALUES ('busy', 'busy')")
    const writer = new Client({ connectionString: databaseUrl.toString() })
    const migration = new Client({ connectionString: databaseUrl.toString() })
    await writer.connect()
    await migration.connect()
    try {
      await writer.query('BEGIN')
      const writerPid = (await writer.query<{ pid: number }>('SELECT pg_backend_pid() AS pid'))
        .rows[0].pid
      await writer.query("SELECT 1 FROM storage.buckets WHERE id = 'busy'")
      await migration.query('BEGIN')
      await migration.query("SET LOCAL lock_timeout = '2s'")
      await migration.query(
        await readFile('migrations/tenant/0079-lifecycle-writer-coordination.sql', 'utf8')
      )
      expect((await migration.query('SHOW lock_timeout')).rows[0].lock_timeout).toBe('2s')

      // The migration retains its EXCLUSIVE lock until commit. A row-locking
      // request waits for it but cannot form a cycle with a trigger drop.
      const rowLock = writer.query("SELECT 1 FROM storage.buckets WHERE id = 'busy' FOR UPDATE")
      rowLock.catch(() => undefined)
      await waitUntilRows(
        client,
        `SELECT 1 FROM pg_catalog.pg_locks
         WHERE pid = ${writerPid} AND relation = 'storage.buckets'::regclass
           AND mode = 'RowShareLock' AND NOT granted`
      )
      await migration.query('COMMIT')
      await expect(rowLock).resolves.toMatchObject({ rowCount: 1 })
      await writer.query('COMMIT')
    } finally {
      await migration.query('ROLLBACK').catch(() => undefined)
      await writer.query('ROLLBACK').catch(() => undefined)
      await migration.end()
      await writer.end()
    }
  })

  it.each(rerunCases)(
    'reruns $file without blocking a lifecycle table reader and writer',
    async ({ file, table }) => {
      await runMigrationsOnTenant({ databaseUrl: databaseUrl.toString() })
      const worker = new Client({ connectionString: databaseUrl.toString() })
      const migration = new Client({ connectionString: databaseUrl.toString() })
      await worker.connect()
      await migration.connect()
      try {
        await worker.query('BEGIN')
        await worker.query(`SELECT 1 FROM storage.${table} LIMIT 1`)
        await migration.query('BEGIN')
        await migration.query("SET LOCAL lock_timeout = '2s'")
        await migration.query(await readFile(`migrations/tenant/${file}`, 'utf8'))
        await worker.query("SET LOCAL lock_timeout = '2s'")
        await worker.query(`DELETE FROM storage.${table} WHERE false`)
        await worker.query('COMMIT')
        await migration.query('COMMIT')
      } finally {
        await worker.query('ROLLBACK').catch(() => undefined)
        await migration.query('ROLLBACK').catch(() => undefined)
        await worker.end()
        await migration.end()
      }
    }
  )

  it.each(rerunCases)('keeps or restores index/RLS in $file', async ({ file, table, index }) => {
    await runMigrationsOnTenant({ databaseUrl: databaseUrl.toString() })
    const sql = await readFile(`migrations/tenant/${file}`, 'utf8')
    const state = async () =>
      (
        await client.query<{ index_oid: number | null; rls: boolean }>(
          `SELECT to_regclass($1)::oid AS index_oid, relrowsecurity AS rls
           FROM pg_catalog.pg_class WHERE oid = $2::regclass`,
          [`storage.${index}`, `storage.${table}`]
        )
      ).rows[0]
    const original = await state()
    expect(original.index_oid).not.toBeNull()
    expect(original.rls).toBe(true)
    await client.query(sql)
    expect(await state()).toEqual(original)
    await client.query(`DROP INDEX storage.${index}`)
    await client.query(`ALTER TABLE storage.${table} DISABLE ROW LEVEL SECURITY`)
    await client.query(sql)
    expect(await state()).toMatchObject({ index_oid: expect.any(Number), rls: true })
  })

  it.each(rerunCases)('takes RLS lock before index in $file', async ({ file, table, index }) => {
    await runMigrationsOnTenant({ databaseUrl: databaseUrl.toString() })
    await client.query(`DROP INDEX storage.${index}`)
    await client.query(`ALTER TABLE storage.${table} DISABLE ROW LEVEL SECURITY`)
    const reader = new Client({ connectionString: databaseUrl.toString() })
    const migration = new Client({ connectionString: databaseUrl.toString() })
    await Promise.all([reader.connect(), migration.connect()])
    try {
      await reader.query('BEGIN')
      await reader.query(`SELECT 1 FROM storage.${table} LIMIT 1`)
      await migration.query('BEGIN')
      await migration.query("SET LOCAL lock_timeout = '5s'")
      const pid = (await migration.query<{ pid: number }>('SELECT pg_backend_pid() AS pid')).rows[0]
        .pid
      const replay = migration.query(await readFile(`migrations/tenant/${file}`, 'utf8'))
      replay.catch(() => undefined)
      await waitUntilRows(
        client,
        `SELECT 1 FROM pg_catalog.pg_locks
         WHERE pid = ${pid} AND relation = 'storage.${table}'::regclass
           AND mode = 'AccessExclusiveLock' AND NOT granted`
      )
      expect(
        (
          await client.query(
            `SELECT 1 FROM pg_catalog.pg_locks
             WHERE pid = $1 AND relation = $2::regclass AND mode = 'ShareLock' AND granted`,
            [pid, `storage.${table}`]
          )
        ).rowCount
      ).toBe(0)
      await reader.query('COMMIT')
      await replay
      await migration.query('COMMIT')
    } finally {
      await reader.query('ROLLBACK').catch(() => undefined)
      await migration.query('ROLLBACK').catch(() => undefined)
      await Promise.all([reader.end(), migration.end()])
    }
  })

  it('preserves existing lifecycle triggers and recreates each missing one', async () => {
    await runMigrationsOnTenant({ databaseUrl: databaseUrl.toString() })
    const sql = await readFile('migrations/tenant/0079-lifecycle-writer-coordination.sql', 'utf8')
    const names = [
      'protect_active_lifecycle_states',
      'wake_lifecycle_on_versioning_enable',
      'wake_lifecycle_on_policy_write',
    ]
    const triggers = async () =>
      (
        await client.query<{ oid: number; tgname: string; definition: string }>(
          `SELECT oid, tgname, pg_get_triggerdef(oid) AS definition
           FROM pg_catalog.pg_trigger
           WHERE tgrelid = 'storage.buckets'::regclass AND tgname = ANY($1::text[])
           ORDER BY tgname`,
          [names]
        )
      ).rows
    const original = await triggers()
    expect(original).toHaveLength(names.length)
    await client.query(sql)
    expect(await triggers()).toEqual(original)

    for (const name of names) {
      await client.query(`DROP TRIGGER ${name} ON storage.buckets`)
      const remaining = await triggers()
      await client.query(sql)
      const restored = await triggers()
      expect(restored.filter((trigger) => trigger.tgname !== name)).toEqual(remaining)
      expect(restored.find((trigger) => trigger.tgname === name)?.definition).toEqual(
        original.find((trigger) => trigger.tgname === name)?.definition
      )
    }
  })

  it('revokes execute from roles no longer configured when upgrading', async () => {
    const role = `lifecycle_former_${randomUUID().replaceAll('-', '')}`
    await runMigrationsOnTenant({
      databaseUrl: databaseUrl.toString(),
      upToMigration: 'noncurrent-lifecycle-archived-due-index',
    })
    await admin.query(`CREATE ROLE ${role} NOLOGIN`)
    try {
      await client.query(
        `ALTER DEFAULT PRIVILEGES IN SCHEMA storage GRANT EXECUTE ON FUNCTIONS TO ${role}`
      )
      await runMigrationsOnTenant({ databaseUrl: databaseUrl.toString() })

      const { rows } = await client.query(
        `SELECT
           has_function_privilege($1, 'storage.reconcile_lifecycle_state(text,boolean)', 'EXECUTE') AS former_reconcile,
           has_function_privilege($1, 'storage.delete_lifecycle_objects(uuid[])', 'EXECUTE') AS former_delete,
           has_function_privilege($2, 'storage.reconcile_lifecycle_state(text,boolean)', 'EXECUTE') AS service_reconcile`,
        [role, getConfig().dbServiceRole]
      )
      expect(rows[0]).toEqual({
        former_reconcile: false,
        former_delete: false,
        service_reconcile: true,
      })
    } finally {
      await client.query(`DROP OWNED BY ${role}`)
      await admin.query(`DROP ROLE ${role}`)
    }
  })

  it('wakes flagged tenants when their migrations_version changes, without postponing or enrolling others', async () => {
    const insertTenant = await prepareCentralRegistry(client)
    await insertTenant('late', true)
    await insertTenant('early', true)
    await insertTenant('unflagged', false)
    await applyEnrollment(client)
    await setNextDispatch(client, 'late', FUTURE)
    await setNextDispatch(client, 'early', PAST)

    await client.query(
      "UPDATE public.tenants SET migrations_version = 'lifecycle-writer-coordination'"
    )

    const state = await nextDispatches(client)
    expect([...state.keys()]).toEqual(['early', 'late'])
    expect(state.get('late')).toMatchObject({ due: true })
    expect(state.get('early')).toEqual({ at: Date.parse(PAST), due: true })
  })

  it('wakes with LEAST on feature activation and backfill re-runs', async () => {
    const insertTenant = await prepareCentralRegistry(client)
    await applyEnrollment(client)
    for (const id of ['flip-future', 'flip-past']) {
      await insertTenant(id, false)
    }
    for (const id of ['rerun-future', 'rerun-past']) {
      await insertTenant(id, true)
    }
    await setNextDispatch(client, 'flip-future', FUTURE)
    await setNextDispatch(client, 'flip-past', PAST)
    await setNextDispatch(client, 'rerun-future', FUTURE)
    await setNextDispatch(client, 'rerun-past', PAST)

    await applyEnrollment(client)

    const rerun = await nextDispatches(client)
    expect(rerun.get('rerun-future')).toMatchObject({ due: true })
    expect(rerun.get('rerun-past')).toEqual({ at: Date.parse(PAST), due: true })
    expect(rerun.get('flip-future')).toEqual({ at: Date.parse(FUTURE), due: false })

    await client.query(
      "UPDATE public.tenants SET feature_object_versioning = true WHERE id LIKE 'flip-%'"
    )

    const flipped = await nextDispatches(client)
    expect(flipped.get('flip-future')).toMatchObject({ due: true })
    expect(flipped.get('flip-past')).toEqual({ at: Date.parse(PAST), due: true })
  })
})

const FUTURE = '2099-01-01T00:00:00Z'
const PAST = '2000-01-01T00:00:00Z'

async function waitUntilRows(client: Client, sql: string) {
  const timeoutMs = 3_000
  const deadline = performance.now() + timeoutMs
  while (!(await client.query(sql)).rowCount) {
    if (performance.now() >= deadline) {
      throw new Error(`Timed out after ${timeoutMs}ms waiting for rows: ${sql}`)
    }
    await setTimeout(20)
  }
}

async function prepareCentralRegistry(client: Client) {
  const directory = 'migrations/multitenant'
  await client.query("SET storage.iceberg_shards = '{}'")
  for (const file of (await readdir(directory))
    .filter((name) => name.endsWith('.sql') && name < '0031')
    .sort()) {
    await client.query(await readFile(`${directory}/${file}`, 'utf8'))
  }
  return (id: string, enabled: boolean) =>
    client.query(
      `INSERT INTO public.tenants (id, anon_key, database_url, jwt_secret, service_key, feature_object_versioning)
       VALUES ($1, 'fixture', 'fixture', 'fixture', 'fixture', $2)`,
      [id, enabled]
    )
}

async function applyEnrollment(client: Client) {
  await client.query(await readFile('migrations/multitenant/0031-lifecycle-tenants.sql', 'utf8'))
}

function setNextDispatch(client: Client, tenantId: string, at: string) {
  return client.query(
    `INSERT INTO public.lifecycle_tenants (tenant_id, next_dispatch_at) VALUES ($1, $2)
     ON CONFLICT (tenant_id) DO UPDATE SET next_dispatch_at = EXCLUDED.next_dispatch_at`,
    [tenantId, at]
  )
}

async function nextDispatches(client: Client) {
  const result = await client.query<{
    tenant_id: string
    next_dispatch_at: Date
    due: boolean
  }>(
    `SELECT tenant_id, next_dispatch_at, next_dispatch_at <= clock_timestamp() AS due
     FROM public.lifecycle_tenants ORDER BY tenant_id`
  )
  return new Map(
    result.rows.map((row) => [row.tenant_id, { at: row.next_dispatch_at.getTime(), due: row.due }])
  )
}
