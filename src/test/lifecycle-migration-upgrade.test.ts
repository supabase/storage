import { randomUUID } from 'node:crypto'
import { readdir, readFile } from 'node:fs/promises'
import { runMigrationsOnTenant } from '@internal/database/migrations'
import { Client } from 'pg'
import { getConfig } from '../config'

// Upgrade fixtures intentionally use the old schema rather than current writers.
describe('lifecycle migration upgrades', () => {
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
      bucket_id, scan_kind, shard_id, shard_epoch, shard_count,
      next_run_at, continuation, claim_id, claim_until
    ) SELECT id, 'NONCURRENT', 0, 1, 1,
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
    await client.query(
      await readFile('migrations/tenant/0079-lifecycle-writer-coordination.sql', 'utf8')
    )
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

  it('backfills the central registry and enrolls later feature activation and restored tenants', async () => {
    const directory = 'migrations/multitenant'
    await client.query("SET storage.iceberg_shards = '{}'")
    for (const file of (await readdir(directory))
      .filter((name) => name.endsWith('.sql') && name < '0032')
      .sort()) {
      await client.query(await readFile(`${directory}/${file}`, 'utf8'))
    }
    const insertTenant = (id: string, enabled: boolean) =>
      client.query(
        `INSERT INTO public.tenants (id, anon_key, database_url, jwt_secret, service_key, feature_object_versioning)
       VALUES ($1, 'fixture', 'fixture', 'fixture', 'fixture', $2)`,
        [id, enabled]
      )
    await insertTenant('existing-enabled', true)
    await insertTenant('enable-later', false)
    const migration = await readFile(`${directory}/0032-enroll-lifecycle-tenants.sql`, 'utf8')
    await client.query(migration)
    expect((await client.query('SELECT tenant_id FROM public.lifecycle_tenants')).rows).toEqual([
      { tenant_id: 'existing-enabled' },
    ])
    await client.query(
      "UPDATE public.lifecycle_tenants SET claim_id = gen_random_uuid(), claim_until = '2099-01-01'"
    )
    const before = (await client.query('SELECT * FROM public.lifecycle_tenants')).rows
    await client.query(migration)
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

async function prepareCentralRegistry(client: Client) {
  const directory = 'migrations/multitenant'
  await client.query("SET storage.iceberg_shards = '{}'")
  for (const file of (await readdir(directory))
    .filter((name) => name.endsWith('.sql') && name < '0032')
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
  await client.query(
    await readFile('migrations/multitenant/0032-enroll-lifecycle-tenants.sql', 'utf8')
  )
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
