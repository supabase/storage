import { randomUUID } from 'node:crypto'
import { migrate } from '@internal/database/migrations/migrate'
import { Client } from 'pg'
import { loadMigrationFiles } from 'postgres-migrations'
import { runMigration } from 'postgres-migrations/dist/run-migration'
import type { Migration } from 'postgres-migrations/dist/types'
import { getConfig } from '../config'

describe.skipIf(getConfig().databaseEngine === 'multigres')(
  'lifecycle tenant migration execution',
  () => {
    const databaseName = `lifecycle_enrollment_${randomUUID().replaceAll('-', '')}`
    const url = new URL(getConfig().databaseURL)
    url.pathname = `/${databaseName}`
    const admin = new Client({ connectionString: getConfig().databaseURL })
    let observer: Client
    let runner: Client
    let migrations: Migration[]
    let runnerPid: number

    beforeAll(async () => {
      await admin.connect()
      migrations = await loadMigrationFiles('migrations/multitenant')
    })

    beforeEach(async () => {
      await admin.query(`CREATE DATABASE ${databaseName}`)
      observer = new Client({ connectionString: url.toString() })
      runner = new Client({ connectionString: url.toString() })
      await Promise.all([observer.connect(), runner.connect()])
      await observer.query("SET storage.iceberg_shards = '{}'")
      for (const migration of migrations.filter(({ id }) => id < 31)) {
        await runMigration('migrations', observer)(migration)
      }
      runnerPid = (await runner.query('SELECT pg_backend_pid() AS pid')).rows[0].pid
      await runner.query("SET lock_timeout = '7s'")
    })

    afterEach(async () => {
      await Promise.all([runner.end(), observer.end()])
      await admin.query(`DROP DATABASE IF EXISTS ${databaseName} WITH (FORCE)`)
    })

    afterAll(async () => {
      await admin.end()
    })

    const run = () =>
      migrate({
        client: runner,
        migrationsDirectory: 'migrations/multitenant',
        migrationsTableSchema: 'public',
        waitForLock: true,
      })

    async function insertTenants(count: number) {
      await observer.query(
        `INSERT INTO public.tenants
        (id, anon_key, database_url, jwt_secret, service_key, feature_object_versioning)
       SELECT 'tenant' || lpad(i::text, 5, '0'), 'fixture', 'fixture', 'fixture', 'fixture', true
       FROM generate_series(1, $1::integer) AS i`,
        [count]
      )
    }

    async function installBackfillHook(body: string) {
      // Model an interrupted run with committed schema but no backfill/ledger entry.
      await observer.query(migrations[31].sql)
      await observer.query('TRUNCATE public.lifecycle_tenants')
      await observer.query(`CREATE FUNCTION public.test_enrollment_hook() RETURNS trigger
      LANGUAGE plpgsql AS $$ BEGIN
        IF NEW.tenant_id = 'tenant01001' THEN ${body} END IF;
        RETURN NEW;
      END; $$;
      CREATE TRIGGER test_enrollment_hook BEFORE INSERT ON public.lifecycle_tenants
      FOR EACH ROW EXECUTE FUNCTION public.test_enrollment_hook()`)
    }

    async function recorded() {
      return (await observer.query('SELECT * FROM public.migrations WHERE id = 31')).rows
    }

    it('commits each batch without retaining DDL locks or blocking ordinary tenant writes', async () => {
      await insertTenants(1005)
      await installBackfillHook('PERFORM pg_sleep(2);')
      const migration = run()
      migration.catch(() => undefined)
      try {
        await waitFor(
          observer,
          `SELECT 1 FROM pg_stat_activity
        WHERE pid = ${runnerPid} AND wait_event = 'PgSleep'`
        )

        expect(
          (await observer.query('SELECT count(*)::int FROM public.lifecycle_tenants')).rows
        ).toEqual([{ count: 1000 }])
        expect(await recorded()).toEqual([])
        expect(
          (
            await observer.query(
              `SELECT mode FROM pg_locks
        WHERE pid = $1 AND relation = 'public.tenants'::regclass AND granted
          AND mode IN ('ShareLock', 'ShareRowExclusiveLock', 'ExclusiveLock', 'AccessExclusiveLock')`,
              [runnerPid]
            )
          ).rows
        ).toEqual([])

        await observer.query("SET statement_timeout = '500ms'")
        await observer.query(
          "UPDATE public.tenants SET file_size_limit = 1 WHERE id = 'tenant01002'"
        )
        await observer.query("DELETE FROM public.tenants WHERE id = 'tenant00001'")
        await observer.query(`INSERT INTO public.tenants
        (id, anon_key, database_url, jwt_secret, service_key, feature_object_versioning)
        VALUES ('new-before-cursor', 'fixture', 'fixture', 'fixture', 'fixture', true),
               ('zz-after-upper-bound', 'fixture', 'fixture', 'fixture', 'fixture', true)`)
        await observer.query('SET statement_timeout = 0')
        await migration

        expect(
          (await observer.query('SELECT count(*)::int FROM public.lifecycle_tenants')).rows
        ).toEqual([{ count: 1006 }])
        expect(await recorded()).toMatchObject([{ hash: migrations[31].hash }])
        expect((await runner.query('SHOW lock_timeout')).rows).toEqual([{ lock_timeout: '7s' }])
      } finally {
        await migration
      }
    })

    it('releases locks after a short timeout and retries after the writer commits', async () => {
      await insertTenants(1)
      const writer = new Client({ connectionString: url.toString() })
      await writer.connect()
      await writer.query('BEGIN')
      await writer.query("UPDATE public.tenants SET file_size_limit = 1 WHERE id = 'tenant00001'")
      const migration = run()
      migration.catch(() => undefined)
      try {
        await waitFor(
          observer,
          `SELECT 1 FROM pg_stat_activity
        WHERE pid = ${runnerPid} AND wait_event = 'PgSleep'`
        )
        expect(
          (await observer.query("SELECT to_regclass('public.lifecycle_tenants') AS registry")).rows
        ).toEqual([{ registry: null }])
        await observer.query("SET statement_timeout = '150ms'")
        await observer.query("UPDATE public.tenants SET file_size_limit = 2 WHERE id = 'missing'")
        await observer.query('SET statement_timeout = 0')
        await writer.query('COMMIT')
        await migration
        expect(await recorded()).toHaveLength(1)
        expect((await runner.query('SHOW lock_timeout')).rows).toEqual([{ lock_timeout: '7s' }])
      } finally {
        await writer.query('ROLLBACK')
        await writer.end()
        await migration
      }
    })

    it('bounds lock retries and leaves a failed migration pending and retryable', async () => {
      await insertTenants(1)
      await observer.query('BEGIN')
      await observer.query("UPDATE public.tenants SET file_size_limit = 1 WHERE id = 'tenant00001'")
      const retries: string[] = []
      runner.on('notice', (notice) => {
        if (notice.message?.startsWith('Retrying lifecycle tenant schema')) {
          retries.push(notice.message)
        }
      })
      try {
        await expect(run()).rejects.toThrow('lock timeout')
        expect(retries).toHaveLength(4)
        expect(await recorded()).toEqual([])
        expect((await runner.query('SHOW lock_timeout')).rows).toEqual([{ lock_timeout: '7s' }])
      } finally {
        await observer.query('ROLLBACK')
      }
      await run()
      expect(await recorded()).toHaveLength(1)
    })

    it('resumes a partial backfill without losing claims or postponing existing schedules', async () => {
      await insertTenants(1005)
      await installBackfillHook("RAISE EXCEPTION 'backfill fixture failed';")
      await expect(run()).rejects.toThrow('backfill fixture failed')
      expect(await recorded()).toEqual([])
      expect(
        (await observer.query('SELECT count(*)::int FROM public.lifecycle_tenants')).rows
      ).toEqual([{ count: 1000 }])
      await observer.query(`UPDATE public.lifecycle_tenants
      SET next_dispatch_at = '2000-01-01', claim_id = gen_random_uuid(), claim_until = '2099-01-01',
          failure_count = 3, last_error = '{"preserve":true}'
      WHERE tenant_id = 'tenant00001'`)
      const before = (
        await observer.query(
          "SELECT * FROM public.lifecycle_tenants WHERE tenant_id = 'tenant00001'"
        )
      ).rows
      await observer.query('DROP TRIGGER test_enrollment_hook ON public.lifecycle_tenants')
      await run()
      expect(
        (await observer.query('SELECT count(*)::int FROM public.lifecycle_tenants')).rows
      ).toEqual([{ count: 1005 }])
      expect(
        (
          await observer.query(
            "SELECT * FROM public.lifecycle_tenants WHERE tenant_id = 'tenant00001'"
          )
        ).rows
      ).toEqual(before)
      expect(await recorded()).toMatchObject([{ hash: migrations[31].hash }])
    })

    it('retries a failed backfill batch without advancing past its tenants', async () => {
      await insertTenants(1005)
      await observer.query('CREATE SEQUENCE public.enrollment_attempts')
      await installBackfillHook(`IF nextval('public.enrollment_attempts') = 1 THEN
        RAISE EXCEPTION 'retry backfill fixture' USING ERRCODE = '55P03';
      END IF;`)
      await run()
      expect(
        (await observer.query('SELECT last_value::int FROM public.enrollment_attempts')).rows
      ).toEqual([{ last_value: 2 }])
      expect(
        (await observer.query('SELECT count(*)::int FROM public.lifecycle_tenants')).rows
      ).toEqual([{ count: 1005 }])
      expect(await recorded()).toHaveLength(1)
    })

    it('skips locked disabled tenants when using the enabled-tenant index', async () => {
      await insertTenants(2)
      await observer.query(
        "UPDATE public.tenants SET feature_object_versioning = false WHERE id = 'tenant00001'"
      )
      await observer.query(`CREATE INDEX tenants_versioning_enabled_id_idx
        ON public.tenants (id) WHERE feature_object_versioning`)
      await observer.query('BEGIN')
      await observer.query("SELECT id FROM public.tenants WHERE id = 'tenant00001' FOR UPDATE")
      try {
        await run()
      } finally {
        await observer.query('ROLLBACK')
      }
      expect((await observer.query('SELECT tenant_id FROM public.lifecycle_tenants')).rows).toEqual(
        [{ tenant_id: 'tenant00002' }]
      )
      expect(await recorded()).toHaveLength(1)
    })

    it('enrolls a disabled tenant enabled behind the backfill cursor', async () => {
      await insertTenants(1005)
      await observer.query(`INSERT INTO public.tenants
        (id, anon_key, database_url, jwt_secret, service_key, feature_object_versioning)
        VALUES ('before-cursor', 'fixture', 'fixture', 'fixture', 'fixture', false)`)
      await observer.query(`CREATE INDEX tenants_versioning_enabled_id_idx
        ON public.tenants (id) WHERE feature_object_versioning`)
      await installBackfillHook('PERFORM pg_sleep(2);')
      const migration = run()
      migration.catch(() => undefined)
      try {
        await waitFor(
          observer,
          `SELECT 1 FROM pg_stat_activity
          WHERE pid = ${runnerPid} AND wait_event = 'PgSleep'`
        )
        await observer.query(
          "UPDATE public.tenants SET feature_object_versioning = true WHERE id = 'before-cursor'"
        )
        await migration
        expect(
          (
            await observer.query(
              "SELECT tenant_id FROM public.lifecycle_tenants WHERE tenant_id = 'before-cursor'"
            )
          ).rows
        ).toEqual([{ tenant_id: 'before-cursor' }])
        expect(
          (await observer.query('SELECT count(*)::int FROM public.lifecycle_tenants')).rows
        ).toEqual([{ count: 1006 }])
      } finally {
        await migration
      }
    })

    it.each([0, 1001])(
      'finishes with %s tenants when most tenants are ineligible',
      async (count) => {
        await insertTenants(count)
        await observer.query(
          "UPDATE public.tenants SET feature_object_versioning = false WHERE id < 'tenant01001'"
        )
        await run()
        expect(
          (await observer.query('SELECT count(*)::int FROM public.lifecycle_tenants')).rows
        ).toEqual([{ count: count === 0 ? 0 : 1 }])
        expect(await recorded()).toHaveLength(1)
      }
    )

    it('handles the original hash through the configured refresh policy without replaying the backfill', async () => {
      await insertTenants(1)
      await runMigration('migrations', observer)(migrations[31])
      await observer.query(
        "UPDATE public.migrations SET hash = '8b621d6dcc538a2501c2823c80cd6497025e9134' WHERE id = 31"
      )
      const before = await recorded()
      await observer.query("UPDATE public.lifecycle_tenants SET next_dispatch_at = '2099-01-01'")
      if (getConfig().dbRefreshMigrationHashesOnMismatch) {
        await run()
        expect(await recorded()).toEqual([{ ...before[0], hash: migrations[31].hash }])
      } else {
        await expect(run()).rejects.toThrow("Hashes don't match")
        expect(await recorded()).toEqual(before)
      }
      expect(
        (
          await observer.query(
            "SELECT next_dispatch_at = '2099-01-01'::timestamptz AS unchanged FROM public.lifecycle_tenants"
          )
        ).rows
      ).toEqual([{ unchanged: true }])
    })
  }
)

async function waitFor(client: Client, sql: string) {
  await expect
    .poll(async () => (await client.query(sql)).rowCount, {
      timeout: 3000,
      interval: 10,
      message: sql,
    })
    .toBeGreaterThan(0)
}
