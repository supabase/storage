import { randomUUID } from 'node:crypto'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { migrate } from '@internal/database/migrations/migrate'
import { Client } from 'pg'
import { getConfig } from '../config'

// Real PostgreSQL connections exercise migration serialization, cancellation,
// failure rollback and lock ownership. Run with the normal integration config.
// Multigres cannot create disposable databases, as in the other migration fixtures.
describe.skipIf(getConfig().databaseEngine === 'multigres')(
  'migration advisory lock lifecycle',
  () => {
    const key = '-8525285245963000605'
    const databaseName = `migration_lock_${randomUUID().replaceAll('-', '')}`
    const url = new URL(getConfig().databaseURL)
    url.pathname = `/${databaseName}`
    const admin = new Client({ connectionString: getConfig().databaseURL })
    let holder: Client
    let runner: Client
    let directory: string

    beforeAll(() => admin.connect())
    beforeEach(async () => {
      await admin.query(`CREATE DATABASE ${databaseName}`)
      directory = await mkdtemp(join(tmpdir(), 'storage-migration-lock-'))
      await writeFile(join(directory, '0001-probe.sql'), 'CREATE TABLE lock_probe(v int);')
      holder = new Client({ connectionString: url.toString() })
      runner = new Client({ connectionString: url.toString() })
      await Promise.all([holder.connect(), runner.connect()])
      await runner.query("SET statement_timeout = '15s'")
    })
    afterEach(async () => {
      await Promise.all([holder.end(), runner.end()])
      await admin.query(`DROP DATABASE ${databaseName} WITH (FORCE)`)
      await rm(directory, { recursive: true, force: true })
    })
    afterAll(() => admin.end())

    const run = (waitForLock: boolean) =>
      migrate({
        client: runner,
        migrationsDirectory: directory,
        migrationsTableSchema: 'public',
        waitForLock,
      })

    it('waits for the preceding migration beyond three seconds', async () => {
      await holder.query(`SELECT pg_advisory_lock(${key})`)
      let finished = false
      const outcome = run(true).then(
        () => {
          finished = true
          return { success: true }
        },
        (error: unknown) => {
          finished = true
          return { success: false, error }
        }
      )
      try {
        await new Promise((resolve) => setTimeout(resolve, 4200))
        expect(finished).toBe(false)
      } finally {
        await holder.query(`SELECT pg_advisory_unlock(${key})`)
        await outcome
      }
      expect(await outcome).toEqual({ success: true })
      expect((await runner.query('SELECT id FROM migrations ORDER BY id')).rows).toEqual([
        { id: 0 },
        { id: 1 },
      ])
    }, 20000)

    it('retains fail-fast behavior when waiting is disabled', async () => {
      await holder.query(`SELECT pg_advisory_lock(${key})`)
      await expect(run(false)).rejects.toMatchObject({ code: 'LockTimeout' })
      expect(
        (await runner.query(`SELECT pg_try_advisory_lock(${key}) AS acquired`)).rows[0].acquired
      ).toBe(false)
    })

    it('uses the database statement deadline while waiting and leaves no lock', async () => {
      await holder.query(`SELECT pg_advisory_lock(${key})`)
      await runner.query("SET statement_timeout = '200ms'")
      await expect(run(true)).rejects.toMatchObject({ code: '57014' })
      await holder.query(`SELECT pg_advisory_unlock(${key})`)
      expect(
        (await holder.query(`SELECT pg_try_advisory_lock(${key}) AS acquired`)).rows[0].acquired
      ).toBe(true)
    })

    it('rolls back failed migrations and releases its acquired lock', async () => {
      await writeFile(
        join(directory, '0002-fail.sql'),
        'CREATE TABLE failed_probe(v int); SELECT 1/0;'
      )
      await expect(run(true)).rejects.toBeDefined()
      expect(
        (await holder.query(`SELECT pg_try_advisory_lock(${key}) AS acquired`)).rows[0].acquired
      ).toBe(true)
      expect(
        (await runner.query("SELECT to_regclass('public.failed_probe') AS relation")).rows[0]
          .relation
      ).toBeNull()
      expect((await runner.query('SELECT id FROM migrations ORDER BY id')).rows).toEqual([
        { id: 0 },
        { id: 1 },
      ])
    })
  }
)
