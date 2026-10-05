import { PgPoolExecutor } from '@internal/database/pg-connection'
import { TenantConfigStorePg } from '@internal/database/tenant-store-pg'
import { Pool } from 'pg'
import { getConfig } from '../config'
import { DBMigration } from '../internal/database/migrations/types'

const knownVersions = Object.keys(DBMigration)
const target = knownVersions.at(-2)!
const statuses = [null, 'COMPLETED', 'FAILED', 'FAILED_STALE', 'PENDING'] as const
const failedStatuses = ['FAILED', 'FAILED_STALE']
const runnableStatuses = [null, 'COMPLETED', 'PENDING']
const versions = [
  { label: 'missing', version: null, selectedStatuses: runnableStatuses },
  { label: 'empty', version: '', selectedStatuses: runnableStatuses },
  { label: 'older', version: knownVersions[1], selectedStatuses: runnableStatuses },
  { label: 'target', version: target, selectedStatuses: [null] },
  { label: 'future', version: 'future-migration', selectedStatuses: [null, 'PENDING'] },
]
const fixtures = versions.flatMap(({ label, version, selectedStatuses }) =>
  statuses.map((status) => ({
    id: `${label}-${status ?? 'null'}`,
    version,
    status,
    selected: selectedStatuses.some((selectedStatus) => selectedStatus === status),
  }))
)
const expected = fixtures.flatMap(({ id, selected }, index) =>
  selected ? [{ id, cursor_id: index + 1 }] : []
)

describe('tenant migration selection', () => {
  let pool: Pool
  let store: TenantConfigStorePg
  let executor: PgPoolExecutor

  beforeAll(async () => {
    // A single session keeps this temporary table isolated from real tenant rows.
    pool = new Pool({
      connectionString: getConfig().multitenantDatabaseUrl,
      max: 1,
      idleTimeoutMillis: 0,
    })
    executor = new PgPoolExecutor(pool)
    store = new TenantConfigStorePg(executor)
    await executor.query(`
      CREATE TEMP TABLE tenants (
        id text NOT NULL,
        cursor_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        database_url text,
        migrations_version text,
        migrations_status text
      )
    `)
    await executor.query({
      text: `
        INSERT INTO tenants (id, migrations_version, migrations_status)
        SELECT * FROM unnest($1::text[], $2::text[], $3::text[])
      `,
      values: [
        fixtures.map(({ id }) => id),
        fixtures.map(({ version }) => version),
        fixtures.map(({ status }) => status),
      ],
    })
  })

  afterAll(async () => {
    await pool.end()
  })

  it('selects the expected tenants across migration names and statuses without repeating pages', async () => {
    const batchSize = 3
    let cursor = 0
    for (let offset = 0; offset < expected.length; offset += batchSize) {
      const batch = await store.listTenantsToMigrateBatch(
        target,
        cursor,
        failedStatuses,
        batchSize,
        knownVersions
      )
      expect(batch).toEqual(expected.slice(offset, offset + batchSize))
      cursor = batch.at(-1)!.cursor_id
    }
    await expect(
      store.listTenantsToMigrateBatch(target, cursor, failedStatuses, batchSize, knownVersions)
    ).resolves.toEqual([])
  })
  it.each([
    ['version', { expectedMigrationVersion: knownVersions[1] }],
    ['database URL', { expectedDatabaseUrl: 'other-url' }],
    ['status', { expectedMigrationStatus: 'FAILED' }],
  ])('marks migrations failed only while the captured %s is current', async (_, stale) => {
    const captured = {
      expectedMigrationVersion: target,
      expectedDatabaseUrl: 'url',
      expectedMigrationStatus: 'COMPLETED',
      state: 'FAILED' as const,
    }
    await executor.query({
      text: `INSERT INTO tenants (id, database_url, migrations_version, migrations_status)
             VALUES ('captured', 'url', $1, 'COMPLETED')`,
      values: [target],
    })
    try {
      await expect(store.failMigrations('captured', { ...captured, ...stale })).resolves.toBe(0)
      await expect(store.failMigrations('captured', captured)).resolves.toBe(1)
    } finally {
      await executor.query(`DELETE FROM tenants WHERE id = 'captured'`)
    }
  })
})
