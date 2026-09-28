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
  { label: 'missing', version: null, selectedStatuses: [null] },
  { label: 'empty', version: '', selectedStatuses: runnableStatuses },
  { label: 'older', version: knownVersions[1], selectedStatuses: runnableStatuses },
  { label: 'target', version: target, selectedStatuses: [null] },
  { label: 'newer-known', version: knownVersions.at(-1)!, selectedStatuses: runnableStatuses },
  { label: 'future', version: 'future-migration', selectedStatuses: [] },
  { label: 'prototype', version: 'constructor', selectedStatuses: [] },
  { label: 'case-change', version: target.toUpperCase(), selectedStatuses: [] },
  { label: 'whitespace', version: `${target} `, selectedStatuses: [] },
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

  beforeAll(async () => {
    // A single session keeps this temporary table isolated from real tenant rows.
    pool = new Pool({
      connectionString: getConfig().multitenantDatabaseUrl,
      max: 1,
      idleTimeoutMillis: 0,
    })
    const executor = new PgPoolExecutor(pool)
    store = new TenantConfigStorePg(executor)
    await executor.query(`
      CREATE TEMP TABLE tenants (
        id text NOT NULL,
        cursor_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
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

  it('selects the expected tenants across migration names and statuses', async () => {
    await expect(
      store.listTenantsToMigrateBatch(target, 0, failedStatuses, 200, knownVersions)
    ).resolves.toEqual(expected)
  })

  it('paginates by the last selected cursor without duplicates or skipped tenants', async () => {
    const first = await store.listTenantsToMigrateBatch(target, 0, failedStatuses, 3, knownVersions)
    expect(first).toEqual(expected.slice(0, 3))

    const second = await store.listTenantsToMigrateBatch(
      target,
      first.at(-1)!.cursor_id,
      failedStatuses,
      3,
      knownVersions
    )
    expect(second).toEqual(expected.slice(3, 6))

    const rest = await store.listTenantsToMigrateBatch(
      target,
      second.at(-1)!.cursor_id,
      failedStatuses,
      200,
      knownVersions
    )
    expect([...first, ...second, ...rest]).toEqual(expected)
    await expect(
      store.listTenantsToMigrateBatch(
        target,
        rest.at(-1)!.cursor_id,
        failedStatuses,
        3,
        knownVersions
      )
    ).resolves.toEqual([])
  })
})
