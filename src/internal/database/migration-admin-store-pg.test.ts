import { MigrationAdminStorePg } from './migration-admin-store-pg'
import type { PgExecutor, PgStatement } from './pg-connection'

function createMigrationAdminStore() {
  const query = vi.fn().mockResolvedValue({
    rows: [],
    rowCount: 1,
  })
  const store = new MigrationAdminStorePg({ query } as unknown as PgExecutor, 'pgboss')

  return { query, store }
}

function getLastStatement(query: ReturnType<typeof vi.fn>): PgStatement {
  const [statement] = query.mock.calls.at(-1) || []

  if (!statement || typeof statement === 'string') {
    throw new Error('Expected a PgStatement query')
  }

  return statement
}

describe('MigrationAdminStorePg', () => {
  it('does not add an internal timeout to admin pg-boss queries', async () => {
    const { query, store } = createMigrationAdminStore()

    await store.listActiveJobs('migrations', 2000)

    expect(query).toHaveBeenCalledWith(expect.anything())
  })

  it('completes all active jobs instead of a limited page', async () => {
    const { query, store } = createMigrationAdminStore()

    await store.completeActiveJobs('migrations')

    const statement = getLastStatement(query)
    expect(statement.text).not.toMatch(/\bLIMIT\b/i)
    expect(statement.values).toEqual(['migrations'])
  })

  it('deletes all tenant jobs instead of a limited page', async () => {
    const { query, store } = createMigrationAdminStore()

    await store.deleteTenantJobs('tenant-id', 'migrations')

    const statement = getLastStatement(query)
    expect(statement.text).not.toMatch(/\bLIMIT\b/i)
    expect(statement.values).toEqual(['tenant-id', 'migrations'])
  })
})
