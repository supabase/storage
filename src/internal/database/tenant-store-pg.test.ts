import type { PgExecutor, PgStatement } from './pg-connection'
import { TenantConfigStorePg } from './tenant-store-pg'

function createTenantStore() {
  const query = vi.fn().mockResolvedValue({
    rows: [],
    rowCount: 1,
  })
  const store = new TenantConfigStorePg({ query } as unknown as PgExecutor)

  return { query, store }
}

function getLastStatement(query: ReturnType<typeof vi.fn>): PgStatement {
  const [statement] = query.mock.calls.at(-1) || []

  if (!statement || typeof statement === 'string') {
    throw new Error('Expected a PgStatement query')
  }

  return statement
}

describe('TenantConfigStorePg', () => {
  it('quotes insert column identifiers', async () => {
    const { query, store } = createTenantStore()

    await store.insert({
      id: 'tenant-id',
      database_url: 'postgres://tenant',
      jwks: { keys: [] },
    })

    const statement = getLastStatement(query)
    expect(statement.text).toContain('INSERT INTO tenants ("id", "database_url", "jwks")')
    expect(statement.text).toContain('VALUES ($1, $2, $3)')
    expect(statement.values).toEqual(['tenant-id', 'postgres://tenant', { keys: [] }])
  })

  it('quotes upsert insert and update column identifiers', async () => {
    const { query, store } = createTenantStore()

    await store.upsert({
      id: 'tenant-id',
      database_url: 'postgres://tenant',
      max_connections: 10,
    })

    const statement = getLastStatement(query)
    expect(statement.text).toContain(
      'INSERT INTO tenants ("id", "database_url", "max_connections")'
    )
    expect(statement.text).toContain(
      'ON CONFLICT ("id") DO UPDATE SET "database_url" = EXCLUDED."database_url", "max_connections" = EXCLUDED."max_connections"'
    )
    expect(statement.values).toEqual(['tenant-id', 'postgres://tenant', 10])
  })

  it('quotes update column identifiers', async () => {
    const { query, store } = createTenantStore()

    await store.update('tenant-id', {
      id: 'ignored',
      database_url: 'postgres://tenant',
      max_connections: 10,
    })

    const statement = getLastStatement(query)
    expect(statement.text).toContain('SET "database_url" = $1, "max_connections" = $2')
    expect(statement.text).toContain('WHERE id = $3')
    expect(statement.values).toEqual(['postgres://tenant', 10, 'tenant-id'])
  })

  it('uses the caller signal without adding an internal timeout for migration listing', async () => {
    const { query, store } = createTenantStore()
    const signal = new AbortController().signal

    await store.listTenantsToMigrateBatch('storage-schema', 0, ['FAILED'], 200, signal)

    expect(query).toHaveBeenCalledWith(expect.anything(), { signal })
  })

  it('does not add an internal timeout for reset migration listing', async () => {
    const { query, store } = createTenantStore()

    await store.listTenantsToResetMigrationsBatch(['storage-schema'], 0, 200)

    expect(query).toHaveBeenCalledWith(expect.anything())
  })
})
