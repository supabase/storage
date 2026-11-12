import { describe, expect, it, vi } from 'vitest'
import { PgMetastore } from './pg'

function createMetastore(query: ReturnType<typeof vi.fn>) {
  return new PgMetastore({ query } as never, {
    multiTenant: false,
    schema: 'storage',
  })
}

function getLastStatement(query: ReturnType<typeof vi.fn>): string {
  const [statement] = query.mock.calls.at(-1) || []

  if (!statement) {
    throw new Error('No query calls found')
  }

  if (typeof statement === 'string') {
    return statement
  }

  if (typeof statement === 'object' && 'text' in statement) {
    return String(statement.text)
  }

  throw new Error('Expected a DatabaseStatement query with text property')
}

describe('PgMetastore.countCatalogs', () => {
  it('excludes soft-deleted catalogs by default', async () => {
    const query = vi.fn().mockResolvedValue({ rows: [{ count: 5 }] })
    const metastore = createMetastore(query)

    await metastore.countCatalogs({
      tenantId: 'tenant-id',
      limit: 100,
    })

    const statement = getLastStatement(query)
    expect(statement).toContain('deleted_at IS NULL')
  })

  it('includes soft-deleted catalogs when deleted parameter is true', async () => {
    const query = vi.fn().mockResolvedValue({ rows: [{ count: 10 }] })
    const metastore = createMetastore(query)

    await metastore.countCatalogs({
      tenantId: 'tenant-id',
      limit: 100,
      deleted: true,
    })

    const statement = getLastStatement(query)
    expect(statement).not.toContain('deleted_at IS NULL')
  })
})

describe('PgMetastore.listTables', () => {
  it('applies a stable ordered limit and offset for a continuation page', async () => {
    const query = vi.fn().mockResolvedValue({ rows: [] })
    const metastore = createMetastore(query)

    await metastore.listTables({
      tenantId: 'tenant-id',
      namespaceId: 'namespace-id',
      pageSize: 3,
      offset: 2,
    })

    const statement = getLastStatement(query)
    expect(statement).toContain('ORDER BY created_at ASC, id ASC')
    expect(statement).toContain('LIMIT $2')
    expect(statement).toContain('OFFSET $3')
    expect(query.mock.calls.at(-1)?.[0].values).toEqual(['namespace-id', 3, 2])
  })
})
