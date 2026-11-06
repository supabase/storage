import { describe, expect, it, vi } from 'vitest'
import { PgMetastore } from './pg'

function createMetastore(query: ReturnType<typeof vi.fn>, multiTenant = false) {
  return new PgMetastore({ query } as never, {
    multiTenant,
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

describe('PgMetastore.createNamespace', () => {
  const params = {
    name: 'namespace',
    bucketId: 'catalog-id',
    bucketName: 'catalog',
    tenantId: 'tenant-id',
    metadata: { overwrite: 'new', added: 'value' },
  }
  const namespace = {
    id: 'namespace-id',
    name: params.name,
    catalog_id: params.bucketId,
    bucket_name: params.bucketName,
    metadata: { stable: 'kept', ...params.metadata },
  }

  it('returns a successful upsert without probing the table or updating it again', async () => {
    const query = vi.fn().mockResolvedValue({ rows: [namespace] })

    await expect(createMetastore(query).createNamespace(params)).resolves.toEqual(namespace)
    expect(query).toHaveBeenCalledTimes(1)
  })

  it.each([false, true])(
    'recovers an empty OrioleDB upsert with a scoped metadata update (multitenant=%s)',
    async (multiTenant) => {
      const query = vi
        .fn()
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: [{ amname: 'orioledb', has_unsafe_trigger: false }] })
        .mockResolvedValueOnce({ rows: [namespace] })

      await expect(createMetastore(query, multiTenant).createNamespace(params)).resolves.toEqual(
        namespace
      )

      expect(query).toHaveBeenCalledTimes(3)
      expect(query.mock.calls[1][0].values).toEqual(['"storage"."iceberg_namespaces"'])
      const update = query.mock.calls[2][0]
      expect(update.text).toContain('UPDATE "storage"."iceberg_namespaces"')
      expect(update.text).toContain('metadata = metadata || $1::jsonb')
      expect(update.text).toContain('updated_at = now()')
      expect(update.text).toContain('WHERE catalog_id = $2 AND name = $3')
      expect(update.text).toContain('RETURNING *')
      expect(update.values).toEqual([
        params.metadata,
        params.bucketId,
        params.name,
        ...(multiTenant ? [params.tenantId] : []),
      ])
      expect(update.text.includes('tenant_id = $4')).toBe(multiTenant)
    }
  )

  it('does not retry an empty OrioleDB upsert when a BEFORE write trigger may have skipped it', async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ amname: 'orioledb', has_unsafe_trigger: true }] })

    await expect(createMetastore(query).createNamespace(params)).rejects.toMatchObject({
      code: 'NoSuchKey',
    })
    expect(query).toHaveBeenCalledTimes(2)
  })

  it.each([{ amname: 'heap' }, undefined])(
    'preserves NoSuchKey when the table is not known to use OrioleDB (%s)',
    async (accessMethod) => {
      const query = vi
        .fn()
        .mockResolvedValueOnce({ rows: [] })
        .mockResolvedValueOnce({ rows: accessMethod ? [accessMethod] : [] })

      await expect(createMetastore(query).createNamespace(params)).rejects.toMatchObject({
        code: 'NoSuchKey',
      })
      expect(query).toHaveBeenCalledTimes(2)
    }
  )

  it('preserves NoSuchKey when the OrioleDB fallback finds no namespace', async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce({ rows: [] })
      .mockResolvedValueOnce({ rows: [{ amname: 'orioledb', has_unsafe_trigger: false }] })
      .mockResolvedValueOnce({ rows: [] })

    await expect(createMetastore(query).createNamespace(params)).rejects.toMatchObject({
      code: 'NoSuchKey',
    })
    expect(query).toHaveBeenCalledTimes(3)
  })

  it('propagates an upsert error without running the fallback', async () => {
    const error = new Error('connection lost')
    const query = vi.fn().mockRejectedValue(error)

    await expect(createMetastore(query).createNamespace(params)).rejects.toBe(error)
    expect(query).toHaveBeenCalledTimes(1)
  })
})
