import { ERRORS, ErrorCode } from '@internal/errors'
import type { Sharder } from '@internal/sharding'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { Metastore } from '../metastore'
import { IcebergErrorType } from './errors'
import type { CatalogAuthType } from './rest-catalog-client'
import { TenantAwareRestCatalog } from './tenant-catalog'

function createCatalog(metastore: Partial<Metastore>, sharding: Partial<Sharder> = {}) {
  const auth: CatalogAuthType = {
    authorize: (req) => req,
  }

  return new TenantAwareRestCatalog({
    tenantId: 'tenant-id',
    restCatalogUrl: 'https://catalog.example.com/v1',
    metastore: metastore as unknown as Metastore,
    auth,
    sharding: sharding as Sharder,
    limits: {
      maxCatalogsCount: 10,
      maxNamespaceCount: 10,
      maxTableCount: 10,
    },
  })
}

function expectNoSuchCatalog(error: Promise<unknown>) {
  return expect(error).rejects.toMatchObject({
    code: ErrorCode.NoSuchCatalog,
    message: 'Catalog name "warehouse" not found',
  })
}

describe('TenantAwareRestCatalog exists checks', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('maps a local missing table row to NoSuchTableException', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
      findNamespaceByName: vi.fn().mockResolvedValue({ id: 'namespace-id', name: 'namespace' }),
      findTableByName: vi.fn().mockRejectedValue(ERRORS.NoSuchKey('table')),
    })

    await expect(
      catalog.tableExists({ warehouse: 'warehouse', namespace: 'namespace', table: 'table' })
    ).rejects.toMatchObject({
      code: 404,
      message: 'Table not found',
      type: IcebergErrorType.NoSuchTableException,
    })

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('keeps a missing warehouse distinct for tableExists', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockRejectedValue(ERRORS.NoSuchCatalog('warehouse')),
    })

    await expectNoSuchCatalog(
      catalog.tableExists({ warehouse: 'warehouse', namespace: 'namespace', table: 'table' })
    )

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('maps a local missing namespace row to NoSuchNamespaceException for tableExists', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
      findNamespaceByName: vi.fn().mockRejectedValue(ERRORS.NoSuchKey('namespace')),
    })

    await expect(
      catalog.tableExists({ warehouse: 'warehouse', namespace: 'namespace', table: 'table' })
    ).rejects.toMatchObject({
      code: 404,
      message: 'Namespace not found',
      type: IcebergErrorType.NoSuchNamespaceException,
    })

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('keeps a missing warehouse distinct for namespaceExists', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockRejectedValue(ERRORS.NoSuchCatalog('warehouse')),
    })

    await expectNoSuchCatalog(
      catalog.namespaceExists({ warehouse: 'warehouse', namespace: 'namespace' })
    )

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('forwards tableExists to the upstream catalog with shard_key as warehouse and tenant-prefixed namespace', async () => {
    const fetchMock = vi.fn<typeof fetch>(async () => new Response(null, { status: 204 }))
    vi.stubGlobal('fetch', fetchMock)

    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
      findNamespaceByName: vi.fn().mockResolvedValue({ id: 'abc-def-ghi', name: 'namespace' }),
      findTableByName: vi
        .fn()
        .mockResolvedValue({ id: 'table-id', name: 'table', shard_key: 'shard-1' }),
    })

    await expect(
      catalog.tableExists({ warehouse: 'warehouse', namespace: 'namespace', table: 'table' })
    ).resolves.toBeUndefined()

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [fetchedInput, init] = fetchMock.mock.calls[0]
    const url = new URL(String(fetchedInput))
    expect(init?.method).toBe('HEAD')
    expect(url.pathname).toBe('/v1/shard-1/namespaces/tenant-id_abc_def_ghi/tables/table')
  })

  it('throws ShardNotFound when the local table row has no shard_key', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
      findNamespaceByName: vi.fn().mockResolvedValue({ id: 'namespace-id', name: 'namespace' }),
      findTableByName: vi.fn().mockResolvedValue({ id: 'table-id', name: 'table' }),
    })

    await expect(
      catalog.tableExists({ warehouse: 'warehouse', namespace: 'namespace', table: 'table' })
    ).rejects.toMatchObject({
      code: ErrorCode.ShardNotFound,
    })

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('resolves namespaceExists from the local metastore without an HTTP call', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const findNamespaceByName = vi.fn().mockResolvedValue({ id: 'namespace-id', name: 'namespace' })
    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
      findNamespaceByName,
    })

    await expect(
      catalog.namespaceExists({ warehouse: 'warehouse', namespace: 'namespace' })
    ).resolves.toBeUndefined()

    expect(findNamespaceByName).toHaveBeenCalledWith({
      tenantId: 'tenant-id',
      name: 'namespace',
      catalogId: 'catalog-id',
    })
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('maps a local missing namespace row to NoSuchNamespaceException', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
      findNamespaceByName: vi.fn().mockRejectedValue(ERRORS.NoSuchKey('namespace')),
    })

    await expect(
      catalog.namespaceExists({ warehouse: 'warehouse', namespace: 'namespace' })
    ).rejects.toMatchObject({
      code: 404,
      message: 'Namespace not found',
      type: IcebergErrorType.NoSuchNamespaceException,
    })

    expect(fetchMock).not.toHaveBeenCalled()
  })
})

describe('TenantAwareRestCatalog metadata loads', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('keeps a missing warehouse distinct for loadTable', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockRejectedValue(ERRORS.NoSuchCatalog('warehouse')),
    })

    await expectNoSuchCatalog(
      catalog.loadTable({ warehouse: 'warehouse', namespace: 'namespace', table: 'table' })
    )

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('maps a local missing namespace row to NoSuchNamespaceException for loadTable', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
      findNamespaceByName: vi.fn().mockRejectedValue(ERRORS.NoSuchKey('namespace')),
    })

    await expect(
      catalog.loadTable({ warehouse: 'warehouse', namespace: 'namespace', table: 'table' })
    ).rejects.toMatchObject({
      code: 404,
      message: 'Namespace not found',
      type: IcebergErrorType.NoSuchNamespaceException,
    })

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('maps a local missing table row to NoSuchTableException for loadTable', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
      findNamespaceByName: vi.fn().mockResolvedValue({ id: 'namespace-id', name: 'namespace' }),
      findTableByName: vi.fn().mockRejectedValue(ERRORS.NoSuchKey('table')),
    })

    await expect(
      catalog.loadTable({ warehouse: 'warehouse', namespace: 'namespace', table: 'table' })
    ).rejects.toMatchObject({
      code: 404,
      message: 'Table not found',
      type: IcebergErrorType.NoSuchTableException,
    })

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('throws ShardNotFound when the loadTable row has no shard_key', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
      findNamespaceByName: vi.fn().mockResolvedValue({ id: 'namespace-id', name: 'namespace' }),
      findTableByName: vi.fn().mockResolvedValue({ id: 'table-id', name: 'table' }),
    })

    await expect(
      catalog.loadTable({ warehouse: 'warehouse', namespace: 'namespace', table: 'table' })
    ).rejects.toMatchObject({
      code: ErrorCode.ShardNotFound,
    })

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('keeps a missing warehouse distinct for loadNamespaceMetadata', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockRejectedValue(ERRORS.NoSuchCatalog('warehouse')),
    })

    await expectNoSuchCatalog(
      catalog.loadNamespaceMetadata({ warehouse: 'warehouse', namespace: 'namespace' })
    )

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('maps a local missing namespace row to NoSuchNamespaceException for loadNamespaceMetadata', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
      findNamespaceByName: vi.fn().mockRejectedValue(ERRORS.NoSuchKey('namespace')),
    })

    await expect(
      catalog.loadNamespaceMetadata({ warehouse: 'warehouse', namespace: 'namespace' })
    ).rejects.toMatchObject({
      code: 404,
      message: 'Namespace not found',
      type: IcebergErrorType.NoSuchNamespaceException,
    })

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('maps a local missing namespace row to NoSuchNamespaceException for listTables', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    const listTables = vi.fn()

    const catalog = createCatalog({
      findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
      findNamespaceByName: vi.fn().mockRejectedValue(ERRORS.NoSuchKey('namespace')),
      listTables,
    })

    await expect(
      catalog.listTables({ warehouse: 'warehouse', namespace: 'namespace' })
    ).rejects.toMatchObject({
      code: 404,
      message: 'Namespace not found',
      type: IcebergErrorType.NoSuchNamespaceException,
    })

    expect(listTables).not.toHaveBeenCalled()
    expect(fetchMock).not.toHaveBeenCalled()
  })
})

describe('TenantAwareRestCatalog resource mutations', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('frees the same shard resource it allocated when the catalog ID differs from its name', async () => {
    const fetchMock = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(Response.json({ namespace: ['tenant-id_namespace_id'] }))
      .mockResolvedValueOnce(
        Response.json({
          metadata: { location: 's3://shard-1/table', 'table-uuid': 'remote-table-id' },
        })
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(Response.json({ identifiers: [] }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
    vi.stubGlobal('fetch', fetchMock)

    const store = {
      findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
      findNamespaceByName: vi.fn().mockResolvedValue({ id: 'namespace-id', name: 'namespace' }),
      findTableByName: vi
        .fn()
        .mockRejectedValueOnce(ERRORS.NoSuchKey('table'))
        .mockResolvedValue({ name: 'table', shard_id: '1', shard_key: 'shard-1' }),
      lockResource: vi.fn().mockResolvedValue(undefined),
      countTables: vi.fn().mockResolvedValue(0),
      getTnx: vi.fn().mockReturnValue({}),
      createTable: vi.fn().mockResolvedValue(undefined),
      dropTable: vi.fn().mockResolvedValue(undefined),
    }
    const withTnx = vi.fn()
    const sharding = {
      withTnx,
      reserve: vi.fn<Sharder['reserve']>().mockResolvedValue({
        reservationId: 'reservation-id',
        shardId: '1',
        shardKey: 'shard-1',
        slotNo: 0,
        leaseExpiresAt: '2099-01-01T00:00:00Z',
      }),
      confirm: vi.fn<Sharder['confirm']>().mockResolvedValue(undefined),
      freeByResource: vi.fn<Sharder['freeByResource']>().mockResolvedValue(undefined),
    }
    withTnx.mockReturnValue(sharding)
    const catalog = createCatalog(
      { ...store, transaction: vi.fn(async (callback) => callback(store)) },
      sharding
    )

    await catalog.createTable({
      warehouse: 'warehouse',
      namespace: 'namespace',
      name: 'table',
      schema: { type: 'struct', fields: [] },
      spec: { fields: [] },
    })
    await catalog.dropTable({
      warehouse: 'warehouse',
      namespace: 'namespace',
      table: 'table',
      purgeRequested: true,
    })

    const resource = {
      tenantId: 'tenant-id',
      kind: 'iceberg-table',
      bucketName: 'catalog-id',
      logicalName: 'namespace-id/table',
    }
    expect(sharding.reserve).toHaveBeenCalledExactlyOnceWith(resource)
    expect(sharding.confirm).toHaveBeenCalledExactlyOnceWith('reservation-id', resource)
    expect(sharding.freeByResource).toHaveBeenCalledExactlyOnceWith('1', resource)
  })

  it('maps a local missing namespace row to NoSuchNamespaceException for createTable', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const store = {
      findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
      findNamespaceByName: vi.fn().mockRejectedValue(ERRORS.NoSuchKey('namespace')),
    }
    const transaction = vi.fn(async (callback) => callback(store))
    const catalog = createCatalog({ transaction })

    await expect(
      catalog.createTable({
        warehouse: 'warehouse',
        namespace: 'namespace',
        name: 'table',
        schema: { type: 'struct', fields: [] },
        spec: { fields: [] },
      })
    ).rejects.toMatchObject({
      code: 404,
      message: 'Namespace not found',
      type: IcebergErrorType.NoSuchNamespaceException,
    })

    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('maps a local duplicate table row to AlreadyExistsException for createTable', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    const store = {
      findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
      findNamespaceByName: vi.fn().mockResolvedValue({ id: 'namespace-id', name: 'namespace' }),
      findTableByName: vi.fn().mockResolvedValue({ id: 'table-id', name: 'table' }),
    }
    const transaction = vi.fn(async (callback) => callback(store))
    const catalog = createCatalog({ transaction })

    await expect(
      catalog.createTable({
        warehouse: 'warehouse',
        namespace: 'namespace',
        name: 'table',
        schema: { type: 'struct', fields: [] },
        spec: { fields: [] },
      })
    ).rejects.toMatchObject({
      code: 409,
      message: 'Table already exists',
      type: IcebergErrorType.AlreadyExistsException,
    })

    expect(fetchMock).not.toHaveBeenCalled()
  })
})
