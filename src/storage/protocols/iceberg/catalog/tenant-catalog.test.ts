import { ERRORS, ErrorCode } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import type { Sharder } from '@internal/sharding'
import { describe, expect, it, vi } from 'vitest'
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

  describe('createTable compensation', () => {
    function setup(
      deleteResponse: () => Response | Promise<Response> = () => new Response(null, { status: 204 })
    ) {
      const fetchMock = vi
        .fn<typeof fetch>()
        .mockResolvedValueOnce(Response.json({ namespace: ['tenant-id_namespace_id'] }))
        .mockResolvedValueOnce(
          Response.json({
            metadata: { location: 's3://shard-1/table', 'table-uuid': 'remote-table-id' },
          })
        )
        .mockImplementation(async () => deleteResponse())
      vi.stubGlobal('fetch', fetchMock)

      const store = {
        findCatalogByName: vi.fn().mockResolvedValue({ id: 'catalog-id', name: 'warehouse' }),
        findNamespaceByName: vi.fn().mockResolvedValue({ id: 'namespace-id', name: 'namespace' }),
        findTableByName: vi.fn().mockRejectedValue(ERRORS.NoSuchKey('table')),
        lockResource: vi.fn().mockResolvedValue(undefined),
        countTables: vi.fn().mockResolvedValue(0),
        getTnx: vi.fn().mockReturnValue({}),
        createTable: vi.fn().mockResolvedValue(undefined),
      }
      const sharding = {
        withTnx: vi.fn(),
        reserve: vi.fn<Sharder['reserve']>().mockResolvedValue({
          reservationId: 'reservation-id',
          shardId: '1',
          shardKey: 'shard-1',
          slotNo: 0,
          leaseExpiresAt: '2099-01-01T00:00:00Z',
        }),
        confirm: vi.fn<Sharder['confirm']>().mockResolvedValue(undefined),
      }
      sharding.withTnx.mockReturnValue(sharding)

      const transaction = vi.fn(async (callback) => callback(store))
      const catalog = createCatalog({ ...store, transaction }, sharding)
      return { fetchMock, store, sharding, transaction, catalog }
    }

    const request = {
      warehouse: 'warehouse',
      namespace: 'namespace',
      name: 'table',
      schema: { type: 'struct' as const, fields: [] },
      spec: { fields: [] },
    }

    it.each(['store.createTable', 'sharding.confirm'])(
      'rolls back before a slow compensating drop when %s fails',
      async (failingStep) => {
        const dropStarted = Promise.withResolvers<void>()
        const dropResponse = Promise.withResolvers<Response>()
        const steps: string[] = []
        const { fetchMock, store, sharding, transaction, catalog } = setup(() => {
          steps.push('drop')
          dropStarted.resolve()
          return dropResponse.promise
        })
        const error = new Error('local failure')
        if (failingStep === 'store.createTable') {
          store.createTable.mockRejectedValue(error)
        } else {
          sharding.confirm.mockRejectedValue(error)
        }
        transaction.mockImplementation(async (callback) => {
          try {
            return await callback(store)
          } finally {
            steps.push('rollback')
          }
        })
        const result = catalog.createTable(request).catch((e) => e)

        await dropStarted.promise
        try {
          expect(steps).toEqual(['rollback', 'drop'])
          expect(fetchMock).toHaveBeenCalledTimes(3)
          const [url, init] = fetchMock.mock.calls[2]
          expect(init?.method).toBe('DELETE')
          const dropUrl = new URL(String(url))
          expect(dropUrl.pathname).toBe(
            '/v1/shard-1/namespaces/tenant-id_namespace_id/tables/table'
          )
          expect(dropUrl.searchParams.get('purgeRequested')).toBe('true')
        } finally {
          dropResponse.resolve(new Response(null, { status: 204 }))
          expect(await result).toBe(error)
        }
      }
    )

    it.each([
      { status: 500, body: null },
      { status: 404, body: null },
      {
        status: 404,
        body: {
          error: { code: 404, type: 'NoSuchWarehouseException', message: 'warehouse missing' },
        },
      },
      {
        status: 500,
        body: { error: { code: 404, type: 'NoSuchTableException', message: 'status mismatch' } },
      },
      {
        status: 500,
        body: { error: { code: 500, type: 'NoSuchTableException', message: 'server failure' } },
      },
    ])('logs the failed drop context for HTTP $status and $body', async ({ status, body }) => {
      const { fetchMock, store, catalog } = setup(() =>
        body ? Response.json(body, { status }) : new Response(null, { status })
      )
      const error = new Error('local failure')
      store.createTable.mockRejectedValue(error)
      const logError = vi.spyOn(logSchema, 'error').mockImplementation(() => undefined)

      await expect(catalog.createTable(request)).rejects.toBe(error)

      expect(fetchMock).toHaveBeenCalledTimes(3)
      expect(logError).toHaveBeenCalledExactlyOnceWith(
        logger,
        '[Iceberg] Failed to drop remote table after local create failed',
        {
          type: 'iceberg',
          project: 'tenant-id',
          error: expect.any(Error),
          metadata: JSON.stringify({
            warehouse: 'shard-1',
            namespace: 'tenant-id_namespace_id',
            table: 'table',
          }),
        }
      )
    })

    it.each([IcebergErrorType.NoSuchTableException, IcebergErrorType.NoSuchNamespaceException])(
      'does not log a compensating drop when the remote resource is already gone (%s)',
      async (type) => {
        const { fetchMock, store, catalog } = setup(() =>
          Response.json({ error: { code: 404, type, message: 'already gone' } }, { status: 404 })
        )
        const error = new Error('local failure')
        store.createTable.mockRejectedValue(error)
        const logError = vi.spyOn(logSchema, 'error').mockImplementation(() => undefined)

        await expect(catalog.createTable(request)).rejects.toBe(error)

        expect(fetchMock).toHaveBeenCalledTimes(3)
        expect(logError).not.toHaveBeenCalled()
      }
    )

    it('does not compensate when the commit fails after the callback succeeds', async () => {
      const { fetchMock, store, catalog, transaction } = setup()
      const commitError = new Error('commit failure')
      transaction.mockImplementation(async (callback) => {
        await callback(store)
        throw commitError
      })

      await expect(catalog.createTable(request)).rejects.toBe(commitError)

      expect(fetchMock).toHaveBeenCalledTimes(2)
    })

    it('rejects a staged table committed while waiting for the namespace lock', async () => {
      const { fetchMock, store, sharding, catalog } = setup()
      let stagedTableCommitted = false
      store.findTableByName.mockImplementation(async () => {
        if (!stagedTableCommitted) throw ERRORS.NoSuchKey('table')
        return { name: 'table', location: 's3://staged/table', remote_table_id: 'staged-id' }
      })
      store.lockResource.mockImplementation(async () => {
        // The staged writer commits before this waiter acquires its namespace lock.
        stagedTableCommitted = true
      })

      await expect(catalog.createTable(request)).rejects.toMatchObject({
        code: 409,
        type: IcebergErrorType.AlreadyExistsException,
      })

      expect(store.createTable).not.toHaveBeenCalled()
      expect(sharding.reserve).not.toHaveBeenCalled()
      expect(fetchMock).not.toHaveBeenCalled()
    })

    it('does not compensate a staged create', async () => {
      const { fetchMock, store, catalog } = setup()
      const error = new Error('local failure')
      store.createTable.mockRejectedValue(error)

      await expect(catalog.createTable({ ...request, 'stage-create': true })).rejects.toBe(error)

      expect(fetchMock).toHaveBeenCalledTimes(2)
      expect(JSON.parse(fetchMock.mock.calls[1][1]?.body as string)).toMatchObject({
        'stage-create': true,
      })
    })
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
      lockResource: vi.fn().mockResolvedValue(undefined),
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
