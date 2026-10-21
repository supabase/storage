import { vi } from 'vitest'

const {
  MockPgMetastore,
  MockRestCatalogClient,
  MockShardCatalog,
  MockPgShardStoreFactory,
  mockConfig,
  mockCreateStorage,
  mockMultitenantPgExecutor,
} = vi.hoisted(() => ({
  MockPgMetastore: vi.fn(),
  MockRestCatalogClient: vi.fn(),
  MockShardCatalog: vi.fn(),
  MockPgShardStoreFactory: vi.fn(),
  mockConfig: {
    icebergCatalogUrl: 'http://catalog',
    icebergCatalogAuthType: 'none',
    isMultitenant: true,
  },
  mockCreateStorage: vi.fn(),
  mockMultitenantPgExecutor: 'mock-multitenant-executor',
}))

vi.mock('../../../config', () => ({
  getConfig: () => mockConfig,
}))

vi.mock('@internal/database', () => ({
  multitenantPgExecutor: mockMultitenantPgExecutor,
}))

vi.mock('@storage/protocols/iceberg/pg', () => ({
  PgMetastore: MockPgMetastore,
}))

vi.mock('@storage/protocols/iceberg/catalog', () => ({
  RestCatalogClient: MockRestCatalogClient,
  getCatalogAuthStrategy: vi.fn().mockReturnValue('mock-auth'),
}))

vi.mock('@internal/sharding', () => ({
  ShardCatalog: MockShardCatalog,
  PgShardStoreFactory: MockPgShardStoreFactory,
}))

vi.mock('@internal/monitoring', () => ({
  logger: { error: vi.fn() },
}))

vi.mock('../base-event', () => ({
  BaseEvent: class {
    static createStorage = mockCreateStorage
    static getQueueName(this: { queueName: string }) {
      return this.queueName
    }
  },
}))

async function importHandler(isMultitenant: boolean) {
  mockConfig.isMultitenant = isMultitenant
  vi.resetModules()
  return (await import('./delete-iceberg-resources')).DeleteIcebergResources
}

const jobData = {
  catalogId: 'catalog-123',
  tenant: {
    ref: 'tenant-a',
    host: '',
  },
  sbReqId: 'sb-req-123',
}

const remoteNamespace = `${jobData.tenant.ref}_${'ns-1'.replaceAll('-', '_')}`

const metastore = { transaction: vi.fn() }

const store = {
  lockResource: vi.fn(),
  findCatalogById: vi.fn(),
  listNamespaces: vi.fn(),
  listTables: vi.fn(),
  dropTable: vi.fn(),
  dropNamespace: vi.fn(),
  dropCatalog: vi.fn(),
  getTnx: vi.fn(),
}

const restCatalog = {
  dropTable: vi.fn(),
  listNamespaces: vi.fn(),
  listTables: vi.fn(),
  dropNamespace: vi.fn(),
}

const shardCatalog = { withTnx: vi.fn() }
const sharder = { freeByResource: vi.fn() }

const db = {
  connection: {
    query: vi.fn(),
  },
  deleteAnalyticsBucket: vi.fn(),
  destroyConnection: vi.fn(),
}

function expectIcebergCleanup({ multitenant }: { multitenant: boolean }) {
  expect(mockCreateStorage).toHaveBeenCalledWith(jobData)
  expect(MockRestCatalogClient).toHaveBeenCalledWith({
    catalogUrl: 'http://catalog',
    auth: 'mock-auth',
    requireStructuredErrors: true,
  })
  expect(MockPgMetastore).toHaveBeenCalledWith(
    multitenant ? mockMultitenantPgExecutor : db.connection,
    {
      multiTenant: multitenant,
      schema: multitenant ? 'public' : 'storage',
    }
  )
  expect(metastore.transaction).toHaveBeenCalled()
  expect(store.lockResource).toHaveBeenCalledWith('catalog', 'catalog-123')
  expect(store.findCatalogById).toHaveBeenCalledWith({
    id: 'catalog-123',
    deleted: true,
    tenantId: 'tenant-a',
  })
  expect(store.listNamespaces).toHaveBeenCalledWith({
    catalogId: 'catalog-123',
    tenantId: 'tenant-a',
  })
  expect(store.listTables).toHaveBeenCalledWith({
    namespaceId: 'ns-1',
    pageSize: undefined,
    tenantId: 'tenant-a',
  })
  expect(restCatalog.dropTable).toHaveBeenCalledWith({
    namespace: remoteNamespace,
    table: 'table-1',
    purgeRequested: true,
    warehouse: 'shard-key-1',
  })
  expect(restCatalog.listNamespaces).toHaveBeenCalledWith({
    warehouse: 'shard-key-1',
    pageSize: 1,
  })
  expect(store.dropTable).toHaveBeenCalledWith({
    name: 'table-1',
    namespaceId: 'ns-1',
    catalogId: 'catalog-123',
    tenantId: 'tenant-a',
  })
  expect(restCatalog.listTables).toHaveBeenCalledWith({
    namespace: remoteNamespace,
    warehouse: 'shard-key-1',
    pageSize: 1000,
  })
  expect(restCatalog.dropNamespace).toHaveBeenCalledWith({
    namespace: remoteNamespace,
    warehouse: 'shard-key-1',
  })
  expect(store.dropNamespace).toHaveBeenCalledWith({
    namespace: 'namespace-1',
    catalogId: 'catalog-123',
    tenantId: 'tenant-a',
  })
  expect(store.dropCatalog).toHaveBeenCalledWith({
    bucketId: 'catalog-123',
    tenantId: 'tenant-a',
    soft: false,
  })

  if (multitenant) {
    expect(MockPgShardStoreFactory).toHaveBeenCalledWith(mockMultitenantPgExecutor)
    expect(MockShardCatalog).toHaveBeenCalled()
    expect(shardCatalog.withTnx).toHaveBeenCalledWith('mock-transaction')
    expect(sharder.freeByResource).toHaveBeenCalledWith('shard-id-1', {
      kind: 'iceberg-table',
      tenantId: 'tenant-a',
      bucketName: 'catalog-123',
      logicalName: 'ns-1/table-1',
    })
  } else {
    expect(MockShardCatalog).not.toHaveBeenCalled()
  }
}

describe('DeleteIcebergResources.handle', () => {
  let DeleteIcebergResources: Awaited<ReturnType<typeof importHandler>>

  const makeJob = () => ({
    id: 'job-1',
    name: DeleteIcebergResources.getQueueName(),
    data: jobData,
  })

  beforeEach(() => {
    MockPgMetastore.mockImplementation(function () {
      return metastore
    })
    MockRestCatalogClient.mockImplementation(function () {
      return restCatalog
    })
    MockShardCatalog.mockImplementation(function () {
      return shardCatalog
    })

    metastore.transaction.mockImplementation(async (fn) => fn(store))
    store.getTnx.mockReturnValue('mock-transaction')
    shardCatalog.withTnx.mockReturnValue(sharder)
    store.findCatalogById.mockResolvedValue({ id: 'catalog-123', deleted_at: new Date() })
    store.listNamespaces.mockResolvedValue([{ id: 'ns-1', name: 'namespace-1' }])
    store.listTables.mockResolvedValue([
      { name: 'table-1', shard_key: 'shard-key-1', shard_id: 'shard-id-1' },
    ])
    store.dropTable.mockResolvedValue(undefined)
    store.dropNamespace.mockResolvedValue(undefined)
    restCatalog.dropTable.mockResolvedValue(undefined)
    restCatalog.listNamespaces.mockResolvedValue({ namespaces: [] })
    restCatalog.listTables.mockResolvedValue({ identifiers: [] })
    restCatalog.dropNamespace.mockResolvedValue(undefined)
    db.deleteAnalyticsBucket.mockResolvedValue(undefined)
    db.destroyConnection.mockReturnValue(undefined)
  })

  describe('multitenant', () => {
    let notFound: (type?: string) => Error

    beforeAll(async () => {
      DeleteIcebergResources = await importHandler(true)
      const { IcebergError } = await import('@storage/protocols/iceberg/catalog/errors')
      notFound = (type = 'NoSuchNamespaceException') =>
        IcebergError.fromResponse({ error: { message: 'missing', type, code: 404 } })
    })

    function fakeUpstream(upstream: Map<string, Set<string>>) {
      restCatalog.dropTable.mockImplementation(
        async ({ namespace, table, warehouse, purgeRequested }) => {
          if (namespace !== remoteNamespace || !purgeRequested) throw notFound()
          if (!upstream.get(warehouse)?.delete(table)) throw notFound()
        }
      )
      restCatalog.listTables.mockImplementation(async ({ namespace, warehouse, pageToken }) => {
        const tables = upstream.get(warehouse)
        if (namespace !== remoteNamespace || !tables) throw notFound()
        const i = Number(pageToken ?? 0)
        return {
          identifiers: [...tables]
            .slice(i, i + 1)
            .map((name) => ({ namespace: [namespace], name })),
          'next-page-token': i + 1 < tables.size ? String(i + 1) : undefined,
        }
      })
      restCatalog.dropNamespace.mockImplementation(async ({ namespace, warehouse }) => {
        if (upstream.get(warehouse)?.size) throw new Error('namespace not empty')
        if (namespace !== remoteNamespace || !upstream.delete(warehouse)) throw notFound()
      })
    }

    it('should remove all resources and multitenant db rows when createStorage fails', async () => {
      mockCreateStorage.mockRejectedValue(new Error('Tenant not found'))

      await expect(DeleteIcebergResources.handle(makeJob() as never)).resolves.toBeUndefined()

      expectIcebergCleanup({ multitenant: true })
      expect(db.deleteAnalyticsBucket).not.toHaveBeenCalled()
      expect(db.destroyConnection).not.toHaveBeenCalled()
    })

    it('should remove all resources, multitenant db rows, and clean up tenant db when createStorage succeeds', async () => {
      mockCreateStorage.mockResolvedValue({ db })

      await expect(DeleteIcebergResources.handle(makeJob() as never)).resolves.toBeUndefined()

      expectIcebergCleanup({ multitenant: true })
      expect(db.deleteAnalyticsBucket).toHaveBeenCalledWith('catalog-123')
      expect(db.destroyConnection).toHaveBeenCalled()
    })

    it('should still pass when deleteAnalyticsBucket fails with NoSuchBucket', async () => {
      const { ERRORS } = await import('@internal/errors')
      mockCreateStorage.mockResolvedValue({ db })
      db.deleteAnalyticsBucket.mockRejectedValue(ERRORS.NoSuchBucket('catalog-123'))

      await expect(DeleteIcebergResources.handle(makeJob() as never)).resolves.toBeUndefined()

      expectIcebergCleanup({ multitenant: true })
      expect(db.deleteAnalyticsBucket).toHaveBeenCalledWith('catalog-123')
      expect(db.destroyConnection).toHaveBeenCalled()
    })

    it.each([2, 1001])(
      'should drop a namespace spanning multiple shards only after all %i tables',
      async (count) => {
        mockCreateStorage.mockResolvedValue({ db })
        const tables = Array.from({ length: count }, (_, i) => ({
          name: `table-${i + 1}`,
          shard_key: i === count - 1 ? 'shard-key-2' : 'shard-key-1',
          shard_id: i === count - 1 ? 'shard-id-2' : 'shard-id-1',
        }))
        const localTables = new Map(tables.map((table) => [table.name, table]))
        store.listTables.mockImplementation(async ({ pageSize }) =>
          [...localTables.values()].slice(0, pageSize)
        )
        store.dropTable.mockImplementation(async ({ name }) => {
          localTables.delete(name)
        })
        store.dropNamespace.mockImplementation(async () => {
          if (localTables.size > 0) throw new Error('namespace not empty')
        })
        const upstream = new Map([
          ['shard-key-1', new Set(tables.slice(0, -1).map(({ name }) => name))],
          ['shard-key-2', new Set([tables[count - 1].name])],
        ])
        fakeUpstream(upstream)

        await expect(DeleteIcebergResources.handle(makeJob() as never)).resolves.toBeUndefined()

        expect(upstream.size).toBe(0)
        expect(restCatalog.listNamespaces).toHaveBeenCalledTimes(2)
        expect(restCatalog.dropNamespace).toHaveBeenCalledTimes(2)
        expect(store.dropNamespace).toHaveBeenCalledTimes(1)
        expect(sharder.freeByResource).toHaveBeenCalledTimes(count)
        expect(sharder.freeByResource).toHaveBeenCalledWith('shard-id-2', {
          kind: 'iceberg-table',
          tenantId: 'tenant-a',
          bucketName: 'catalog-123',
          logicalName: `ns-1/table-${count}`,
        })
      }
    )

    it('should purge remote tables without a local row before dropping the namespace', async () => {
      mockCreateStorage.mockResolvedValue({ db })
      const upstream = new Map([['shard-key-1', new Set(['table-1', 'orphan-1', 'orphan-2'])]])
      fakeUpstream(upstream)

      await expect(DeleteIcebergResources.handle(makeJob() as never)).resolves.toBeUndefined()

      expect(upstream.size).toBe(0)
      expect(store.dropNamespace).toHaveBeenCalledWith({
        namespace: 'namespace-1',
        catalogId: 'catalog-123',
        tenantId: 'tenant-a',
      })
    })

    it('should retain local rows when warehouse validation returns 404', async () => {
      mockCreateStorage.mockResolvedValue({ db })
      const error = notFound()
      restCatalog.listNamespaces.mockRejectedValue(error)

      await expect(DeleteIcebergResources.handle(makeJob() as never)).rejects.toBe(error)

      expect(restCatalog.dropTable).not.toHaveBeenCalled()
      expect(store.dropTable).not.toHaveBeenCalled()
      expect(store.dropNamespace).not.toHaveBeenCalled()
      expect(store.dropCatalog).not.toHaveBeenCalled()
      expect(sharder.freeByResource).not.toHaveBeenCalled()
    })

    it.each([
      ['dropTable', 'NoSuchWarehouseException'],
      ['listTables', 'NoSuchWarehouseException'],
      ['dropNamespace', 'NoSuchWarehouseException'],
      ['listTables', 'NoSuchTableException'],
    ] as const)('should propagate a non-resource 404 from %s (%s)', async (operation, type) => {
      const error = notFound(type)
      mockCreateStorage.mockResolvedValue({ db })
      restCatalog[operation].mockRejectedValue(error)

      await expect(DeleteIcebergResources.handle(makeJob() as never)).rejects.toBe(error)

      expect(store.dropNamespace).not.toHaveBeenCalled()
      expect(store.dropCatalog).not.toHaveBeenCalled()
    })

    it.each(['known', 'listed'] as const)(
      'should finish cleanup when a previous attempt already removed the %s remote table',
      async (source) => {
        const error = notFound('NoSuchTableException')
        mockCreateStorage.mockResolvedValue({ db })
        if (source === 'listed') {
          restCatalog.dropTable.mockResolvedValueOnce(undefined).mockRejectedValueOnce(error)
          restCatalog.listTables.mockResolvedValue({
            identifiers: [{ namespace: [remoteNamespace], name: 'orphan' }],
          })
        } else {
          restCatalog.dropTable.mockRejectedValue(error)
        }

        await expect(DeleteIcebergResources.handle(makeJob() as never)).resolves.toBeUndefined()

        expect(store.dropCatalog).toHaveBeenCalled()
        expect(sharder.freeByResource).toHaveBeenCalled()
      }
    )

    it('should finish cleanup when a previous attempt already removed the remote namespace', async () => {
      mockCreateStorage.mockResolvedValue({ db })
      restCatalog.dropTable.mockRejectedValue(notFound())
      restCatalog.listTables.mockRejectedValue(notFound())

      await expect(DeleteIcebergResources.handle(makeJob() as never)).resolves.toBeUndefined()

      expect(restCatalog.dropNamespace).not.toHaveBeenCalled()
      expect(store.dropNamespace).toHaveBeenCalledWith({
        namespace: 'namespace-1',
        catalogId: 'catalog-123',
        tenantId: 'tenant-a',
      })
      expect(store.dropCatalog).toHaveBeenCalled()
      expect(sharder.freeByResource).toHaveBeenCalled()
    })

    it('should fail when deleteAnalyticsBucket fails with any other error', async () => {
      mockCreateStorage.mockResolvedValue({ db })
      db.deleteAnalyticsBucket.mockRejectedValue(new Error('connection reset'))

      await expect(DeleteIcebergResources.handle(makeJob() as never)).rejects.toThrow(
        'connection reset'
      )

      expect(db.deleteAnalyticsBucket).toHaveBeenCalledWith('catalog-123')
      expect(db.destroyConnection).toHaveBeenCalled()
    })
  })

  describe('non-multitenant', () => {
    beforeAll(async () => {
      DeleteIcebergResources = await importHandler(false)
    })

    it('should error when createStorage fails', async () => {
      mockCreateStorage.mockRejectedValue(new Error('Failed to create storage'))

      await expect(DeleteIcebergResources.handle(makeJob() as never)).rejects.toThrow(
        'Failed to create storage'
      )

      expect(mockCreateStorage).toHaveBeenCalledWith(jobData)
      expect(MockPgMetastore).not.toHaveBeenCalled()
      expect(metastore.transaction).not.toHaveBeenCalled()
      expect(store.lockResource).not.toHaveBeenCalled()
      expect(store.dropCatalog).not.toHaveBeenCalled()
      expect(db.connection.query).not.toHaveBeenCalled()
      expect(db.deleteAnalyticsBucket).not.toHaveBeenCalled()
      expect(db.destroyConnection).not.toHaveBeenCalled()
    })

    it('should remove all resources when createStorage succeeds', async () => {
      mockCreateStorage.mockResolvedValue({ db })

      await expect(DeleteIcebergResources.handle(makeJob() as never)).resolves.toBeUndefined()

      expectIcebergCleanup({ multitenant: false })
      expect(db.deleteAnalyticsBucket).not.toHaveBeenCalled()
      expect(db.destroyConnection).toHaveBeenCalled()
    })
  })
})
