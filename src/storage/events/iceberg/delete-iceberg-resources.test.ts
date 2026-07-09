import { vi } from 'vitest'

const {
  MockPgMetastore,
  MockRestCatalogClient,
  MockShardCatalog,
  MockPgShardStoreFactory,
  mockWithTnx,
  mockCreateStorage,
  mockMultitenantPgExecutor,
  mockPgMetastoreTransaction,
  mockLockResource,
  mockFindCatalogById,
  mockListNamespaces,
  mockListTables,
  mockDropTable,
  mockDropNamespace,
  mockDropCatalog,
  mockDeleteAnalyticsBucket,
  mockDestroyConnection,
  mockRestCatalogDropTable,
  mockRestCatalogListTables,
  mockRestCatalogDropNamespace,
  mockFreeByResource,
  mockGetTnx,
} = vi.hoisted(() => {
  const mockWithTnx = vi.fn()
  return {
    MockPgMetastore: vi.fn(),
    MockRestCatalogClient: vi.fn(),
    MockShardCatalog: vi.fn(),
    MockPgShardStoreFactory: vi.fn(),
    mockWithTnx,
    mockCreateStorage: vi.fn(),
    mockMultitenantPgExecutor: 'mock-multitenant-executor',
    mockPgMetastoreTransaction: vi.fn(),
    mockLockResource: vi.fn(),
    mockFindCatalogById: vi.fn(),
    mockListNamespaces: vi.fn(),
    mockListTables: vi.fn(),
    mockDropTable: vi.fn(),
    mockDropNamespace: vi.fn(),
    mockDropCatalog: vi.fn(),
    mockDeleteAnalyticsBucket: vi.fn(),
    mockDestroyConnection: vi.fn(),
    mockRestCatalogDropTable: vi.fn(),
    mockRestCatalogListTables: vi.fn(),
    mockRestCatalogDropNamespace: vi.fn(),
    mockFreeByResource: vi.fn(),
    mockGetTnx: vi.fn(),
  }
})

vi.mock('../../../config', () => ({
  getConfig: () => ({
    icebergCatalogUrl: 'http://catalog',
    icebergCatalogAuthType: 'none',
    isMultitenant: true,
  }),
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

import { DeleteIcebergResources } from './delete-iceberg-resources'

function makeJob(overrides?: Partial<Record<string, unknown>>) {
  return {
    id: 'job-1',
    name: DeleteIcebergResources.getQueueName(),
    data: {
      catalogId: 'catalog-123',
      tenant: {
        ref: 'tenant-a',
        host: '',
      },
      sbReqId: 'sb-req-123',
    },
    ...overrides,
  }
}

function createMockStorage() {
  return {
    db: {
      deleteAnalyticsBucket: mockDeleteAnalyticsBucket,
      destroyConnection: mockDestroyConnection,
    },
  }
}

function createMockStore() {
  return {
    lockResource: mockLockResource,
    findCatalogById: mockFindCatalogById,
    listNamespaces: mockListNamespaces,
    listTables: mockListTables,
    dropTable: mockDropTable,
    dropNamespace: mockDropNamespace,
    dropCatalog: mockDropCatalog,
    getTnx: mockGetTnx,
  }
}

describe('DeleteIcebergResources.handle - multitenant', () => {
  beforeEach(() => {
    vi.clearAllMocks()

    const mockStore = createMockStore()
    mockPgMetastoreTransaction.mockImplementation(async (fn) => fn(mockStore))
    mockGetTnx.mockReturnValue('mock-transaction')
    mockWithTnx.mockReturnValue({ freeByResource: mockFreeByResource })

    MockPgMetastore.mockImplementation(function () {
      return { transaction: mockPgMetastoreTransaction }
    })

    MockRestCatalogClient.mockImplementation(function () {
      return {
        dropTable: mockRestCatalogDropTable,
        listTables: mockRestCatalogListTables,
        dropNamespace: mockRestCatalogDropNamespace,
      }
    })

    MockShardCatalog.mockImplementation(function () {
      return { withTnx: mockWithTnx }
    })

    mockFindCatalogById.mockResolvedValue({ id: 'catalog-123', deleted_at: new Date() })
    mockListNamespaces.mockResolvedValue([{ id: 'ns-1', name: 'namespace-1' }])
    mockListTables.mockResolvedValue([
      { name: 'table-1', shard_key: 'shard-key-1', shard_id: 'shard-id-1' },
    ])
    mockRestCatalogDropTable.mockResolvedValue(undefined)
    mockRestCatalogListTables.mockResolvedValue({ identifiers: [] })
    mockRestCatalogDropNamespace.mockResolvedValue(undefined)
    mockDropTable.mockResolvedValue(undefined)
    mockDropNamespace.mockResolvedValue(undefined)
    mockDropCatalog.mockResolvedValue(undefined)
    mockLockResource.mockResolvedValue(undefined)
    mockFreeByResource.mockResolvedValue(undefined)
    mockDeleteAnalyticsBucket.mockResolvedValue(undefined)
    mockDestroyConnection.mockResolvedValue(undefined)
  })

  it('should remove all resources and multitenant db rows when createStorage fails', async () => {
    const error = new Error('Tenant not found')
    mockCreateStorage.mockRejectedValue(error)

    await expect(DeleteIcebergResources.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockCreateStorage).toHaveBeenCalledWith({
      catalogId: 'catalog-123',
      tenant: { ref: 'tenant-a', host: '' },
      sbReqId: 'sb-req-123',
    })
    expect(MockPgMetastore).toHaveBeenCalledWith(mockMultitenantPgExecutor, {
      multiTenant: true,
      schema: 'public',
    })
    expect(mockPgMetastoreTransaction).toHaveBeenCalled()
    expect(mockLockResource).toHaveBeenCalledWith('catalog', 'catalog-123')
    expect(mockFindCatalogById).toHaveBeenCalledWith({
      id: 'catalog-123',
      deleted: true,
      tenantId: 'tenant-a',
    })
    expect(mockListNamespaces).toHaveBeenCalledWith({
      catalogId: 'catalog-123',
      tenantId: 'tenant-a',
    })
    expect(mockListTables).toHaveBeenCalledWith({
      namespaceId: 'ns-1',
      pageSize: 1000,
      tenantId: 'tenant-a',
    })
    expect(mockRestCatalogDropTable).toHaveBeenCalledWith({
      namespace: 'namespace-1',
      table: 'table-1',
      purgeRequested: true,
      warehouse: 'shard-key-1',
    })
    expect(mockDropTable).toHaveBeenCalledWith({
      name: 'table-1',
      namespaceId: 'ns-1',
      catalogId: 'catalog-123',
      tenantId: 'tenant-a',
    })
    expect(mockRestCatalogListTables).toHaveBeenCalledWith({
      namespace: 'tenant-a_ns_1',
      warehouse: 'shard-key-1',
      pageSize: 1,
    })
    expect(mockRestCatalogDropNamespace).toHaveBeenCalledWith({
      namespace: 'namespace-1',
      warehouse: 'shard-key-1',
    })
    expect(mockDropNamespace).toHaveBeenCalledWith({
      namespace: 'namespace-1',
      catalogId: 'catalog-123',
      tenantId: 'tenant-a',
    })
    expect(MockPgShardStoreFactory).toHaveBeenCalledWith(mockMultitenantPgExecutor)
    expect(MockShardCatalog).toHaveBeenCalled()
    expect(mockWithTnx).toHaveBeenCalledWith('mock-transaction')
    expect(mockFreeByResource).toHaveBeenCalledWith('shard-id-1', {
      kind: 'iceberg-table',
      tenantId: 'tenant-a',
      bucketName: 'catalog-123',
      logicalName: 'ns-1/table-1',
    })
    expect(mockDropCatalog).toHaveBeenCalledWith({
      bucketId: 'catalog-123',
      tenantId: 'tenant-a',
      soft: false,
    })
    expect(mockDeleteAnalyticsBucket).not.toHaveBeenCalled()
    expect(mockDestroyConnection).not.toHaveBeenCalled()
  })

  it('should remove all resources, multitenant db rows, and clean up tenant db when createStorage succeeds', async () => {
    const mockStorage = createMockStorage()
    mockCreateStorage.mockResolvedValue(mockStorage)

    await expect(DeleteIcebergResources.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockCreateStorage).toHaveBeenCalledWith({
      catalogId: 'catalog-123',
      tenant: { ref: 'tenant-a', host: '' },
      sbReqId: 'sb-req-123',
    })
    expect(MockPgMetastore).toHaveBeenCalledWith(mockMultitenantPgExecutor, {
      multiTenant: true,
      schema: 'public',
    })
    expect(mockPgMetastoreTransaction).toHaveBeenCalled()
    expect(mockLockResource).toHaveBeenCalledWith('catalog', 'catalog-123')
    expect(mockFindCatalogById).toHaveBeenCalledWith({
      id: 'catalog-123',
      deleted: true,
      tenantId: 'tenant-a',
    })
    expect(mockListNamespaces).toHaveBeenCalledWith({
      catalogId: 'catalog-123',
      tenantId: 'tenant-a',
    })
    expect(mockListTables).toHaveBeenCalledWith({
      namespaceId: 'ns-1',
      pageSize: 1000,
      tenantId: 'tenant-a',
    })
    expect(mockRestCatalogDropTable).toHaveBeenCalledWith({
      namespace: 'namespace-1',
      table: 'table-1',
      purgeRequested: true,
      warehouse: 'shard-key-1',
    })
    expect(mockDropTable).toHaveBeenCalledWith({
      name: 'table-1',
      namespaceId: 'ns-1',
      catalogId: 'catalog-123',
      tenantId: 'tenant-a',
    })
    expect(mockRestCatalogListTables).toHaveBeenCalledWith({
      namespace: 'tenant-a_ns_1',
      warehouse: 'shard-key-1',
      pageSize: 1,
    })
    expect(mockRestCatalogDropNamespace).toHaveBeenCalledWith({
      namespace: 'namespace-1',
      warehouse: 'shard-key-1',
    })
    expect(mockDropNamespace).toHaveBeenCalledWith({
      namespace: 'namespace-1',
      catalogId: 'catalog-123',
      tenantId: 'tenant-a',
    })
    expect(MockPgShardStoreFactory).toHaveBeenCalledWith(mockMultitenantPgExecutor)
    expect(MockShardCatalog).toHaveBeenCalled()
    expect(mockWithTnx).toHaveBeenCalledWith('mock-transaction')
    expect(mockFreeByResource).toHaveBeenCalledWith('shard-id-1', {
      kind: 'iceberg-table',
      tenantId: 'tenant-a',
      bucketName: 'catalog-123',
      logicalName: 'ns-1/table-1',
    })
    expect(mockDropCatalog).toHaveBeenCalledWith({
      bucketId: 'catalog-123',
      tenantId: 'tenant-a',
      soft: false,
    })
    expect(mockDeleteAnalyticsBucket).toHaveBeenCalledWith('catalog-123')
    expect(mockDestroyConnection).toHaveBeenCalled()
  })
})
