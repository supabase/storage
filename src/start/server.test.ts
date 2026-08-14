import { vi } from 'vitest'

const bootOrder = vi.hoisted(() => [] as string[])

vi.mock('@internal/monitoring/otel-tracing', () => ({}))
vi.mock('@internal/monitoring/otel-metrics', () => ({}))
vi.mock('@internal/monitoring', () => ({
  logger: { info: vi.fn() },
  logSchema: { info: vi.fn(), error: vi.fn() },
}))
vi.mock('@internal/cluster/cluster', () => ({
  Cluster: {
    size: 0,
    init: vi.fn(async () => {
      bootOrder.push('cluster.init')
    }),
    on: vi.fn(() => {
      bootOrder.push('cluster.on')
    }),
  },
}))
vi.mock('@internal/database', () => ({
  listenForTenantUpdate: vi.fn(async () => {}),
  multitenantPgExecutor: {},
  PgTenantConnection: {
    poolManager: {
      setNumWorkers: vi.fn(() => {
        bootOrder.push('poolManager.setNumWorkers')
      }),
      monitor: vi.fn(() => {
        bootOrder.push('poolManager.monitor')
      }),
      rebalanceAll: vi.fn(),
    },
  },
  PubSub: { start: vi.fn(async () => {}) },
}))
vi.mock('@internal/database/migrations', () => ({
  runMigrationsOnTenant: vi.fn(async () => {}),
  runMultitenantMigrations: vi.fn(async () => {}),
  startAsyncMigrations: vi.fn(),
}))
vi.mock('@internal/queue', () => ({
  Queue: {
    start: vi.fn(async () => {
      bootOrder.push('queue.start')
    }),
  },
  SYSTEM_TENANT: 'system',
}))
vi.mock('@internal/sharding', () => ({
  PgShardStoreFactory: class {},
  ShardCatalog: class {
    createShards = vi.fn(async () => {})
  },
}))
vi.mock('@platformatic/globals', () => ({ getGlobal: () => undefined }))
vi.mock('@storage/events', () => ({ registerWorkers: vi.fn() }))
vi.mock('@storage/events/upgrades/sync-catalog-ids', () => ({
  SyncCatalogIds: { invoke: vi.fn(async () => {}) },
}))
vi.mock('fastify', () => ({ LogController: class {} }))
vi.mock('../admin-app', () => ({ default: vi.fn() }))
vi.mock('../app', () => ({
  default: vi.fn(() => ({ server: {}, listen: vi.fn(async () => {}) })),
}))
vi.mock('../config', () => ({
  getConfig: () => ({
    databaseURL: 'postgres://example',
    isMultitenant: false,
    pgQueueEnable: true,
    dbMigrationFreezeAt: undefined,
    vectorBucketProvider: 's3',
    vectorDatabaseURL: undefined,
    vectorEnabled: false,
    vectorStoreMigrationsEnabled: false,
    vectorS3Buckets: [],
    icebergShards: [],
    numWorkers: 4,
    exposeDocs: false,
    requestTraceHeader: undefined,
    port: 0,
    host: '127.0.0.1',
  }),
}))
vi.mock('./shutdown', () => ({
  bindShutdownSignals: vi.fn(),
  createServerClosedPromise: vi.fn(() => Promise.resolve()),
  shutdown: vi.fn(async () => {}),
}))

describe('server boot order', () => {
  afterEach(() => {
    bootOrder.length = 0
    vi.restoreAllMocks()
    vi.resetModules()
  })

  test('initializes pool sizing and cluster discovery before queue workers start', async () => {
    // Track exit to find out mock gaps and silently pass the test.
    const exitSpy = vi.spyOn(process, 'exit').mockImplementation((() => undefined) as never)

    await import('./server')
    await vi.waitFor(() => expect(bootOrder).toContain('queue.start'))

    expect(bootOrder).toEqual([
      'poolManager.setNumWorkers',
      'poolManager.monitor',
      'cluster.init',
      'cluster.on',
      'queue.start',
    ])
    expect(exitSpy).not.toHaveBeenCalled()
  })
})
