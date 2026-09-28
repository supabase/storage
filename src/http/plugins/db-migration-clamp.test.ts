import type { Database } from '@storage/database'
import Fastify from 'fastify'
import { vi } from 'vitest'

const TENANT = 'clamp-tenant'
const UNRECOGNIZED = 'a-migration-this-binary-does-not-know'

async function load(
  version: string | null,
  status: string,
  strategy: 'progressive' | 'on_request' | 'full_fleet' = 'progressive',
  freeze?: string
) {
  vi.resetModules()

  const row: Record<string, unknown> = {}
  const storeCalls: string[] = []

  vi.doMock('@internal/database/tenant-store-pg', async (importOriginal) => {
    const actual = await importOriginal<typeof import('@internal/database/tenant-store-pg')>()
    class TenantConfigStorePg {
      async findById() {
        return { ...row }
      }
      async update() {
        storeCalls.push('update')
        return 1
      }
      async completeMigrations(
        _tenantId: string,
        migrationVersion: string,
        expectedVersion: string | null
      ) {
        storeCalls.push(`complete:${String(expectedVersion)}:${migrationVersion}`)
        row.migrations_version = migrationVersion
        row.migrations_status = 'COMPLETED'
        return 1
      }
      async listTenantsToMigrateBatch() {
        return []
      }
    }
    return { ...actual, TenantConfigStorePg }
  })

  const configModule = await import('../../config')
  configModule.getConfig({ reload: true })
  configModule.mergeConfig({
    isMultitenant: true,
    dbMigrationStrategy: strategy as never,
    dbMigrationFreezeAt: freeze as never,
  })

  const { encrypt } = await import('@internal/auth')
  Object.assign(row, {
    id: TENANT,
    anon_key: encrypt('anon'),
    database_url: encrypt('postgres://tenant-db-must-not-be-contacted:1/db'),
    database_pool_url: null,
    jwt_secret: encrypt('secret'),
    service_key: encrypt('service'),
    jwks: null,
    file_size_limit: 1000,
    migrations_version: version,
    migrations_status: status,
  })

  const mig = await import('@internal/database/migrations')
  const { migrations } = await import('./db')
  const { storage } = await import('./storage')
  const tenant = await import('@internal/database/tenant')

  return {
    mig,
    migrations,
    storage,
    tenant,
    storeCalls,
    tenantHasMigrations: vi.spyOn(mig, 'tenantHasMigrations'),
    addTenant: vi.spyOn(mig.progressiveMigrations, 'addTenant').mockImplementation(() => {}),
    runMigrationsOnTenant: vi.spyOn(mig, 'runMigrationsOnTenant').mockRejectedValue(new Error()),
  }
}

type Loaded = Awaited<ReturnType<typeof load>>

function fakeConnection() {
  const transaction = {
    commit: vi.fn(),
    rollback: vi.fn(),
    isCompleted: vi.fn().mockReturnValue(false),
    query: vi.fn().mockResolvedValue({ rows: [{ id: 'row' }], rowCount: 1 }),
  }
  const connection = {
    getAbortSignal: vi.fn().mockReturnValue(undefined),
    transaction: vi.fn().mockResolvedValue(transaction),
    setScope: vi.fn(),
    dispose: vi.fn(),
    setAbortSignal: vi.fn(),
  }
  return { connection, transaction }
}

async function upsertAndFind(
  db: Pick<Database, 'upsertObject' | 'findObject'>,
  transaction: ReturnType<typeof fakeConnection>['transaction']
) {
  await db.upsertObject({
    name: 'a.txt',
    bucket_id: 'b',
    metadata: { size: 1 },
    user_metadata: { k: 'v' },
    version: 'v1',
  })
  await db.findObject('b', 'a.txt', 'id,name,metadata,user_metadata,version')
  return transaction.query.mock.calls.map((c) => {
    const q = c[0] as { text: string; values: unknown[] }
    return { text: q.text.replace(/\s+/g, ' ').trim(), values: q.values }
  })
}

async function requestSnapshot(L: Loaded) {
  const { connection, transaction } = fakeConnection()
  const app = Fastify()
  app.decorateRequest('tenantId')
  app.addHook('onRequest', async (request) => {
    request.tenantId = TENANT
  })
  await app.register(L.migrations)
  app.decorateRequest('db')
  app.addHook('preHandler', async (request) => {
    ;(request as unknown as { db: unknown }).db = connection
  })
  await app.register(L.storage)

  let snapshot:
    | { latestMigration: string | undefined; sql: Awaited<ReturnType<typeof upsertAndFind>> }
    | undefined
  app.get('/test', async (request) => {
    snapshot = {
      latestMigration: request.latestMigration,
      sql: await upsertAndFind(request.storage.db, transaction),
    }
    return { ok: true }
  })
  try {
    const response = await app.inject({ method: 'GET', url: '/test' })
    expect(response.statusCode).toBe(200)
  } finally {
    await app.close()
  }
  return snapshot!
}

function expectCurrentSql(sql: Awaited<ReturnType<typeof upsertAndFind>>) {
  expect(sql[0].text).toContain(
    'ON CONFLICT (bucket_id, name COLLATE "C") WHERE archived_at IS NULL'
  )
  expect(sql[0].text).toContain('"user_metadata"')
  expect(sql[1].text).toContain('"user_metadata"')
}

describe('migrations plugin clamp', () => {
  afterEach(() => {
    vi.doUnmock('@internal/database/tenant-store-pg')
    vi.restoreAllMocks()
    vi.resetModules()
  })

  it.each([
    ['COMPLETED', 0],
    ['FAILED', 1],
  ])('clamps an unrecognized migration with status %s to the highest local migration', async (status, migrationRuns) => {
    const L = await load(UNRECOGNIZED, status)
    L.runMigrationsOnTenant.mockResolvedValue(UNRECOGNIZED as never)
    const { latestMigration, sql } = await requestSnapshot(L)

    expect(latestMigration).toBe(L.mig.highestLocalMigrationName())
    expectCurrentSql(sql)
    expect(L.tenantHasMigrations).not.toHaveBeenCalled()
    expect(L.addTenant).not.toHaveBeenCalled()
    expect(L.storeCalls).toEqual([])
    expect(L.runMigrationsOnTenant).toHaveBeenCalledTimes(migrationRuns)
  })

  it('keeps a recognized behind migration as the tenant position', async () => {
    const L = await load('initialmigration', 'COMPLETED')
    const { latestMigration, sql } = await requestSnapshot(L)

    expect(latestMigration).toBe('initialmigration')
    expect(sql[0].text).toContain('ON CONFLICT (name, bucket_id)')
    expect(sql[0].text).not.toContain('"user_metadata"')
    expect(L.addTenant).toHaveBeenCalledTimes(1)
    expect(L.runMigrationsOnTenant).not.toHaveBeenCalled()
  })

  it.each([
    'progressive',
    'full_fleet',
    'on_request',
  ] as const)('repairs a null snapshot before serving under %s', async (strategy) => {
    const L = await load(null, '', strategy)
    L.runMigrationsOnTenant.mockResolvedValue(L.mig.highestLocalMigrationName() as never)

    const { latestMigration, sql } = await requestSnapshot(L)

    expect(latestMigration).toBe(L.mig.highestLocalMigrationName())
    expectCurrentSql(sql)
    expect(L.runMigrationsOnTenant).toHaveBeenCalledTimes(1)
    expect(L.storeCalls).toContain(`complete:null:${L.mig.highestLocalMigrationName()}`)
  })

  it('clamps to the highest local migration rather than the freeze point', async () => {
    const L = await load(UNRECOGNIZED, 'COMPLETED', 'progressive', 'initialmigration')
    const { latestMigration, sql } = await requestSnapshot(L)

    expect(latestMigration).toBe(L.mig.highestLocalMigrationName())
    expectCurrentSql(sql)
    await expect(L.tenant.getTenantCapabilities(TENANT)).resolves.toEqual({
      list_V2: true,
      iceberg_catalog: true,
    })
  })

  it.each([
    ['COMPLETED', undefined, 0],
    ['FAILED', undefined, 1],
    ['FAILED_STALE', undefined, 1],
    ['FAILED', 'objects-key-version-index', 1],
    ['FAILED_STALE', 'objects-key-version-index', 1],
  ])('handles an unrecognized %s migration on request with freeze %s', async (status, freeze, migrationRuns) => {
    const L = await load(UNRECOGNIZED, status, 'on_request', freeze)
    L.runMigrationsOnTenant.mockResolvedValue(UNRECOGNIZED as never)

    const { latestMigration, sql } = await requestSnapshot(L)

    expect(latestMigration).toBe(L.mig.highestLocalMigrationName())
    expectCurrentSql(sql)
    expect(L.runMigrationsOnTenant).toHaveBeenCalledTimes(migrationRuns)
    expect(L.storeCalls).toEqual([])
  })

  it.each([
    'on_request',
    'progressive',
    'full_fleet',
  ] as const)('repairs a restored future failure before serving under %s', async (strategy) => {
    const physical = 'objects-key-version-index'
    const L = await load(UNRECOGNIZED, 'FAILED', strategy, physical)
    L.runMigrationsOnTenant.mockResolvedValue(physical as never)

    const { latestMigration, sql } = await requestSnapshot(L)

    expect(latestMigration).toBe(physical)
    expect(sql[0].text).toContain('ON CONFLICT (name, bucket_id)')
    expect(L.runMigrationsOnTenant).toHaveBeenCalledTimes(1)
    expect(L.storeCalls).toEqual([`complete:${UNRECOGNIZED}:${physical}`])
    expect(L.addTenant).not.toHaveBeenCalled()
  })

  it.each([
    [null, false],
    ['optimise-existing-functions', true],
  ] as const)('preserves capabilities at the recorded version %s', async (version, listV2) => {
    const L = await load(version, 'COMPLETED')

    await expect(L.tenant.getTenantCapabilities(TENANT)).resolves.toEqual({
      list_V2: listV2,
      iceberg_catalog: false,
    })
  })

  it('loads failed future capabilities without querying the tenant database', async () => {
    const L = await load(UNRECOGNIZED, 'FAILED')
    const { Client } = await import('pg')
    const connect = vi
      .spyOn(Client.prototype, 'connect')
      .mockRejectedValue(new Error('Unavailable'))

    await expect(L.tenant.getTenantConfig(TENANT)).resolves.toMatchObject({
      migrationVersion: UNRECOGNIZED,
      migrationStatus: 'FAILED',
    })
    await expect(L.tenant.getTenantCapabilities(TENANT)).resolves.toEqual({
      list_V2: true,
      iceberg_catalog: true,
    })
    expect(connect).not.toHaveBeenCalled()
  })

  it('warns once per tenant cache load', async () => {
    const L = await load(UNRECOGNIZED, 'COMPLETED')
    const { logSchema } = await import('@internal/monitoring')
    const warn = vi.spyOn(logSchema, 'warning')

    await Promise.all(Array.from({ length: 20 }, () => L.tenant.getTenantConfig(TENANT)))
    expect(warn).toHaveBeenCalledTimes(1)
    expect(warn.mock.calls[0][2]).toMatchObject({ project: TENANT, type: 'migrations' })
    expect(JSON.parse(warn.mock.calls[0][2].metadata!)).toEqual({
      tenantId: TENANT,
      recordedMigration: UNRECOGNIZED,
      localLatest: L.mig.highestLocalMigrationName(),
    })

    L.tenant.deleteTenantConfig(TENANT)
    await L.tenant.getTenantConfig(TENANT)
    expect(warn).toHaveBeenCalledTimes(2)
  })
})
