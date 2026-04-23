import { vi } from 'vitest'

const {
  mockRunBatchSend,
  mockResetBatchSend,
  mockInfo,
  mockTransaction,
  mockTable,
  mockLastLocalMigrationName,
} = vi.hoisted(() => ({
  mockRunBatchSend: vi.fn(),
  mockResetBatchSend: vi.fn(),
  mockInfo: vi.fn(),
  mockTransaction: vi.fn(),
  mockTable: vi.fn(),
  mockLastLocalMigrationName: vi.fn(),
}))

vi.mock('../../../config', () => ({
  MultitenantMigrationStrategy: {
    ON_REQUEST: 'ON_REQUEST',
    PROGRESSIVE: 'PROGRESSIVE',
    FULL_FLEET: 'FULL_FLEET',
  },
  getConfig: () => ({
    isMultitenant: true,
    multitenantDatabaseUrl: '',
    pgQueueEnable: true,
    databaseSSLRootCert: '',
    dbMigrationStrategy: 'ON_REQUEST',
    dbAnonRole: 'anon',
    dbAuthenticatedRole: 'authenticated',
    dbSuperUser: 'postgres',
    dbServiceRole: 'service_role',
    dbInstallRoles: false,
    dbRefreshMigrationHashesOnMismatch: false,
    dbMigrationFreezeAt: undefined,
    icebergShards: 0,
    multitenantDatabaseQueryTimeout: 1000,
  }),
}))

vi.mock('@storage/events', () => ({
  RunMigrationsOnTenants: class {
    static batchSend = mockRunBatchSend
    payload: Record<string, unknown>

    constructor(payload: Record<string, unknown>) {
      this.payload = payload
    }
  },
  ResetMigrationsOnTenant: class {
    static batchSend = mockResetBatchSend
    payload: Record<string, unknown>

    constructor(payload: Record<string, unknown>) {
      this.payload = payload
    }
  },
}))

vi.mock('../../monitoring', () => ({
  logger: {},
  logSchema: {
    info: mockInfo,
    warning: vi.fn(),
    error: vi.fn(),
  },
}))

vi.mock('../multitenant-db', () => ({
  multitenantKnex: {
    transaction: mockTransaction,
    table: mockTable,
  },
}))

vi.mock('../tenant', () => ({
  getTenantConfig: vi.fn(),
  TenantMigrationStatus: {
    COMPLETED: 'COMPLETED',
    FAILED: 'FAILED',
    FAILED_STALE: 'FAILED_STALE',
  },
}))

vi.mock('../pool', () => ({
  searchPath: ['storage', 'public'],
}))

vi.mock('./files', () => ({
  lastLocalMigrationName: mockLastLocalMigrationName,
  loadMigrationFilesCached: vi.fn(),
  localMigrationFiles: vi.fn(),
}))

vi.mock('./progressive', () => ({
  ProgressiveMigrations: class {
    start() {
      return undefined
    }
  },
}))

import { resetMigrationsOnTenants, runMigrationsOnAllTenants } from './migrate'

function createTenantQueryMock(batches: Array<Array<{ id: string; cursor_id: number }>>) {
  let batchIndex = 0

  return vi.fn(() => {
    const nestedBuilder = {
      where: (arg?: unknown) => {
        if (typeof arg === 'function') {
          arg(nestedBuilder)
        }
        return nestedBuilder
      },
      whereNotIn: () => nestedBuilder,
      orWhere: () => nestedBuilder,
    }

    const query = {
      select: () => query,
      where: (arg?: unknown) => {
        if (typeof arg === 'function') {
          arg(nestedBuilder)
        }
        return query
      },
      whereIn: () => query,
      orderBy: () => query,
      limit: vi.fn(async () => batches[batchIndex++] ?? []),
    }

    return query
  })
}

function makeTransaction() {
  return {
    raw: vi.fn().mockResolvedValue({
      rows: [{ locked: true }],
    }),
    commit: vi.fn(),
    rollback: vi.fn(),
  }
}

describe('migration helper request id propagation', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockLastLocalMigrationName.mockResolvedValue('storage-schema')
  })

  it('passes sbReqId into fleet migration jobs and scheduler logs', async () => {
    const trx = makeTransaction()
    mockTransaction.mockResolvedValue(trx)
    mockTable.mockImplementation(
      createTenantQueryMock([
        [
          { id: 'tenant-a', cursor_id: 1 },
          { id: 'tenant-b', cursor_id: 2 },
        ],
        [],
      ])
    )

    await expect(
      runMigrationsOnAllTenants({ signal: new AbortController().signal, sbReqId: 'sb-req-123' })
    ).resolves.toBeUndefined()

    expect(mockRunBatchSend).toHaveBeenCalledTimes(1)
    const [[batch]] = mockRunBatchSend.mock.calls
    expect((batch[0] as { payload: Record<string, unknown> }).payload).toMatchObject({
      tenantId: 'tenant-a',
      sbReqId: 'sb-req-123',
      tenant: {
        host: '',
        ref: 'tenant-a',
      },
    })
    expect((batch[1] as { payload: Record<string, unknown> }).payload).toMatchObject({
      tenantId: 'tenant-b',
      sbReqId: 'sb-req-123',
      tenant: {
        host: '',
        ref: 'tenant-b',
      },
    })
    expect(mockInfo).toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] Instance acquired the lock',
      expect.objectContaining({
        type: 'migrations',
        sbReqId: 'sb-req-123',
      })
    )
    expect(mockInfo).toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] Async migrations jobs completed',
      expect.objectContaining({
        type: 'migrations',
        sbReqId: 'sb-req-123',
      })
    )
  })

  it('passes sbReqId into fleet reset jobs and scheduler logs', async () => {
    const trx = makeTransaction()
    mockTransaction.mockResolvedValue(trx)
    mockTable.mockImplementation(createTenantQueryMock([[{ id: 'tenant-c', cursor_id: 1 }], []]))

    await expect(
      resetMigrationsOnTenants({
        till: 'storage-schema',
        markCompletedTillMigration: 'create-migrations-table',
        signal: new AbortController().signal,
        sbReqId: 'sb-req-123',
      })
    ).resolves.toBeUndefined()

    expect(mockResetBatchSend).toHaveBeenCalledTimes(1)
    const [[batch]] = mockResetBatchSend.mock.calls
    expect((batch[0] as { payload: Record<string, unknown> }).payload).toMatchObject({
      tenantId: 'tenant-c',
      untilMigration: 'storage-schema',
      markCompletedTillMigration: 'create-migrations-table',
      sbReqId: 'sb-req-123',
      tenant: {
        host: '',
        ref: 'tenant-c',
      },
    })
    expect(mockInfo).toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] reset migrations jobs scheduled',
      expect.objectContaining({
        type: 'migrations',
        sbReqId: 'sb-req-123',
      })
    )
  })
})
