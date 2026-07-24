import { vi } from 'vitest'

const {
  mockRunBatchSend,
  mockResetBatchSend,
  mockInfo,
  mockBeginTransaction,
  mockWarning,
  mockQuery,
  mockLastLocalMigrationName,
  mockLocalMigrationFiles,
  mockPgClientConstructor,
} = vi.hoisted(() => ({
  mockRunBatchSend: vi.fn(),
  mockResetBatchSend: vi.fn(),
  mockInfo: vi.fn(),
  mockBeginTransaction: vi.fn(),
  mockWarning: vi.fn(),
  mockQuery: vi.fn(),
  mockLastLocalMigrationName: vi.fn(),
  mockLocalMigrationFiles: vi.fn(),
  mockPgClientConstructor: vi.fn(),
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
    warning: mockWarning,
    error: vi.fn(),
  },
}))

vi.mock('pg', () => ({
  Client: class {
    constructor(...args: unknown[]) {
      return mockPgClientConstructor(...args)
    }
  },
  types: {
    getTypeParser: vi.fn(),
  },
}))

vi.mock('../multitenant-pg', () => ({
  multitenantPgExecutor: {
    beginTransaction: mockBeginTransaction,
    query: mockQuery,
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
  localMigrationFiles: mockLocalMigrationFiles,
}))

vi.mock('./progressive', () => ({
  ProgressiveMigrations: class {
    start() {
      return undefined
    }
  },
}))

import {
  obtainLockOnMultitenantDB,
  resetMigration,
  resetMigrationsOnTenants,
  runMigrationsOnAllTenants,
} from './migrate'

type MockPgClient = {
  connect: ReturnType<typeof vi.fn>
  end: ReturnType<typeof vi.fn>
  on: ReturnType<typeof vi.fn>
  query: ReturnType<typeof vi.fn>
}

type QueryResult = {
  rows: unknown[]
  rowCount?: number
}

function getQueryText(statement: unknown): string {
  if (typeof statement === 'string') {
    return statement
  }

  if (statement && typeof statement === 'object' && 'text' in statement) {
    return String((statement as { text: string }).text)
  }

  return String(statement)
}

function normalizeSql(sql: string): string {
  return sql.replace(/\s+/g, ' ').trim()
}

function createMigrationClient(migrations: Array<{ id: number; name: string }>): MockPgClient {
  const client: MockPgClient = {
    connect: vi.fn().mockResolvedValue(undefined),
    end: vi.fn().mockResolvedValue(undefined),
    on: vi.fn(),
    query: vi.fn(async (statement: unknown): Promise<QueryResult> => {
      const text = getQueryText(statement)

      if (text === 'SELECT pg_try_advisory_lock(-8525285245963000605);') {
        return { rows: [{ pg_try_advisory_lock: true }] }
      }

      if (text === 'SELECT * from migrations') {
        return { rows: migrations }
      }

      return { rows: [], rowCount: 0 }
    }),
  }

  mockPgClientConstructor.mockReturnValue(client)

  return client
}

function getMigrationQueryCall(client: MockPgClient, sql: string) {
  return client.query.mock.calls.find(([statement]) => {
    return normalizeSql(getQueryText(statement)).startsWith(sql)
  })
}

function createTenantQueryMock(batches: Array<Array<{ id: string; cursor_id: number }>>) {
  let batchIndex = 0

  return vi.fn(async () => ({
    rows: batches[batchIndex++] ?? [],
  }))
}

function makeTransaction() {
  return {
    query: vi.fn().mockResolvedValue({
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
    mockLocalMigrationFiles.mockResolvedValue([])
    mockQuery.mockResolvedValue({ rows: [], rowCount: 1 })
  })

  it('passes sbReqId into fleet migration jobs and scheduler logs', async () => {
    const trx = makeTransaction()
    mockBeginTransaction.mockResolvedValue(trx)
    mockQuery.mockImplementation(
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
    mockBeginTransaction.mockResolvedValue(trx)
    mockQuery.mockImplementation(createTenantQueryMock([[{ id: 'tenant-c', cursor_id: 1 }], []]))

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

  it('preserves lock callback errors when rollback fails', async () => {
    const error = new Error('migration callback failed')
    const rollbackError = new Error('rollback failed')
    const trx = makeTransaction()
    trx.rollback.mockRejectedValue(rollbackError)
    mockBeginTransaction.mockResolvedValue(trx)

    await expect(
      obtainLockOnMultitenantDB(
        async () => {
          throw error
        },
        { sbReqId: 'sb-req-123' }
      )
    ).rejects.toBe(error)

    expect(trx.rollback).toHaveBeenCalledTimes(1)
    expect(mockWarning).toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] Failed to rollback transaction',
      expect.objectContaining({
        type: 'migrations',
        sbReqId: 'sb-req-123',
        error: rollbackError,
      })
    )
  })
})

describe('resetMigration', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockLastLocalMigrationName.mockResolvedValue('storage-schema')
    mockLocalMigrationFiles.mockResolvedValue([])
    mockQuery.mockResolvedValue({ rows: [], rowCount: 1 })
  })

  it('uses the advisory lock and marks skipped migrations as completed', async () => {
    const client = createMigrationClient([
      { id: 0, name: 'create-migrations-table' },
      { id: 1, name: 'initialmigration' },
      { id: 5, name: 'add-size-functions' },
    ])
    mockLocalMigrationFiles.mockResolvedValue([
      { id: 3, name: 'pathtoken-column', hash: 'hash-3' },
      { id: 4, name: 'add-migrations-rls', hash: 'hash-4' },
    ])

    await expect(
      resetMigration({
        tenantId: 'tenant-reset',
        untilMigration: 'storage-schema',
        markCompletedTillMigration: 'add-migrations-rls',
        databaseUrl: 'postgres://tenant',
      })
    ).resolves.toBe(true)

    const queryTexts = client.query.mock.calls.map(([statement]) =>
      normalizeSql(getQueryText(statement))
    )

    expect(queryTexts).toEqual([
      'SELECT pg_try_advisory_lock(-8525285245963000605);',
      'SET search_path TO storage,public',
      'SELECT * from migrations',
      'BEGIN',
      'DELETE FROM migrations WHERE id > $1',
      'INSERT INTO migrations(id, name, hash, executed_at) VALUES ($1, $2, $3, NOW()),($4, $5, $6, NOW())',
      'COMMIT',
      'SELECT pg_advisory_unlock(-8525285245963000605);',
    ])

    const deleteCall = getMigrationQueryCall(client, 'DELETE FROM migrations WHERE id >')
    expect(deleteCall?.[0]).toMatchObject({
      text: 'DELETE FROM migrations WHERE id > $1',
      values: [2],
    })

    const insertCall = getMigrationQueryCall(client, 'INSERT INTO migrations')
    expect(insertCall?.[0]).toMatchObject({
      values: [3, 'pathtoken-column', 'hash-3', 4, 'add-migrations-rls', 'hash-4'],
    })

    expect(mockQuery).toHaveBeenCalledWith(
      expect.objectContaining({
        text: expect.stringContaining('UPDATE tenants'),
        values: ['COMPLETED', 'add-migrations-rls', 'tenant-reset'],
      }),
      expect.objectContaining({
        signal: expect.any(AbortSignal),
      })
    )
    expect(client.end).toHaveBeenCalledTimes(1)
  })

  it('does not open a transaction for no-op resets', async () => {
    const client = createMigrationClient([
      { id: 0, name: 'create-migrations-table' },
      { id: 1, name: 'initialmigration' },
      { id: 2, name: 'storage-schema' },
    ])

    await expect(
      resetMigration({
        tenantId: 'tenant-reset',
        untilMigration: 'storage-schema',
        databaseUrl: 'postgres://tenant',
      })
    ).resolves.toBe(false)

    const queryTexts = client.query.mock.calls.map(([statement]) =>
      normalizeSql(getQueryText(statement))
    )

    expect(queryTexts).toEqual([
      'SELECT pg_try_advisory_lock(-8525285245963000605);',
      'SET search_path TO storage,public',
      'SELECT * from migrations',
      'SELECT pg_advisory_unlock(-8525285245963000605);',
    ])
    expect(mockQuery).not.toHaveBeenCalled()
    expect(client.end).toHaveBeenCalledTimes(1)
  })

  it('rolls back, unlocks, and closes the client when mark-completed migration files are missing', async () => {
    const client = createMigrationClient([
      { id: 0, name: 'create-migrations-table' },
      { id: 1, name: 'initialmigration' },
      { id: 5, name: 'add-size-functions' },
    ])
    mockLocalMigrationFiles.mockResolvedValue([{ id: 3, name: 'pathtoken-column', hash: 'hash-3' }])

    await expect(
      resetMigration({
        tenantId: 'tenant-reset',
        untilMigration: 'storage-schema',
        markCompletedTillMigration: 'add-migrations-rls',
        databaseUrl: 'postgres://tenant',
      })
    ).rejects.toThrow('Migration add-migrations-rls not found')

    const queryTexts = client.query.mock.calls.map(([statement]) =>
      normalizeSql(getQueryText(statement))
    )

    expect(queryTexts).toContain('BEGIN')
    expect(queryTexts).toContain('ROLLBACK')
    expect(queryTexts).not.toContain('COMMIT')
    expect(queryTexts.at(-1)).toBe('SELECT pg_advisory_unlock(-8525285245963000605);')
    expect(mockQuery).not.toHaveBeenCalled()
    expect(client.end).toHaveBeenCalledTimes(1)
  })
})
