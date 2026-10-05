import { vi } from 'vitest'

const {
  mockGetTenantConfig,
  mockDeleteTenantConfig,
  mockAreMigrationsUpToDate,
  mockReadTenantMigrationVersion,
  mockCacheTenantMigration,
  mockRunMigrationsOnTenant,
  mockCompleteTenantMigrations,
  mockFailTenantMigrations,
  mockDeleteIfActiveExists,
  mockInfo,
  mockError,
} = vi.hoisted(() => ({
  mockGetTenantConfig: vi.fn(),
  mockDeleteTenantConfig: vi.fn(),
  mockAreMigrationsUpToDate: vi.fn(),
  mockReadTenantMigrationVersion: vi.fn(),
  mockCacheTenantMigration: vi.fn(),
  mockRunMigrationsOnTenant: vi.fn(),
  mockCompleteTenantMigrations: vi.fn(),
  mockFailTenantMigrations: vi.fn(),
  mockDeleteIfActiveExists: vi.fn(),
  mockInfo: vi.fn(),
  mockError: vi.fn(),
}))

vi.mock('@internal/database', () => ({
  deleteTenantConfig: mockDeleteTenantConfig,
  getTenantConfig: mockGetTenantConfig,
  TenantMigrationStatus: {
    COMPLETED: 'COMPLETED',
    FAILED: 'FAILED',
    FAILED_STALE: 'FAILED_STALE',
  },
}))

vi.mock('@internal/database/migrations', async () => ({
  ...(await vi.importActual('@internal/database/migrations/guards')),
  areMigrationsUpToDate: mockAreMigrationsUpToDate,
  readTenantMigrationVersion: mockReadTenantMigrationVersion,
  cacheTenantMigration: mockCacheTenantMigration,
  completeTenantMigrations: mockCompleteTenantMigrations,
  runMigrationsOnTenant: mockRunMigrationsOnTenant,
  failTenantMigrations: mockFailTenantMigrations,
}))

vi.mock('../base-event', () => ({
  BaseEvent: class {
    static deleteIfActiveExists = mockDeleteIfActiveExists

    static getQueueName(this: { queueName: string }) {
      return this.queueName
    }
  },
}))

vi.mock('@internal/monitoring', () => ({
  logger: {},
  logSchema: {
    info: mockInfo,
    error: mockError,
    warning: vi.fn(),
  },
}))

import { TenantMigrationStatus } from '@internal/database'
import { ERRORS } from '@internal/errors'
import { RunMigrationsOnTenants } from './run-migrations'

function makeJob(overrides?: Partial<Record<string, unknown>>) {
  return {
    id: 'job-1',
    name: RunMigrationsOnTenants.getQueueName(),
    retryCount: 0,
    retryLimit: 3,
    singletonKey: 'migrations_tenant-a',
    data: {
      tenantId: 'tenant-a',
      upToMigration: 'storage-schema',
      sbReqId: 'sb-req-123',
      tenant: {
        ref: 'tenant-a',
        host: '',
      },
    },
    ...overrides,
  }
}

describe('RunMigrationsOnTenants.handle', () => {
  beforeEach(() => {
    mockGetTenantConfig.mockResolvedValue({
      databaseUrl: 'postgres://tenant-db',
      databaseUrlEncrypted: 'encrypted-tenant-db',
    })
    mockAreMigrationsUpToDate.mockResolvedValue(false)
    mockRunMigrationsOnTenant.mockResolvedValue('storage-schema')
    mockCompleteTenantMigrations.mockResolvedValue(1)
    mockFailTenantMigrations.mockResolvedValue(undefined)
    mockDeleteIfActiveExists.mockResolvedValue(undefined)
  })

  it.each([
    1, 0,
  ])('reports completion only when the state was written (%s rows)', async (updated) => {
    mockRunMigrationsOnTenant.mockResolvedValue('objects-key-version-index')
    mockCompleteTenantMigrations.mockResolvedValue(updated)
    await expect(RunMigrationsOnTenants.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockDeleteTenantConfig).toHaveBeenCalledTimes(2)
    expect(mockDeleteTenantConfig.mock.invocationCallOrder[1]).toBeGreaterThan(
      mockCompleteTenantMigrations.mock.invocationCallOrder[0]
    )
    expect(mockDeleteTenantConfig).toHaveBeenCalledWith('tenant-a')
    expect(mockDeleteTenantConfig.mock.invocationCallOrder[0]).toBeLessThan(
      mockGetTenantConfig.mock.invocationCallOrder[0]
    )
    expect(mockRunMigrationsOnTenant).toHaveBeenCalledWith({
      databaseUrl: 'postgres://tenant-db',
      tenantId: 'tenant-a',
      waitForLock: false,
      upToMigration: 'storage-schema',
    })
    expect(mockCompleteTenantMigrations).toHaveBeenCalledWith('tenant-a', {
      expectedMigrationVersion: null,
      expectedDatabaseUrl: 'encrypted-tenant-db',
      migration: 'objects-key-version-index',
    })
    expect(mockDeleteIfActiveExists).not.toHaveBeenCalled()
    const completionLogs = mockInfo.mock.calls.filter(
      ([, message]) => message === '[Migrations] completed for tenant tenant-a'
    )
    expect(completionLogs).toHaveLength(updated)
    if (updated) {
      expect(completionLogs[0][2]).toMatchObject({
        type: 'migrations',
        project: 'tenant-a',
        sbReqId: 'sb-req-123',
      })
    }
  })

  it('short-circuits when migrations are already up to date', async () => {
    mockAreMigrationsUpToDate.mockResolvedValue(true)

    await expect(RunMigrationsOnTenants.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockRunMigrationsOnTenant).not.toHaveBeenCalled()
    expect(mockCompleteTenantMigrations).not.toHaveBeenCalled()
    expect(mockFailTenantMigrations).not.toHaveBeenCalled()
    expect(mockDeleteIfActiveExists).not.toHaveBeenCalled()
  })

  it('retries a failed completed-row ledger read without marking migrations failed', async () => {
    mockGetTenantConfig.mockResolvedValue({
      databaseUrl: 'postgres://tenant-db',
      databaseUrlEncrypted: 'encrypted-tenant-db',
      migrationVersion: 'revoke-grants-to-unused-operations',
      migrationStatus: TenantMigrationStatus.COMPLETED,
    })
    mockAreMigrationsUpToDate.mockResolvedValue(true)
    const error = new Error('ledger unavailable')
    mockReadTenantMigrationVersion.mockRejectedValueOnce(error)

    await expect(RunMigrationsOnTenants.handle(makeJob() as never)).rejects.toBe(error)

    expect(mockDeleteTenantConfig).toHaveBeenCalledTimes(2)
    expect(mockRunMigrationsOnTenant).not.toHaveBeenCalled()
    expect(mockCacheTenantMigration).not.toHaveBeenCalled()
    expect(mockFailTenantMigrations).not.toHaveBeenCalled()
    expect(mockCompleteTenantMigrations).not.toHaveBeenCalled()
    expect(mockDeleteIfActiveExists).toHaveBeenCalledWith(
      RunMigrationsOnTenants.getQueueName(),
      'migrations_tenant-a',
      'job-1'
    )
  })

  it('retains the existing completed-row policy for an unknown control name', async () => {
    mockGetTenantConfig.mockResolvedValue({
      migrationVersion: 'future-migration',
      migrationStatus: TenantMigrationStatus.COMPLETED,
    })
    mockAreMigrationsUpToDate.mockResolvedValue(true)

    await RunMigrationsOnTenants.handle(makeJob() as never)

    expect(mockReadTenantMigrationVersion).not.toHaveBeenCalled()
    expect(mockRunMigrationsOnTenant).not.toHaveBeenCalled()
  })

  it('returns without marking the tenant failed on lock timeout', async () => {
    mockRunMigrationsOnTenant.mockRejectedValue(ERRORS.LockTimeout())

    await expect(RunMigrationsOnTenants.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockFailTenantMigrations).not.toHaveBeenCalled()
    expect(mockDeleteIfActiveExists).not.toHaveBeenCalled()
    expect(mockInfo).toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] lock timeout for tenant tenant-a',
      expect.objectContaining({
        type: 'migrations',
        project: 'tenant-a',
        sbReqId: 'sb-req-123',
      })
    )
  })

  describe.each([
    [0, TenantMigrationStatus.FAILED, undefined],
    [3, TenantMigrationStatus.FAILED_STALE, TenantMigrationStatus.FAILED],
  ])('on retry %s', (retryCount, state, migrationStatus) => {
    it('marks the tenant failed and rethrows on migration failure', async () => {
      mockGetTenantConfig.mockResolvedValue({
        databaseUrl: 'postgres://tenant-db',
        databaseUrlEncrypted: 'encrypted-tenant-db',
        migrationStatus,
      })
      mockRunMigrationsOnTenant.mockRejectedValue(new Error('migration failed'))

      await expect(RunMigrationsOnTenants.handle(makeJob({ retryCount }) as never)).rejects.toThrow(
        'migration failed'
      )

      expect(mockDeleteTenantConfig).toHaveBeenCalledTimes(2)
      expect(mockDeleteTenantConfig.mock.invocationCallOrder[1]).toBeGreaterThan(
        mockFailTenantMigrations.mock.invocationCallOrder[0]
      )
      expect(mockCompleteTenantMigrations).not.toHaveBeenCalled()
      expect(mockFailTenantMigrations).toHaveBeenCalledWith('tenant-a', {
        state,
        expectedMigrationVersion: null,
        expectedDatabaseUrl: 'encrypted-tenant-db',
        expectedMigrationStatus: migrationStatus ?? null,
      })
      expect(mockDeleteIfActiveExists).toHaveBeenCalledWith(
        RunMigrationsOnTenants.getQueueName(),
        'migrations_tenant-a',
        'job-1'
      )
      expect(mockError).toHaveBeenCalledWith(
        expect.anything(),
        '[Migrations] failed for tenant tenant-a',
        expect.objectContaining({
          type: 'migrations',
          project: 'tenant-a',
          sbReqId: 'sb-req-123',
        })
      )
    })
  })
})
