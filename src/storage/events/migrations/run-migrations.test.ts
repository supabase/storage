import { vi } from 'vitest'

const {
  mockGetTenantConfig,
  mockDeleteTenantConfig,
  mockAreMigrationsUpToDate,
  mockRunMigrationsOnTenant,
  mockCompleteTenantMigrations,
  mockUpdateTenantMigrationsState,
  mockDeleteIfActiveExists,
  mockInfo,
  mockError,
} = vi.hoisted(() => ({
  mockGetTenantConfig: vi.fn(),
  mockDeleteTenantConfig: vi.fn(),
  mockAreMigrationsUpToDate: vi.fn(),
  mockRunMigrationsOnTenant: vi.fn(),
  mockCompleteTenantMigrations: vi.fn(),
  mockUpdateTenantMigrationsState: vi.fn(),
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

vi.mock('@internal/database/migrations', () => ({
  areMigrationsUpToDate: mockAreMigrationsUpToDate,
  completeTenantMigrations: mockCompleteTenantMigrations,
  isDBMigrationName: (value: unknown) =>
    value === 'storage-schema' || value === 'objects-key-version-index',
  runMigrationsOnTenant: mockRunMigrationsOnTenant,
  updateTenantMigrationsState: mockUpdateTenantMigrationsState,
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
    })
    mockAreMigrationsUpToDate.mockResolvedValue(false)
    mockRunMigrationsOnTenant.mockResolvedValue('storage-schema')
    mockCompleteTenantMigrations.mockResolvedValue(1)
    mockUpdateTenantMigrationsState.mockResolvedValue(undefined)
    mockDeleteIfActiveExists.mockResolvedValue(undefined)
  })

  it('runs migrations and marks the tenant completed on success', async () => {
    await expect(RunMigrationsOnTenants.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockDeleteTenantConfig).toHaveBeenCalledWith('tenant-a')
    expect(mockDeleteTenantConfig.mock.invocationCallOrder[0]).toBeLessThan(
      mockGetTenantConfig.mock.invocationCallOrder[0]
    )
    expect(mockRunMigrationsOnTenant).toHaveBeenCalledWith({
      databaseUrl: 'postgres://tenant-db',
      tenantId: 'tenant-a',
      waitForLock: false,
      upToMigration: 'storage-schema',
      returnMigrationVersion: true,
    })
    expect(mockCompleteTenantMigrations).toHaveBeenCalledWith('tenant-a', {
      expectedMigrationVersion: null,
      migration: 'storage-schema',
    })
    expect(mockDeleteIfActiveExists).not.toHaveBeenCalled()
    expect(mockInfo).toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] completed for tenant tenant-a',
      expect.objectContaining({
        type: 'migrations',
        project: 'tenant-a',
        sbReqId: 'sb-req-123',
      })
    )
  })

  it('short-circuits when migrations are already up to date', async () => {
    mockAreMigrationsUpToDate.mockResolvedValue(true)

    await expect(RunMigrationsOnTenants.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockRunMigrationsOnTenant).not.toHaveBeenCalled()
    expect(mockCompleteTenantMigrations).not.toHaveBeenCalled()
    expect(mockUpdateTenantMigrationsState).not.toHaveBeenCalled()
    expect(mockDeleteIfActiveExists).not.toHaveBeenCalled()
  })

  it('does not log completion after the captured version loses its compare', async () => {
    mockGetTenantConfig.mockResolvedValue({
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: 'initialmigration',
    })
    mockCompleteTenantMigrations.mockResolvedValue(0)

    await expect(RunMigrationsOnTenants.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockCompleteTenantMigrations).toHaveBeenCalledWith('tenant-a', {
      expectedMigrationVersion: 'initialmigration',
      migration: 'storage-schema',
    })
    expect(mockInfo).not.toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] completed for tenant tenant-a',
      expect.anything()
    )
  })

  it('does not certify an unrecognized physical ledger position', async () => {
    const migration = 'future-migration'
    mockRunMigrationsOnTenant.mockResolvedValue(migration)
    mockCompleteTenantMigrations.mockResolvedValue(0)

    await expect(RunMigrationsOnTenants.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockCompleteTenantMigrations).toHaveBeenCalledWith('tenant-a', {
      expectedMigrationVersion: null,
      migration,
    })
    expect(mockInfo).not.toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] completed for tenant tenant-a',
      expect.anything()
    )
  })

  it('uses the observed ledger to repair an unrecognized failed control version', async () => {
    mockGetTenantConfig.mockResolvedValue({
      databaseUrl: 'postgres://tenant-db',
      migrationVersion: 'future-migration',
      migrationStatus: TenantMigrationStatus.FAILED,
    })

    await expect(RunMigrationsOnTenants.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockRunMigrationsOnTenant).toHaveBeenCalledTimes(1)
    expect(mockCompleteTenantMigrations).toHaveBeenCalledWith('tenant-a', {
      expectedMigrationVersion: 'future-migration',
      migration: 'storage-schema',
    })
    expect(mockUpdateTenantMigrationsState).not.toHaveBeenCalled()
  })

  it('returns without marking the tenant failed on lock timeout', async () => {
    mockRunMigrationsOnTenant.mockRejectedValue(ERRORS.LockTimeout())

    await expect(RunMigrationsOnTenants.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockUpdateTenantMigrationsState).not.toHaveBeenCalled()
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
    [0, TenantMigrationStatus.FAILED],
    [3, TenantMigrationStatus.FAILED_STALE],
  ])('on retry %s', (retryCount, state) => {
    it.each([
      ['migration failure', new Error('migration failed')],
      ['missing ledger', undefined],
      ['empty ledger', ''],
    ])('marks the tenant failed and rethrows on %s', async (_label, result) => {
      if (result instanceof Error) {
        mockRunMigrationsOnTenant.mockRejectedValue(result)
      } else {
        mockRunMigrationsOnTenant.mockResolvedValue(result)
      }

      await expect(RunMigrationsOnTenants.handle(makeJob({ retryCount }) as never)).rejects.toThrow(
        result instanceof Error ? result.message : 'Migration run returned no ledger position'
      )

      expect(mockCompleteTenantMigrations).not.toHaveBeenCalled()
      expect(mockUpdateTenantMigrationsState).toHaveBeenCalledWith('tenant-a', { state })
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
