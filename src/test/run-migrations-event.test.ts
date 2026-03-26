const mockGetTenantConfig = jest.fn()
const mockAreMigrationsUpToDate = jest.fn()
const mockRunMigrationsOnTenant = jest.fn()
const mockUpdateTenantMigrationsState = jest.fn()
const mockDeleteIfActiveExists = jest.fn()
const mockInfo = jest.fn()
const mockError = jest.fn()

jest.mock('@internal/database', () => ({
  getTenantConfig: mockGetTenantConfig,
  TenantMigrationStatus: {
    COMPLETED: 'COMPLETED',
    FAILED: 'FAILED',
    FAILED_STALE: 'FAILED_STALE',
  },
}))

jest.mock('@internal/database/migrations', () => ({
  areMigrationsUpToDate: mockAreMigrationsUpToDate,
  runMigrationsOnTenant: mockRunMigrationsOnTenant,
  updateTenantMigrationsState: mockUpdateTenantMigrationsState,
}))

jest.mock('../storage/events/base-event', () => ({
  BaseEvent: class {
    static deleteIfActiveExists = mockDeleteIfActiveExists

    static getQueueName(this: { queueName: string }) {
      return this.queueName
    }
  },
}))

jest.mock('@internal/monitoring', () => ({
  logger: {},
  logSchema: {
    info: mockInfo,
    error: mockError,
    warning: jest.fn(),
  },
}))

import { TenantMigrationStatus } from '@internal/database'
import { ERRORS } from '@internal/errors'
import { RunMigrationsOnTenants } from '../storage/events/migrations/run-migrations'

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
    jest.clearAllMocks()

    mockGetTenantConfig.mockResolvedValue({
      databaseUrl: 'postgres://tenant-db',
    })
    mockAreMigrationsUpToDate.mockResolvedValue(false)
    mockRunMigrationsOnTenant.mockResolvedValue(undefined)
    mockUpdateTenantMigrationsState.mockResolvedValue(undefined)
    mockDeleteIfActiveExists.mockResolvedValue(undefined)
  })

  it('runs migrations and marks the tenant completed on success', async () => {
    await expect(RunMigrationsOnTenants.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockRunMigrationsOnTenant).toHaveBeenCalledWith({
      databaseUrl: 'postgres://tenant-db',
      tenantId: 'tenant-a',
      waitForLock: false,
      upToMigration: 'storage-schema',
    })
    expect(mockUpdateTenantMigrationsState).toHaveBeenCalledWith('tenant-a', {
      migration: 'storage-schema',
      state: TenantMigrationStatus.COMPLETED,
    })
    expect(mockDeleteIfActiveExists).not.toHaveBeenCalled()
  })

  it('short-circuits when migrations are already up to date', async () => {
    mockAreMigrationsUpToDate.mockResolvedValue(true)

    await expect(RunMigrationsOnTenants.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockRunMigrationsOnTenant).not.toHaveBeenCalled()
    expect(mockUpdateTenantMigrationsState).not.toHaveBeenCalled()
    expect(mockDeleteIfActiveExists).not.toHaveBeenCalled()
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
      })
    )
  })

  it('marks the tenant FAILED and rethrows when a retryable failure happens', async () => {
    mockRunMigrationsOnTenant.mockRejectedValue(new Error('migration failed'))

    await expect(RunMigrationsOnTenants.handle(makeJob() as never)).rejects.toThrow(
      'migration failed'
    )

    expect(mockUpdateTenantMigrationsState).toHaveBeenCalledWith('tenant-a', {
      state: TenantMigrationStatus.FAILED,
    })
    expect(mockDeleteIfActiveExists).toHaveBeenCalledWith(
      RunMigrationsOnTenants.getQueueName(),
      'migrations_tenant-a',
      'job-1'
    )
  })

  it('marks the tenant FAILED_STALE on the final retry before rethrowing', async () => {
    mockRunMigrationsOnTenant.mockRejectedValue(new Error('migration failed'))

    await expect(
      RunMigrationsOnTenants.handle(
        makeJob({
          retryCount: 3,
          retryLimit: 3,
        }) as never
      )
    ).rejects.toThrow('migration failed')

    expect(mockUpdateTenantMigrationsState).toHaveBeenCalledWith('tenant-a', {
      state: TenantMigrationStatus.FAILED_STALE,
    })
    expect(mockDeleteIfActiveExists).toHaveBeenCalledWith(
      RunMigrationsOnTenants.getQueueName(),
      'migrations_tenant-a',
      'job-1'
    )
  })
})
