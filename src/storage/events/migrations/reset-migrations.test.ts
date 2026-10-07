import { vi } from 'vitest'

const { mockGetTenantConfig, mockResetMigration, mockRunMigrationsSend, mockInfo, mockWarning } =
  vi.hoisted(() => ({
    mockGetTenantConfig: vi.fn(),
    mockResetMigration: vi.fn(),
    mockRunMigrationsSend: vi.fn(),
    mockInfo: vi.fn(),
    mockWarning: vi.fn(),
  }))

vi.mock('@internal/database', () => ({
  getTenantConfig: mockGetTenantConfig,
}))

vi.mock('@internal/database/migrations', async () => ({
  DBMigration: {
    'create-migrations-table': 0,
    'storage-schema': 2,
  },
  isDBMigrationName: (
    await vi.importActual<typeof import('@internal/database/migrations/guards')>(
      '@internal/database/migrations/guards'
    )
  ).isDBMigrationName,
  resetMigration: mockResetMigration,
}))

vi.mock('@internal/monitoring', () => ({
  logger: {},
  logSchema: {
    info: mockInfo,
    error: vi.fn(),
    warning: mockWarning,
  },
}))

vi.mock('../base-event', () => ({
  BaseEvent: class {},
}))

vi.mock('./run-migrations', () => ({
  RunMigrationsOnTenants: class {
    static send = mockRunMigrationsSend
  },
}))

import { ResetMigrationsOnTenant } from './reset-migrations'

function makeJob(overrides?: Partial<Record<string, unknown>>) {
  return {
    data: {
      tenantId: 'tenant-a',
      untilMigration: 'storage-schema',
      markCompletedTillMigration: 'create-migrations-table',
      sbReqId: 'sb-req-123',
      tenant: {
        ref: 'tenant-a',
      },
    },
    ...overrides,
  }
}

describe('ResetMigrationsOnTenant.handle', () => {
  beforeEach(() => {
    mockGetTenantConfig.mockResolvedValue({
      databaseUrl: 'postgres://tenant-db',
    })
    mockResetMigration.mockResolvedValue(true)
    mockRunMigrationsSend.mockResolvedValue(undefined)
  })

  it('threads sbReqId through logs and the follow-up migration job', async () => {
    await expect(ResetMigrationsOnTenant.handle(makeJob() as never)).resolves.toBeUndefined()

    expect(mockResetMigration).toHaveBeenCalledWith({
      tenantId: 'tenant-a',
      markCompletedTillMigration: 'create-migrations-table',
      untilMigration: 'storage-schema',
      databaseUrl: 'postgres://tenant-db',
    })
    expect(mockRunMigrationsSend).toHaveBeenCalledWith(
      expect.objectContaining({
        tenantId: 'tenant-a',
        singletonKey: 'tenant-a',
        sbReqId: 'sb-req-123',
      })
    )
    expect(mockInfo).toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] resetting migrations for tenant-a',
      expect.objectContaining({
        type: 'migrations',
        project: 'tenant-a',
        sbReqId: 'sb-req-123',
      })
    )
    expect(mockInfo).toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] reset successful for tenant-a',
      expect.objectContaining({
        type: 'migrations',
        project: 'tenant-a',
        sbReqId: 'sb-req-123',
      })
    )
  })

  it.each([
    ['untilMigration', { untilMigration: 'future-migration' }],
    ['markCompletedTillMigration', { markCompletedTillMigration: 'future-migration' }],
  ])('fails a reset whose %s is unknown without touching tenant state', async (_, data) => {
    const job = makeJob()
    Object.assign(job.data, data)

    await expect(ResetMigrationsOnTenant.handle(job as never)).rejects.toThrow(
      'Migration future-migration is unknown to this release'
    )

    expect(mockGetTenantConfig).not.toHaveBeenCalled()
    expect(mockResetMigration).not.toHaveBeenCalled()
    expect(mockRunMigrationsSend).not.toHaveBeenCalled()
    expect(mockWarning).toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] reset job targets an unknown migration, retrying',
      expect.objectContaining({ project: 'tenant-a' })
    )
  })

  it('accepts a reset without markCompletedTillMigration', async () => {
    const job = makeJob()
    delete (job.data as { markCompletedTillMigration?: string }).markCompletedTillMigration

    await expect(ResetMigrationsOnTenant.handle(job as never)).resolves.toBeUndefined()

    expect(mockResetMigration).toHaveBeenCalledWith(
      expect.objectContaining({
        untilMigration: 'storage-schema',
        markCompletedTillMigration: undefined,
      })
    )
  })
})
