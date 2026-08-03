import type { WirePayload } from '@internal/queue'
import type { JobContext } from '@supabase-labs/wave-core'
import { vi } from 'vitest'
import type { RunMigrationsPayload } from './run-migrations'

const {
  mockGetTenantConfig,
  mockDeleteTenantConfig,
  mockAreMigrationsUpToDate,
  mockRunMigrationsOnTenant,
  mockUpdateTenantMigrationsState,
  mockInfo,
  mockError,
} = vi.hoisted(() => ({
  mockGetTenantConfig: vi.fn(),
  mockDeleteTenantConfig: vi.fn(),
  mockAreMigrationsUpToDate: vi.fn(),
  mockRunMigrationsOnTenant: vi.fn(),
  mockUpdateTenantMigrationsState: vi.fn(),
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
  runMigrationsOnTenant: mockRunMigrationsOnTenant,
  updateTenantMigrationsState: mockUpdateTenantMigrationsState,
}))

vi.mock('@internal/monitoring', () => ({
  logger: {},
  logSchema: {
    info: mockInfo,
    error: mockError,
    warning: vi.fn(),
  },
}))

// Minimal stand-in for `storageEvent`: enough class surface for TopicHandler, without
// pulling base.ts's storage/database import graph into the unit test.
vi.mock('../base', () => ({
  DEDUP_TTL_1H: 3_600_000,
  storageEvent: (opts: { type: string }) =>
    class {
      static readonly eventType = opts.type
      constructor(readonly data: unknown) {}
    },
}))

vi.mock('../topics', () => ({
  TOPICS: { runMigrations: 'tenants-migrations-v2' },
  systemRetry: (topic: string) => ({
    maxAttempts: 4,
    backoffMs: 5_000,
    deadLetter: `${topic}-dead-letter`,
  }),
}))

import { TenantMigrationStatus } from '@internal/database'
import { ERRORS } from '@internal/errors'
import { RunMigrationsHandler } from './run-migrations'

function makeCtx(
  attempt = 1,
  data: Partial<WirePayload<RunMigrationsPayload>> = {}
): JobContext<WirePayload<RunMigrationsPayload>> {
  return {
    topic: 'tenants-migrations-v2',
    group: 'tenants-migrations-v2',
    message: {
      id: 'job-1',
      data: {
        tenantId: 'tenant-a',
        upToMigration: 'storage-schema',
        sbReqId: 'sb-req-123',
        tenant: {
          ref: 'tenant-a',
          host: '',
        },
        region: 'local',
        ...data,
      },
      headers: {},
      timestamp: 0,
      attempt,
    },
    signal: new AbortController().signal,
    heartbeat: async () => {},
  }
}

describe('RunMigrationsHandler.handle', () => {
  const handler = new RunMigrationsHandler()

  beforeEach(() => {
    vi.clearAllMocks()

    mockGetTenantConfig.mockResolvedValue({
      databaseUrl: 'postgres://tenant-db',
    })
    mockAreMigrationsUpToDate.mockResolvedValue(false)
    mockRunMigrationsOnTenant.mockResolvedValue(undefined)
    mockUpdateTenantMigrationsState.mockResolvedValue(undefined)
  })

  it('runs migrations and marks the tenant completed on success', async () => {
    await expect(handler.handle(makeCtx())).resolves.toBeUndefined()

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
    expect(mockUpdateTenantMigrationsState).toHaveBeenCalledWith('tenant-a', {
      migration: 'storage-schema',
      state: TenantMigrationStatus.COMPLETED,
    })
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

    await expect(handler.handle(makeCtx())).resolves.toBeUndefined()

    expect(mockRunMigrationsOnTenant).not.toHaveBeenCalled()
    expect(mockUpdateTenantMigrationsState).not.toHaveBeenCalled()
  })

  it('returns without marking the tenant failed on lock timeout', async () => {
    mockRunMigrationsOnTenant.mockRejectedValue(ERRORS.LockTimeout())

    await expect(handler.handle(makeCtx())).resolves.toBeUndefined()

    expect(mockUpdateTenantMigrationsState).not.toHaveBeenCalled()
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

  it('marks the tenant FAILED and rethrows when a retryable failure happens', async () => {
    mockRunMigrationsOnTenant.mockRejectedValue(new Error('migration failed'))

    await expect(handler.handle(makeCtx())).rejects.toThrow('migration failed')

    expect(mockUpdateTenantMigrationsState).toHaveBeenCalledWith('tenant-a', {
      state: TenantMigrationStatus.FAILED,
    })
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

  it('marks the tenant FAILED_STALE on the last budgeted delivery before rethrowing', async () => {
    mockRunMigrationsOnTenant.mockRejectedValue(new Error('migration failed'))

    // v1: retryCount === retryLimit ⇒ FAILED_STALE. Wave's `attempt` counts deliveries, so
    // the last budgeted delivery is systemRetry's maxAttempts (4).
    await expect(handler.handle(makeCtx(4))).rejects.toThrow('migration failed')

    expect(mockUpdateTenantMigrationsState).toHaveBeenCalledWith('tenant-a', {
      state: TenantMigrationStatus.FAILED_STALE,
    })
  })
})
