import type { QueuedPayload, StorageJob } from '@internal/queue'
import { vi } from 'vitest'
import type { RunMigrationsPayload } from './run-migrations'

const {
  mockGetTenantConfig,
  mockDeleteTenantConfig,
  mockAreMigrationsUpToDate,
  mockRunMigrationsOnTenant,
  mockUpdateTenantMigrationsState,
  mockInfo,
  mockWarning,
  mockError,
} = vi.hoisted(() => ({
  mockGetTenantConfig: vi.fn(),
  mockDeleteTenantConfig: vi.fn(),
  mockAreMigrationsUpToDate: vi.fn(),
  mockRunMigrationsOnTenant: vi.fn(),
  mockUpdateTenantMigrationsState: vi.fn(),
  mockInfo: vi.fn(),
  mockWarning: vi.fn(),
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
  isDBMigrationName: (
    await vi.importActual<typeof import('@internal/database/migrations/guards')>(
      '@internal/database/migrations/guards'
    )
  ).isDBMigrationName,
  areMigrationsUpToDate: mockAreMigrationsUpToDate,
  runMigrationsOnTenant: mockRunMigrationsOnTenant,
  updateTenantMigrationsState: mockUpdateTenantMigrationsState,
}))

vi.mock('@internal/monitoring', () => ({
  logger: {},
  logSchema: {
    info: mockInfo,
    error: mockError,
    warning: mockWarning,
  },
}))

// Minimal stand-in for `storageEvent`: enough class surface for QueueHandler, without
// pulling base.ts's storage/database import graph into the unit test.
vi.mock('../base', () => ({
  DEDUP_TTL_1H: 3_600_000,
  storageEvent: (opts: { type: string }) =>
    class {
      static readonly eventType = opts.type
      constructor(readonly data: unknown) {}
    },
}))

vi.mock('../registry', () => ({
  QUEUES: { runMigrations: 'tenants-migrations-v2' },
  systemRetry: (name: string) => ({
    maxAttempts: 4,
    backoffMs: 5_000,
    deadLetter: `${name}-dead-letter`,
  }),
}))

import { TenantMigrationStatus } from '@internal/database'
import { ERRORS } from '@internal/errors'
import {
  MIGRATION_MAX_RUNTIME_SECONDS,
  migrationAttemptLimitSeconds,
  RunMigrationsHandler,
} from './run-migrations'

function makeCtx(
  attempt = 1,
  data: Partial<QueuedPayload<RunMigrationsPayload>> = {}
): StorageJob<RunMigrationsPayload> {
  return {
    queue: 'tenants-migrations-v2',
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

  it('fails a job targeting an unknown migration without touching tenant state', async () => {
    // A newer release's migration name: not a key of this release's DBMigration.
    const ctx = makeCtx(1, { upToMigration: 'future-migration' as never })

    await expect(handler.handle(ctx)).rejects.toThrow(
      'Migration future-migration is unknown to this release'
    )

    expect(mockDeleteTenantConfig).not.toHaveBeenCalled()
    expect(mockRunMigrationsOnTenant).not.toHaveBeenCalled()
    expect(mockUpdateTenantMigrationsState).not.toHaveBeenCalled()
    expect(mockWarning).toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] job targets an unknown migration, retrying',
      expect.objectContaining({ project: 'tenant-a' })
    )
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

    // v1: retryCount === retryLimit ⇒ FAILED_STALE. The queue's `attempt` counts deliveries, so
    // the last budgeted delivery is systemRetry's maxAttempts (4).
    await expect(handler.handle(makeCtx(4))).rejects.toThrow('migration failed')

    expect(mockUpdateTenantMigrationsState).toHaveBeenCalledWith('tenant-a', {
      state: TenantMigrationStatus.FAILED_STALE,
    })
  })

  it('warns when the attempt limit gives the delivery up under a run still going', async () => {
    const ac = new AbortController()
    let finishRun = () => {}
    mockRunMigrationsOnTenant.mockReturnValue(
      new Promise<void>((resolve) => {
        finishRun = resolve
      })
    )
    mockWarning.mockClear()

    const run = handler.handle({ ...makeCtx(), signal: ac.signal })
    await vi.waitFor(() => expect(mockRunMigrationsOnTenant).toHaveBeenCalled())
    ac.abort(new DOMException('consumeTimeout elapsed', 'TimeoutError'))

    expect(mockWarning).toHaveBeenCalledTimes(1)
    expect(mockWarning.mock.calls[0][1]).toBe(
      '[Migrations] run for tenant tenant-a outlived its attempt limit, delivery given up'
    )

    finishRun()
    await expect(run).resolves.toBeUndefined()
  })

  it('does not warn when the delivery is released for a shutdown', async () => {
    const ac = new AbortController()
    let finishRun = () => {}
    mockRunMigrationsOnTenant.mockReturnValue(
      new Promise<void>((resolve) => {
        finishRun = resolve
      })
    )
    mockWarning.mockClear()

    const run = handler.handle({ ...makeCtx(), signal: ac.signal })
    await vi.waitFor(() => expect(mockRunMigrationsOnTenant).toHaveBeenCalled())
    ac.abort(new Error('worker stopping'))
    finishRun()
    await run

    expect(mockWarning).not.toHaveBeenCalled()
  })
})

describe('migrationAttemptLimitSeconds', () => {
  it('keeps the full run length on pg-boss', () => {
    expect(migrationAttemptLimitSeconds('pgboss')).toBe(MIGRATION_MAX_RUNTIME_SECONDS)
  })

  it('stays under the twelve hours SQS holds a delivery for', () => {
    expect(migrationAttemptLimitSeconds('sqs')).toBe(11.5 * 3600)
    expect(migrationAttemptLimitSeconds('sqs')).toBeLessThan(12 * 3600)
  })
})
