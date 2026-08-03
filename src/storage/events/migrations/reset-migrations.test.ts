import type { JobContext } from '@supabase-labs/wave-core'
import { vi } from 'vitest'
import type { WirePayload } from '@internal/queue'
import type { ResetMigrationsPayload } from './reset-migrations'

const { mockGetTenantConfig, mockResetMigration, mockProduce, mockInfo } = vi.hoisted(() => ({
  mockGetTenantConfig: vi.fn(),
  mockResetMigration: vi.fn(),
  mockProduce: vi.fn(),
  mockInfo: vi.fn(),
}))

vi.mock('@internal/database', () => ({
  getTenantConfig: mockGetTenantConfig,
}))

vi.mock('@internal/database/migrations', () => ({
  DBMigration: {
    'create-migrations-table': 0,
    'storage-schema': 2,
  },
  resetMigration: mockResetMigration,
}))

vi.mock('@internal/monitoring', () => ({
  logger: {},
  logSchema: {
    info: mockInfo,
    error: vi.fn(),
    warning: vi.fn(),
  },
}))

// Minimal stand-in for `storageEvent`: enough class surface for TopicHandler and produce
// assertions (idempotencyKey stamping included), without pulling base.ts's storage/database
// import graph into the unit test.
vi.mock('../base', () => ({
  DEDUP_TTL_1H: 3_600_000,
  storageEvent: (opts: { type: string; idempotencyKey?: (data: never) => string }) =>
    class {
      static readonly eventType = opts.type
      readonly idempotencyKey?: string
      constructor(readonly data: never) {
        this.idempotencyKey = opts.idempotencyKey?.(data)
      }
    },
}))

vi.mock('../topics', () => ({
  TOPICS: {
    runMigrations: 'tenants-migrations-v2',
    resetMigrations: 'tenants-migrations-reset-v2',
  },
  systemRetry: (topic: string) => ({
    maxAttempts: 4,
    backoffMs: 5_000,
    deadLetter: `${topic}-dead-letter`,
  }),
}))

vi.mock('../queue', () => ({
  getStorageQueue: () => ({ produce: mockProduce }),
}))

import { ResetMigrationsHandler } from './reset-migrations'
import { RunMigrationsOnTenants } from './run-migrations'

function makeCtx(
  data: Partial<WirePayload<ResetMigrationsPayload>> = {}
): JobContext<WirePayload<ResetMigrationsPayload>> {
  return {
    topic: 'tenants-migrations-reset-v2',
    group: 'tenants-migrations-reset-v2',
    message: {
      id: 'job-1',
      data: {
        tenantId: 'tenant-a',
        untilMigration: 'storage-schema',
        markCompletedTillMigration: 'create-migrations-table',
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
      attempt: 1,
    },
    signal: new AbortController().signal,
    heartbeat: async () => {},
  }
}

describe('ResetMigrationsHandler.handle', () => {
  const handler = new ResetMigrationsHandler()

  beforeEach(() => {
    vi.clearAllMocks()

    mockGetTenantConfig.mockResolvedValue({
      databaseUrl: 'postgres://tenant-db',
    })
    mockResetMigration.mockResolvedValue(true)
    mockProduce.mockResolvedValue(undefined)
  })

  it('threads sbReqId through logs and the follow-up migration job', async () => {
    await expect(handler.handle(makeCtx())).resolves.toBeUndefined()

    expect(mockResetMigration).toHaveBeenCalledWith({
      tenantId: 'tenant-a',
      markCompletedTillMigration: 'create-migrations-table',
      untilMigration: 'storage-schema',
      databaseUrl: 'postgres://tenant-db',
    })
    expect(mockProduce).toHaveBeenCalledTimes(1)
    const produced = mockProduce.mock.calls[0][0]
    expect(produced).toBeInstanceOf(RunMigrationsOnTenants)
    expect(produced.data).toEqual(
      expect.objectContaining({
        tenantId: 'tenant-a',
        sbReqId: 'sb-req-123',
        tenant: { ref: 'tenant-a', host: '' },
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

  it('does not enqueue a follow-up migration when nothing was reset', async () => {
    mockResetMigration.mockResolvedValue(false)

    await expect(handler.handle(makeCtx())).resolves.toBeUndefined()

    expect(mockProduce).not.toHaveBeenCalled()
  })
})
