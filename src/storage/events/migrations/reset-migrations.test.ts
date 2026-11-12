import type { QueuedPayload, StorageJob } from '@internal/queue'
import { vi } from 'vitest'
import type { ResetMigrationsPayload } from './reset-migrations'

const { mockGetTenantConfig, mockResetMigration, mockProduce, mockInfo, mockWarning } = vi.hoisted(
  () => ({
    mockGetTenantConfig: vi.fn(),
    mockResetMigration: vi.fn(),
    mockProduce: vi.fn(),
    mockInfo: vi.fn(),
    mockWarning: vi.fn(),
  })
)

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

// Minimal stand-in for `storageEvent`: enough class surface for QueueHandler and produce
// assertions (idempotencyKey stamping included), without pulling base.ts's storage/database
// import graph into the unit test. The `send`/`batchSend` statics forward message instances to
// `mockProduce`, as the real ones do to the queue's `produce`.
vi.mock('../base', () => ({
  DEDUP_TTL_1H: 3_600_000,
  storageEvent: (opts: { type: string; idempotencyKey?: (data: never) => string }) =>
    class {
      static readonly eventType = opts.type
      readonly idempotencyKey?: string
      constructor(readonly data: never) {
        this.idempotencyKey = opts.idempotencyKey?.(data)
      }
      static send(payload: never, options?: unknown) {
        return options === undefined
          ? mockProduce(new this(payload))
          : mockProduce(new this(payload), options)
      }
      static batchSend(payloads: never[], options?: unknown) {
        const messages = payloads.map((payload) => new this(payload))
        return options === undefined ? mockProduce(messages) : mockProduce(messages, options)
      }
    },
}))

vi.mock('../registry', () => ({
  QUEUES: {
    runMigrations: 'tenants-migrations-v2',
    resetMigrations: 'tenants-migrations-reset-v2',
  },
  systemRetry: (name: string) => ({
    maxAttempts: 4,
    backoffMs: 5_000,
    deadLetter: `${name}-dead-letter`,
  }),
}))

import { ResetMigrationsHandler } from './reset-migrations'
import { RunMigrationsOnTenants } from './run-migrations'

function makeCtx(
  data: Partial<QueuedPayload<ResetMigrationsPayload>> = {}
): StorageJob<ResetMigrationsPayload> {
  return {
    queue: 'tenants-migrations-reset-v2',
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

  it.each([
    ['untilMigration', { untilMigration: 'future-migration' }],
    ['markCompletedTillMigration', { markCompletedTillMigration: 'future-migration' }],
  ])('fails a reset whose %s is unknown without touching tenant state', async (_, data) => {
    const ctx = makeCtx()
    Object.assign(ctx.message.data, data)

    await expect(handler.handle(ctx)).rejects.toThrow(
      'Migration future-migration is unknown to this release'
    )

    expect(mockGetTenantConfig).not.toHaveBeenCalled()
    expect(mockResetMigration).not.toHaveBeenCalled()
    expect(mockProduce).not.toHaveBeenCalled()
    expect(mockWarning).toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] reset job targets an unknown migration, retrying',
      expect.objectContaining({ project: 'tenant-a' })
    )
  })

  it('accepts a reset without markCompletedTillMigration', async () => {
    const ctx = makeCtx()
    delete (ctx.message.data as { markCompletedTillMigration?: string }).markCompletedTillMigration

    await expect(handler.handle(ctx)).resolves.toBeUndefined()

    expect(mockResetMigration).toHaveBeenCalledWith(
      expect.objectContaining({
        untilMigration: 'storage-schema',
        markCompletedTillMigration: undefined,
      })
    )
  })
})
