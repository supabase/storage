const mockBatchSend = jest.fn()
const mockWarning = jest.fn()
const mockError = jest.fn()

jest.mock('../internal/database/tenant', () => ({
  getTenantConfig: jest.fn(),
  TenantMigrationStatus: {
    FAILED_STALE: 'FAILED_STALE',
  },
}))

jest.mock('@internal/database/migrations/migrate', () => ({
  areMigrationsUpToDate: jest.fn(),
}))

jest.mock('@storage/events', () => ({
  RunMigrationsOnTenants: class {
    static batchSend = mockBatchSend
    payload: Record<string, unknown>

    constructor(payload: Record<string, unknown>) {
      this.payload = payload
    }
  },
}))

jest.mock('../internal/monitoring', () => ({
  logger: {},
  logSchema: {
    info: jest.fn(),
    warning: mockWarning,
    error: mockError,
  },
}))

import { areMigrationsUpToDate } from '@internal/database/migrations/migrate'
import { ERRORS } from '@internal/errors'
import { RunMigrationsOnTenants } from '@storage/events'
import { ProgressiveMigrations } from '../internal/database/migrations/progressive'
import { getTenantConfig } from '../internal/database/tenant'

class TestProgressiveMigrations extends ProgressiveMigrations {
  seed(...tenants: string[]) {
    this.tenants.push(...tenants)
  }

  pending() {
    return [...this.tenants]
  }

  isEmitting() {
    return this.emittingJobs
  }

  flush(maxJobs: number) {
    return this.createJobs(maxJobs)
  }
}

const mockGetTenantConfig = jest.mocked(getTenantConfig)
const mockAreMigrationsUpToDate = jest.mocked(areMigrationsUpToDate)
const mockRunMigrationsBatchSend = jest.mocked(RunMigrationsOnTenants.batchSend)

describe('ProgressiveMigrations', () => {
  beforeEach(() => {
    jest.clearAllMocks()

    mockGetTenantConfig.mockResolvedValue({
      migrationStatus: undefined,
      syncMigrationsDone: false,
    } as Awaited<ReturnType<typeof getTenantConfig>>)
    mockAreMigrationsUpToDate.mockResolvedValue(false)
  })

  it('keeps queued tenants and resets emittingJobs when batchSend fails', async () => {
    mockRunMigrationsBatchSend
      .mockRejectedValueOnce(new Error('queue unavailable'))
      .mockResolvedValueOnce(undefined as never)

    const migrations = new TestProgressiveMigrations({
      maxSize: 10,
      interval: 1000,
      watch: false,
    })

    migrations.seed('tenant-a')

    await expect(migrations.flush(1)).rejects.toThrow('queue unavailable')
    expect(migrations.pending()).toEqual(['tenant-a'])
    expect(migrations.isEmitting()).toBe(false)

    await expect(migrations.flush(1)).resolves.toBeUndefined()
    expect(migrations.pending()).toEqual([])
    expect(migrations.isEmitting()).toBe(false)
    expect(mockRunMigrationsBatchSend).toHaveBeenCalledTimes(2)
  })

  it('logs batch enqueue failures at the caller boundary', async () => {
    mockRunMigrationsBatchSend.mockRejectedValueOnce(new Error('queue unavailable'))

    const migrations = new TestProgressiveMigrations({
      maxSize: 10,
      interval: 1000,
      watch: false,
    })

    migrations.seed('tenant-a')

    await expect(migrations.drain()).resolves.toBeUndefined()

    expect(migrations.pending()).toEqual(['tenant-a'])
    expect(migrations.isEmitting()).toBe(false)
    expect(mockError).toHaveBeenCalledTimes(1)
    expect(mockError).toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] Error creating migration jobs',
      expect.objectContaining({
        type: 'migrations',
      })
    )
  })

  it('keeps new tenants queued while a batch is in flight and ignores duplicate adds', async () => {
    const deferredBatch = Promise.withResolvers<void>()
    mockRunMigrationsBatchSend.mockReturnValueOnce(deferredBatch.promise as never)

    const migrations = new TestProgressiveMigrations({
      maxSize: 10,
      interval: 1000,
      watch: false,
    })

    migrations.seed('tenant-a')

    const flushPromise = migrations.flush(1)
    await new Promise((resolve) => setImmediate(resolve))

    expect(mockRunMigrationsBatchSend).toHaveBeenCalledTimes(1)
    expect(migrations.isEmitting()).toBe(true)

    migrations.addTenant('tenant-a')
    migrations.addTenant('tenant-b')

    expect(migrations.pending()).toEqual(['tenant-a', 'tenant-b'])

    deferredBatch.resolve()

    await expect(flushPromise).resolves.toBeUndefined()
    expect(migrations.pending()).toEqual(['tenant-b'])
    expect(migrations.isEmitting()).toBe(false)
  })

  it('serializes drain with an in-flight batch and drains the remaining tenants after it finishes', async () => {
    const deferredBatch = Promise.withResolvers<void>()
    mockRunMigrationsBatchSend
      .mockReturnValueOnce(deferredBatch.promise as never)
      .mockResolvedValueOnce(undefined as never)

    const migrations = new TestProgressiveMigrations({
      maxSize: 1,
      interval: 1000,
      watch: false,
    })

    migrations.seed('tenant-a', 'tenant-b')

    const flushPromise = migrations.flush(1)
    await new Promise((resolve) => setImmediate(resolve))

    const drainPromise = migrations.drain()

    expect(mockRunMigrationsBatchSend).toHaveBeenCalledTimes(1)
    expect(migrations.isEmitting()).toBe(true)

    deferredBatch.resolve()

    await expect(Promise.all([flushPromise, drainPromise])).resolves.toEqual([undefined, undefined])

    expect(mockRunMigrationsBatchSend).toHaveBeenCalledTimes(2)
    expect(
      (mockRunMigrationsBatchSend.mock.calls[0][0][0] as { payload: { tenantId: string } }).payload
    ).toMatchObject({
      tenantId: 'tenant-a',
    })
    expect(
      (mockRunMigrationsBatchSend.mock.calls[1][0][0] as { payload: { tenantId: string } }).payload
    ).toMatchObject({
      tenantId: 'tenant-b',
    })
    expect(migrations.pending()).toEqual([])
    expect(migrations.isEmitting()).toBe(false)
  })

  it('starts a follow-up run when drain is requested in a late microtask after a batch settles', async () => {
    const migrations = new TestProgressiveMigrations({
      maxSize: 1,
      interval: 1000,
      watch: false,
    })

    mockRunMigrationsBatchSend
      .mockImplementationOnce(async () => {
        queueMicrotask(() => {
          migrations.addTenant('tenant-b')
          void migrations.drain()
        })
      })
      .mockResolvedValueOnce(undefined as never)

    migrations.seed('tenant-a')

    await expect(migrations.flush(1)).resolves.toBeUndefined()
    await new Promise((resolve) => setImmediate(resolve))

    expect(mockRunMigrationsBatchSend).toHaveBeenCalledTimes(2)
    expect(
      (mockRunMigrationsBatchSend.mock.calls[0][0][0] as { payload: { tenantId: string } }).payload
    ).toMatchObject({
      tenantId: 'tenant-a',
    })
    expect(
      (mockRunMigrationsBatchSend.mock.calls[1][0][0] as { payload: { tenantId: string } }).payload
    ).toMatchObject({
      tenantId: 'tenant-b',
    })
    expect(migrations.pending()).toEqual([])
    expect(migrations.isEmitting()).toBe(false)
  })

  it('moves prep-failed tenants to the back so later tenants can still be scheduled', async () => {
    mockGetTenantConfig.mockImplementation(async (tenantId) => {
      if (tenantId === 'tenant-b') {
        throw new Error('tenant lookup failed')
      }

      return {
        migrationStatus: undefined,
        syncMigrationsDone: false,
      } as Awaited<ReturnType<typeof getTenantConfig>>
    })

    const migrations = new TestProgressiveMigrations({
      maxSize: 1,
      interval: 1000,
      watch: false,
    })

    migrations.seed('tenant-b', 'tenant-a')

    await expect(migrations.flush(1)).resolves.toBeUndefined()
    expect(migrations.pending()).toEqual(['tenant-a', 'tenant-b'])

    await expect(migrations.flush(1)).resolves.toBeUndefined()
    expect(mockRunMigrationsBatchSend).toHaveBeenCalledTimes(1)
    expect(
      (mockRunMigrationsBatchSend.mock.calls[0][0][0] as { payload: { tenantId: string } }).payload
    ).toMatchObject({
      tenantId: 'tenant-a',
    })
    expect(migrations.pending()).toEqual(['tenant-b'])
  })

  it('keeps tenants queued when preparing a migration job fails', async () => {
    mockGetTenantConfig.mockImplementation(async (tenantId) => {
      if (tenantId === 'tenant-b') {
        throw new Error('tenant lookup failed')
      }

      return {
        migrationStatus: undefined,
        syncMigrationsDone: false,
      } as Awaited<ReturnType<typeof getTenantConfig>>
    })

    const migrations = new TestProgressiveMigrations({
      maxSize: 10,
      interval: 1000,
      watch: false,
    })

    migrations.seed('tenant-a', 'tenant-b')

    await expect(migrations.flush(2)).resolves.toBeUndefined()

    expect(mockRunMigrationsBatchSend).toHaveBeenCalledTimes(1)
    expect(mockRunMigrationsBatchSend.mock.calls[0][0]).toHaveLength(1)
    expect(
      (mockRunMigrationsBatchSend.mock.calls[0][0][0] as { payload: { tenantId: string } }).payload
    ).toMatchObject({
      tenantId: 'tenant-a',
    })
    expect(migrations.pending()).toEqual(['tenant-b'])
    expect(migrations.isEmitting()).toBe(false)
    expect(mockWarning).toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] Failed to prepare migration job for tenant tenant-b; keeping tenant queued for retry',
      expect.objectContaining({
        type: 'migrations',
        project: 'tenant-b',
      })
    )
  })

  it('drops tenants whose config no longer exists instead of retrying forever', async () => {
    mockGetTenantConfig.mockImplementation(async (tenantId) => {
      if (tenantId === 'tenant-b') {
        throw ERRORS.MissingTenantConfig(tenantId)
      }

      return {
        migrationStatus: undefined,
        syncMigrationsDone: false,
      } as Awaited<ReturnType<typeof getTenantConfig>>
    })

    const migrations = new TestProgressiveMigrations({
      maxSize: 10,
      interval: 1000,
      watch: false,
    })

    migrations.seed('tenant-a', 'tenant-b')

    await expect(migrations.flush(2)).resolves.toBeUndefined()

    expect(mockRunMigrationsBatchSend).toHaveBeenCalledTimes(1)
    expect(mockRunMigrationsBatchSend.mock.calls[0][0]).toHaveLength(1)
    expect(
      (mockRunMigrationsBatchSend.mock.calls[0][0][0] as { payload: { tenantId: string } }).payload
    ).toMatchObject({
      tenantId: 'tenant-a',
    })
    expect(migrations.pending()).toEqual([])
    expect(migrations.isEmitting()).toBe(false)
    expect(mockWarning).toHaveBeenCalledWith(
      expect.anything(),
      '[Migrations] Failed to prepare migration job for tenant tenant-b; dropping tenant from queue because it no longer exists',
      expect.objectContaining({
        type: 'migrations',
        project: 'tenant-b',
      })
    )
  })
})
