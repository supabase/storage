import { AsyncAbortController } from '@internal/concurrency'
import { getTenantConfig } from '@internal/database'
import { logger, logSchema } from '@internal/monitoring'
import { lifecycleRunDuration } from '@internal/monitoring/metrics'
import { type BasePayload, Queue } from '@internal/queue'
import { vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  config: {
    storageVersioningEnabled: false,
    storageLifecyclePaused: false,
    storageLifecycleClaimLeaseMs: 120_000,
    storageLifecycleJobBudgetMs: 20_000,
    isMultitenant: true,
    tenantId: 'single-tenant',
    storageS3Bucket: 'data',
    storageS3MaxSockets: 10,
    storageBackendType: 's3',
    region: 'test',
    pgQueueEnable: true,
    pgQueueEnableWorkers: true,
  },
  central: {
    claimDueTenantsWithinLeaseLimit: vi.fn(),
    releaseTenantDispatch: vi.fn(),
    recordExpiredTenantDispatch: vi.fn(),
    completeTenantDispatch: vi.fn(),
    failTenantDispatch: vi.fn(),
    hasObjectVersioningFeature: vi.fn(),
  },
  getQueue: vi.fn(),
  transaction: { query: vi.fn(), commit: vi.fn(), rollback: vi.fn() },
  beginTransaction: vi.fn(),
  executorRun: vi.fn(),
  retryAbandoned: vi.fn(),
  tenantHasFeature: vi.fn(),
}))

vi.mock('../../../config', () => ({
  getConfig: () => mocks.config,
}))

vi.mock('@internal/database', () => ({
  LifecycleTenantStorePg: class {
    claimDueTenantsWithinLeaseLimit = mocks.central.claimDueTenantsWithinLeaseLimit
    releaseTenantDispatch = mocks.central.releaseTenantDispatch
    recordExpiredTenantDispatch = mocks.central.recordExpiredTenantDispatch
    completeTenantDispatch = mocks.central.completeTenantDispatch
    failTenantDispatch = mocks.central.failTenantDispatch
    hasObjectVersioningFeature = mocks.central.hasObjectVersioningFeature
  },
  multitenantPgExecutor: { beginTransaction: mocks.beginTransaction },
  getPostgresConnection: vi.fn(),
  getServiceKeyUser: vi.fn(),
  getTenantConfig: vi.fn(),
  tenantHasFeature: mocks.tenantHasFeature,
}))

vi.mock('@internal/monitoring', () => ({
  logger: {
    error: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
  },
  logSchema: {
    error: vi.fn(),
    info: vi.fn(),
    warning: vi.fn(),
  },
}))

vi.mock('../../lifecycle/abandoned-artifacts', () => ({
  retryAbandonedLifecycleArtifacts: mocks.retryAbandoned,
}))

vi.mock('../../lifecycle/executor', () => ({
  LIFECYCLE_EVALUATION_INTERVAL_MS: 24 * 60 * 60 * 1000,
  evaluationJitterMs: () => 1_234,
  lifecycleErrorMessage: (error: unknown) =>
    (error instanceof Error ? error.message : String(error)).slice(0, 1000),
  NoncurrentLifecycleExecutor: class {
    run = mocks.executorRun
  },
}))

vi.mock('../base-event', async () => {
  const { Event } = await vi.importActual<typeof import('@internal/queue')>('@internal/queue')

  return {
    BaseEvent: class<T extends Omit<BasePayload, '$version'>> extends Event<T> {
      protected static createStorage = vi.fn()
    },
  }
})

import { LifecycleBucketRun } from './bucket-run'
import { dispatchDueLifecycleTenants, startLifecycleScheduler } from './scheduler'
import { LifecycleTenantRun } from './tenant-run'

const tenant = { ref: 'tenant-a', host: '' }
const claimId = '5bd8d514-9db1-4b1d-98a4-4a56fc569d95'
const claimUntil = new Date('2026-01-01T00:00:00.000Z')
const bucketId = 'bucket-a'
const dispatchState = (nextDispatchAt: Date | null = null) => ({
  nextDispatchAt,
  maxFailureCount: 0,
  oldestInFlightStartedAt: null,
  hasAbandonedArtifacts: false,
})
const evaluationIntervalMs = 24 * 60 * 60 * 1000
const revisitMs = evaluationIntervalMs + 1_234
const schedulers: AsyncAbortController[] = []

function startScheduler(controller = new AsyncAbortController()) {
  vi.useFakeTimers()
  schedulers.push(controller)
  startLifecycleScheduler(controller.signal)
  return controller
}

function job<T>(data: T, retryCount = 0, startedOn = new Date()) {
  return { id: 'job-a', data, retryCount, retryLimit: 10, startedOn } as never
}

function mockCreateStorage(target: unknown, storage: unknown) {
  return vi
    .spyOn(target as { createStorage(payload: BasePayload): Promise<unknown> }, 'createStorage')
    .mockResolvedValue(storage)
}

function mockTenantDb(buckets: string[] = [], state: object = dispatchState()) {
  const database = {
    hasMigration: vi.fn().mockResolvedValue(true),
    listDueLifecycleBuckets: vi.fn().mockResolvedValue(buckets),
    findLifecycleDispatchState: vi.fn().mockResolvedValue(state),
  }
  const destroyConnection = vi.fn()
  const storage = { db: { asSuperUser: () => database, destroyConnection } }
  mockCreateStorage(LifecycleTenantRun, storage)
  return { database, destroyConnection, storage }
}

function mockClaim(reclaimed?: boolean) {
  mocks.central.claimDueTenantsWithinLeaseLimit.mockResolvedValue([
    { tenantId: tenant.ref, claimId, claimUntil, reclaimed },
  ])
}

function completedNextDispatchAt(call = 0) {
  return Date.parse(mocks.central.completeTenantDispatch.mock.calls[call][2])
}

describe('lifecycle scheduling and events', () => {
  beforeEach(() => {
    mocks.config.isMultitenant = true
    mocks.config.pgQueueEnable = true
    mocks.config.pgQueueEnableWorkers = true
    mocks.config.storageVersioningEnabled = false
    mocks.config.storageLifecyclePaused = false
    mocks.central.claimDueTenantsWithinLeaseLimit.mockResolvedValue([])
    mocks.central.completeTenantDispatch.mockResolvedValue(true)
    mocks.central.releaseTenantDispatch.mockResolvedValue(true)
    mocks.central.recordExpiredTenantDispatch.mockResolvedValue(true)
    mocks.central.failTenantDispatch.mockResolvedValue(true)
    mocks.central.hasObjectVersioningFeature.mockResolvedValue(false)
    mocks.executorRun.mockResolvedValue({ status: 'COMPLETED' })
    mocks.tenantHasFeature.mockResolvedValue(true)
    mocks.getQueue.mockResolvedValue({ name: 'lifecycle:tenant:run-v1' })
    mocks.transaction = { query: vi.fn(), commit: vi.fn(), rollback: vi.fn() }
    mocks.beginTransaction.mockResolvedValue(mocks.transaction)
    vi.spyOn(Queue, 'getInstance').mockReturnValue({ getQueue: mocks.getQueue } as never)
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
  })

  afterEach(async () => {
    await Promise.all(schedulers.splice(0).map((controller) => controller.abortAsync()))
    vi.useRealTimers()
  })

  test('claims central tenants and sends only the central dispatch fence', async () => {
    mockClaim()
    const send = vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue('tenant-job')

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(mocks.central.claimDueTenantsWithinLeaseLimit).toHaveBeenCalledWith(
      100,
      180_000,
      expect.any(AbortSignal)
    )
    expect(send).toHaveBeenCalledWith({ tenant, centralClaim: { claimId } })
    expect(mocks.central.recordExpiredTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.releaseTenantDispatch).not.toHaveBeenCalled()
    expect(logSchema.warning).not.toHaveBeenCalled()
  })

  test('single-tenant deployments dispatch the configured tenant without a central claim', async () => {
    mocks.config.isMultitenant = false
    const send = vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue('tenant-job')

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(send).toHaveBeenCalledWith({ tenant: { ref: 'single-tenant', host: '' } })
    expect(mocks.central.claimDueTenantsWithinLeaseLimit).not.toHaveBeenCalled()
  })

  test('releases a central claim with backoff when the tenant enqueue fails', async () => {
    mockClaim(false)
    vi.spyOn(LifecycleTenantRun, 'send').mockRejectedValue(new Error('queue unavailable'))

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(mocks.central.failTenantDispatch).toHaveBeenCalledWith(
      'tenant-a',
      claimId,
      { code: 'tenant_dispatch_enqueue_failed', message: 'queue unavailable' },
      claimUntil.toISOString()
    )
  })

  test('keeps a reclaimed claim when re-sending fails', async () => {
    const error = new Error('queue unavailable')
    mockClaim(true)
    vi.spyOn(LifecycleTenantRun, 'send').mockRejectedValue(error)

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(logSchema.error).toHaveBeenCalledWith(
      logger,
      '[Lifecycle] Failed to enqueue tenant dispatcher',
      { type: 'event', project: 'tenant-a', error }
    )
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.releaseTenantDispatch).not.toHaveBeenCalled()
  })

  test('releases a fresh claim for the next poll when exactly-once drops its job', async () => {
    mockClaim(false)
    vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue(null)
    vi.useFakeTimers({ toFake: ['Date'] })

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(mocks.central.releaseTenantDispatch).toHaveBeenCalledExactlyOnceWith(
      'tenant-a',
      claimId,
      new Date(Date.now() + 60_000).toISOString(),
      claimUntil.toISOString()
    )
    expect(mocks.central.recordExpiredTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(logSchema.error).not.toHaveBeenCalled()
    expect(logSchema.warning).not.toHaveBeenCalled()
  })

  test('leaves a reclaimed claim to its queued job when exactly-once drops the re-send', async () => {
    mockClaim(true)
    vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue(null)
    vi.useFakeTimers({ toFake: ['Date'] })

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(mocks.central.releaseTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.recordExpiredTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(logSchema.error).not.toHaveBeenCalled()
    expect(logSchema.warning).not.toHaveBeenCalled()
  })

  test("records a lease expiry only when a reclaimed claim's re-send creates a new job", async () => {
    mockClaim(true)
    vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue('tenant-job')

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(mocks.central.recordExpiredTenantDispatch).toHaveBeenCalledExactlyOnceWith(
      'tenant-a',
      claimId
    )
    expect(mocks.central.releaseTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(logSchema.error).not.toHaveBeenCalled()
    expect(logSchema.warning).toHaveBeenCalledExactlyOnceWith(
      logger,
      '[Lifecycle] Tenant dispatch re-enqueued after claim expiry',
      { type: 'event', project: 'tenant-a' }
    )
  })

  test.each([
    {
      name: 'fresh-claim release',
      reclaimed: false,
      jobId: null,
      write: 'releaseTenantDispatch',
    },
    {
      name: 'reclaimed-claim expiry record',
      reclaimed: true,
      jobId: 'tenant-job',
      write: 'recordExpiredTenantDispatch',
    },
  ] as const)(
    'logs a failed $name without rejecting the poll',
    async ({ reclaimed, jobId, write }) => {
      const writeError = new Error('central database unavailable')
      mockClaim(reclaimed)
      mocks.central[write].mockRejectedValue(writeError)
      vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue(jobId)

      await expect(
        dispatchDueLifecycleTenants(new AbortController().signal)
      ).resolves.toBeUndefined()

      expect(logSchema.error).toHaveBeenCalledWith(
        logger,
        '[Lifecycle] Failed to update tenant dispatch claim',
        { type: 'event', project: 'tenant-a', error: writeError }
      )
    }
  )

  test.each([
    { lifecycle: true, queue: false, workers: true, warns: true },
    { lifecycle: true, queue: false, workers: false, warns: true },
    { lifecycle: true, queue: true, workers: false, warns: false },
    { lifecycle: false, queue: false, workers: true, warns: false },
  ])(
    'does not start a timer and warns=$warns when %o',
    async ({ lifecycle, queue, workers, warns }) => {
      mocks.config.storageVersioningEnabled = lifecycle
      mocks.config.pgQueueEnable = queue
      mocks.config.pgQueueEnableWorkers = workers
      const send = vi.spyOn(LifecycleTenantRun, 'send')
      startScheduler()
      await vi.advanceTimersByTimeAsync(180_000)

      if (warns) {
        expect(logSchema.warning).toHaveBeenCalledExactlyOnceWith(
          logger,
          expect.stringContaining('PG_QUEUE_ENABLE'),
          { type: 'event' }
        )
      } else {
        expect(logSchema.warning).not.toHaveBeenCalled()
      }
      expect(mocks.central.claimDueTenantsWithinLeaseLimit).not.toHaveBeenCalled()
      expect(send).not.toHaveBeenCalled()
      expect(vi.getTimerCount()).toBe(0)
    }
  )

  test('does not claim tenants or start a timer when lifecycle is paused', async () => {
    mocks.config.storageLifecyclePaused = true
    const send = vi.spyOn(LifecycleTenantRun, 'send')
    startScheduler()
    await vi.advanceTimersByTimeAsync(180_000)

    expect(logSchema.warning).toHaveBeenCalledExactlyOnceWith(
      logger,
      expect.stringContaining('STORAGE_LIFECYCLE_PAUSED'),
      { type: 'event' }
    )
    expect(mocks.central.claimDueTenantsWithinLeaseLimit).not.toHaveBeenCalled()
    expect(send).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)
  })

  test('does not dispatch or start a timer with an already aborted signal', async () => {
    const controller = new AsyncAbortController()
    controller.abort()
    startScheduler(controller)
    await dispatchDueLifecycleTenants(controller.signal)

    expect(mocks.central.claimDueTenantsWithinLeaseLimit).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)
  })

  test('polls immediately, then about every minute with jitter, and stops on abort', async () => {
    vi.mocked(Math.random).mockReturnValueOnce(0).mockReturnValueOnce(0.9)
    const controller = startScheduler()
    await vi.advanceTimersByTimeAsync(0)
    expect(mocks.central.claimDueTenantsWithinLeaseLimit).toHaveBeenCalledTimes(1)

    await vi.advanceTimersByTimeAsync(54_999)
    expect(mocks.central.claimDueTenantsWithinLeaseLimit).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1)
    expect(mocks.central.claimDueTenantsWithinLeaseLimit).toHaveBeenCalledTimes(2)
    await vi.advanceTimersByTimeAsync(64_000)
    expect(mocks.central.claimDueTenantsWithinLeaseLimit).toHaveBeenCalledTimes(3)

    await controller.abortAsync()
    expect(vi.getTimerCount()).toBe(0)
    await vi.advanceTimersByTimeAsync(180_000)
    expect(mocks.central.claimDueTenantsWithinLeaseLimit).toHaveBeenCalledTimes(3)
  })

  test('retries a failed poll on the next timer', async () => {
    const error = new Error('central database unavailable')
    mocks.central.claimDueTenantsWithinLeaseLimit.mockRejectedValueOnce(error)
    startScheduler()
    await vi.advanceTimersByTimeAsync(0)

    expect(logSchema.error).toHaveBeenCalledWith(logger, '[Lifecycle] Scheduler poll failed', {
      type: 'event',
      error,
    })
    await vi.advanceTimersByTimeAsync(60_000)
    expect(mocks.central.claimDueTenantsWithinLeaseLimit).toHaveBeenCalledTimes(2)
    expect(vi.getTimerCount()).toBe(1)
  })

  test('never overlaps a slow poll', async () => {
    const pendingClaim = Promise.withResolvers<[]>()
    mocks.central.claimDueTenantsWithinLeaseLimit.mockReturnValueOnce(pendingClaim.promise)
    startScheduler()
    try {
      await vi.advanceTimersByTimeAsync(180_000)
      expect(mocks.central.claimDueTenantsWithinLeaseLimit).toHaveBeenCalledTimes(1)
      expect(vi.getTimerCount()).toBe(0)
    } finally {
      pendingClaim.resolve([])
    }
    await vi.advanceTimersByTimeAsync(60_000)
    expect(mocks.central.claimDueTenantsWithinLeaseLimit).toHaveBeenCalledTimes(2)
  })

  test('drains all claimed sends before queue shutdown, even if releasing a failed claim fails', async () => {
    const recoveryError = new Error('central database unavailable')
    const pendingSend = Promise.withResolvers<string>()
    mocks.central.claimDueTenantsWithinLeaseLimit.mockResolvedValue([
      { tenantId: 'tenant-a', claimId, claimUntil },
      { tenantId: 'tenant-b', claimId, claimUntil },
    ])
    mocks.central.failTenantDispatch.mockRejectedValueOnce(recoveryError)
    const send = vi
      .spyOn(LifecycleTenantRun, 'send')
      .mockRejectedValueOnce(new Error('enqueue failed'))
      .mockReturnValueOnce(pendingSend.promise)
    const controller = startScheduler()
    const stopQueue = vi.fn()
    controller.nextGroup.signal.addEventListener('abort', stopQueue)

    try {
      await vi.advanceTimersByTimeAsync(0)
      expect(send).toHaveBeenCalledTimes(2)
      const shutdown = controller.abortAsync()
      await vi.advanceTimersByTimeAsync(0)
      expect(stopQueue).not.toHaveBeenCalled()
      pendingSend.resolve('tenant-job')
      await shutdown

      expect(stopQueue).toHaveBeenCalledOnce()
      expect(logSchema.error).toHaveBeenCalledWith(
        logger,
        '[Lifecycle] Failed to update tenant dispatch claim',
        { type: 'event', project: 'tenant-a', error: recoveryError }
      )
      expect(vi.getTimerCount()).toBe(0)
    } finally {
      pendingSend.resolve('tenant-job')
    }
  })

  test('claims within the lease limit in a committed central transaction', async () => {
    mockClaim()
    const send = vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue('tenant-job')

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(mocks.beginTransaction).toHaveBeenCalledExactlyOnceWith({ timeout: 10_000 })
    expect(mocks.transaction.query).toHaveBeenCalledExactlyOnceWith(
      "SET LOCAL idle_in_transaction_session_timeout = '30000ms'",
      { signal: expect.any(AbortSignal) }
    )
    expect(mocks.central.claimDueTenantsWithinLeaseLimit).toHaveBeenCalledExactlyOnceWith(
      100,
      180_000,
      expect.any(AbortSignal)
    )
    expect(mocks.transaction.commit).toHaveBeenCalled()
    expect(mocks.transaction.query.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.central.claimDueTenantsWithinLeaseLimit.mock.invocationCallOrder[0]
    )
    expect(mocks.transaction.commit.mock.invocationCallOrder[0]).toBeLessThan(
      send.mock.invocationCallOrder[0]
    )
  })

  test('rolls back when the idle transaction timeout cannot be set', async () => {
    const error = new Error('timeout setup failed')
    mocks.transaction.query.mockRejectedValueOnce(error)

    await expect(dispatchDueLifecycleTenants(new AbortController().signal)).rejects.toBe(error)

    expect(mocks.transaction.rollback).toHaveBeenCalledOnce()
    expect(mocks.transaction.commit).not.toHaveBeenCalled()
    expect(mocks.central.claimDueTenantsWithinLeaseLimit).not.toHaveBeenCalled()
  })

  test('rolls back the claim transaction when claiming fails', async () => {
    const error = new Error('claim failed')
    mocks.central.claimDueTenantsWithinLeaseLimit.mockRejectedValue(error)
    const send = vi.spyOn(LifecycleTenantRun, 'send')

    await expect(dispatchDueLifecycleTenants(new AbortController().signal)).rejects.toBe(error)

    expect(mocks.transaction.rollback).toHaveBeenCalled()
    expect(mocks.transaction.commit).not.toHaveBeenCalled()
    expect(send).not.toHaveBeenCalled()
  })

  test('rolls back claims if shutdown arrives before commit', async () => {
    const controller = new AbortController()
    mocks.central.claimDueTenantsWithinLeaseLimit.mockImplementationOnce(async () => {
      controller.abort()
      return [{ tenantId: tenant.ref, claimId, claimUntil }]
    })
    const send = vi.spyOn(LifecycleTenantRun, 'send')

    await dispatchDueLifecycleTenants(controller.signal)

    expect(mocks.transaction.rollback).toHaveBeenCalledOnce()
    expect(mocks.transaction.commit).not.toHaveBeenCalled()
    expect(send).not.toHaveBeenCalled()
  })

  test('cancels a blocked claim before stopping the queue', async () => {
    mocks.central.claimDueTenantsWithinLeaseLimit.mockImplementation(
      (_limit: number, _leaseMs: number, signal: AbortSignal) =>
        new Promise((_resolve, reject) => {
          signal.addEventListener('abort', () => reject(new Error('claim aborted')), { once: true })
        })
    )
    const controller = startScheduler()
    const stopQueue = vi.fn()
    controller.nextGroup.signal.addEventListener('abort', stopQueue)

    await vi.advanceTimersByTimeAsync(0)
    expect(mocks.central.claimDueTenantsWithinLeaseLimit).toHaveBeenCalledOnce()
    await controller.abortAsync()

    expect(mocks.transaction.rollback).toHaveBeenCalledOnce()
    expect(mocks.transaction.commit).not.toHaveBeenCalled()
    expect(stopQueue).toHaveBeenCalledOnce()
    expect(logSchema.error).not.toHaveBeenCalled()
  })

  test.each([true, false])(
    'fails the poll without claiming when the tenant queue is missing (multitenant=%s)',
    async (multitenant) => {
      mocks.config.isMultitenant = multitenant
      mocks.getQueue.mockResolvedValue(null)
      const send = vi.spyOn(LifecycleTenantRun, 'send')

      await expect(dispatchDueLifecycleTenants(new AbortController().signal)).rejects.toThrow(
        /does not exist/
      )

      expect(mocks.getQueue).toHaveBeenCalledWith('lifecycle:tenant:run-v1')
      expect(mocks.beginTransaction).not.toHaveBeenCalled()
      expect(mocks.central.claimDueTenantsWithinLeaseLimit).not.toHaveBeenCalled()
      expect(send).not.toHaveBeenCalled()
    }
  )

  test('tenant dispatcher sends bounded bucket ids', async () => {
    const observation = {
      ...dispatchState(new Date('2026-08-16T00:00:00.000Z')),
      maxFailureCount: 7,
      oldestInFlightStartedAt: '2026-08-15T00:00:00.000Z',
    }
    const { database, destroyConnection } = mockTenantDb([bucketId], observation)
    const send = vi.spyOn(LifecycleBucketRun, 'send').mockResolvedValue('bucket-job')

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

    expect(database.listDueLifecycleBuckets).toHaveBeenCalledWith(8)
    expect(database.findLifecycleDispatchState).toHaveBeenCalledWith({ runBudgetMs: 20_000 })
    expect(send).toHaveBeenCalledWith({ tenant, bucketId })
    expect(mocks.central.completeTenantDispatch).toHaveBeenCalledWith(
      'tenant-a',
      claimId,
      expect.any(String),
      expect.any(String),
      observation
    )
    expect(destroyConnection).toHaveBeenCalledOnce()
  })

  test.each([true, false])(
    'reports a missing bucket queue when a send is dropped (multitenant=%s)',
    async (multitenant) => {
      mocks.config.isMultitenant = multitenant
      mocks.config.storageVersioningEnabled = !multitenant
      mocks.getQueue.mockResolvedValue(null)
      const { destroyConnection } = mockTenantDb([bucketId, 'bucket-b'])
      vi.spyOn(LifecycleBucketRun, 'send')
        .mockResolvedValueOnce('bucket-job')
        .mockResolvedValueOnce(null)
      const payload = multitenant ? { tenant, centralClaim: { claimId } } : { tenant }

      if (multitenant) {
        await LifecycleTenantRun.handle(job(payload))
        expect(mocks.central.failTenantDispatch).toHaveBeenCalledWith(
          'tenant-a',
          claimId,
          {
            code: 'tenant_dispatch_failed',
            message: 'Lifecycle queue lifecycle:bucket:run-v1 does not exist',
          },
          expect.any(String)
        )
        expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
      } else {
        await expect(LifecycleTenantRun.handle(job(payload))).rejects.toThrow(
          'Lifecycle queue lifecycle:bucket:run-v1 does not exist'
        )
      }
      expect(mocks.getQueue).toHaveBeenCalledWith('lifecycle:bucket:run-v1')
      expect(destroyConnection).toHaveBeenCalledOnce()
    }
  )

  test('treats a dropped bucket send as a duplicate when its queue exists', async () => {
    mockTenantDb([bucketId])
    vi.spyOn(LifecycleBucketRun, 'send').mockResolvedValue(null)

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

    expect(mocks.getQueue).toHaveBeenCalledWith('lifecycle:bucket:run-v1')
    expect(mocks.central.completeTenantDispatch).toHaveBeenCalledOnce()
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
  })

  test('sends every due bucket and caps a distant next dispatch at the evaluation interval', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const second = 'bucket-b'
    mockTenantDb([bucketId, second], dispatchState(new Date(Date.now() + 3 * evaluationIntervalMs)))
    const send = vi.spyOn(LifecycleBucketRun, 'send').mockResolvedValue('bucket-job')

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

    expect(send).toHaveBeenCalledTimes(2)
    expect(send).toHaveBeenCalledWith({ tenant, bucketId })
    expect(send).toHaveBeenCalledWith({ tenant, bucketId: second })
    expect(completedNextDispatchAt()).toBe(Date.now() + revisitMs)
  })

  test.each([
    { due: 3, floored: true },
    { due: 8, floored: false },
  ])('floors the redispatch after $due sent buckets: $floored', async ({ due, floored }) => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const past = new Date(Date.now() - 1000)
    mockTenantDb(
      Array.from({ length: due }, (_, index) => `bucket-${index}`),
      dispatchState(past)
    )
    vi.spyOn(LifecycleBucketRun, 'send').mockResolvedValue('bucket-job')

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

    expect(completedNextDispatchAt()).toBe(
      floored ? Date.now() + mocks.config.storageLifecycleJobBudgetMs : past.getTime()
    )
  })

  test('does not floor a near-term revisit when no buckets were due', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const next = new Date(Date.now() + 1_000)
    mockTenantDb([], dispatchState(next))
    const send = vi.spyOn(LifecycleBucketRun, 'send')

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

    expect(send).not.toHaveBeenCalled()
    expect(completedNextDispatchAt()).toBe(next.getTime())
  })

  test('fences central completion with the job deadline', async () => {
    mockTenantDb()
    mocks.tenantHasFeature.mockResolvedValue(false)
    const startedOn = new Date(Date.now() - 180_000)
    const deadline = new Date(startedOn.getTime() + 180_000).toISOString()

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }, 0, startedOn))

    expect(mocks.central.completeTenantDispatch.mock.calls[0][3]).toBe(deadline)
  })

  test('fails the central dispatch with tenant_dispatch_failed fenced by the job deadline', async () => {
    const failure = new Error('tenant offline')
    mockCreateStorage(LifecycleTenantRun, {}).mockRejectedValue(failure)
    const startedOn = new Date(Date.now() - 180_000)

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }, 0, startedOn))

    expect(logSchema.error).toHaveBeenCalledWith(logger, '[Lifecycle] Tenant dispatcher failed', {
      type: 'event',
      project: 'tenant-a',
      error: failure,
    })
    expect(mocks.central.failTenantDispatch).toHaveBeenCalledWith(
      'tenant-a',
      claimId,
      { code: 'tenant_dispatch_failed', message: 'tenant offline' },
      new Date(startedOn.getTime() + 180_000).toISOString()
    )
    expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
  })

  test('logs the original error before failTenantDispatch can throw', async () => {
    const failure = new Error('tenant offline')
    mockCreateStorage(LifecycleTenantRun, {}).mockRejectedValue(failure)
    mocks.central.failTenantDispatch.mockRejectedValueOnce(new Error('central down'))

    await expect(
      LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }, 0))
    ).rejects.toThrow('central down')

    expect(logSchema.error).toHaveBeenCalledWith(logger, '[Lifecycle] Tenant dispatcher failed', {
      type: 'event',
      project: 'tenant-a',
      error: failure,
    })
  })

  test.each([true, false])(
    'warns when completing the central dispatch finds no claim (enabled=%s)',
    async (enabled) => {
      mocks.tenantHasFeature.mockResolvedValueOnce(enabled)
      mockTenantDb()
      mocks.central.completeTenantDispatch.mockResolvedValue(false)

      await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

      expect(logSchema.warning).toHaveBeenCalledWith(
        logger,
        '[Lifecycle] Tenant dispatch lost its claim',
        { type: 'event', project: 'tenant-a' }
      )
    }
  )

  test.each([true, false])(
    'bounds the revisit for a tenant (versioning=%s)',
    async (versioning) => {
      mocks.tenantHasFeature.mockResolvedValueOnce(versioning)
      const { database, destroyConnection } = mockTenantDb([bucketId])
      const send = vi.spyOn(LifecycleBucketRun, 'send').mockResolvedValue('bucket-job')
      vi.useFakeTimers({ toFake: ['Date'] })

      await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

      expect(mocks.tenantHasFeature).toHaveBeenCalledWith('tenant-a', 'objectVersioning')
      expect(database.findLifecycleDispatchState).toHaveBeenCalledWith({ runBudgetMs: 20_000 })
      if (versioning) {
        expect(database.listDueLifecycleBuckets).toHaveBeenCalledWith(8)
        expect(send).toHaveBeenCalledWith({ tenant, bucketId })
      } else {
        expect(database.listDueLifecycleBuckets).not.toHaveBeenCalled()
        expect(send).not.toHaveBeenCalled()
      }
      expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
      expect(completedNextDispatchAt()).toBe(Date.now() + (versioning ? revisitMs : 60 * 60_000))
      expect(destroyConnection).toHaveBeenCalledOnce()
    }
  )

  test('retries abandoned artifacts at most hourly', async () => {
    const { database, storage } = mockTenantDb()
    const signal = new AbortController().signal
    vi.useFakeTimers({ toFake: ['Date'] })

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }), { signal })

    expect(mocks.retryAbandoned).toHaveBeenCalledExactlyOnceWith({
      storage,
      database,
      attemptedBefore: new Date(Date.now() - 60 * 60_000).toISOString(),
      budgetMs: 60_000,
      signal,
    })
  })

  test.each([
    { label: 'no bucket is due', next: null, expected: 60 * 60_000 },
    { label: 'a bucket is due sooner', next: 10 * 60_000, expected: 10 * 60_000 },
  ])(
    'retries abandoned artifacts of an enabled tenant within the hour when $label',
    async ({ next, expected }) => {
      vi.useFakeTimers({ toFake: ['Date'] })
      mockTenantDb([], {
        ...dispatchState(next === null ? null : new Date(Date.now() + next)),
        hasAbandonedArtifacts: true,
      })

      await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

      expect(completedNextDispatchAt()).toBe(Date.now() + expected)
    }
  )

  test.each([true, false])(
    'retries soon when the feature cache is stale (artifacts=%s)',
    async (hasAbandonedArtifacts) => {
      mocks.tenantHasFeature.mockResolvedValueOnce(false)
      mocks.central.hasObjectVersioningFeature.mockResolvedValueOnce(true)
      mockTenantDb([], { ...dispatchState(), hasAbandonedArtifacts })
      vi.useFakeTimers({ toFake: ['Date'] })

      await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

      expect(mocks.central.hasObjectVersioningFeature).toHaveBeenCalledExactlyOnceWith('tenant-a')
      expect(completedNextDispatchAt()).toBe(Date.now() + 5 * 60_000)
    }
  )

  test('revisits a disabled tenant when outbox work appears after its empty observation', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    mocks.tenantHasFeature.mockResolvedValue(false)
    const { database } = mockTenantDb()

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

    const nextDispatchAt = completedNextDispatchAt()
    expect(nextDispatchAt).toBe(Date.now() + 60 * 60_000)

    // A previously started bucket worker commits outbox rows after the first observation.
    database.findLifecycleDispatchState.mockResolvedValue({
      ...dispatchState(),
      hasAbandonedArtifacts: true,
    })
    vi.setSystemTime(nextDispatchAt)
    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

    expect(mocks.retryAbandoned).toHaveBeenCalledTimes(2)
    expect(mocks.central.completeTenantDispatch).toHaveBeenCalledTimes(2)
    expect(completedNextDispatchAt(1)).toBe(Date.now() + 60 * 60_000)
  })

  test.each([
    { multitenant: true, enabled: true, interval: 60_000 },
    { multitenant: true, enabled: false, interval: 60_000 },
    { multitenant: false, enabled: true, interval: 60_000 },
    { multitenant: false, enabled: false, interval: 600_000 },
  ])('polls every $interval ms when %o', async (c) => {
    mocks.config.isMultitenant = c.multitenant
    mocks.config.storageVersioningEnabled = c.enabled
    const send = vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue('tenant-job')
    const polls = () =>
      c.multitenant
        ? mocks.central.claimDueTenantsWithinLeaseLimit.mock.calls.length
        : send.mock.calls.length
    startScheduler()
    await vi.advanceTimersByTimeAsync(0)

    expect(polls()).toBe(1)
    await vi.advanceTimersByTimeAsync(c.interval - 5_001)
    expect(polls()).toBe(1)
    await vi.advanceTimersByTimeAsync(10_001)
    expect(polls()).toBe(2)
  })

  test('defers enrolled tenants until promotion fencing is installed', async () => {
    const database = {
      hasMigration: vi.fn().mockResolvedValue(false),
      listDueLifecycleBuckets: vi.fn(),
    }
    const destroyConnection = vi.fn()
    mockCreateStorage(LifecycleTenantRun, {
      db: { asSuperUser: () => database, destroyConnection },
    })
    const send = vi.spyOn(LifecycleBucketRun, 'send').mockResolvedValue('bucket-job')
    vi.useFakeTimers({ toFake: ['Date'] })

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

    expect(database.hasMigration).toHaveBeenCalledWith('lifecycle-writer-coordination')
    expect(database.listDueLifecycleBuckets).not.toHaveBeenCalled()
    expect(send).not.toHaveBeenCalled()
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(completedNextDispatchAt()).toBe(Date.now() + revisitMs)
    expect(destroyConnection).toHaveBeenCalledOnce()
  })

  test('queue options pin identity, expiry, retries, batch size and unconditional sending', async () => {
    expect(LifecycleBucketRun.getSendOptions({ tenant, bucketId })).toEqual({
      singletonKey: 'tenant-a/bucket-a',
      expireInMinutes: Math.ceil(
        (mocks.config.storageLifecycleJobBudgetMs + mocks.config.storageLifecycleClaimLeaseMs) /
          60_000
      ),
      retryLimit: 0,
    })
    expect(LifecycleBucketRun.getSendOptions({ tenant, bucketId }).expireInMinutes).toBe(3)
    expect(LifecycleTenantRun.getSendOptions({ tenant })).toEqual({
      singletonKey: 'tenant-a',
      expireInMinutes: 3,
      retryLimit: 0,
    })

    for (const event of [LifecycleTenantRun, LifecycleBucketRun]) {
      expect(event.getQueueOptions()).toMatchObject({ policy: 'exactly_once' })
      expect(event.allowSync).toBe(false)
      expect(event.getWorkerOptions().pollingIntervalSeconds).toBeUndefined()
    }

    const { concurrentTaskCount, batchSize } = LifecycleBucketRun.getWorkerOptions()
    expect(batchSize).toBe(concurrentTaskCount)
    const tenantOptions = LifecycleTenantRun.getWorkerOptions()
    expect(tenantOptions.batchSize).toBe(tenantOptions.concurrentTaskCount)

    vi.mocked(getTenantConfig).mockResolvedValue({
      disableEvents: [LifecycleTenantRun.eventName(), LifecycleBucketRun.eventName()],
    } as Awaited<ReturnType<typeof getTenantConfig>>)
    await expect(LifecycleTenantRun.shouldSend()).resolves.toBe(true)
    await expect(LifecycleBucketRun.shouldSend()).resolves.toBe(true)
  })

  test.each([true, false])(
    'bucket delivery passes tenantLifecycleEnabled=%s from the objectVersioning feature',
    async (enabled) => {
      mocks.tenantHasFeature.mockResolvedValue(enabled)
      const destroyConnection = vi.fn()
      mockCreateStorage(LifecycleBucketRun, { db: { destroyConnection } })
      const duration = vi.spyOn(lifecycleRunDuration, 'record')

      await LifecycleBucketRun.handle(job({ tenant, bucketId }))

      expect(mocks.executorRun).toHaveBeenCalledWith(bucketId, {
        tenantLifecycleEnabled: enabled,
      })
      expect(mocks.tenantHasFeature).toHaveBeenCalledWith('tenant-a', 'objectVersioning')
      expect(destroyConnection).toHaveBeenCalledOnce()
      expect(duration).toHaveBeenCalledExactlyOnceWith(expect.any(Number), { status: 'COMPLETED' })
    }
  )

  test.each([true, false])(
    'global versioning enables bucket delivery (multitenant=%s)',
    async (multitenant) => {
      mocks.config.isMultitenant = multitenant
      mocks.config.storageVersioningEnabled = true
      mocks.tenantHasFeature.mockResolvedValue(false)
      mockCreateStorage(LifecycleBucketRun, { db: { destroyConnection: vi.fn() } })

      await LifecycleBucketRun.handle(job({ tenant, bucketId }))

      expect(mocks.executorRun).toHaveBeenCalledWith(bucketId, { tenantLifecycleEnabled: true })
      expect(mocks.tenantHasFeature).not.toHaveBeenCalled()
    }
  )

  test('bucket delivery records the ineligible status', async () => {
    mocks.executorRun.mockResolvedValue({ status: 'INELIGIBLE' })
    mockCreateStorage(LifecycleBucketRun, { db: { destroyConnection: vi.fn() } })
    const duration = vi.spyOn(lifecycleRunDuration, 'record')

    await LifecycleBucketRun.handle(job({ tenant, bucketId }))

    expect(duration).toHaveBeenCalledExactlyOnceWith(expect.any(Number), { status: 'INELIGIBLE' })
  })

  test('bucket delivery records the run duration in seconds', async () => {
    mockCreateStorage(LifecycleBucketRun, { db: { destroyConnection: vi.fn() } })
    vi.spyOn(performance, 'now').mockReturnValueOnce(1_000).mockReturnValueOnce(2_500)
    const duration = vi.spyOn(lifecycleRunDuration, 'record')

    await LifecycleBucketRun.handle(job({ tenant, bucketId }))

    expect(duration).toHaveBeenCalledExactlyOnceWith(1.5, { status: 'COMPLETED' })
  })

  test('bucket delivery forwards the abort signal to the executor', async () => {
    mockCreateStorage(LifecycleBucketRun, { db: { destroyConnection: vi.fn() } })
    const { signal } = new AbortController()

    await LifecycleBucketRun.handle(job({ tenant, bucketId }), { signal })

    expect(mocks.executorRun).toHaveBeenCalledWith(bucketId, {
      tenantLifecycleEnabled: true,
      signal,
    })
  })

  test('bucket delivery failure propagates, closes the connection and records an ERROR run', async () => {
    const failure = new Error('executor failed')
    mocks.executorRun.mockRejectedValue(failure)
    const destroyConnection = vi.fn()
    mockCreateStorage(LifecycleBucketRun, { db: { destroyConnection } })
    const duration = vi.spyOn(lifecycleRunDuration, 'record')

    await expect(LifecycleBucketRun.handle(job({ tenant, bucketId }))).rejects.toBe(failure)

    expect(destroyConnection).toHaveBeenCalledOnce()
    expect(duration).toHaveBeenCalledExactlyOnceWith(expect.any(Number), { status: 'ERROR' })
  })

  test('records an ERROR duration if bucket storage creation fails', async () => {
    const failure = new Error('storage unavailable')
    mockCreateStorage(LifecycleBucketRun, {}).mockRejectedValue(failure)
    const duration = vi.spyOn(lifecycleRunDuration, 'record')

    await expect(LifecycleBucketRun.handle(job({ tenant, bucketId }))).rejects.toBe(failure)
    expect(duration).toHaveBeenCalledExactlyOnceWith(expect.any(Number), { status: 'ERROR' })
  })

  test.each([true, false])(
    'single-tenant dispatch (versioning=%s) sweeps abandoned artifacts without computing the unused dispatch state',
    async (versioning) => {
      mocks.config.isMultitenant = false
      mocks.config.storageVersioningEnabled = versioning
      const { database } = mockTenantDb([bucketId])
      const send = vi.spyOn(LifecycleBucketRun, 'send').mockResolvedValue('bucket-job')

      await LifecycleTenantRun.handle(job({ tenant }))

      expect(mocks.retryAbandoned).toHaveBeenCalledOnce()
      if (versioning) {
        expect(database.listDueLifecycleBuckets).toHaveBeenCalled()
        expect(send).toHaveBeenCalledWith({ tenant, bucketId })
      } else {
        expect(database.listDueLifecycleBuckets).not.toHaveBeenCalled()
        expect(send).not.toHaveBeenCalled()
      }
      expect(database.findLifecycleDispatchState).not.toHaveBeenCalled()
      expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
      expect(mocks.tenantHasFeature).not.toHaveBeenCalled()
    }
  )

  test('single-tenant tenant dispatcher failure is rethrown without a central failure', async () => {
    mocks.config.isMultitenant = false
    const failure = new Error('tenant offline')
    mockCreateStorage(LifecycleTenantRun, {}).mockRejectedValue(failure)

    await expect(LifecycleTenantRun.handle(job({ tenant }))).rejects.toBe(failure)
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
  })

  test('a failed bucket enqueue fails the central dispatch instead of completing it', async () => {
    mockTenantDb([bucketId])
    vi.spyOn(LifecycleBucketRun, 'send').mockRejectedValue(new Error('enqueue offline'))

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

    expect(mocks.central.failTenantDispatch).toHaveBeenCalledWith(
      'tenant-a',
      claimId,
      { code: 'tenant_dispatch_failed', message: 'enqueue offline' },
      expect.any(String)
    )
    expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
  })
})
