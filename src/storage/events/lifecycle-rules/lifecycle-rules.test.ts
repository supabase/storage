import { AsyncAbortController } from '@internal/concurrency'
import { logger, logSchema } from '@internal/monitoring'
import { lifecycleRunOutcomes } from '@internal/monitoring/metrics'
import { PartialSendError } from '@internal/queue/manager'
import { shutdownReason, timeoutReason } from '@internal/queue/manager/signals'
import { vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  config: {
    storageVersioningEnabled: false,
    storageLifecyclePaused: false,
    storageLifecycleClaimLeaseMs: 120_000,
    storageLifecycleTenantDispatchLimit: 100,
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
    startTenantDispatch: vi.fn(),
    completeTenantDispatch: vi.fn(),
    failTenantDispatch: vi.fn(),
    releaseTenantDispatch: vi.fn(),
    releaseTenantDispatches: vi.fn(),
    hasObjectVersioningFeature: vi.fn(),
  },
  createStorage: vi.fn(),
  transaction: { query: vi.fn(), commit: vi.fn(), rollback: vi.fn() },
  beginTransaction: vi.fn(),
  executorRun: vi.fn(),
  retryAbandoned: vi.fn(),
  tenantHasFeature: vi.fn(),
  deleteTenantConfig: vi.fn(),
}))

vi.mock('../../../config', () => ({
  getConfig: () => mocks.config,
}))

vi.mock('@internal/database', () => ({
  LifecycleTenantStorePg: class {
    constructor(readonly executor: unknown) {}
    claimDueTenantsWithinLeaseLimit = mocks.central.claimDueTenantsWithinLeaseLimit
    startTenantDispatch = mocks.central.startTenantDispatch
    completeTenantDispatch = mocks.central.completeTenantDispatch
    failTenantDispatch = mocks.central.failTenantDispatch
    releaseTenantDispatch = mocks.central.releaseTenantDispatch
    releaseTenantDispatches = mocks.central.releaseTenantDispatches
    hasObjectVersioningFeature = mocks.central.hasObjectVersioningFeature
  },
  multitenantPgExecutor: { beginTransaction: mocks.beginTransaction },
  getPostgresConnection: vi.fn(),
  getServiceKeyUser: vi.fn(),
  getTenantConfig: vi.fn(),
  deleteTenantConfig: mocks.deleteTenantConfig,
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

vi.mock('../base', async () => {
  const { defineMessage } =
    await vi.importActual<typeof import('@internal/queue/manager')>('@internal/queue/manager')
  return {
    createStorage: mocks.createStorage,
    storageEvent: (options: {
      idempotencyKey: (data: never) => string
      disableKeys: () => string[]
    }) =>
      class extends defineMessage({ idempotencyKey: options.idempotencyKey }) {
        static send = vi.fn()
        static batchSend = vi.fn()
        static disableKeys = options.disableKeys
      },
  }
})

import { LifecycleBucketRun, LifecycleBucketRunHandler } from './bucket-run'
import { dispatchDueLifecycleTenants, startLifecycleScheduler } from './scheduler'
import { LifecycleTenantRun, LifecycleTenantRunHandler } from './tenant-run'

const tenant = { ref: 'tenant-a', host: '' }
const claimId = '5bd8d514-9db1-4b1d-98a4-4a56fc569d95'
const claimUntil = new Date('2026-01-01T00:00:00.000Z')
const rotatedClaimId = '7c1f4a52-3e0b-4f5d-9a57-0d6c1f3b8e21'
const rotatedDeadline = new Date(Date.now() + 120_000)
const bucketId = 'bucket-a'
const dispatchId = '3e9a5442-0549-456d-8d86-2e66a8f15c0b'
const dispatchState = (nextDispatchAt: Date | null = null, quietMs: number | null = 0) => ({
  nextDispatchAt,
  maxFailureCount: 0,
  oldestInFlightStartedAt: null,
  hasAbandonedArtifacts: false,
  quietMs,
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

function job<T>(
  data: T,
  _retryCount = 0,
  startedOn = new Date(),
  signal = new AbortController().signal
) {
  const payload = data as T & { centralClaim?: { claimId: string; claimUntil?: string } }
  return {
    message: {
      data: payload.centralClaim
        ? {
            ...payload,
            centralClaim: {
              ...payload.centralClaim,
              claimUntil: new Date(startedOn.getTime() + 180_000).toISOString(),
            },
          }
        : payload,
    },
    signal,
  } as never
}

function mockCreateStorage(_target: unknown, storage: unknown) {
  return mocks.createStorage.mockResolvedValue(storage)
}

const tenantHandler = new LifecycleTenantRunHandler()
const bucketHandler = new LifecycleBucketRunHandler()

function mockTenantDb(buckets: string[] = [], state: object = dispatchState()) {
  const dispatches = buckets.map((id, index) => ({
    bucketId: id,
    dispatchId: index === 0 ? dispatchId : `${dispatchId.slice(0, -1)}${index}`,
  }))
  const database = {
    hasMigration: vi.fn().mockResolvedValue(true),
    dispatchDueLifecycleBuckets: vi.fn().mockResolvedValue(dispatches),
    releaseDispatchedLifecycleBuckets: vi.fn().mockResolvedValue(undefined),
    findLifecycleDispatchState: vi.fn().mockResolvedValue(state),
  }
  const destroyConnection = vi.fn()
  const storage = { db: { asSuperUser: () => database, destroyConnection } }
  mockCreateStorage(LifecycleTenantRun, storage)
  return { database, destroyConnection, dispatches, storage }
}

function mockBucketDb(buckets: string[] = []) {
  const database = {
    dispatchDueLifecycleBuckets: vi
      .fn()
      .mockResolvedValue(buckets.map((id) => ({ bucketId: id, dispatchId }))),
    releaseDispatchedLifecycleBuckets: vi.fn().mockResolvedValue(undefined),
  }
  const destroyConnection = vi.fn()
  mockCreateStorage(LifecycleBucketRun, {
    db: { asSuperUser: () => database, destroyConnection },
  })
  return { database, destroyConnection }
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
    mocks.central.startTenantDispatch.mockResolvedValue({
      claimId: rotatedClaimId,
      claimUntil: rotatedDeadline,
    })
    mocks.central.completeTenantDispatch.mockResolvedValue(true)
    mocks.central.failTenantDispatch.mockResolvedValue(true)
    mocks.central.releaseTenantDispatch.mockResolvedValue(true)
    mocks.central.releaseTenantDispatches.mockResolvedValue(0)
    mocks.central.hasObjectVersioningFeature.mockResolvedValue(false)
    mocks.executorRun.mockResolvedValue({ status: 'COMPLETED' })
    mocks.tenantHasFeature.mockResolvedValue(true)
    mocks.transaction = { query: vi.fn(), commit: vi.fn(), rollback: vi.fn() }
    mocks.beginTransaction.mockResolvedValue(mocks.transaction)
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
  })

  afterEach(async () => {
    await Promise.all(schedulers.splice(0).map((controller) => controller.abortAsync()))
    vi.useRealTimers()
  })

  test('single-tenant deployments dispatch the configured tenant without a central claim', async () => {
    mocks.config.isMultitenant = false
    const send = vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue(undefined)

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(send).toHaveBeenCalledWith(
      { tenant: { ref: 'single-tenant', host: '' } },
      { signal: expect.any(AbortSignal) }
    )
    expect(mocks.central.claimDueTenantsWithinLeaseLimit).not.toHaveBeenCalled()
  })

  test.each([
    [true, true],
    [undefined, false],
  ])(
    'a tenant delivery with reclaimed=%s starts its claim with reclaimed=%s',
    async (reclaimed, counted) => {
      mocks.central.startTenantDispatch.mockResolvedValueOnce(undefined)
      const send = vi.spyOn(LifecycleBucketRun, 'batchSend')

      await tenantHandler.handle(job({ tenant, centralClaim: { claimId, reclaimed } }))

      expect(mocks.central.startTenantDispatch).toHaveBeenCalledExactlyOnceWith(
        'tenant-a',
        claimId,
        expect.any(String),
        180_000,
        counted
      )
      expect(mocks.createStorage).not.toHaveBeenCalled()
      expect(send).not.toHaveBeenCalled()
      expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
      expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    }
  )

  test('releases a central claim without failing it when the tenant enqueue fails', async () => {
    const error = new Error('queue unavailable')
    mockClaim(false)
    vi.spyOn(LifecycleTenantRun, 'batchSend').mockRejectedValue(error)

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(mocks.central.releaseTenantDispatches).toHaveBeenCalledExactlyOnceWith(
      [{ tenantId: 'tenant-a', claimId, claimUntil, reclaimed: false }],
      expect.any(AbortSignal)
    )
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(logSchema.error).toHaveBeenCalledExactlyOnceWith(
      logger,
      '[LifecycleRule] Failed to enqueue tenant dispatchers',
      { type: 'lifecycle-rule', error, metadata: JSON.stringify({ unsent: 1 }) }
    )
  })

  test('releases unsent claims on shutdown without logging an enqueue error', async () => {
    const controller = new AbortController()
    mocks.central.claimDueTenantsWithinLeaseLimit.mockResolvedValue([
      { tenantId: 'tenant-a', claimId, claimUntil, reclaimed: false },
      { tenantId: 'tenant-b', claimId, claimUntil, reclaimed: true },
      { tenantId: 'tenant-c', claimId, claimUntil, reclaimed: false },
    ])
    vi.spyOn(LifecycleTenantRun, 'batchSend').mockImplementation(async () => {
      controller.abort()
      throw new Error('send aborted')
    })

    await dispatchDueLifecycleTenants(controller.signal)

    expect(mocks.central.releaseTenantDispatches).toHaveBeenCalledOnce()
    const [released, releaseSignal] = mocks.central.releaseTenantDispatches.mock.calls[0]
    expect(released.map(({ tenantId }: { tenantId: string }) => tenantId)).toEqual([
      'tenant-a',
      'tenant-c',
    ])
    expect(releaseSignal.aborted).toBe(false)
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(logSchema.error).not.toHaveBeenCalled()
  })

  test('marks a re-sent claim as reclaimed and keeps it when re-sending fails', async () => {
    const error = new Error('queue unavailable')
    mockClaim(true)
    const send = vi.spyOn(LifecycleTenantRun, 'batchSend').mockRejectedValue(error)

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(send).toHaveBeenCalledWith(
      [
        {
          tenant,
          centralClaim: { claimId, claimUntil: claimUntil.toISOString(), reclaimed: true },
        },
      ],
      { signal: expect.any(AbortSignal) }
    )
    expect(logSchema.warning).toHaveBeenCalledWith(
      logger,
      '[LifecycleRule] Re-sending tenant dispatches after their lease lapsed',
      { type: 'lifecycle-rule', metadata: JSON.stringify({ resent: 1 }) }
    )
    expect(logSchema.error).toHaveBeenCalledWith(
      logger,
      '[LifecycleRule] Failed to enqueue tenant dispatchers',
      { type: 'lifecycle-rule', error, metadata: JSON.stringify({ unsent: 1 }) }
    )
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.releaseTenantDispatches).not.toHaveBeenCalled()
  })

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
          { type: 'lifecycle-rule' }
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
      { type: 'lifecycle-rule' }
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

    expect(logSchema.error).toHaveBeenCalledWith(logger, '[LifecycleRule] Scheduler poll failed', {
      type: 'lifecycle-rule',
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

  test('drains claimed sends before queue shutdown and gives them the shutdown signal', async () => {
    mockClaim()
    const pendingSend = Promise.withResolvers<void>()
    const send = vi.spyOn(LifecycleTenantRun, 'batchSend').mockReturnValueOnce(pendingSend.promise)
    const controller = startScheduler()
    const stopQueue = vi.fn()
    controller.nextGroup.signal.addEventListener('abort', stopQueue)

    try {
      await vi.advanceTimersByTimeAsync(0)
      expect(send).toHaveBeenCalledExactlyOnceWith(expect.any(Array), {
        signal: controller.signal,
      })
      const shutdown = controller.abortAsync()
      await vi.advanceTimersByTimeAsync(0)
      expect(stopQueue).not.toHaveBeenCalled()
      pendingSend.reject(new Error('send aborted'))
      await shutdown

      expect(stopQueue).toHaveBeenCalledOnce()
      expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
      expect(logSchema.error).not.toHaveBeenCalled()
      expect(vi.getTimerCount()).toBe(0)
    } finally {
      pendingSend.resolve()
    }
  })

  test('releases only the claims a partial send did not take, and survives a failed release', async () => {
    const recoveryError = new Error('central database unavailable')
    const sendError = new PartialSendError([0, 2])
    mocks.central.claimDueTenantsWithinLeaseLimit.mockResolvedValue([
      { tenantId: 'tenant-a', claimId, claimUntil, reclaimed: false },
      { tenantId: 'tenant-b', claimId, claimUntil, reclaimed: false },
      { tenantId: 'tenant-c', claimId, claimUntil, reclaimed: false },
    ])
    mocks.central.releaseTenantDispatches.mockRejectedValueOnce(recoveryError)
    vi.spyOn(LifecycleTenantRun, 'batchSend').mockRejectedValue(sendError)

    await expect(dispatchDueLifecycleTenants(new AbortController().signal)).resolves.toBeUndefined()

    expect(mocks.central.releaseTenantDispatches).toHaveBeenCalledOnce()
    expect(
      mocks.central.releaseTenantDispatches.mock.calls[0][0].map(
        ({ tenantId }: { tenantId: string }) => tenantId
      )
    ).toEqual(['tenant-a', 'tenant-c'])
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(logSchema.error).toHaveBeenCalledWith(
      logger,
      '[LifecycleRule] Failed to update tenant dispatch claim',
      { type: 'lifecycle-rule', error: recoveryError, metadata: JSON.stringify({ unreleased: 2 }) }
    )
    expect(logSchema.error).toHaveBeenCalledWith(
      logger,
      '[LifecycleRule] Failed to enqueue tenant dispatchers',
      { type: 'lifecycle-rule', error: sendError, metadata: JSON.stringify({ unsent: 2 }) }
    )
  })

  test('claims within the lease limit and sends the fence after committing', async () => {
    mockClaim()
    const send = vi.spyOn(LifecycleTenantRun, 'batchSend').mockResolvedValue(undefined)

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
    expect(mocks.central.claimDueTenantsWithinLeaseLimit.mock.contexts[0]).toMatchObject({
      executor: mocks.transaction,
    })
    expect(mocks.transaction.commit).toHaveBeenCalled()
    expect(mocks.transaction.query.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.central.claimDueTenantsWithinLeaseLimit.mock.invocationCallOrder[0]
    )
    expect(mocks.transaction.commit.mock.invocationCallOrder[0]).toBeLessThan(
      send.mock.invocationCallOrder[0]
    )
    expect(send).toHaveBeenCalledExactlyOnceWith(
      [{ tenant, centralClaim: { claimId, claimUntil: claimUntil.toISOString() } }],
      { signal: expect.any(AbortSignal) }
    )
    expect(logSchema.warning).not.toHaveBeenCalled()
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
    const send = vi.spyOn(LifecycleTenantRun, 'batchSend')

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
    const send = vi.spyOn(LifecycleTenantRun, 'batchSend')

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

  test('tenant dispatcher sends bounded bucket ids', async () => {
    const observation = {
      ...dispatchState(new Date('2026-08-16T00:00:00.000Z')),
      maxFailureCount: 7,
      oldestInFlightStartedAt: '2026-08-15T00:00:00.000Z',
    }
    const { database, destroyConnection } = mockTenantDb([bucketId], observation)
    const send = vi.spyOn(LifecycleBucketRun, 'batchSend').mockResolvedValue(undefined)

    await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

    expect(database.dispatchDueLifecycleBuckets).toHaveBeenCalledWith(
      8,
      900_000,
      120_000,
      undefined
    )
    expect(database.findLifecycleDispatchState).toHaveBeenCalledWith({ runBudgetMs: 20_000 })
    expect(send).toHaveBeenCalledWith([{ tenant, bucketId, dispatchId }], {
      signal: expect.any(AbortSignal),
    })
    expect(mocks.central.completeTenantDispatch).toHaveBeenCalledWith(
      'tenant-a',
      rotatedClaimId,
      expect.any(String),
      rotatedDeadline.toISOString(),
      observation
    )
    expect(destroyConnection).toHaveBeenCalledOnce()
  })

  test('sends every due bucket and caps a distant next dispatch at the evaluation interval', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const second = 'bucket-b'
    const { dispatches } = mockTenantDb(
      [bucketId, second],
      dispatchState(new Date(Date.now() + 3 * evaluationIntervalMs))
    )
    const send = vi.spyOn(LifecycleBucketRun, 'batchSend').mockResolvedValue(undefined)

    await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

    expect(send).toHaveBeenCalledExactlyOnceWith(
      dispatches.map((entry) => ({ tenant, ...entry })),
      { signal: expect.any(AbortSignal) }
    )
    expect(completedNextDispatchAt()).toBe(Date.now() + revisitMs)
  })

  test('revisits at the observed next dispatch when no buckets were due', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    const next = new Date(Date.now() + 1_000)
    mockTenantDb([], dispatchState(next))
    const send = vi.spyOn(LifecycleBucketRun, 'batchSend')

    await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

    expect(send).not.toHaveBeenCalled()
    expect(completedNextDispatchAt()).toBe(next.getTime())
  })

  test.each([
    { gateClosed: false, level: 'error', other: 'warning', message: 'tenant offline' },
    {
      gateClosed: true,
      level: 'warning',
      other: 'error',
      message: 'paused tenant unreachable',
      backoff: 24 * 60 * 60,
    },
  ] as const)(
    'fails the central dispatch with tenant_dispatch_failed fenced by the job deadline (gateClosed=$gateClosed)',
    async ({ gateClosed, level, other, message, backoff }) => {
      if (gateClosed) mocks.tenantHasFeature.mockResolvedValue(false)
      const failure = new Error(message)
      mockCreateStorage(LifecycleTenantRun, {}).mockRejectedValue(failure)

      await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

      expect(logSchema[level]).toHaveBeenCalledWith(
        logger,
        '[LifecycleRule] Tenant dispatcher failed',
        {
          type: 'lifecycle-rule',
          project: 'tenant-a',
          error: failure,
        }
      )
      expect(logSchema[other]).not.toHaveBeenCalled()
      expect(mocks.central.failTenantDispatch).toHaveBeenCalledWith(
        'tenant-a',
        rotatedClaimId,
        { code: 'tenant_dispatch_failed', message },
        rotatedDeadline.toISOString(),
        backoff
      )
      expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
    }
  )

  test('logs the original error before failTenantDispatch can throw', async () => {
    const failure = new Error('tenant offline')
    mockCreateStorage(LifecycleTenantRun, {}).mockRejectedValue(failure)
    mocks.central.failTenantDispatch.mockRejectedValueOnce(new Error('central down'))

    await expect(
      tenantHandler.handle(job({ tenant, centralClaim: { claimId } }, 0))
    ).rejects.toThrow('central down')

    expect(logSchema.error).toHaveBeenCalledWith(
      logger,
      '[LifecycleRule] Tenant dispatcher failed',
      {
        type: 'lifecycle-rule',
        project: 'tenant-a',
        error: failure,
      }
    )
  })

  test('warns when completing the central dispatch finds no claim', async () => {
    mockTenantDb()
    mocks.central.completeTenantDispatch.mockResolvedValue(false)

    await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

    expect(logSchema.warning).toHaveBeenCalledWith(
      logger,
      '[LifecycleRule] Tenant dispatch lost its claim',
      { type: 'lifecycle-rule', project: 'tenant-a' }
    )
  })

  test('does not dispatch buckets for a gate-closed tenant', async () => {
    mocks.tenantHasFeature.mockResolvedValueOnce(false)
    const { database, destroyConnection } = mockTenantDb([bucketId])
    const send = vi.spyOn(LifecycleBucketRun, 'batchSend').mockResolvedValue(undefined)
    vi.useFakeTimers({ toFake: ['Date'] })

    await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

    expect(mocks.tenantHasFeature).toHaveBeenCalledWith('tenant-a', 'objectVersioning')
    expect(database.findLifecycleDispatchState).toHaveBeenCalledWith({ runBudgetMs: 20_000 })
    expect(database.dispatchDueLifecycleBuckets).not.toHaveBeenCalled()
    expect(send).not.toHaveBeenCalled()
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(completedNextDispatchAt()).toBe(Date.now() + 60 * 60_000)
    expect(destroyConnection).toHaveBeenCalledOnce()
  })

  test('retries abandoned artifacts at most hourly', async () => {
    const { database, storage } = mockTenantDb()
    const signal = new AbortController().signal
    vi.useFakeTimers({ toFake: ['Date'] })

    await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }, 0, new Date(), signal))

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

      await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

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

      await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

      expect(mocks.central.hasObjectVersioningFeature).toHaveBeenCalledExactlyOnceWith(
        'tenant-a',
        expect.any(AbortSignal)
      )
      expect(mocks.deleteTenantConfig).toHaveBeenCalledExactlyOnceWith('tenant-a')
      expect(completedNextDispatchAt()).toBe(Date.now() + 5 * 60_000)
    }
  )

  test('revisits a disabled tenant when outbox work appears after its empty observation', async () => {
    vi.useFakeTimers({ toFake: ['Date'] })
    mocks.tenantHasFeature.mockResolvedValue(false)
    const { database } = mockTenantDb()

    await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

    const nextDispatchAt = completedNextDispatchAt()
    expect(nextDispatchAt).toBe(Date.now() + 60 * 60_000)
    expect(mocks.deleteTenantConfig).not.toHaveBeenCalled()

    // A previously started bucket worker commits outbox rows after the first observation.
    database.findLifecycleDispatchState.mockResolvedValue({
      ...dispatchState(),
      hasAbandonedArtifacts: true,
    })
    vi.setSystemTime(nextDispatchAt)
    await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

    expect(mocks.retryAbandoned).toHaveBeenCalledTimes(2)
    expect(mocks.central.completeTenantDispatch).toHaveBeenCalledTimes(2)
    expect(completedNextDispatchAt(1)).toBe(Date.now() + 60 * 60_000)
  })

  describe('quiet backoff and dispatch safeguards', () => {
    test.each([
      { label: 'recently active (10m)', quietMs: 10 * 60_000, expected: 60 * 60_000 },
      { label: 'quiet for 4h', quietMs: 4 * 60 * 60_000, expected: 4 * 60 * 60_000 },
      { label: 'quiet for 48h (clamped to daily)', quietMs: 48 * 60 * 60_000, expected: revisitMs },
      { label: 'no bucket state (null)', quietMs: null, expected: revisitMs },
    ])('backs off a gate-closed empty-outbox tenant when $label', async ({ quietMs, expected }) => {
      vi.useFakeTimers({ toFake: ['Date'] })
      mocks.tenantHasFeature.mockResolvedValue(false)
      mockTenantDb([], dispatchState(null, quietMs))

      await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

      expect(completedNextDispatchAt()).toBe(Date.now() + expected)
    })

    test('keeps hourly cadence for a gate-closed tenant while abandoned artifacts remain', async () => {
      vi.useFakeTimers({ toFake: ['Date'] })
      mocks.tenantHasFeature.mockResolvedValue(false)
      mockTenantDb([], { ...dispatchState(null, 48 * 60 * 60_000), hasAbandonedArtifacts: true })

      await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

      expect(completedNextDispatchAt()).toBe(Date.now() + 60 * 60_000)
    })

    test('floors a past nextDispatchAt on an open-gate run to avoid spin-looping', async () => {
      vi.useFakeTimers({ toFake: ['Date'] })
      mockTenantDb([], dispatchState(new Date(Date.now() - 10_000)))

      await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

      expect(completedNextDispatchAt()).toBe(Date.now() + 20_000)
    })

    test('releases only unsent bucket dispatches on a partial send failure', async () => {
      const { database, dispatches } = mockTenantDb([bucketId, 'bucket-b', 'bucket-c'])
      vi.spyOn(LifecycleBucketRun, 'batchSend').mockRejectedValue(new PartialSendError([0, 2]))

      await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

      expect(database.releaseDispatchedLifecycleBuckets).toHaveBeenCalledExactlyOnceWith(
        [dispatches[0], dispatches[2]],
        expect.any(AbortSignal)
      )
      expect(mocks.central.failTenantDispatch).toHaveBeenCalledOnce()
    })

    test('does not dispatch buckets when the signal is already aborted', async () => {
      const { database } = mockTenantDb([bucketId])
      const controller = new AbortController()
      const timeout = timeoutReason()
      controller.abort(timeout)

      await expect(
        tenantHandler.handle(
          job({ tenant, centralClaim: { claimId } }, 0, new Date(), controller.signal)
        )
      ).rejects.toBe(timeout)

      expect(database.dispatchDueLifecycleBuckets).not.toHaveBeenCalled()
      expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
    })
  })

  test.each([
    { multitenant: true, enabled: true, interval: 60_000 },
    { multitenant: true, enabled: false, interval: 60_000 },
    { multitenant: false, enabled: true, interval: 60_000 },
    { multitenant: false, enabled: false, interval: 600_000 },
  ])('polls every $interval ms when %o', async (c) => {
    mocks.config.isMultitenant = c.multitenant
    mocks.config.storageVersioningEnabled = c.enabled
    const send = vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue(undefined)
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
    const { database, destroyConnection } = mockTenantDb([bucketId])
    database.hasMigration.mockResolvedValue(false)
    const send = vi.spyOn(LifecycleBucketRun, 'batchSend').mockResolvedValue(undefined)
    vi.useFakeTimers({ toFake: ['Date'] })

    await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

    expect(database.hasMigration).toHaveBeenCalledWith('lifecycle-writer-coordination')
    expect(database.dispatchDueLifecycleBuckets).not.toHaveBeenCalled()
    expect(send).not.toHaveBeenCalled()
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(completedNextDispatchAt()).toBe(Date.now() + revisitMs)
    expect(destroyConnection).toHaveBeenCalledOnce()
  })

  test('queue messages carry dedup identities and bypass event suppression', () => {
    expect(new LifecycleBucketRun({ tenant, bucketId, dispatchId }).idempotencyKey).toBe(
      `tenant-a/bucket-a/${dispatchId}`
    )
    expect(
      new LifecycleBucketRun({ tenant, bucketId, dispatchId: `${dispatchId.slice(0, -1)}1` })
        .idempotencyKey
    ).not.toBe(new LifecycleBucketRun({ tenant, bucketId, dispatchId }).idempotencyKey)
    expect(
      new LifecycleTenantRun({
        tenant,
        centralClaim: { claimId, claimUntil: claimUntil.toISOString() },
      }).idempotencyKey
    ).toBe(`tenant-a/${claimId}/${claimUntil.toISOString()}`)
    expect(
      new LifecycleTenantRun({
        tenant,
        centralClaim: { claimId, claimUntil: new Date(claimUntil.getTime() + 1_000).toISOString() },
      }).idempotencyKey
    ).not.toBe(
      new LifecycleTenantRun({
        tenant,
        centralClaim: { claimId, claimUntil: claimUntil.toISOString() },
      }).idempotencyKey
    )
    expect(LifecycleTenantRun.disableKeys?.({ tenant } as never)).toEqual([])
    expect(LifecycleBucketRun.disableKeys?.({ tenant, bucketId, dispatchId } as never)).toEqual([])
    expect(tenantHandler.options.prefetch).toBe(tenantHandler.options.parallelism)
    expect(bucketHandler.options.prefetch).toBe(bucketHandler.options.parallelism)
  })

  test.each([true, false])(
    'bucket delivery passes tenantLifecycleEnabled=%s from the objectVersioning feature',
    async (enabled) => {
      mocks.tenantHasFeature.mockResolvedValue(enabled)
      const { destroyConnection } = mockBucketDb()
      const outcomes = vi.spyOn(lifecycleRunOutcomes, 'add')
      const { signal } = new AbortController()

      await bucketHandler.handle(job({ tenant, bucketId, dispatchId }, 0, new Date(), signal))

      expect(mocks.executorRun).toHaveBeenCalledWith(bucketId, {
        tenantLifecycleEnabled: enabled,
        signal,
      })
      expect(mocks.tenantHasFeature).toHaveBeenCalledWith('tenant-a', 'objectVersioning')
      expect(destroyConnection).toHaveBeenCalledOnce()
      expect(outcomes).toHaveBeenCalledExactlyOnceWith(1, { status: 'COMPLETED' })
    }
  )

  test('bucket delivery failure propagates, closes the connection and records an ERROR outcome', async () => {
    const failure = new Error('executor failed')
    mocks.executorRun.mockRejectedValue(failure)
    const { database, destroyConnection } = mockBucketDb(['bucket-b'])
    const outcomes = vi.spyOn(lifecycleRunOutcomes, 'add')

    await expect(bucketHandler.handle(job({ tenant, bucketId, dispatchId }))).rejects.toBe(failure)

    expect(destroyConnection).toHaveBeenCalledOnce()
    expect(outcomes).toHaveBeenCalledExactlyOnceWith(1, { status: 'ERROR' })
    expect(database.dispatchDueLifecycleBuckets).not.toHaveBeenCalled()
  })

  describe('bucket delivery top-up', () => {
    const topUpWarning = '[LifecycleRule] Failed to dispatch the next lifecycle bucket'

    test('dispatches the next due bucket after a COMPLETED run', async () => {
      const { database } = mockBucketDb(['bucket-b'])
      const send = vi.spyOn(LifecycleBucketRun, 'batchSend').mockResolvedValue(undefined)

      await bucketHandler.handle(job({ tenant, bucketId, dispatchId }))

      expect(database.dispatchDueLifecycleBuckets).toHaveBeenCalledExactlyOnceWith(
        8,
        900_000,
        120_000,
        1
      )
      expect(send).toHaveBeenCalledExactlyOnceWith([{ tenant, bucketId: 'bucket-b', dispatchId }], {
        signal: expect.any(AbortSignal),
      })
    })

    test.each(['PARTIAL', 'NOT_CLAIMED', 'PAUSED', 'LOST_CLAIM', 'INELIGIBLE'])(
      'records a %s outcome without dispatching another bucket',
      async (status) => {
        mocks.executorRun.mockResolvedValue({ status })
        const { database } = mockBucketDb(['bucket-b'])
        const outcomes = vi.spyOn(lifecycleRunOutcomes, 'add')

        await bucketHandler.handle(job({ tenant, bucketId, dispatchId }))

        expect(database.dispatchDueLifecycleBuckets).not.toHaveBeenCalled()
        expect(outcomes).toHaveBeenCalledExactlyOnceWith(1, { status })
      }
    )

    test('does not dispatch once the signal is aborted', async () => {
      const controller = new AbortController()
      controller.abort()
      const { database } = mockBucketDb(['bucket-b'])

      await bucketHandler.handle(
        job({ tenant, bucketId, dispatchId }, 0, new Date(), controller.signal)
      )

      expect(database.dispatchDueLifecycleBuckets).not.toHaveBeenCalled()
    })

    test('does not fail the run when the dispatch query fails', async () => {
      const failure = new Error('db down')
      const { database } = mockBucketDb()
      database.dispatchDueLifecycleBuckets.mockRejectedValue(failure)

      await expect(
        bucketHandler.handle(job({ tenant, bucketId, dispatchId }))
      ).resolves.toBeUndefined()

      expect(logSchema.warning).toHaveBeenCalledWith(logger, topUpWarning, {
        type: 'lifecycle-rule',
        project: tenant.ref,
        error: failure,
      })
    })

    test('releases the unsent dispatch and does not fail the run when the send fails', async () => {
      const failure = new Error('queue down')
      const { database } = mockBucketDb(['bucket-b'])
      vi.spyOn(LifecycleBucketRun, 'batchSend').mockRejectedValue(failure)

      await expect(
        bucketHandler.handle(job({ tenant, bucketId, dispatchId }))
      ).resolves.toBeUndefined()

      expect(database.releaseDispatchedLifecycleBuckets).toHaveBeenCalledExactlyOnceWith(
        [{ bucketId: 'bucket-b', dispatchId }],
        expect.any(AbortSignal)
      )
      expect(logSchema.warning).toHaveBeenCalledWith(logger, topUpWarning, {
        type: 'lifecycle-rule',
        project: tenant.ref,
        error: failure,
      })
    })
  })

  test('records an ERROR outcome if bucket storage creation fails', async () => {
    const failure = new Error('storage unavailable')
    mockCreateStorage(LifecycleBucketRun, {}).mockRejectedValue(failure)
    const outcomes = vi.spyOn(lifecycleRunOutcomes, 'add')

    await expect(bucketHandler.handle(job({ tenant, bucketId, dispatchId }))).rejects.toBe(failure)
    expect(outcomes).toHaveBeenCalledExactlyOnceWith(1, { status: 'ERROR' })
  })

  test.each([true, false])(
    'single-tenant dispatch (versioning=%s) sweeps abandoned artifacts without computing the unused dispatch state',
    async (versioning) => {
      mocks.config.isMultitenant = false
      mocks.config.storageVersioningEnabled = versioning
      const { database } = mockTenantDb([bucketId])
      const send = vi.spyOn(LifecycleBucketRun, 'batchSend').mockResolvedValue(undefined)

      await tenantHandler.handle(job({ tenant }))

      expect(mocks.retryAbandoned).toHaveBeenCalledOnce()
      if (versioning) {
        expect(database.dispatchDueLifecycleBuckets).toHaveBeenCalled()
        expect(send).toHaveBeenCalledWith([{ tenant, bucketId, dispatchId }], {
          signal: expect.any(AbortSignal),
        })
      } else {
        expect(database.dispatchDueLifecycleBuckets).not.toHaveBeenCalled()
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

    await expect(tenantHandler.handle(job({ tenant }))).rejects.toBe(failure)
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
  })

  test('a failed bucket enqueue releases dispatched buckets and fails the central dispatch instead of completing it', async () => {
    const { database, dispatches } = mockTenantDb([bucketId])
    vi.spyOn(LifecycleBucketRun, 'batchSend').mockRejectedValue(new Error('enqueue offline'))

    await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

    expect(database.releaseDispatchedLifecycleBuckets).toHaveBeenCalledExactlyOnceWith(
      dispatches,
      expect.any(AbortSignal)
    )
    expect(mocks.central.failTenantDispatch).toHaveBeenCalledWith(
      'tenant-a',
      rotatedClaimId,
      { code: 'tenant_dispatch_failed', message: 'enqueue offline' },
      rotatedDeadline.toISOString(),
      undefined
    )
    expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
  })

  test('a shutdown after start releases the claim and rethrows', async () => {
    mockTenantDb([bucketId])
    const controller = new AbortController()
    const aborted = new Error('shutting down')
    vi.spyOn(LifecycleBucketRun, 'batchSend').mockImplementation(async () => {
      controller.abort(shutdownReason())
      throw aborted
    })

    await expect(
      tenantHandler.handle(
        job({ tenant, centralClaim: { claimId } }, 0, new Date(), controller.signal)
      )
    ).rejects.toBe(aborted)

    expect(mocks.central.releaseTenantDispatch).toHaveBeenCalledWith(
      'tenant-a',
      rotatedClaimId,
      rotatedDeadline.toISOString(),
      expect.any(AbortSignal)
    )
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
  })

  test('a shutdown that interrupts the sweep releases instead of completing', async () => {
    mockTenantDb()
    const controller = new AbortController()
    const shutdown = shutdownReason()
    mocks.retryAbandoned.mockImplementationOnce(async () => controller.abort(shutdown))

    await expect(
      tenantHandler.handle(
        job({ tenant, centralClaim: { claimId } }, 0, new Date(), controller.signal)
      )
    ).rejects.toBe(shutdown)

    expect(mocks.central.releaseTenantDispatch).toHaveBeenCalledWith(
      'tenant-a',
      rotatedClaimId,
      rotatedDeadline.toISOString(),
      expect.any(AbortSignal)
    )
    expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
  })

  test('a timeout that interrupts the sweep records a failure and frees the claim', async () => {
    mockTenantDb()
    const controller = new AbortController()
    const timeout = timeoutReason()
    mocks.retryAbandoned.mockImplementationOnce(async () => controller.abort(timeout))

    await expect(
      tenantHandler.handle(
        job({ tenant, centralClaim: { claimId } }, 0, new Date(), controller.signal)
      )
    ).rejects.toBe(timeout)

    expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.releaseTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.failTenantDispatch).toHaveBeenCalledWith(
      'tenant-a',
      rotatedClaimId,
      {
        code: 'tenant_dispatch_timeout',
        message: 'Tenant dispatch exceeded consume timeout',
      },
      rotatedDeadline.toISOString(),
      60 * 60,
      expect.any(AbortSignal)
    )
  })

  test('a shutdown during the central flag read releases instead of completing', async () => {
    mockTenantDb()
    mocks.tenantHasFeature.mockResolvedValue(false)
    const controller = new AbortController()
    const shutdown = shutdownReason()
    mocks.central.hasObjectVersioningFeature.mockImplementationOnce(async () => {
      controller.abort(shutdown)
      return false
    })

    await expect(
      tenantHandler.handle(
        job({ tenant, centralClaim: { claimId } }, 0, new Date(), controller.signal)
      )
    ).rejects.toBe(shutdown)

    expect(mocks.central.releaseTenantDispatch).toHaveBeenCalledWith(
      'tenant-a',
      rotatedClaimId,
      rotatedDeadline.toISOString(),
      expect.any(AbortSignal)
    )
    expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
  })

  test('a timeout after start rethrows and settles the claim as failed', async () => {
    mockTenantDb()
    const controller = new AbortController()
    const failError = new Error('central down')
    mocks.central.failTenantDispatch.mockRejectedValueOnce(failError)
    mocks.retryAbandoned.mockImplementationOnce(async () => {
      const timeout = timeoutReason()
      controller.abort(timeout)
      throw timeout
    })

    await expect(
      tenantHandler.handle(
        job({ tenant, centralClaim: { claimId } }, 0, new Date(), controller.signal)
      )
    ).rejects.toThrow()

    expect(mocks.central.failTenantDispatch).toHaveBeenCalledWith(
      'tenant-a',
      rotatedClaimId,
      {
        code: 'tenant_dispatch_timeout',
        message: 'Tenant dispatch exceeded consume timeout',
      },
      rotatedDeadline.toISOString(),
      60 * 60,
      expect.any(AbortSignal)
    )
    expect(logSchema.warning).toHaveBeenCalledWith(
      logger,
      '[LifecycleRule] Tenant dispatch timeout settlement failed',
      expect.objectContaining({ project: 'tenant-a', error: failError })
    )
    expect(mocks.central.releaseTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
  })

  test('an abort with an unknown reason rethrows and leaves the claim to lapse', async () => {
    mockTenantDb([bucketId])
    const controller = new AbortController()
    const aborted = new Error('x')
    vi.spyOn(LifecycleBucketRun, 'batchSend').mockImplementation(async () => {
      controller.abort(new Error('x'))
      throw aborted
    })

    await expect(
      tenantHandler.handle(
        job({ tenant, centralClaim: { claimId } }, 0, new Date(), controller.signal)
      )
    ).rejects.toBe(aborted)

    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.releaseTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
  })

  test('a release failure does not mask the shutdown error', async () => {
    mockTenantDb([bucketId])
    const controller = new AbortController()
    const aborted = new Error('shutting down')
    const releaseError = new Error('central down')
    mocks.central.releaseTenantDispatch.mockRejectedValue(releaseError)
    vi.spyOn(LifecycleBucketRun, 'batchSend').mockImplementation(async () => {
      controller.abort(shutdownReason())
      throw aborted
    })

    await expect(
      tenantHandler.handle(
        job({ tenant, centralClaim: { claimId } }, 0, new Date(), controller.signal)
      )
    ).rejects.toBe(aborted)

    expect(logSchema.warning).toHaveBeenCalledWith(
      logger,
      '[LifecycleRule] Tenant dispatch release failed',
      { type: 'lifecycle-rule', project: 'tenant-a', error: releaseError }
    )
  })

  test('a failed completion releases the claim instead of recording a failure', async () => {
    mockTenantDb()
    const completeError = new Error('central blip')
    mocks.central.completeTenantDispatch.mockRejectedValue(completeError)

    await expect(
      tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))
    ).resolves.toBeUndefined()

    expect(mocks.central.releaseTenantDispatch).toHaveBeenCalledExactlyOnceWith(
      'tenant-a',
      rotatedClaimId,
      rotatedDeadline.toISOString(),
      expect.any(AbortSignal)
    )
    expect(mocks.central.releaseTenantDispatch.mock.calls[0][3].aborted).toBe(false)
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(logSchema.error).toHaveBeenCalledWith(
      logger,
      '[LifecycleRule] Tenant dispatch completion failed',
      { type: 'lifecycle-rule', project: 'tenant-a', error: completeError }
    )
  })

  test('a failed completion and a failed release still resolve without recording a failure', async () => {
    mockTenantDb()
    const releaseError = new Error('central down')
    mocks.central.completeTenantDispatch.mockRejectedValue(new Error('central blip'))
    mocks.central.releaseTenantDispatch.mockRejectedValue(releaseError)

    await expect(
      tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))
    ).resolves.toBeUndefined()

    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(logSchema.warning).toHaveBeenCalledWith(
      logger,
      '[LifecycleRule] Tenant dispatch release failed',
      { type: 'lifecycle-rule', project: 'tenant-a', error: releaseError }
    )
  })
})
