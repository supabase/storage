import { AsyncAbortController } from '@internal/concurrency'
import { logger, logSchema } from '@internal/monitoring'
import { PartialSendError } from '@internal/queue/manager'
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
    failTenantDispatch: vi.fn(),
    releaseTenantDispatches: vi.fn(),
  },
  transaction: { query: vi.fn(), commit: vi.fn(), rollback: vi.fn() },
  beginTransaction: vi.fn(),
}))

vi.mock('../../../config', () => ({
  getConfig: () => mocks.config,
}))

vi.mock('@internal/database', () => ({
  LifecycleTenantStorePg: class {
    constructor(readonly executor: unknown) {}
    claimDueTenantsWithinLeaseLimit = mocks.central.claimDueTenantsWithinLeaseLimit
    failTenantDispatch = mocks.central.failTenantDispatch
    releaseTenantDispatches = mocks.central.releaseTenantDispatches
  },
  multitenantPgExecutor: { beginTransaction: mocks.beginTransaction },
  deleteTenantConfig: vi.fn(),
  tenantHasFeature: vi.fn(),
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

vi.mock('../base', async () => {
  const { defineMessage } =
    await vi.importActual<typeof import('@internal/queue/manager')>('@internal/queue/manager')
  return {
    createStorage: vi.fn(),
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

import { dispatchDueLifecycleTenants, startLifecycleScheduler } from './scheduler'
import { LifecycleTenantRun } from './tenant-run'

const tenant = { ref: 'tenant-a', host: '' }
const claimId = '5bd8d514-9db1-4b1d-98a4-4a56fc569d95'
const claimUntil = new Date('2026-01-01T00:00:00.000Z')
const schedulers: AsyncAbortController[] = []

function startScheduler(controller = new AsyncAbortController()) {
  vi.useFakeTimers()
  schedulers.push(controller)
  startLifecycleScheduler(controller.signal)
  return controller
}

function mockClaim(reclaimed?: boolean) {
  mocks.central.claimDueTenantsWithinLeaseLimit.mockResolvedValue([
    { tenantId: tenant.ref, claimId, claimUntil, reclaimed },
  ])
}

describe('lifecycle scheduler', () => {
  beforeEach(() => {
    mocks.config.isMultitenant = true
    mocks.config.pgQueueEnable = true
    mocks.config.pgQueueEnableWorkers = true
    mocks.config.storageVersioningEnabled = false
    mocks.config.storageLifecyclePaused = false
    mocks.central.claimDueTenantsWithinLeaseLimit.mockResolvedValue([])
    mocks.central.failTenantDispatch.mockResolvedValue(true)
    mocks.central.releaseTenantDispatches.mockResolvedValue(0)
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
    expect(mocks.central.claimDueTenantsWithinLeaseLimit).toHaveBeenCalledExactlyOnceWith({
      maxLeased: 100,
      leaseMs: 180_000,
      signal: expect.any(AbortSignal),
    })
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
      ({ signal }: { signal: AbortSignal }) =>
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
})
