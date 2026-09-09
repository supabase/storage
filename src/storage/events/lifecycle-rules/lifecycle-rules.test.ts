import { AsyncAbortController } from '@internal/concurrency'
import { logger, logSchema } from '@internal/monitoring'
import { lifecycleRunDuration, lifecycleShardRuns } from '@internal/monitoring/metrics'
import type { BasePayload } from '@internal/queue'
import { vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  config: {
    storageLifecycleEnabled: true,
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
    claimDueTenants: vi.fn(),
    completeTenantDispatch: vi.fn(),
    failTenantDispatch: vi.fn(),
  },
  executorRun: vi.fn(),
  tenantHasFeature: vi.fn(),
}))

vi.mock('../../../config', () => ({
  getConfig: () => mocks.config,
}))

vi.mock('@internal/database', () => ({
  LifecycleTenantStorePg: class {
    claimDueTenants = mocks.central.claimDueTenants
    completeTenantDispatch = mocks.central.completeTenantDispatch
    failTenantDispatch = mocks.central.failTenantDispatch
  },
  multitenantPgExecutor: {},
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

vi.mock('../../lifecycle/executor', () => ({
  LIFECYCLE_EVALUATION_INTERVAL_MS: 24 * 60 * 60 * 1000,
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

import { LifecycleBucketShardRun } from './bucket-shard-run'
import { dispatchDueLifecycleTenants, startLifecycleScheduler } from './scheduler'
import { LifecycleTenantRun } from './tenant-run'

const tenant = { ref: 'tenant-a', host: '' }
const claimId = '5bd8d514-9db1-4b1d-98a4-4a56fc569d95'
const coordinate = {
  bucketId: 'bucket-a',
  scanKind: 'NONCURRENT' as const,
  shardEpoch: '1',
  shardId: 0,
}
const dispatchState = (nextDispatchAt: Date | null = null) => ({
  nextDispatchAt,
  maxFailureCount: 0,
  oldestInFlightStartedAt: null,
})
const evaluationIntervalMs = 24 * 60 * 60 * 1000
const schedulers: AsyncAbortController[] = []

function startScheduler(controller = new AsyncAbortController()) {
  vi.useFakeTimers()
  schedulers.push(controller)
  startLifecycleScheduler(controller.signal)
  return controller
}

function job<T>(data: T, retryCount = 0) {
  return { id: 'job-a', data, retryCount, retryLimit: 10 } as never
}

function mockCreateStorage(target: unknown, storage: unknown) {
  return vi
    .spyOn(target as { createStorage(payload: BasePayload): Promise<unknown> }, 'createStorage')
    .mockResolvedValue(storage)
}

describe('lifecycle scheduling and events', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mocks.config.storageLifecycleEnabled = true
    mocks.config.storageBackendType = 's3'
    mocks.config.isMultitenant = true
    mocks.config.pgQueueEnable = true
    mocks.config.pgQueueEnableWorkers = true
    mocks.central.claimDueTenants.mockResolvedValue([])
    mocks.central.completeTenantDispatch.mockResolvedValue(true)
    mocks.central.failTenantDispatch.mockResolvedValue(true)
    mocks.executorRun.mockResolvedValue({ status: 'COMPLETED' })
    mocks.tenantHasFeature.mockResolvedValue(true)
    vi.spyOn(Math, 'random').mockReturnValue(0.5)
  })

  afterEach(async () => {
    await Promise.all(schedulers.splice(0).map((controller) => controller.abortAsync()))
    vi.useRealTimers()
    vi.restoreAllMocks()
  })

  test('claims central tenants and sends only the central dispatch fence', async () => {
    mocks.central.claimDueTenants.mockResolvedValue([{ tenantId: 'tenant-a', claimId }])
    const send = vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue('tenant-job')

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(mocks.central.claimDueTenants).toHaveBeenCalledWith(100, 120_000)
    expect(send).toHaveBeenCalledWith({ tenant, centralClaim: { claimId } })
  })

  test('single-tenant deployments dispatch the configured tenant without a central claim', async () => {
    mocks.config.isMultitenant = false
    const send = vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue('tenant-job')

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(send).toHaveBeenCalledWith({ tenant: { ref: 'single-tenant', host: '' } })
    expect(mocks.central.claimDueTenants).not.toHaveBeenCalled()
  })

  test('releases a central claim with backoff when the tenant enqueue fails', async () => {
    mocks.central.claimDueTenants.mockResolvedValue([{ tenantId: 'tenant-a', claimId }])
    vi.spyOn(LifecycleTenantRun, 'send').mockRejectedValue(new Error('queue unavailable'))

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(mocks.central.failTenantDispatch).toHaveBeenCalledWith('tenant-a', claimId, {
      code: 'tenant_dispatch_enqueue_failed',
      message: 'queue unavailable',
    })
  })

  test('leaves the claim to lapse when exactly-once drops a duplicate tenant job', async () => {
    mocks.central.claimDueTenants.mockResolvedValue([{ tenantId: 'tenant-a', claimId }])
    vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue(null)

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    expect(logSchema.error).not.toHaveBeenCalled()
  })

  test.each([
    'pgQueueEnable',
    'pgQueueEnableWorkers',
  ] as const)('does not start a timer when %s is disabled', async (flag) => {
    mocks.config[flag] = false
    const send = vi.spyOn(LifecycleTenantRun, 'send')
    startScheduler()
    await vi.advanceTimersByTimeAsync(180_000)

    expect(mocks.central.claimDueTenants).not.toHaveBeenCalled()
    expect(send).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)
  })

  test.each(
    [
      { isMultitenant: true, flag: false, starts: true },
      { isMultitenant: true, flag: true, starts: true },
      { isMultitenant: false, flag: true, starts: true },
      { isMultitenant: false, flag: false, starts: false },
    ].flatMap((settings) => ['s3', 'file'].map((backend) => ({ ...settings, backend })))
  )('scheduler starts=$starts with multitenant=$isMultitenant lifecycle flag=$flag backend=$backend', async ({
    isMultitenant,
    flag,
    starts,
    backend,
  }) => {
    mocks.config.isMultitenant = isMultitenant
    mocks.config.storageLifecycleEnabled = flag
    mocks.config.storageBackendType = backend
    const send = vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue('tenant-job')
    startScheduler()
    await vi.advanceTimersByTimeAsync(0)

    expect(send.mock.calls.length > 0 || mocks.central.claimDueTenants.mock.calls.length > 0).toBe(
      starts
    )
    expect(vi.getTimerCount()).toBe(starts ? 1 : 0)
  })

  test.each([
    { isMultitenant: false, storageLifecycleEnabled: false, storageBackendType: 's3' },
    { isMultitenant: false, storageLifecycleEnabled: false, storageBackendType: 'file' },
  ])('leaves dispatch state alone while execution is unavailable: %j', async (settings) => {
    Object.assign(mocks.config, settings)
    const send = vi.spyOn(LifecycleTenantRun, 'send')
    startScheduler()
    await vi.advanceTimersByTimeAsync(180_000)

    expect(mocks.central.claimDueTenants).not.toHaveBeenCalled()
    expect(send).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)
  })

  test('dispatches pending work immediately on a new enabled startup after a disabled startup', async () => {
    mocks.config.storageBackendType = 'file'
    mocks.config.isMultitenant = false
    mocks.config.storageLifecycleEnabled = false
    const send = vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue('pending-tenant-job')
    const stopped = startScheduler()
    await vi.advanceTimersByTimeAsync(180_000)
    expect(send).not.toHaveBeenCalled()
    await stopped.abortAsync()

    mocks.config.storageLifecycleEnabled = true
    startScheduler()
    await vi.advanceTimersByTimeAsync(0)

    expect(send).toHaveBeenCalledExactlyOnceWith({
      tenant: { ref: 'single-tenant', host: '' },
    })
  })

  test('does not dispatch or start a timer with an already aborted signal', async () => {
    const controller = new AsyncAbortController()
    controller.abort()
    startScheduler(controller)
    await dispatchDueLifecycleTenants(controller.signal)

    expect(mocks.central.claimDueTenants).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)
  })

  test('polls immediately, then about every minute with jitter, and stops on abort', async () => {
    vi.mocked(Math.random).mockReturnValueOnce(0).mockReturnValueOnce(0.9)
    const controller = startScheduler()
    await vi.advanceTimersByTimeAsync(0)
    expect(mocks.central.claimDueTenants).toHaveBeenCalledTimes(1)

    await vi.advanceTimersByTimeAsync(54_999)
    expect(mocks.central.claimDueTenants).toHaveBeenCalledTimes(1)
    await vi.advanceTimersByTimeAsync(1)
    expect(mocks.central.claimDueTenants).toHaveBeenCalledTimes(2)
    await vi.advanceTimersByTimeAsync(64_000)
    expect(mocks.central.claimDueTenants).toHaveBeenCalledTimes(3)

    await controller.abortAsync()
    await vi.advanceTimersByTimeAsync(180_000)
    expect(mocks.central.claimDueTenants).toHaveBeenCalledTimes(3)
    expect(vi.getTimerCount()).toBe(0)
  })

  test('retries a failed poll on the next timer', async () => {
    const error = new Error('central database unavailable')
    mocks.central.claimDueTenants.mockRejectedValueOnce(error)
    startScheduler()
    await vi.advanceTimersByTimeAsync(0)

    expect(logSchema.error).toHaveBeenCalledWith(logger, '[Lifecycle] Scheduler poll failed', {
      type: 'event',
      error,
    })
    await vi.advanceTimersByTimeAsync(60_000)
    expect(mocks.central.claimDueTenants).toHaveBeenCalledTimes(2)
    expect(vi.getTimerCount()).toBe(1)
  })

  test('never overlaps a slow poll', async () => {
    const pendingClaim = Promise.withResolvers<[]>()
    mocks.central.claimDueTenants.mockReturnValueOnce(pendingClaim.promise)
    startScheduler()
    try {
      await vi.advanceTimersByTimeAsync(180_000)
      expect(mocks.central.claimDueTenants).toHaveBeenCalledTimes(1)
      expect(vi.getTimerCount()).toBe(0)
    } finally {
      pendingClaim.resolve([])
    }
    await vi.advanceTimersByTimeAsync(60_000)
    expect(mocks.central.claimDueTenants).toHaveBeenCalledTimes(2)
  })

  test('drains all claimed sends before queue shutdown, even if releasing a failed claim fails', async () => {
    const recoveryError = new Error('central database unavailable')
    const pendingSend = Promise.withResolvers<string>()
    mocks.central.claimDueTenants.mockResolvedValue([
      { tenantId: 'tenant-a', claimId },
      { tenantId: 'tenant-b', claimId },
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
        '[Lifecycle] Failed to release tenant dispatch claim',
        { type: 'event', project: 'tenant-a', error: recoveryError }
      )
      expect(vi.getTimerCount()).toBe(0)
    } finally {
      pendingSend.resolve('tenant-job')
    }
  })

  test('drains full batches up to a bounded number of tenant dispatches per poll', async () => {
    mocks.central.claimDueTenants.mockResolvedValue(
      Array.from({ length: 100 }, (_, index) => ({ tenantId: `tenant-${index}`, claimId }))
    )
    const send = vi.spyOn(LifecycleTenantRun, 'send').mockResolvedValue('tenant-job')

    await dispatchDueLifecycleTenants(new AbortController().signal)

    expect(mocks.central.claimDueTenants).toHaveBeenCalledTimes(10)
    expect(send).toHaveBeenCalledTimes(1000)
  })

  test('tenant dispatcher sends bounded shard coordinates without the central claim id', async () => {
    const observation = {
      ...dispatchState(new Date('2026-08-16T00:00:00.000Z')),
      maxFailureCount: 7,
      oldestInFlightStartedAt: '2026-08-15T00:00:00.000Z',
    }
    const database = {
      hasMigration: vi.fn().mockResolvedValue(true),
      listDueLifecycleShards: vi.fn().mockResolvedValue([coordinate]),
      findLifecycleDispatchState: vi.fn().mockResolvedValue(observation),
    }
    const destroyConnection = vi.fn()
    mockCreateStorage(LifecycleTenantRun, {
      db: { asSuperUser: () => database, destroyConnection },
    })
    const send = vi.spyOn(LifecycleBucketShardRun, 'send').mockResolvedValue('shard-job')

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

    expect(database.listDueLifecycleShards).toHaveBeenCalledWith(100, { inFlightOnly: false })
    expect(database.findLifecycleDispatchState).toHaveBeenCalledWith({ inFlightOnly: false })
    expect(send).toHaveBeenCalledWith({ tenant, ...coordinate })
    expect(send.mock.calls[0]?.[0]).not.toHaveProperty('claimId')
    expect(mocks.central.completeTenantDispatch).toHaveBeenCalledWith(
      'tenant-a',
      claimId,
      expect.any(String),
      observation
    )
    expect(destroyConnection).toHaveBeenCalledOnce()
  })

  test('sends every due coordinate and caps a distant next dispatch at the evaluation interval', async () => {
    const second = { ...coordinate, shardId: 1 }
    const database = {
      hasMigration: vi.fn().mockResolvedValue(true),
      listDueLifecycleShards: vi.fn().mockResolvedValue([coordinate, second]),
      findLifecycleDispatchState: vi
        .fn()
        .mockResolvedValue(dispatchState(new Date(Date.now() + 3 * evaluationIntervalMs))),
    }
    mockCreateStorage(LifecycleTenantRun, {
      db: { asSuperUser: () => database, destroyConnection: vi.fn() },
    })
    const send = vi.spyOn(LifecycleBucketShardRun, 'send').mockResolvedValue('shard-job')
    const startedAt = Date.now()

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

    expect(send).toHaveBeenCalledTimes(2)
    expect(send).toHaveBeenCalledWith({ tenant, ...coordinate })
    expect(send).toHaveBeenCalledWith({ tenant, ...second })
    const scheduledAt = Date.parse(mocks.central.completeTenantDispatch.mock.calls[0][2])
    expect(scheduledAt).toBeGreaterThanOrEqual(startedAt + evaluationIntervalMs)
    expect(scheduledAt).toBeLessThanOrEqual(Date.now() + evaluationIntervalMs)
  })

  test('does nothing for a job that starts after the queue shutdown signal', async () => {
    const createStorage = mockCreateStorage(LifecycleTenantRun, {})
    const send = vi.spyOn(LifecycleBucketShardRun, 'send')

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }), {
      signal: AbortSignal.abort(),
    })

    expect(createStorage).not.toHaveBeenCalled()
    expect(send).not.toHaveBeenCalled()
    expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
  })

  test('fails the central dispatch with tenant_dispatch_failed and does not complete it', async () => {
    mockCreateStorage(LifecycleTenantRun, {}).mockRejectedValue(new Error('tenant offline'))

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }, 0))

    expect(mocks.central.failTenantDispatch).toHaveBeenCalledWith('tenant-a', claimId, {
      code: 'tenant_dispatch_failed',
      message: 'tenant offline',
    })
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

  test('warns when completing the central dispatch finds no claim', async () => {
    const database = {
      hasMigration: vi.fn().mockResolvedValue(false),
    }
    mockCreateStorage(LifecycleTenantRun, {
      db: { asSuperUser: () => database, destroyConnection: vi.fn() },
    })
    mocks.central.completeTenantDispatch.mockResolvedValue(false)

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

    expect(logSchema.warning).toHaveBeenCalledWith(
      logger,
      '[Lifecycle] Tenant dispatch lost its claim',
      { type: 'event', project: 'tenant-a' }
    )
  })

  test.each([
    true,
    false,
  ])('bounds the revisit for an inactive tenant (versioning=%s)', async (versioning) => {
    mocks.tenantHasFeature.mockResolvedValueOnce(versioning)
    const database = {
      hasMigration: vi.fn().mockResolvedValue(true),
      listDueLifecycleShards: vi.fn().mockResolvedValue(versioning ? [] : [coordinate]),
      findLifecycleDispatchState: vi.fn().mockResolvedValue(dispatchState()),
    }
    const destroyConnection = vi.fn()
    mockCreateStorage(LifecycleTenantRun, {
      db: { asSuperUser: () => database, destroyConnection },
    })
    const send = vi.spyOn(LifecycleBucketShardRun, 'send').mockResolvedValue('shard-job')

    const startedAt = Date.now()
    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))
    const completedAt = Date.now()

    expect(mocks.tenantHasFeature).toHaveBeenCalledWith('tenant-a', 'objectVersioning')
    expect(database.listDueLifecycleShards).toHaveBeenCalledWith(100, { inFlightOnly: !versioning })
    expect(database.findLifecycleDispatchState).toHaveBeenCalledWith({ inFlightOnly: !versioning })
    if (versioning) expect(send).not.toHaveBeenCalled()
    else expect(send).toHaveBeenCalledWith({ tenant, ...coordinate })
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    const scheduledAt = Date.parse(mocks.central.completeTenantDispatch.mock.calls[0][2])
    expect(scheduledAt).toBeGreaterThanOrEqual(startedAt + evaluationIntervalMs)
    expect(scheduledAt).toBeLessThanOrEqual(completedAt + evaluationIntervalMs)
    expect(destroyConnection).toHaveBeenCalledOnce()
  })

  test('defers enrolled tenants until promotion fencing is installed', async () => {
    const database = {
      hasMigration: vi.fn().mockResolvedValue(false),
      listDueLifecycleShards: vi.fn(),
    }
    const destroyConnection = vi.fn()
    mockCreateStorage(LifecycleTenantRun, {
      db: { asSuperUser: () => database, destroyConnection },
    })
    const send = vi.spyOn(LifecycleBucketShardRun, 'send').mockResolvedValue('shard-job')
    const startedAt = Date.now()

    await LifecycleTenantRun.handle(job({ tenant, centralClaim: { claimId } }))

    expect(database.hasMigration).toHaveBeenCalledWith('lifecycle-writer-coordination')
    expect(database.listDueLifecycleShards).not.toHaveBeenCalled()
    expect(send).not.toHaveBeenCalled()
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
    const scheduledAt = Date.parse(mocks.central.completeTenantDispatch.mock.calls[0][2])
    expect(scheduledAt).toBeGreaterThanOrEqual(startedAt + evaluationIntervalMs)
    expect(scheduledAt).toBeLessThanOrEqual(Date.now() + evaluationIntervalMs)
    expect(destroyConnection).toHaveBeenCalledOnce()
  })

  test('bucket singleton identity includes tenant, scan kind, epoch, and shard', () => {
    expect(LifecycleBucketShardRun.getSendOptions({ tenant, ...coordinate })).toEqual(
      expect.objectContaining({
        singletonKey: 'tenant-a/bucket-a/NONCURRENT/1/0',
      })
    )
  })

  test.each([
    ['tenant', () => LifecycleTenantRun.getSendOptions({ tenant })],
    ['bucket', () => LifecycleBucketShardRun.getSendOptions({ tenant, ...coordinate })],
  ])('%s jobs leave retries to the durable lifecycle state', (_name, options) => {
    expect(options()).toMatchObject({ retryLimit: 0 })
    expect(options()).not.toHaveProperty('retryDelay')
  })

  test.each([
    true,
    false,
  ])('bucket delivery passes tenantLifecycleEnabled=%s from the objectVersioning feature', async (enabled) => {
    mocks.tenantHasFeature.mockResolvedValue(enabled)
    const destroyConnection = vi.fn()
    mockCreateStorage(LifecycleBucketShardRun, { db: { destroyConnection } })
    const runs = vi.spyOn(lifecycleShardRuns, 'add')
    const duration = vi.spyOn(lifecycleRunDuration, 'record')

    await LifecycleBucketShardRun.handle(job({ tenant, ...coordinate }))

    expect(mocks.executorRun).toHaveBeenCalledWith(coordinate, {
      tenantLifecycleEnabled: enabled,
    })
    expect(mocks.tenantHasFeature).toHaveBeenCalledWith('tenant-a', 'objectVersioning')
    expect(destroyConnection).toHaveBeenCalledOnce()
    const attributes = { status: 'COMPLETED', scan_kind: 'NONCURRENT' }
    expect(runs).toHaveBeenCalledExactlyOnceWith(1, attributes)
    expect(duration).toHaveBeenCalledExactlyOnceWith(expect.any(Number), attributes)
  })

  test('bucket delivery failure propagates, closes the connection and records an ERROR run', async () => {
    const failure = new Error('executor failed')
    mocks.executorRun.mockRejectedValue(failure)
    const destroyConnection = vi.fn()
    mockCreateStorage(LifecycleBucketShardRun, { db: { destroyConnection } })
    const runs = vi.spyOn(lifecycleShardRuns, 'add')
    const duration = vi.spyOn(lifecycleRunDuration, 'record')

    await expect(LifecycleBucketShardRun.handle(job({ tenant, ...coordinate }))).rejects.toBe(
      failure
    )

    expect(destroyConnection).toHaveBeenCalledOnce()
    expect(runs).toHaveBeenCalledWith(1, {
      status: 'ERROR',
      scan_kind: 'NONCURRENT',
    })
    expect(duration).toHaveBeenCalledOnce()
  })

  test('single-tenant tenant dispatcher failure is rethrown for queue retry', async () => {
    mocks.config.isMultitenant = false
    const failure = new Error('tenant offline')
    mockCreateStorage(LifecycleTenantRun, {}).mockRejectedValue(failure)

    await expect(LifecycleTenantRun.handle(job({ tenant }))).rejects.toBe(failure)
    expect(mocks.central.failTenantDispatch).not.toHaveBeenCalled()
  })
})
