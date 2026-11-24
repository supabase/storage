import { logger, logSchema } from '@internal/monitoring'
import { PartialSendError } from '@internal/queue/manager'
import { shutdownReason, timeoutReason } from '@internal/queue/manager/signals'
import { vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  config: {
    storageVersioningEnabled: false,
    storageLifecycleClaimLeaseMs: 120_000,
    storageLifecycleJobBudgetMs: 20_000,
    isMultitenant: true,
    tenantId: 'single-tenant',
    storageS3Bucket: 'data',
    storageS3MaxSockets: 10,
    storageBackendType: 's3',
    region: 'test',
  },
  central: {
    startTenantDispatch: vi.fn(),
    completeTenantDispatch: vi.fn(),
    failTenantDispatch: vi.fn(),
    releaseTenantDispatch: vi.fn(),
    hasObjectVersioningFeature: vi.fn(),
  },
  createStorage: vi.fn(),
  evaluationJitterMs: vi.fn((_key: string, _windowMs?: number) => 1_234),
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
    startTenantDispatch = mocks.central.startTenantDispatch
    completeTenantDispatch = mocks.central.completeTenantDispatch
    failTenantDispatch = mocks.central.failTenantDispatch
    releaseTenantDispatch = mocks.central.releaseTenantDispatch
    hasObjectVersioningFeature = mocks.central.hasObjectVersioningFeature
  },
  multitenantPgExecutor: {},
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
  evaluationJitterMs: (...args: [string, number?]) => mocks.evaluationJitterMs(...args),
  lifecycleErrorMessage: (error: unknown) =>
    (error instanceof Error ? error.message : String(error)).slice(0, 1000),
  NoncurrentLifecycleExecutor: class {},
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

import { LifecycleBucketRun } from './bucket-run'
import {
  computeNextTenantDispatchAt,
  LifecycleTenantRun,
  LifecycleTenantRunHandler,
  nextIdleRevisitAt,
} from './tenant-run'

const tenant = { ref: 'tenant-a', host: '' }
const claimId = '5bd8d514-9db1-4b1d-98a4-4a56fc569d95'
const claimUntil = new Date('2026-01-01T00:00:00.000Z')
const rotatedClaimId = '7c1f4a52-3e0b-4f5d-9a57-0d6c1f3b8e21'
const rotatedDeadline = new Date(Date.now() + 120_000)
const rotatedStartedAt = new Date('2026-01-01T00:00:00.000Z')
const bucketId = 'bucket-a'
const dispatchId = '3e9a5442-0549-456d-8d86-2e66a8f15c0b'
const evaluationIntervalMs = 24 * 60 * 60 * 1000
const revisitMs = evaluationIntervalMs + 1_234
const idleIntervalMs = 30 * evaluationIntervalMs
const tenantHandler = new LifecycleTenantRunHandler()

const dispatchState = (nextDispatchAt: Date | null = null, quietMs: number | null = 0) => ({
  nextDispatchAt,
  maxFailureCount: 0,
  oldestInFlightStartedAt: null,
  hasAbandonedArtifacts: false,
  quietMs,
})

function job<T>(data: T, startedOn = new Date(), signal = new AbortController().signal) {
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
  mocks.createStorage.mockResolvedValue(storage)
  return { database, destroyConnection, dispatches, storage }
}

function completedNextDispatchAt(call = 0) {
  return Date.parse(mocks.central.completeTenantDispatch.mock.calls[call][2])
}

describe('lifecycle tenant run', () => {
  beforeEach(() => {
    mocks.config.isMultitenant = true
    mocks.config.storageVersioningEnabled = false
    mocks.central.startTenantDispatch.mockResolvedValue({
      claimId: rotatedClaimId,
      claimUntil: rotatedDeadline,
      startedAt: rotatedStartedAt,
    })
    mocks.central.completeTenantDispatch.mockResolvedValue(true)
    mocks.central.failTenantDispatch.mockResolvedValue(true)
    mocks.central.releaseTenantDispatch.mockResolvedValue(true)
    mocks.central.hasObjectVersioningFeature.mockResolvedValue(false)
    mocks.evaluationJitterMs.mockReturnValue(1_234)
    mocks.tenantHasFeature.mockResolvedValue(true)
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  test('queue messages carry dedup identities and bypass event suppression', () => {
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
    expect(tenantHandler.options.prefetch).toBe(tenantHandler.options.parallelism)
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
      observation,
      rotatedStartedAt.toISOString()
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
      mocks.createStorage.mockRejectedValue(failure)

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
    mocks.createStorage.mockRejectedValue(failure)
    mocks.central.failTenantDispatch.mockRejectedValueOnce(new Error('central down'))

    await expect(tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))).rejects.toThrow(
      'central down'
    )

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

    await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }, new Date(), signal))

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
      { label: 'recently active (10m)', quietMs: 10 * 60_000, expectedDelta: 60 * 60_000 },
      { label: 'quiet for 4h', quietMs: 4 * 60 * 60_000, expectedDelta: 4 * 60 * 60_000 },
      { label: 'quiet for 48h', quietMs: 48 * 60 * 60_000, expectedDelta: 48 * 60 * 60_000 },
      {
        label: 'quiet for 45d (clamped to idle phase)',
        quietMs: 45 * evaluationIntervalMs,
        expectedDelta: null,
      },
      { label: 'no bucket state (null)', quietMs: null, expectedDelta: null },
    ])(
      'backs off a gate-closed empty-outbox tenant when $label',
      async ({ quietMs, expectedDelta }) => {
        vi.useFakeTimers({ toFake: ['Date'] })
        vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'))
        mocks.evaluationJitterMs.mockReturnValue(10 * evaluationIntervalMs)
        mocks.tenantHasFeature.mockResolvedValue(false)
        mockTenantDb([], dispatchState(null, quietMs))

        await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

        const expectedAt =
          expectedDelta === null
            ? nextIdleRevisitAt(Date.now(), 'tenant-a')
            : Date.now() + expectedDelta
        expect(completedNextDispatchAt()).toBe(expectedAt)
        expect(mocks.evaluationJitterMs).toHaveBeenCalledWith('tenant-a', idleIntervalMs)
      }
    )

    test('uses the fixed 30-day idle phase slot for an enabled tenant with nothing scheduled', async () => {
      vi.useFakeTimers({ toFake: ['Date'] })
      vi.setSystemTime(new Date('2026-01-01T00:00:00.000Z'))
      mocks.evaluationJitterMs.mockReturnValue(10 * evaluationIntervalMs)
      mockTenantDb([], dispatchState(null))

      await tenantHandler.handle(job({ tenant, centralClaim: { claimId } }))

      const firstSlot = nextIdleRevisitAt(Date.now(), 'tenant-a')
      expect(completedNextDispatchAt()).toBe(firstSlot)
      expect(firstSlot - Date.now()).toBeGreaterThanOrEqual(evaluationIntervalMs)
      expect(firstSlot - Date.now()).toBeLessThan(idleIntervalMs + evaluationIntervalMs)
      // Once on the slot, the next idle revisit is exactly 30 days later.
      expect(nextIdleRevisitAt(firstSlot, 'tenant-a')).toBe(firstSlot + idleIntervalMs)
      expect(mocks.evaluationJitterMs).toHaveBeenCalledWith('tenant-a', idleIntervalMs)
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
          job({ tenant, centralClaim: { claimId } }, new Date(), controller.signal)
        )
      ).rejects.toBe(timeout)

      expect(database.dispatchDueLifecycleBuckets).not.toHaveBeenCalled()
      expect(mocks.central.completeTenantDispatch).not.toHaveBeenCalled()
    })
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
    mocks.createStorage.mockRejectedValue(failure)

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
        job({ tenant, centralClaim: { claimId } }, new Date(), controller.signal)
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
        job({ tenant, centralClaim: { claimId } }, new Date(), controller.signal)
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
        job({ tenant, centralClaim: { claimId } }, new Date(), controller.signal)
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
        job({ tenant, centralClaim: { claimId } }, new Date(), controller.signal)
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
        job({ tenant, centralClaim: { claimId } }, new Date(), controller.signal)
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
        job({ tenant, centralClaim: { claimId } }, new Date(), controller.signal)
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
        job({ tenant, centralClaim: { claimId } }, new Date(), controller.signal)
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

describe('computeNextTenantDispatchAt', () => {
  const now = new Date('2026-08-15T12:00:00.000Z').getTime()

  test('uses scheduled nextDispatchAt when active and within the daily cap', () => {
    const scheduled = new Date(now + 10 * 60_000)
    const next = computeNextTenantDispatchAt({
      now,
      tenantRef: 'tenant-a',
      dispatchState: dispatchState(scheduled),
      gateClosed: false,
      staleFeatureCache: false,
      jobBudgetMs: 20_000,
    })
    expect(next).toEqual(scheduled)
  })

  test('floors overdue dispatches by min(jobBudgetMs, 60s)', () => {
    const overdue = new Date(now - 5_000)
    expect(
      computeNextTenantDispatchAt({
        now,
        tenantRef: 'tenant-a',
        dispatchState: dispatchState(overdue),
        gateClosed: false,
        staleFeatureCache: false,
        jobBudgetMs: 20_000,
      }).getTime()
    ).toBe(now + 20_000)

    expect(
      computeNextTenantDispatchAt({
        now,
        tenantRef: 'tenant-a',
        dispatchState: dispatchState(overdue),
        gateClosed: false,
        staleFeatureCache: false,
        jobBudgetMs: 120_000,
      }).getTime()
    ).toBe(now + 60_000)
  })

  test('caps active revisit at 1h when abandoned artifacts remain', () => {
    const next = computeNextTenantDispatchAt({
      now,
      tenantRef: 'tenant-a',
      dispatchState: {
        ...dispatchState(new Date(now + 6 * 3_600_000)),
        hasAbandonedArtifacts: true,
      },
      gateClosed: false,
      staleFeatureCache: false,
      jobBudgetMs: 20_000,
    })
    expect(next.getTime()).toBe(now + 3_600_000)
  })

  test('routes idle active tenants to the 30-day phase slot and unmigrated tenants to the daily cap', () => {
    const idle = computeNextTenantDispatchAt({
      now,
      tenantRef: 'tenant-a',
      dispatchState: dispatchState(null),
      gateClosed: false,
      staleFeatureCache: false,
      jobBudgetMs: 20_000,
    })
    expect(idle.getTime()).toBe(nextIdleRevisitAt(now, 'tenant-a'))

    const unmigrated = computeNextTenantDispatchAt({
      now,
      tenantRef: 'tenant-a',
      dispatchState: undefined,
      gateClosed: false,
      staleFeatureCache: false,
      jobBudgetMs: 20_000,
    })
    expect(unmigrated.getTime()).toBe(now + revisitMs)
  })

  test('handles closed-gate stale cache (5m), abandoned artifacts (1h), and quiet backoff capped by idle slot', () => {
    const stale = computeNextTenantDispatchAt({
      now,
      tenantRef: 'tenant-a',
      dispatchState: dispatchState(null, 0),
      gateClosed: true,
      staleFeatureCache: true,
      jobBudgetMs: 20_000,
    })
    expect(stale.getTime()).toBe(now + 5 * 60_000)

    const abandoned = computeNextTenantDispatchAt({
      now,
      tenantRef: 'tenant-a',
      dispatchState: { ...dispatchState(null, 12 * 3_600_000), hasAbandonedArtifacts: true },
      gateClosed: true,
      staleFeatureCache: false,
      jobBudgetMs: 20_000,
    })
    expect(abandoned.getTime()).toBe(now + 3_600_000)

    const quiet = computeNextTenantDispatchAt({
      now,
      tenantRef: 'tenant-a',
      dispatchState: dispatchState(null, 6 * 3_600_000),
      gateClosed: true,
      staleFeatureCache: false,
      jobBudgetMs: 20_000,
    })
    expect(quiet.getTime()).toBe(now + 6 * 3_600_000)

    const fullyIdle = computeNextTenantDispatchAt({
      now,
      tenantRef: 'tenant-a',
      dispatchState: dispatchState(null, idleIntervalMs * 2),
      gateClosed: true,
      staleFeatureCache: false,
      jobBudgetMs: 20_000,
    })
    expect(fullyIdle.getTime()).toBe(nextIdleRevisitAt(now, 'tenant-a'))
  })
})
