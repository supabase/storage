import { logger, logSchema } from '@internal/monitoring'
import { lifecycleRunOutcomes } from '@internal/monitoring/metrics'
import { vi } from 'vitest'

const mocks = vi.hoisted(() => ({
  config: {
    storageVersioningEnabled: false,
    storageLifecycleClaimLeaseMs: 120_000,
    storageLifecycleJobBudgetMs: 20_000,
    storageLifecycleBucketConcurrency: 13,
    storageLifecycleTenantFanout: 11,
    isMultitenant: true,
  },
  createStorage: vi.fn(),
  executorRun: vi.fn(),
  tenantHasFeature: vi.fn(),
}))

vi.mock('../../../config', () => ({
  getConfig: () => mocks.config,
}))

vi.mock('@internal/database', () => ({
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

const tenant = { ref: 'tenant-a', host: '' }
const bucketId = 'bucket-a'
const dispatchId = '3e9a5442-0549-456d-8d86-2e66a8f15c0b'
const bucketHandler = new LifecycleBucketRunHandler()

function job<T>(data: T, signal = new AbortController().signal) {
  return {
    message: { data },
    signal,
  } as never
}

function mockBucketDb(buckets: string[] = []) {
  const database = {
    dispatchDueLifecycleBuckets: vi
      .fn()
      .mockResolvedValue(buckets.map((id) => ({ bucketId: id, dispatchId }))),
    releaseDispatchedLifecycleBuckets: vi.fn().mockResolvedValue(undefined),
  }
  const destroyConnection = vi.fn()
  mocks.createStorage.mockResolvedValue({
    db: { asSuperUser: () => database, destroyConnection },
  })
  return { database, destroyConnection }
}

describe('lifecycle bucket run', () => {
  beforeEach(() => {
    mocks.config.isMultitenant = true
    mocks.config.storageVersioningEnabled = false
    mocks.executorRun.mockResolvedValue({ status: 'COMPLETED' })
    mocks.tenantHasFeature.mockResolvedValue(true)
  })

  test('queue messages carry dedup identities and bypass event suppression', () => {
    expect(new LifecycleBucketRun({ tenant, bucketId, dispatchId }).idempotencyKey).toBe(
      `tenant-a/bucket-a/${dispatchId}`
    )
    expect(
      new LifecycleBucketRun({ tenant, bucketId, dispatchId: `${dispatchId.slice(0, -1)}1` })
        .idempotencyKey
    ).not.toBe(new LifecycleBucketRun({ tenant, bucketId, dispatchId }).idempotencyKey)
    expect(LifecycleBucketRun.disableKeys?.({ tenant, bucketId, dispatchId } as never)).toEqual([])
    expect(bucketHandler.options.prefetch).toBe(13)
    expect(bucketHandler.options.parallelism).toBe(13)
  })

  test.each([true, false])(
    'bucket delivery passes tenantLifecycleEnabled=%s from the objectVersioning feature',
    async (enabled) => {
      mocks.tenantHasFeature.mockResolvedValue(enabled)
      const { destroyConnection } = mockBucketDb()
      const outcomes = vi.spyOn(lifecycleRunOutcomes, 'add')
      const { signal } = new AbortController()

      await bucketHandler.handle(job({ tenant, bucketId, dispatchId }, signal))

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

  test('records an ERROR outcome if bucket storage creation fails', async () => {
    const failure = new Error('storage unavailable')
    mocks.createStorage.mockRejectedValue(failure)
    const outcomes = vi.spyOn(lifecycleRunOutcomes, 'add')

    await expect(bucketHandler.handle(job({ tenant, bucketId, dispatchId }))).rejects.toBe(failure)
    expect(outcomes).toHaveBeenCalledExactlyOnceWith(1, { status: 'ERROR' })
  })

  describe('bucket delivery top-up', () => {
    const topUpWarning = '[LifecycleRule] Failed to dispatch the next lifecycle bucket'

    test.each([
      { label: 'COMPLETED', result: { status: 'COMPLETED' } },
      { label: 'PARTIAL budget stop', result: { status: 'PARTIAL', reason: 'budget' } },
    ])('dispatches the next due bucket after a $label run', async ({ result }) => {
      mocks.executorRun.mockResolvedValue(result)
      const { database } = mockBucketDb(['bucket-b'])
      const send = vi.spyOn(LifecycleBucketRun, 'batchSend').mockResolvedValue(undefined)

      await bucketHandler.handle(job({ tenant, bucketId, dispatchId }))

      expect(database.dispatchDueLifecycleBuckets).toHaveBeenCalledExactlyOnceWith({
        limit: 11,
        redispatchMs: 900_000,
        leaseMs: 120_000,
        maxDispatches: 1,
      })
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

      await bucketHandler.handle(job({ tenant, bucketId, dispatchId }, controller.signal))

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
})
