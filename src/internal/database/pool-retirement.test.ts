import { afterEach, describe, expect, it, type Mock, vi } from 'vitest'
import {
  getRejectedReasons,
  PoolLeaseRetirement,
  PoolRetirementCoordinator,
  type PoolRetirementReason,
  type RetirablePool,
} from './pool-retirement'

afterEach(() => {
  vi.useRealTimers()
  vi.restoreAllMocks()
})

function createLeaseRetirement(leaseTimeoutMs = 1_000) {
  const retire = vi.fn(async (_reason: PoolRetirementReason) => undefined)
  const onLeaseTimeout = vi.fn()
  const lifecycle = new PoolLeaseRetirement({
    leaseTimeoutMs,
    retire,
    onLeaseTimeout,
  })
  return { lifecycle, retire, onLeaseTimeout }
}

function createRetirablePool(retirement: Promise<void> = Promise.resolve()): RetirablePool & {
  retire: Mock<() => Promise<void>>
  retireWhenReleased: Mock<() => Promise<void>>
} {
  let startedRetirement: Promise<void> | undefined
  const retire = vi.fn(() => {
    startedRetirement ??= retirement
    return startedRetirement
  })
  const retireWhenReleased = vi.fn(() => retire())
  return { retire, retireWhenReleased }
}

describe('getRejectedReasons', () => {
  it('returns rejection reasons in settlement order', () => {
    const firstError = new Error('first failure')
    const secondError = new Error('second failure')

    expect(
      getRejectedReasons([
        { status: 'fulfilled', value: undefined },
        { status: 'rejected', reason: firstError },
        { status: 'rejected', reason: secondError },
      ])
    ).toEqual([firstError, secondError])
  })
})

describe('PoolLeaseRetirement', () => {
  it('waits for every active lease before eviction retirement', async () => {
    const { lifecycle, retire } = createLeaseRetirement()
    expect(lifecycle.retain()).toBe(true)
    expect(lifecycle.retain()).toBe(true)

    const firstWaiter = lifecycle.retireWhenReleased()
    const secondWaiter = lifecycle.retireWhenReleased()

    expect(secondWaiter).toBe(firstWaiter)
    expect(lifecycle.retain()).toBe(false)
    expect(retire).not.toHaveBeenCalled()

    lifecycle.release()
    expect(retire).not.toHaveBeenCalled()

    lifecycle.release()
    await firstWaiter

    expect(retire).toHaveBeenCalledOnce()
    expect(retire).toHaveBeenCalledWith('evict')
    expect(lifecycle.isRetired).toBe(true)
  })

  it('forces a deferred retirement with destroy provenance', async () => {
    const retirement = Promise.withResolvers<void>()
    const retire = vi.fn((_reason: PoolRetirementReason) => retirement.promise)
    const lifecycle = new PoolLeaseRetirement({
      leaseTimeoutMs: 1_000,
      retire,
      onLeaseTimeout: vi.fn(),
    })
    lifecycle.retain()

    const deferred = lifecycle.retireWhenReleased()
    const forced = lifecycle.retire()

    expect(retire).toHaveBeenCalledOnce()
    expect(retire).toHaveBeenCalledWith('destroy')

    retirement.resolve()
    await expect(Promise.all([deferred, forced])).resolves.toEqual([undefined, undefined])

    lifecycle.release()
    expect(retire).toHaveBeenCalledOnce()
  })

  it('uses the deadline as an eviction backstop', async () => {
    vi.useFakeTimers()
    const { lifecycle, retire, onLeaseTimeout } = createLeaseRetirement()
    lifecycle.retain()

    const retirement = lifecycle.retireWhenReleased()
    await vi.advanceTimersByTimeAsync(1_000)
    await retirement

    expect(onLeaseTimeout).toHaveBeenCalledWith('evict', 1)
    expect(retire).toHaveBeenCalledWith('evict')

    lifecycle.release()
    expect(retire).toHaveBeenCalledOnce()
  })

  it('ignores unmatched releases without corrupting the lease count', async () => {
    const { lifecycle, retire } = createLeaseRetirement()

    lifecycle.release()
    expect(lifecycle.retain()).toBe(true)
    const retirement = lifecycle.retireWhenReleased()
    lifecycle.release()
    await retirement

    expect(retire).toHaveBeenCalledOnce()
  })
})

describe('PoolRetirementCoordinator', () => {
  it('logs a detached failure once and does not replay it during later shutdown', async () => {
    const retirement = Promise.withResolvers<void>()
    const pool = createRetirablePool(retirement.promise)
    const onDetachedFailure = vi.fn()
    const coordinator = new PoolRetirementCoordinator<RetirablePool>({ onDetachedFailure })
    const error = new Error('retirement failed')

    coordinator.defer('tenant-a', pool)
    retirement.reject(error)

    await vi.waitFor(() => {
      expect(onDetachedFailure).toHaveBeenCalledWith('tenant-a', error)
    })
    await expect(coordinator.waitForAll()).resolves.toEqual([])
    expect(onDetachedFailure).toHaveBeenCalledOnce()
  })

  it('leaves observed failures for the shutdown caller', async () => {
    const retirement = Promise.withResolvers<void>()
    const pool = createRetirablePool(retirement.promise)
    const onDetachedFailure = vi.fn()
    const coordinator = new PoolRetirementCoordinator<RetirablePool>({ onDetachedFailure })

    coordinator.defer('tenant-a', pool)
    const shutdown = coordinator.waitForAll()
    const error = new Error('observed failure')
    retirement.reject(error)

    await expect(shutdown).resolves.toEqual([{ status: 'rejected', reason: error }])
    expect(onDetachedFailure).not.toHaveBeenCalled()
    expect(pool.retire).toHaveBeenCalledTimes(2)
  })

  it('aggregates every pending failure for one tenant', async () => {
    const firstRetirement = Promise.withResolvers<void>()
    const secondRetirement = Promise.withResolvers<void>()
    const firstPool = createRetirablePool(firstRetirement.promise)
    const secondPool = createRetirablePool(secondRetirement.promise)
    const onDetachedFailure = vi.fn()
    const coordinator = new PoolRetirementCoordinator<RetirablePool>({ onDetachedFailure })

    coordinator.retireNow('tenant-a', firstPool)
    coordinator.retireNow('tenant-a', secondPool)
    const result = coordinator.waitForTenant('tenant-a').catch((error: unknown) => error)
    const firstError = new Error('first failure')
    const secondError = new Error('second failure')
    firstRetirement.reject(firstError)
    secondRetirement.reject(secondError)

    const error = await result
    expect(error).toBeInstanceOf(AggregateError)
    expect((error as AggregateError).errors).toEqual([firstError, secondError])
    expect(onDetachedFailure).not.toHaveBeenCalled()
  })

  it('exposes deferred pool snapshots, count, and oldest age', async () => {
    let now = 1_000
    vi.spyOn(performance, 'now').mockImplementation(() => now)
    const retirement = Promise.withResolvers<void>()
    const pool = createRetirablePool(retirement.promise)
    const coordinator = new PoolRetirementCoordinator<RetirablePool>({
      onDetachedFailure: vi.fn(),
    })

    coordinator.defer('tenant-a', pool)
    now = 3_500
    const snapshot: RetirablePool[] = []
    coordinator.appendDeferredSnapshotTo(snapshot)

    expect(snapshot).toEqual([pool])
    expect(coordinator.deferredCount).toBe(1)
    expect(coordinator.getOldestDeferredAgeSeconds()).toBe(2.5)

    retirement.resolve()
    await retirement.promise
    await vi.waitFor(() => {
      expect(coordinator.deferredCount).toBe(0)
    })
    expect(coordinator.getOldestDeferredAgeSeconds()).toBe(0)
  })
})
