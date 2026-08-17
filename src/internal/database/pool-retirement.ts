export type PoolRetirementReason = 'destroy' | 'evict'

export interface RetirablePool {
  retire(): Promise<void> // idempotent
  retireWhenReleased(): Promise<void>
}

export function getRejectedReasons(results: readonly PromiseSettledResult<unknown>[]): unknown[] {
  return results.flatMap((result) => (result.status === 'rejected' ? [result.reason] : []))
}

interface PoolLeaseRetirementOptions {
  leaseTimeoutMs: number
  retire: (reason: PoolRetirementReason) => Promise<void>
  onLeaseTimeout: (reason: PoolRetirementReason, activeLeases: number) => void
}

type PoolRetirementState = 'active' | 'draining' | 'retired'

// Owns the per-strategy request-lease state machine.
export class PoolLeaseRetirement {
  private activeLeaseCount = 0
  private state: PoolRetirementState = 'active'
  private retirement?: Promise<void>
  private retirementWaiter?: ReturnType<typeof Promise.withResolvers<void>>
  private leaseTimeout?: ReturnType<typeof setTimeout>
  private readonly options: PoolLeaseRetirementOptions

  constructor(options: PoolLeaseRetirementOptions) {
    this.options = options
  }

  retain(): boolean {
    if (this.state !== 'active') {
      return false
    }

    this.activeLeaseCount++
    return true
  }

  release(): void {
    if (this.activeLeaseCount === 0) {
      return
    }

    this.activeLeaseCount--
    if (this.activeLeaseCount === 0 && this.state === 'draining') {
      void this.startRetirement('evict')
    }
  }

  retireWhenReleased(): Promise<void> {
    if (this.retirement) {
      return this.retirement
    }

    this.state = 'draining'
    if (this.activeLeaseCount === 0) {
      return this.startRetirement('evict')
    }

    this.retirementWaiter ??= Promise.withResolvers<void>()
    if (!this.leaseTimeout) {
      this.leaseTimeout = setTimeout(() => {
        this.options.onLeaseTimeout('evict', this.activeLeaseCount)
        void this.startRetirement('evict')
      }, this.options.leaseTimeoutMs)
      this.leaseTimeout.unref()
    }
    return this.retirementWaiter.promise
  }

  retire(): Promise<void> {
    return this.startRetirement('destroy')
  }

  get isRetired(): boolean {
    return this.state === 'retired'
  }

  private startRetirement(reason: PoolRetirementReason): Promise<void> {
    if (this.retirement) {
      return this.retirement
    }

    this.state = 'retired'
    if (this.leaseTimeout) {
      clearTimeout(this.leaseTimeout)
      this.leaseTimeout = undefined
    }

    const retirement = this.options.retire(reason)
    this.retirement = retirement
    if (this.retirementWaiter) {
      void retirement.then(this.retirementWaiter.resolve, this.retirementWaiter.reject)
    }
    return retirement
  }
}

// Keeps tenant and global lifecycle views derived from one mutation path.
// Global consumers receive snapshots so teardown completion cannot mutate an
// iteration that is already in progress.
class TenantIndexedRegistry<T> {
  private readonly entriesByTenant = new Map<string, Set<T>>()

  add(tenantId: string, value: T): boolean {
    let entries = this.entriesByTenant.get(tenantId)
    if (!entries) {
      entries = new Set()
      this.entriesByTenant.set(tenantId, entries)
    }
    const previousSize = entries.size
    entries.add(value)
    return entries.size !== previousSize
  }

  delete(tenantId: string, value: T): boolean {
    const entries = this.entriesByTenant.get(tenantId)
    if (!entries || !entries.delete(value)) {
      return false
    }
    if (entries.size === 0) {
      this.entriesByTenant.delete(tenantId)
    }
    return true
  }

  get(tenantId: string): ReadonlySet<T> | undefined {
    return this.entriesByTenant.get(tenantId)
  }

  appendSnapshotTo(target: T[]): void {
    for (const entries of this.entriesByTenant.values()) {
      for (const entry of entries) {
        target.push(entry)
      }
    }
  }

  snapshot(): T[] {
    const snapshot: T[] = []
    this.appendSnapshotTo(snapshot)
    return snapshot
  }
}

// Capacity-evicted pools have a single tenant owner. Keep their age metadata
// behind the same add/delete path as the tenant index so count and age reads are O(1).
class DeferredPoolRetirementRegistry<TPool> extends TenantIndexedRegistry<TPool> {
  private readonly startedAt = new Map<TPool, number>()

  override add(tenantId: string, pool: TPool): boolean {
    const added = super.add(tenantId, pool)
    if (added) {
      this.startedAt.set(pool, performance.now())
    }
    return added
  }

  override delete(tenantId: string, pool: TPool): boolean {
    const deleted = super.delete(tenantId, pool)
    if (deleted) {
      this.startedAt.delete(pool)
    }
    return deleted
  }

  get size(): number {
    return this.startedAt.size
  }

  getOldestAgeSeconds(now = performance.now()): number {
    const oldest = this.startedAt.values().next()
    if (oldest.done) {
      return 0
    }
    return Math.max(now - oldest.value, 0) / 1000
  }
}

interface PoolRetirementCoordinatorOptions {
  onDetachedFailure: (tenantId: string, error: unknown) => void
}

// Coordinates pending retirements across cache eviction, explicit tenant
// destruction, and process shutdown without taking ownership of pool creation.
export class PoolRetirementCoordinator<TPool extends RetirablePool> {
  private readonly pending = new TenantIndexedRegistry<TPool>()
  private readonly deferred = new DeferredPoolRetirementRegistry<TPool>()
  private readonly observed = new WeakSet<TPool>()
  private readonly onDetachedFailure: (tenantId: string, error: unknown) => void

  constructor(options: PoolRetirementCoordinatorOptions) {
    this.onDetachedFailure = options.onDetachedFailure
  }

  defer(tenantId: string, pool: TPool): void {
    this.deferred.add(tenantId, pool)

    const retirement = pool.retireWhenReleased()
    const cleanup = () => {
      this.deferred.delete(tenantId, pool)
    }
    void retirement.then(cleanup, cleanup)
    this.track(tenantId, pool, retirement)
  }

  retireNow(tenantId: string, pool: TPool): void {
    this.track(tenantId, pool, pool.retire())
  }

  async waitForTenant(tenantId: string): Promise<void> {
    const pending = this.pending.get(tenantId)
    if (!pending) {
      return
    }

    const results = await this.forceAndObserve([...pending])
    const errors = getRejectedReasons(results)
    if (errors.length === 1) {
      throw errors[0]
    }
    if (errors.length > 1) {
      throw new AggregateError(errors, `Failed to retire tenant database pools for ${tenantId}`)
    }
  }

  waitForAll(): Promise<PromiseSettledResult<void>[]> {
    return this.forceAndObserve(this.pending.snapshot())
  }

  appendDeferredSnapshotTo(target: TPool[]): void {
    this.deferred.appendSnapshotTo(target)
  }

  get deferredCount(): number {
    return this.deferred.size
  }

  getOldestDeferredAgeSeconds(now = performance.now()): number {
    return this.deferred.getOldestAgeSeconds(now)
  }

  private track(tenantId: string, pool: TPool, retirement: Promise<void>): void {
    if (!this.pending.add(tenantId, pool)) {
      return
    }

    const removePendingRetirement = () => {
      this.pending.delete(tenantId, pool)
    }
    // Track only in-flight work. Detached failures are reported immediately,
    // while an explicit waiter owns reporting for retirements it observes.
    void retirement.then(removePendingRetirement, (error) => {
      removePendingRetirement()
      if (!this.observed.has(pool)) {
        this.onDetachedFailure(tenantId, error)
      }
    })
  }

  private forceAndObserve(pools: readonly TPool[]): Promise<PromiseSettledResult<void>[]> {
    for (const pool of pools) {
      this.observed.add(pool)
    }
    return Promise.allSettled(pools.map((pool) => pool.retire()))
  }
}
