import {
  cacheEntries,
  cacheSizeBytes,
  isMetricEnabled,
  meter,
  recordCacheEviction,
  recordCacheRequest,
} from '@internal/monitoring/metrics'
import { Attributes, BatchObservableCallback, Observable } from '@opentelemetry/api'
import { CacheLookupOptions, Disposable, DisposableCache, OutcomeAwareCache } from './adapter'
import { CacheName } from './names'

type CacheDisposeHandler<K, V, R extends string> = (value: V, key: K, reason: R) => void

type MonitorCacheOptions = {
  purgeStale?: () => void
}

const CACHE_OCCUPANCY_OBSERVABLES: Observable[] = [cacheEntries, cacheSizeBytes]

function isDisposable(value: unknown): value is Disposable {
  return Boolean(
    value &&
      typeof value === 'object' &&
      'dispose' in value &&
      typeof (value as { dispose?: unknown }).dispose === 'function'
  )
}

export function withCacheEvictionMetrics<K, V, R extends string>(
  cacheName: CacheName,
  dispose?: CacheDisposeHandler<K, V, R>
): CacheDisposeHandler<K, V, R> {
  return (value, key, reason) => {
    // Track capacity-pressure evictions only.
    // TTL expiry/removal reasons are excluded on purpose.
    if (reason === 'evict') {
      recordCacheEviction(cacheName)
    }

    dispose?.(value, key, reason)
  }
}

class MonitoredCache<K, V, SetOptions = undefined> implements DisposableCache<K, V, SetOptions> {
  private disposed = false
  private readonly cacheAttributes: Attributes
  private readonly observeOccupancy: BatchObservableCallback = (observer) => {
    const cacheEntriesEnabled = isMetricEnabled('cache_entries')
    const cacheSizeBytesEnabled = isMetricEnabled('cache_size_bytes')

    if (!cacheEntriesEnabled && !cacheSizeBytesEnabled) {
      return
    }

    this.options?.purgeStale?.()
    const stats = this.cache.getStats()

    if (cacheEntriesEnabled) {
      observer.observe(cacheEntries, stats.entries, this.cacheAttributes)
    }

    if (cacheSizeBytesEnabled) {
      observer.observe(cacheSizeBytes, stats.sizeBytes, this.cacheAttributes)
    }
  }

  constructor(
    private readonly name: CacheName,
    private readonly cache: OutcomeAwareCache<K, V, SetOptions>,
    private readonly options?: MonitorCacheOptions
  ) {
    this.cacheAttributes = { cache: name }
    meter.addBatchObservableCallback(this.observeOccupancy, CACHE_OCCUPANCY_OBSERVABLES)
  }

  get(key: K, options?: CacheLookupOptions): V | undefined {
    if (options?.recordMetrics === false) {
      return this.cache.get(key, options)
    }

    const { value, outcome } = this.cache.getWithOutcome(key)
    recordCacheRequest(this.name, outcome)

    return value
  }

  getWithOutcome(key: K) {
    return this.cache.getWithOutcome(key)
  }

  set(key: K, value: V, options?: SetOptions): void {
    this.cache.set(key, value, options)
  }

  delete(key: K): boolean {
    return this.cache.delete(key)
  }

  getStats() {
    return this.cache.getStats()
  }

  dispose(): void {
    if (this.disposed) {
      return
    }

    this.disposed = true
    meter.removeBatchObservableCallback(this.observeOccupancy, CACHE_OCCUPANCY_OBSERVABLES)

    if (isDisposable(this.cache)) {
      this.cache.dispose()
    }
  }
}

export function monitorCache<K, V, SetOptions = undefined>(
  cacheName: CacheName,
  cache: OutcomeAwareCache<K, V, SetOptions>,
  options?: MonitorCacheOptions
): DisposableCache<K, V, SetOptions> {
  return new MonitoredCache(cacheName, cache, options)
}
