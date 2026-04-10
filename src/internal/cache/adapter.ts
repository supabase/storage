export type CacheLookupOptions = {
  recordMetrics?: boolean
}

export type CacheLookupOutcome = 'hit' | 'miss' | 'stale'

export type CacheLookupResult<V> = {
  value: V | undefined
  outcome: CacheLookupOutcome
}

export type CacheStats = {
  entries: number
  sizeBytes: number
}

export interface Cache<K, V, SetOptions = undefined> {
  get(key: K, options?: CacheLookupOptions): V | undefined
  set(key: K, value: V, options?: SetOptions): void
  delete(key: K): boolean
}

export interface InspectableCache<K, V, SetOptions = undefined> extends Cache<K, V, SetOptions> {
  getStats(): CacheStats
}

export interface OutcomeAwareCache<K, V, SetOptions = undefined>
  extends InspectableCache<K, V, SetOptions> {
  getWithOutcome(key: K): CacheLookupResult<V>
}

export interface Disposable {
  dispose(): void
}

export interface DisposableCache<K, V, SetOptions = undefined>
  extends OutcomeAwareCache<K, V, SetOptions>,
    Disposable {}
