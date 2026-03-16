import { LRUCache as BaseLruCache } from 'lru-cache'
import { CacheLookupOptions, CacheLookupOutcome, DisposableCache } from './adapter'
import { monitorCache, withCacheEvictionMetrics } from './monitoring'
import { CacheName } from './names'

export type LruCacheSetOptions<K extends {}, V extends {}> = BaseLruCache.SetOptions<K, V, unknown>

export type LruCacheOptions<K extends {}, V extends {}> = BaseLruCache.Options<K, V, unknown> & {
  purgeStaleIntervalMs?: number
}

export const DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS = 1000 * 60 // 1 minute

export class LruCache<K extends {}, V extends {}>
  implements DisposableCache<K, V, LruCacheSetOptions<K, V>>
{
  private readonly cache: BaseLruCache<K, V>
  private readonly purgeStaleTimer?: ReturnType<typeof setInterval>

  constructor(options: LruCacheOptions<K, V>) {
    const { purgeStaleIntervalMs, ...cacheOptions } = options

    this.cache = new BaseLruCache<K, V>({
      ...cacheOptions,
    })

    if (purgeStaleIntervalMs) {
      this.purgeStaleTimer = setInterval(() => {
        this.cache.purgeStale()
      }, purgeStaleIntervalMs)
      this.purgeStaleTimer.unref?.()
    }
  }

  get(key: K, options?: CacheLookupOptions): V | undefined {
    return this.getWithOutcome(key).value
  }

  getWithOutcome(key: K) {
    const status: BaseLruCache.Status<V> = {}
    const value = this.cache.get(key, { status })
    const outcome = (status.get || (value === undefined ? 'miss' : 'hit')) as CacheLookupOutcome

    return { value, outcome }
  }

  set(key: K, value: V, options?: LruCacheSetOptions<K, V>): void {
    this.cache.set(key, value, options)
  }

  delete(key: K): boolean {
    return this.cache.delete(key)
  }

  getStats() {
    return {
      entries: this.cache.size,
      sizeBytes: this.cache.calculatedSize,
    }
  }

  purgeStale(): boolean {
    return this.cache.purgeStale()
  }

  dispose(): void {
    if (this.purgeStaleTimer) {
      clearInterval(this.purgeStaleTimer)
    }
  }
}

export function createLruCache<K extends {}, V extends {}>(
  options: LruCacheOptions<K, V>
): LruCache<K, V>
export function createLruCache<K extends {}, V extends {}>(
  name: CacheName,
  options: LruCacheOptions<K, V>
): DisposableCache<K, V, LruCacheSetOptions<K, V>>
export function createLruCache<K extends {}, V extends {}>(
  nameOrOptions: CacheName | LruCacheOptions<K, V>,
  maybeOptions?: LruCacheOptions<K, V>
) {
  if (typeof nameOrOptions !== 'string') {
    return new LruCache(nameOrOptions)
  }

  const cacheName = nameOrOptions
  const options = maybeOptions as LruCacheOptions<K, V>
  const cache = new LruCache<K, V>({
    ...options,
    disposeAfter: withCacheEvictionMetrics(cacheName, options.disposeAfter),
  })

  return monitorCache(cacheName, cache, {
    purgeStale: () => {
      cache.purgeStale()
    },
  })
}
