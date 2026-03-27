import BaseTtlCache from '@isaacs/ttlcache'
import { CacheLookupOptions, CacheLookupOutcome, DisposableCache } from './adapter'
import { monitorCache, withCacheEvictionMetrics } from './monitoring'
import { CacheName } from './names'

export type TtlCacheSetOptions = BaseTtlCache.SetOptions

export type TtlCacheOptions<K, V> = BaseTtlCache.Options<K, V> & {
  sizeCalculation?: (value: V, key: K) => number
}

export class TtlCache<K, V> implements DisposableCache<K, V, TtlCacheSetOptions> {
  private readonly cache: BaseTtlCache<K, V>
  private readonly sizeCalculation?: (value: V, key: K) => number
  private calculatedSize = 0
  private readonly keySizes = new Map<K, number>()
  private readonly keysInCache = new Set<K>()

  constructor(options: TtlCacheOptions<K, V>) {
    const { dispose, sizeCalculation, ...cacheOptions } = options

    this.sizeCalculation = sizeCalculation
    this.cache = new BaseTtlCache<K, V>({
      ...cacheOptions,
      dispose: (value, key, reason) => {
        this.keysInCache.delete(key)
        this.deleteTrackedSize(key)
        dispose?.(value, key, reason)
      },
    })
  }

  get(key: K, options?: CacheLookupOptions): V | undefined {
    return this.getWithOutcome(key).value
  }

  getWithOutcome(key: K) {
    const remainingTTL = this.cache.getRemainingTTL(key)
    const value = this.cache.get(key)
    const outcome: CacheLookupOutcome =
      remainingTTL > 0 || remainingTTL === Infinity
        ? value === undefined
          ? 'miss'
          : 'hit'
        : value === undefined
          ? 'miss'
          : 'stale'

    return { value, outcome }
  }

  set(key: K, value: V, options?: TtlCacheSetOptions): void {
    this.cache.set(key, value, options)
    this.keysInCache.add(key)
    this.setTrackedSize(key, value)
  }

  delete(key: K): boolean {
    return this.cache.delete(key)
  }

  clear(): void {
    this.cache.clear()
    this.keysInCache.clear()
    this.keySizes.clear()
    this.calculatedSize = 0
  }

  has(key: K): boolean {
    return this.cache.has(key)
  }

  purgeStale(): boolean {
    return Boolean(this.cache.purgeStale())
  }

  cancelTimer(): void {
    this.cache.cancelTimer()
  }

  dispose(): void {
    this.cancelTimer()
  }

  *entries(): Generator<[K, V]> {
    for (const key of this.keysInCache) {
      const value = this.cache.get(key, {
        updateAgeOnGet: false,
        checkAgeOnGet: true,
      })

      if (value === undefined) {
        continue
      }

      yield [key, value]
    }
  }

  *keys(): Generator<K> {
    for (const key of this.keysInCache) {
      if (this.cache.getRemainingTTL(key) === 0) {
        continue
      }

      yield key
    }
  }

  *values(): Generator<V> {
    for (const [, value] of this.entries()) {
      yield value
    }
  }

  getRemainingTTL(key: K): number {
    return this.cache.getRemainingTTL(key)
  }

  setTTL(key: K, ttl?: number): void {
    this.cache.setTTL(key, ttl)
  }

  getStats() {
    return {
      entries: this.cache.size,
      sizeBytes: this.calculatedSize,
    }
  }

  [Symbol.iterator](): Iterator<[K, V]> {
    return this.entries()
  }

  private setTrackedSize(key: K, value: V) {
    if (!this.sizeCalculation) {
      return
    }

    const nextSize = this.sizeCalculation(value, key)
    const previousSize = this.keySizes.get(key) ?? 0
    this.keySizes.set(key, nextSize)
    this.calculatedSize += nextSize - previousSize
  }

  private deleteTrackedSize(key: K) {
    const previousSize = this.keySizes.get(key)
    if (previousSize === undefined) {
      return
    }

    this.keySizes.delete(key)
    this.calculatedSize -= previousSize
  }
}

export function createTtlCache<K, V>(options: TtlCacheOptions<K, V>): TtlCache<K, V>
export function createTtlCache<K, V>(
  name: CacheName,
  options: TtlCacheOptions<K, V>
): DisposableCache<K, V, TtlCacheSetOptions>
export function createTtlCache<K, V>(
  nameOrOptions: CacheName | TtlCacheOptions<K, V>,
  maybeOptions?: TtlCacheOptions<K, V>
) {
  if (typeof nameOrOptions !== 'string') {
    return new TtlCache(nameOrOptions)
  }

  const cacheName = nameOrOptions
  const options = maybeOptions as TtlCacheOptions<K, V>
  const cache = new TtlCache<K, V>({
    ...options,
    dispose: withCacheEvictionMetrics(cacheName, options.dispose),
  })

  return monitorCache(cacheName, cache, {
    purgeStale: () => {
      cache.purgeStale()
    },
  })
}
