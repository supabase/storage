import type { DistanceMetric } from '@aws-sdk/client-s3vectors'
import {
  createLruCache,
  DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS,
  DEFAULT_CACHE_TTL_JITTER_RATIO,
  PGVECTOR_METRIC_CACHE_NAME,
} from '@internal/cache'
import type { Perf } from 'lru-cache'
import { getConfig } from '../../../../../config'

interface MetricCacheOptions {
  ttl?: number
  ttlJitterRatio?: number
  ttlResolution?: number
  perf?: Perf
}

export function createMetricCache(options: MetricCacheOptions = {}) {
  return createLruCache<string, DistanceMetric>(PGVECTOR_METRIC_CACHE_NAME, {
    ttl: options.ttl ?? getConfig().caches.pgvectorMetric.ttlMs,
    ttlJitterRatio: options.ttlJitterRatio ?? DEFAULT_CACHE_TTL_JITTER_RATIO,
    ttlResolution: options.ttlResolution,
    max: getConfig().caches.pgvectorMetric.maxEntries,
    allowStale: false,
    purgeStaleIntervalMs: DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS,
    perf: options.perf,
  })
}

// Module-scoped because the Fastify plugin builds a new PgVectorStore per request.
export const metricCache = createMetricCache()
