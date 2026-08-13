import type { DistanceMetric } from '@aws-sdk/client-s3vectors'
import {
  createLruCache,
  DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS,
  PGVECTOR_METRIC_CACHE_NAME,
} from '@internal/cache'
import type { Perf } from 'lru-cache'

const METRIC_CACHE_TTL_MS = 5 * 60 * 1000
const METRIC_CACHE_MAX = 1_000

interface MetricCacheOptions {
  ttl?: number
  ttlResolution?: number
  perf?: Perf
}

export function createMetricCache(options: MetricCacheOptions = {}) {
  return createLruCache<string, DistanceMetric>(PGVECTOR_METRIC_CACHE_NAME, {
    ttl: options.ttl ?? METRIC_CACHE_TTL_MS,
    ttlResolution: options.ttlResolution,
    max: METRIC_CACHE_MAX,
    updateAgeOnGet: true,
    purgeStaleIntervalMs: DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS,
    perf: options.perf,
  })
}

// Module-scoped because the Fastify plugin builds a new PgVectorStore per request.
export const metricCache = createMetricCache()
