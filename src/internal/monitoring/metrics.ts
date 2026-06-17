import type { CacheLookupOutcome } from '@internal/cache/adapter'
import type { CacheName } from '@internal/cache/names'
import { type Attributes, metrics } from '@opentelemetry/api'
import { createBatchObservableCounterGroup } from './counter'

// ============================================================================
// Metric Registry — tracks all metrics for admin API
// ============================================================================
export type MetricType = 'histogram' | 'counter' | 'gauge' | 'updowncounter'

export interface MetricRegistryEntry {
  name: string
  type: MetricType
  enabled: boolean
}

const metricsRegistry = new Map<string, MetricRegistryEntry>()

const disabledMetrics = new Set(
  (process.env.METRICS_DISABLED || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
)

/** Returns all registered metrics with their status */
export function getMetricsConfig(): MetricRegistryEntry[] {
  return Array.from(metricsRegistry.values())
}

/** Enable or disable specific metrics by OTel instrument name */
export function setMetricsEnabled(changes: { name: string; enabled: boolean }[]): void {
  for (const { name, enabled } of changes) {
    const entry = metricsRegistry.get(name)
    if (entry) {
      entry.enabled = enabled
    }
  }
}

/** Check if a metric is enabled (for observable gauges that emit via callbacks) */
export function isMetricEnabled(name: string): boolean {
  return metricsRegistry.get(name)?.enabled !== false
}

// ============================================================================
// Meter & registration
// ============================================================================
export const meter = metrics.getMeter('storage-api')

/**
 * Registers a metric in the admin registry.
 */
export function registerMetric<T>(name: string, type: MetricType, factory: () => T): T {
  metricsRegistry.set(name, { name, type, enabled: !disabledMetrics.has(name) })
  return factory()
}

// ============================================================================
// HTTP Request Metrics
//
// HTTP byte counters intentionally do not check `isMetricEnabled()`. They use
// cumulative observable counters, so skipping observations on runtime disable
// would leave stale exported points and later jumps on re-enable.
// ============================================================================
type HttpMetricAttributes = {
  method: string
  operation: string
  status_code: string
}

type HttpSizeMetricsState = {
  requestBytes: number
  responseBytes: number
  attributes: Attributes
}

const httpSizeMetrics = createBatchObservableCounterGroup({
  meter,
  registerMetric,
  maxStates: 4096,
  counters: {
    requestBytes: {
      name: 'http_request_size_bytes',
      description: 'Total bytes received in HTTP requests (from content-length header)',
      unit: 'bytes',
    },
    responseBytes: {
      name: 'http_response_size_bytes',
      description: 'Total bytes sent in HTTP responses (from content-length header)',
      unit: 'bytes',
    },
  },
  getKey: (attributes: HttpMetricAttributes) =>
    `${attributes.method}\x00${attributes.operation}\x00${attributes.status_code}`,
  createState: (attributes: HttpMetricAttributes): HttpSizeMetricsState => ({
    requestBytes: 0,
    responseBytes: 0,
    attributes: {
      method: attributes.method,
      operation: attributes.operation,
      status_code: attributes.status_code,
    },
  }),
  observe: (observer, counters, state) => {
    if (state.requestBytes > 0) {
      observer.observe(counters.requestBytes, state.requestBytes, state.attributes)
    }
    if (state.responseBytes > 0) {
      observer.observe(counters.responseBytes, state.responseBytes, state.attributes)
    }
  },
})

function isRecordableMetricValue(value: number | undefined): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value > 0
}

export const httpRequestDuration = registerMetric(
  'http_request_duration_seconds',
  'histogram',
  () =>
    meter.createHistogram('http_request_duration_seconds', {
      description: 'HTTP request duration in seconds',
      unit: 's',
    })
)

/** Records request/response bytes by bumping in-process tallies with one state lookup. */
export function recordHttpSizes(
  requestSizeBytes: number | undefined,
  responseSizeBytes: number | undefined,
  attributes: HttpMetricAttributes
): void {
  const shouldRecordRequestSize = isRecordableMetricValue(requestSizeBytes)
  const shouldRecordResponseSize = isRecordableMetricValue(responseSizeBytes)

  if (!shouldRecordRequestSize && !shouldRecordResponseSize) {
    return
  }

  const state = httpSizeMetrics.state(attributes)

  if (shouldRecordRequestSize) {
    state.requestBytes += requestSizeBytes
  }

  if (shouldRecordResponseSize) {
    state.responseBytes += responseSizeBytes
  }
}

// ============================================================================
// Upload Metrics
//
// Upload counters follow the same cumulative observable-counter semantics as
// HTTP/cache: runtime disables must not suppress observations.
// ============================================================================
type UploadMetricsState = {
  started: number
  success: number
  attributes: Attributes
}

const uploadMetrics = createBatchObservableCounterGroup({
  meter,
  registerMetric,
  maxStates: 32,
  counters: {
    started: {
      name: 'upload_started',
      description: 'Total uploads started',
    },
    success: {
      name: 'upload_success',
      description: 'Total successful uploads',
    },
  },
  getKey: (uploadType: string) => uploadType,
  createState: (uploadType: string): UploadMetricsState => ({
    started: 0,
    success: 0,
    attributes: { uploadType },
  }),
  observe: (observer, counters, state) => {
    if (state.started > 0) {
      observer.observe(counters.started, state.started, state.attributes)
    }
    if (state.success > 0) {
      observer.observe(counters.success, state.success, state.attributes)
    }
  },
})

/** Records an upload start by bumping an in-process tally. */
export function recordUploadStarted(uploadType: string): void {
  uploadMetrics.state(uploadType).started++
}

/** Records an upload success by bumping an in-process tally. */
export function recordUploadSuccess(uploadType: string): void {
  uploadMetrics.state(uploadType).success++
}

// ============================================================================
// Cache Metrics
//
// These observable counters intentionally do not check `isMetricEnabled()`.
// With cumulative async OTel instruments, skipping `observe()` after a runtime
// disable can keep the previous point exported, then jump on re-enable because
// the in-process tally kept advancing. Drop these metrics at exporter/view
// configuration if they need to be suppressed entirely.
// ============================================================================
type CacheRequestMetricsState = {
  count: number
  attributes: Attributes
}

type CacheMetricsState = {
  requests: Record<CacheLookupOutcome, CacheRequestMetricsState>
  evictions: number
  evictionAttributes: Attributes
}

const cacheMetrics = createBatchObservableCounterGroup({
  meter,
  registerMetric,
  maxStates: 64,
  counters: {
    requests: {
      name: 'cache_requests_total',
      description: 'Total cache lookups by cache and outcome',
    },
    evictions: {
      name: 'cache_evictions_total',
      description: 'Total cache evictions',
    },
  },
  getKey: (cache: CacheName) => cache,
  createState: (cache: CacheName): CacheMetricsState => ({
    requests: {
      hit: { count: 0, attributes: { cache, outcome: 'hit' } },
      miss: { count: 0, attributes: { cache, outcome: 'miss' } },
      stale: { count: 0, attributes: { cache, outcome: 'stale' } },
    },
    evictions: 0,
    evictionAttributes: { cache },
  }),
  observe: (observer, counters, state) => {
    const requests = state.requests

    if (requests.hit.count > 0) {
      observer.observe(counters.requests, requests.hit.count, requests.hit.attributes)
    }
    if (requests.miss.count > 0) {
      observer.observe(counters.requests, requests.miss.count, requests.miss.attributes)
    }
    if (requests.stale.count > 0) {
      observer.observe(counters.requests, requests.stale.count, requests.stale.attributes)
    }
    if (state.evictions > 0) {
      observer.observe(counters.evictions, state.evictions, state.evictionAttributes)
    }
  },
})

/** Records a single cache lookup outcome by bumping an in-process tally. */
export function recordCacheRequest(cache: CacheName, outcome: CacheLookupOutcome): void {
  cacheMetrics.state(cache).requests[outcome].count++
}

/** Records a single capacity/ttl cache eviction by bumping an in-process tally. */
export function recordCacheEviction(cache: CacheName): void {
  cacheMetrics.state(cache).evictions++
}

export const cacheEntries = registerMetric('cache_entries', 'gauge', () =>
  meter.createObservableGauge('cache_entries', {
    description: 'Current number of entries stored in each cache',
  })
)

export const cacheSizeBytes = registerMetric('cache_size_bytes', 'gauge', () =>
  meter.createObservableGauge('cache_size_bytes', {
    description: 'Current estimated size of each cache in bytes',
    unit: 'bytes',
  })
)

// ============================================================================
// Database Metrics
// ============================================================================
export const dbQueryPerformance = registerMetric(
  'database_query_performance_seconds',
  'histogram',
  () =>
    meter.createHistogram('database_query_performance_seconds', {
      description: 'Database query performance in seconds',
      unit: 's',
    })
)

export const dbConnectionAcquireTime = registerMetric(
  'db_connection_acquire_seconds',
  'histogram',
  () =>
    meter.createHistogram('db_connection_acquire_seconds', {
      description: 'Time taken to acquire a database connection from the pool in seconds',
      unit: 's',
    })
)

// ============================================================================
// Queue Metrics
// ============================================================================
export const queueJobSchedulingTime = registerMetric(
  'queue_job_scheduled_time_seconds',
  'histogram',
  () =>
    meter.createHistogram('queue_job_scheduled_time_seconds', {
      description: 'Time taken to schedule a job in the queue in seconds',
      unit: 's',
    })
)

export const queueJobScheduled = registerMetric('queue_job_scheduled', 'updowncounter', () =>
  meter.createUpDownCounter('queue_job_scheduled', {
    description: 'Current number of pending messages in the queue',
  })
)

export const queueJobCompleted = registerMetric('queue_job_completed', 'updowncounter', () =>
  meter.createUpDownCounter('queue_job_completed', {
    description: 'Current number of processed messages in the queue',
  })
)

export const queueJobRetryFailed = registerMetric('queue_job_retry_failed', 'updowncounter', () =>
  meter.createUpDownCounter('queue_job_retry_failed', {
    description: 'Current number of failed attempts messages in the queue',
  })
)

export const queueJobError = registerMetric('queue_job_error', 'updowncounter', () =>
  meter.createUpDownCounter('queue_job_error', {
    description: 'Current number of errored messages in the queue',
  })
)

// ============================================================================
// S3 Metrics
// ============================================================================
export const s3UploadPart = registerMetric('s3_upload_part_seconds', 'histogram', () =>
  meter.createHistogram('s3_upload_part_seconds', {
    description: 'S3 upload part performance in seconds',
    unit: 's',
  })
)

// ============================================================================
// HTTP Pool Metrics
// ============================================================================
export const httpPoolBusySockets = registerMetric('http_pool_busy_sockets', 'gauge', () =>
  meter.createGauge('http_pool_busy_sockets', {
    description: 'Number of busy sockets currently in use',
  })
)

export const httpPoolFreeSockets = registerMetric('http_pool_free_sockets', 'gauge', () =>
  meter.createGauge('http_pool_free_sockets', {
    description: 'Number of free sockets available for reuse',
  })
)

export const httpPoolPendingRequests = registerMetric('http_pool_requests', 'gauge', () =>
  meter.createGauge('http_pool_requests', {
    description: 'Number of pending requests waiting for a socket',
  })
)

export const httpPoolErrors = registerMetric('http_pool_errors', 'gauge', () =>
  meter.createGauge('http_pool_errors', {
    description: 'Number of socket errors',
  })
)

// ============================================================================
// Database Pool Metrics (observable — collected only at export time)
// ============================================================================
export const dbActivePool = registerMetric('db_active_local_pools', 'gauge', () =>
  meter.createObservableGauge('db_active_local_pools', {
    description: 'Number of database pools created',
  })
)

export const dbActiveConnection = registerMetric('db_connections', 'gauge', () =>
  meter.createObservableGauge('db_connections', {
    description: 'Number of database connections in the pool',
  })
)

export const dbInUseConnection = registerMetric('db_connections_in_use', 'gauge', () =>
  meter.createObservableGauge('db_connections_in_use', {
    description: 'Number of database connections currently in use',
  })
)
