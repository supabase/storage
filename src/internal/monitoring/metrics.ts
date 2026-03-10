import { Attributes, metrics } from '@opentelemetry/api'
import { getConfig } from '../../config'

const { prometheusMetricsIncludeTenantId } = getConfig()

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

function stripTenantAttrs(attrs: Attributes): Attributes {
  const { tenantId, tenant_id, ...rest } = attrs as Record<string, unknown>
  return rest as Attributes
}

/**
 * Registers a metric in the admin registry and wraps .record()/.add()
 * to automatically strip tenant attributes when prometheusMetricsIncludeTenantId is false.
 */
export function registerMetric<T>(name: string, type: MetricType, factory: () => T): T {
  metricsRegistry.set(name, { name, type, enabled: !disabledMetrics.has(name) })
  const instrument = factory()

  if (prometheusMetricsIncludeTenantId) return instrument

  // biome-ignore lint/suspicious/noExplicitAny: wrapping OTel instrument methods
  const inst = instrument as any
  if (typeof inst.record === 'function') {
    const original = inst.record.bind(inst)
    inst.record = (value: number, attrs?: Attributes) =>
      original(value, attrs ? stripTenantAttrs(attrs) : attrs)
  }
  if (typeof inst.add === 'function') {
    const original = inst.add.bind(inst)
    inst.add = (value: number, attrs?: Attributes) =>
      original(value, attrs ? stripTenantAttrs(attrs) : attrs)
  }

  return instrument
}

// ============================================================================
// HTTP Request Metrics
// ============================================================================
export const httpRequestDuration = registerMetric(
  'http_request_duration_seconds',
  'histogram',
  () =>
    meter.createHistogram('http_request_duration_seconds', {
      description: 'HTTP request duration in seconds',
      unit: 's',
    })
)

export const httpRequestSizeBytes = registerMetric('http_request_size_bytes', 'counter', () =>
  meter.createCounter('http_request_size_bytes', {
    description: 'Total bytes received in HTTP requests (from content-length header)',
    unit: 'bytes',
  })
)

export const httpResponseSizeBytes = registerMetric('http_response_size_bytes', 'counter', () =>
  meter.createCounter('http_response_size_bytes', {
    description: 'Total bytes sent in HTTP responses (from content-length header)',
    unit: 'bytes',
  })
)

// ============================================================================
// Upload Metrics
// ============================================================================
export const fileUploadStarted = registerMetric('upload_started', 'counter', () =>
  meter.createCounter('upload_started', {
    description: 'Total uploads started',
  })
)

export const fileUploadedSuccess = registerMetric('upload_success', 'counter', () =>
  meter.createCounter('upload_success', {
    description: 'Total successful uploads',
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
