import { metrics } from '@opentelemetry/api'
import {
  Counter,
  Gauge,
  Histogram,
  UpDownCounter,
} from '@opentelemetry/api/build/src/metrics/Metric'
import { getConfig } from '../../config'

const { prometheusMetricsIncludeTenantId } = getConfig()

// ============================================================================
// Metric Registry — tracks all metrics and their enabled/disabled state
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

// Get meter from global API - instruments work once MeterProvider is registered
const meter = metrics.getMeter('storage-api')

// ============================================================================
// HTTP Request Metrics
// ============================================================================
export const httpRequestDuration = withMetricLabels(
  'http_request_duration_seconds',
  'histogram',
  meter.createHistogram('http_request_duration_seconds', {
    description: 'HTTP request duration in seconds',
    unit: 's',
  })
)

export const httpRequestsTotal = withMetricLabels(
  'http_requests_total',
  'counter',
  meter.createCounter('http_requests_total', {
    description: 'Total number of HTTP requests',
  })
)

export const httpRequestSizeBytes = withMetricLabels(
  'http_request_size_bytes',
  'counter',
  meter.createCounter('http_request_size_bytes', {
    description: 'Total bytes received in HTTP requests (from content-length header)',
    unit: 'bytes',
  })
)

export const httpResponseSizeBytes = withMetricLabels(
  'http_response_size_bytes',
  'counter',
  meter.createCounter('http_response_size_bytes', {
    description: 'Total bytes sent in HTTP responses (from content-length header)',
    unit: 'bytes',
  })
)

// ============================================================================
// Upload Metrics
// ============================================================================
export const fileUploadStarted = withMetricLabels(
  'upload_started',
  'counter',
  meter.createCounter('upload_started', {
    description: 'Total uploads started',
  })
)

export const fileUploadedSuccess = withMetricLabels(
  'upload_success',
  'counter',
  meter.createCounter('upload_success', {
    description: 'Total successful uploads',
  })
)

// ============================================================================
// Database Metrics
// ============================================================================
export const dbQueryPerformance = withMetricLabels(
  'database_query_performance_seconds',
  'histogram',
  meter.createHistogram('database_query_performance_seconds', {
    description: 'Database query performance in seconds',
    unit: 's',
  })
)

export const dbActivePool = withMetricLabels(
  'db_active_local_pools',
  'gauge',
  meter.createGauge('db_active_local_pools', {
    description: 'Number of database pools created',
  })
)

export const dbActiveConnection = withMetricLabels(
  'db_connections',
  'gauge',
  meter.createGauge('db_connections', {
    description: 'Number of database connections in the pool',
  })
)

export const dbInUseConnection = withMetricLabels(
  'db_connections_in_use',
  'gauge',
  meter.createGauge('db_connections_in_use', {
    description: 'Number of database connections currently in use',
  })
)

export const dbConnectionAcquireTime = withMetricLabels(
  'db_connection_acquire_seconds',
  'histogram',
  meter.createHistogram('db_connection_acquire_seconds', {
    description: 'Time taken to acquire a database connection from the pool in seconds',
    unit: 's',
  })
)

// ============================================================================
// Queue Metrics
// ============================================================================
export const queueJobSchedulingTime = withMetricLabels(
  'queue_job_scheduled_time_seconds',
  'histogram',
  meter.createHistogram('queue_job_scheduled_time_seconds', {
    description: 'Time taken to schedule a job in the queue in seconds',
    unit: 's',
  })
)

export const queueJobScheduled = withMetricLabels(
  'queue_job_scheduled',
  'updowncounter',
  meter.createUpDownCounter('queue_job_scheduled', {
    description: 'Current number of pending messages in the queue',
  })
)

export const queueJobCompleted = withMetricLabels(
  'queue_job_completed',
  'updowncounter',
  meter.createUpDownCounter('queue_job_completed', {
    description: 'Current number of processed messages in the queue',
  })
)

export const queueJobRetryFailed = withMetricLabels(
  'queue_job_retry_failed',
  'updowncounter',
  meter.createUpDownCounter('queue_job_retry_failed', {
    description: 'Current number of failed attempts messages in the queue',
  })
)

export const queueJobError = withMetricLabels(
  'queue_job_error',
  'updowncounter',
  meter.createUpDownCounter('queue_job_error', {
    description: 'Current number of errored messages in the queue',
  })
)

// ============================================================================
// S3 Metrics
// ============================================================================
export const s3UploadPart = withMetricLabels(
  's3_upload_part_seconds',
  'histogram',
  meter.createHistogram('s3_upload_part_seconds', {
    description: 'S3 upload part performance in seconds',
    unit: 's',
  })
)

// ============================================================================
// HTTP Pool Metrics
// ============================================================================
export const httpPoolBusySockets = withMetricLabels(
  'http_pool_busy_sockets',
  'gauge',
  meter.createGauge('http_pool_busy_sockets', {
    description: 'Number of busy sockets currently in use',
  })
)

export const httpPoolFreeSockets = withMetricLabels(
  'http_pool_free_sockets',
  'gauge',
  meter.createGauge('http_pool_free_sockets', {
    description: 'Number of free sockets available for reuse',
  })
)

export const httpPoolPendingRequests = withMetricLabels(
  'http_pool_requests',
  'gauge',
  meter.createGauge('http_pool_requests', {
    description: 'Number of pending requests waiting for a socket',
  })
)

export const httpPoolErrors = withMetricLabels(
  'http_pool_errors',
  'gauge',
  meter.createGauge('http_pool_errors', {
    description: 'Number of socket errors',
  })
)

// ============================================================================
// Metric wrapper — registers in registry, adds enabled guard + tenant label stripping
// ============================================================================
function stripTenantLabels(labels?: Record<string, string>): Record<string, string> | undefined {
  if (!labels) return labels
  const { tenant_id, tenantId, ...rest } = labels
  return rest
}

function withMetricLabels<T extends Counter | UpDownCounter | Gauge | Histogram>(
  name: string,
  type: MetricType,
  metricType: T
): T {
  const entry: MetricRegistryEntry = {
    name,
    type,
    enabled: !disabledMetrics.has(name),
  }
  metricsRegistry.set(name, entry)

  if ('record' in metricType) {
    const originalRecord = metricType.record.bind(metricType)
    metricType.record = (value: number, labels?: Record<string, string>) => {
      if (!entry.enabled) return
      return originalRecord(
        value,
        prometheusMetricsIncludeTenantId ? labels : stripTenantLabels(labels)
      )
    }
    return metricType
  }

  if ('add' in metricType) {
    const originalAdd = metricType.add.bind(metricType)
    metricType.add = (value: number, labels?: Record<string, string>) => {
      if (!entry.enabled) return
      return originalAdd(
        value,
        prometheusMetricsIncludeTenantId ? labels : stripTenantLabels(labels)
      )
    }

    return metricType
  }

  return metricType
}
