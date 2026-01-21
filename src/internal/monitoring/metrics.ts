import { metrics } from '@opentelemetry/api'
import {
  Counter,
  Gauge,
  Histogram,
  UpDownCounter,
} from '@opentelemetry/api/build/src/metrics/Metric'
import { getConfig } from '../../config'

const { prometheusMetricsIncludeTenantId } = getConfig()

// Get meter from global API - instruments work once MeterProvider is registered
const meter = metrics.getMeter('storage-api')

// ============================================================================
// HTTP Request Metrics
// ============================================================================
export const httpRequestDuration = withTenantMetricLabels(
  meter.createHistogram('http_request_duration_seconds', {
    description: 'HTTP request duration in seconds',
    unit: 's',
  })
)

export const httpRequestsTotal = withTenantMetricLabels(
  meter.createCounter('http_requests_total', {
    description: 'Total number of HTTP requests',
  })
)

export const httpRequestSizeBytes = withTenantMetricLabels(
  meter.createCounter('http_request_size_bytes', {
    description: 'Total bytes received in HTTP requests (from content-length header)',
    unit: 'bytes',
  })
)

export const httpResponseSizeBytes = withTenantMetricLabels(
  meter.createCounter('http_response_size_bytes', {
    description: 'Total bytes sent in HTTP responses (from content-length header)',
    unit: 'bytes',
  })
)

// ============================================================================
// Upload Metrics
// ============================================================================
export const fileUploadStarted = withTenantMetricLabels(
  meter.createCounter('upload_started', {
    description: 'Total uploads started',
  })
)

export const fileUploadedSuccess = withTenantMetricLabels(
  meter.createCounter('upload_success', {
    description: 'Total successful uploads',
  })
)

// ============================================================================
// Database Metrics
// ============================================================================
export const dbQueryPerformance = withTenantMetricLabels(
  meter.createHistogram('database_query_performance_seconds', {
    description: 'Database query performance in seconds',
    unit: 's',
  })
)

export const dbActivePool = withTenantMetricLabels(
  meter.createGauge('db_pool', {
    description: 'Number of database pools created',
  })
)

export const dbActiveConnection = withTenantMetricLabels(
  meter.createUpDownCounter('db_connections', {
    description: 'Number of database connections',
  })
)

// ============================================================================
// Queue Metrics
// ============================================================================
export const queueJobSchedulingTime = withTenantMetricLabels(
  meter.createHistogram('queue_job_scheduled_time_seconds', {
    description: 'Time taken to schedule a job in the queue in seconds',
    unit: 's',
  })
)

export const queueJobScheduled = withTenantMetricLabels(
  meter.createUpDownCounter('queue_job_scheduled', {
    description: 'Current number of pending messages in the queue',
  })
)

export const queueJobCompleted = withTenantMetricLabels(
  meter.createUpDownCounter('queue_job_completed', {
    description: 'Current number of processed messages in the queue',
  })
)

export const queueJobRetryFailed = withTenantMetricLabels(
  meter.createUpDownCounter('queue_job_retry_failed', {
    description: 'Current number of failed attempts messages in the queue',
  })
)

export const queueJobError = withTenantMetricLabels(
  meter.createUpDownCounter('queue_job_error', {
    description: 'Current number of errored messages in the queue',
  })
)

// ============================================================================
// S3 Metrics
// ============================================================================
export const s3UploadPart = withTenantMetricLabels(
  meter.createHistogram('s3_upload_part_seconds', {
    description: 'S3 upload part performance in seconds',
    unit: 's',
  })
)

// ============================================================================
// HTTP Pool Metrics
// ============================================================================
export const httpPoolBusySockets = withTenantMetricLabels(
  meter.createGauge('http_pool_busy_sockets', {
    description: 'Number of busy sockets currently in use',
  })
)

export const httpPoolFreeSockets = withTenantMetricLabels(
  meter.createGauge('http_pool_free_sockets', {
    description: 'Number of free sockets available for reuse',
  })
)

export const httpPoolPendingRequests = withTenantMetricLabels(
  meter.createGauge('http_pool_requests', {
    description: 'Number of pending requests waiting for a socket',
  })
)

export const httpPoolErrors = withTenantMetricLabels(
  meter.createGauge('http_pool_errors', {
    description: 'Number of socket errors',
  })
)

function withTenantMetricLabels<T extends Counter | UpDownCounter | Gauge | Histogram>(
  metricType: T
): T {
  if ('record' in metricType) {
    const originalRecord = metricType.record.bind(metricType)
    metricType.record = (value: number, labels?: Record<string, string>) => {
      if (!prometheusMetricsIncludeTenantId) {
        delete labels?.tenantId
      }

      return originalRecord(value, labels)
    }
    return metricType
  }

  if ('add' in metricType) {
    const originalAdd = metricType.add.bind(metricType)
    metricType.add = (value: number, labels?: Record<string, string>) => {
      if (!prometheusMetricsIncludeTenantId) {
        delete labels?.tenantId
      }

      return originalAdd(value, labels)
    }

    return metricType
  }

  return metricType
}
