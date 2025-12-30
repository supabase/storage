import { metrics } from '@opentelemetry/api'

// Get meter from global API - instruments work once MeterProvider is registered
const meter = metrics.getMeter('storage-api')

// ============================================================================
// HTTP Request Metrics
// ============================================================================
export const httpRequestDuration = meter.createHistogram(
  'storage_api_http_request_duration_seconds',
  {
    description: 'HTTP request duration in seconds',
    unit: 's',
  }
)

export const httpRequestsTotal = meter.createCounter('storage_api_http_requests_total', {
  description: 'Total number of HTTP requests',
})

export const httpRequestSizeBytes = meter.createCounter('storage_api_http_request_size_bytes', {
  description: 'Total bytes received in HTTP requests (from content-length header)',
  unit: 'bytes',
})

export const httpResponseSizeBytes = meter.createCounter('storage_api_http_response_size_bytes', {
  description: 'Total bytes sent in HTTP responses (from content-length header)',
  unit: 'bytes',
})

// ============================================================================
// Upload Metrics
// ============================================================================
export const fileUploadStarted = meter.createCounter('storage_api_upload_started', {
  description: 'Total uploads started',
})

export const fileUploadedSuccess = meter.createCounter('storage_api_upload_success', {
  description: 'Total successful uploads',
})

// ============================================================================
// Database Metrics
// ============================================================================
export const dbQueryPerformance = meter.createHistogram(
  'storage_api_database_query_performance_seconds',
  {
    description: 'Database query performance in seconds',
    unit: 's',
  }
)

export const dbActivePool = meter.createGauge('storage_api_db_pool', {
  description: 'Number of database pools created',
})

export const dbActiveConnection = meter.createUpDownCounter('storage_api_db_connections', {
  description: 'Number of database connections',
})

// ============================================================================
// Queue Metrics
// ============================================================================
export const queueJobSchedulingTime = meter.createHistogram(
  'storage_api_queue_job_scheduled_time_seconds',
  {
    description: 'Time taken to schedule a job in the queue in seconds',
    unit: 's',
  }
)

export const queueJobScheduled = meter.createUpDownCounter('storage_api_queue_job_scheduled', {
  description: 'Current number of pending messages in the queue',
})

export const queueJobCompleted = meter.createUpDownCounter('storage_api_queue_job_completed', {
  description: 'Current number of processed messages in the queue',
})

export const queueJobRetryFailed = meter.createUpDownCounter('storage_api_queue_job_retry_failed', {
  description: 'Current number of failed attempts messages in the queue',
})

export const queueJobError = meter.createUpDownCounter('storage_api_queue_job_error', {
  description: 'Current number of errored messages in the queue',
})

// ============================================================================
// S3 Metrics
// ============================================================================
export const s3UploadPart = meter.createHistogram('storage_api_s3_upload_part_seconds', {
  description: 'S3 upload part performance in seconds',
  unit: 's',
})

// ============================================================================
// HTTP Pool Metrics
// ============================================================================
export const httpPoolBusySockets = meter.createGauge('storage_api_http_pool_busy_sockets', {
  description: 'Number of busy sockets currently in use',
})

export const httpPoolFreeSockets = meter.createGauge('storage_api_http_pool_free_sockets', {
  description: 'Number of free sockets available for reuse',
})

export const httpPoolPendingRequests = meter.createGauge('storage_api_http_pool_requests', {
  description: 'Number of pending requests waiting for a socket',
})

export const httpPoolErrors = meter.createGauge('storage_api_http_pool_errors', {
  description: 'Number of socket errors',
})
