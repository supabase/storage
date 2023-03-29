import client from 'prom-client'

const Registry = client.Registry
export const MetricsRegistrar = new Registry()

export const FileUploadStarted = new client.Gauge({
  name: 'storage_api_upload_started',
  help: 'Upload started',
  labelNames: ['tenant_id', 'region', 'is_multipart'],
})

export const FileUploadedSuccess = new client.Gauge({
  name: 'storage_api_upload_success',
  help: 'Successful uploads',
  labelNames: ['tenant_id', 'region', 'is_multipart'],
})

export const DbQueryPerformance = new client.Histogram({
  name: 'storage_api_database_query_performance',
  help: 'Database query performance',
  labelNames: ['tenant_id', 'region', 'name'],
})

export const RequestErrors = new client.Gauge({
  name: 'storage_api_request_errors',
  labelNames: ['tenant_id', 'region', 'method', 'path', 'status', 'name'],
  help: 'Response Errors',
})

export const QueueJobScheduled = new client.Gauge({
  name: 'storage_api_queue_job_scheduled',
  help: 'Current number of pending messages in the queue',
  labelNames: ['region', 'name', 'tenant_id'],
})

export const QueueJobCompleted = new client.Gauge({
  name: 'storage_api_queue_job_completed',
  help: 'Current number of processed messages in the queue',
  labelNames: ['tenant_id', 'region', 'name'],
})

export const QueueJobRetryFailed = new client.Gauge({
  name: 'storage_api_queue_job_retry_failed',
  help: 'Current number of failed attempts messages in the queue',
  labelNames: ['tenant_id', 'region', 'name'],
})

export const QueueJobError = new client.Gauge({
  name: 'storage_api_queue_job_error',
  help: 'Current number of errored messages in the queue',
  labelNames: ['tenant_id', 'region', 'name'],
})
