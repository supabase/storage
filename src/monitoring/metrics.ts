import client from 'prom-client'

const Registry = client.Registry
export const MetricsRegistrar = new Registry()

export const FileUploadStarted = new client.Gauge({
  name: 'storage_api_upload_started',
  help: 'Upload started',
  labelNames: ['region', 'is_multipart'],
})

export const FileUploadedSuccess = new client.Gauge({
  name: 'storage_api_upload_success',
  help: 'Successful uploads',
  labelNames: ['region', 'is_multipart'],
})

export const DbQueryPerformance = new client.Histogram({
  name: 'storage_api_database_query_performance',
  help: 'Database query performance',
  labelNames: ['region', 'name'],
})

export const QueueJobSchedulingTime = new client.Histogram({
  name: 'storage_api_queue_job_scheduled_time',
  help: 'Time taken to schedule a job in the queue',
  labelNames: ['region', 'name'],
})

export const QueueJobScheduled = new client.Gauge({
  name: 'storage_api_queue_job_scheduled',
  help: 'Current number of pending messages in the queue',
  labelNames: ['region', 'name'],
})

export const QueueJobCompleted = new client.Gauge({
  name: 'storage_api_queue_job_completed',
  help: 'Current number of processed messages in the queue',
  labelNames: ['region', 'name'],
})

export const QueueJobRetryFailed = new client.Gauge({
  name: 'storage_api_queue_job_retry_failed',
  help: 'Current number of failed attempts messages in the queue',
  labelNames: ['region', 'name'],
})

export const QueueJobError = new client.Gauge({
  name: 'storage_api_queue_job_error',
  help: 'Current number of errored messages in the queue',
  labelNames: ['region', 'name'],
})

export const S3UploadPart = new client.Histogram({
  name: 'storage_api_s3_upload_part',
  help: 'S3 upload part performance',
  labelNames: ['region'],
})

export const DbActivePool = new client.Gauge({
  name: 'storage_api_db_pool',
  help: 'Number of database pools created',
  labelNames: ['region', 'is_external'],
})

export const DbActiveConnection = new client.Gauge({
  name: 'storage_api_db_connections',
  help: 'Number of database connections',
  labelNames: ['region', 'is_external'],
})
