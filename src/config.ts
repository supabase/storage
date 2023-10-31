import dotenv from 'dotenv'

export type StorageBackendType = 'file' | 's3'

type StorageConfigType = {
  version: string
  keepAliveTimeout: number
  headersTimeout: number
  adminApiKeys: string
  adminRequestIdHeader?: string
  anonKey: string
  encryptionKey: string
  fileSizeLimit: number
  fileStoragePath?: string
  globalS3Protocol: 'http' | 'https'
  globalS3MaxSockets?: number
  globalS3Bucket: string
  globalS3Endpoint?: string
  globalS3ForcePathStyle?: boolean
  isMultitenant: boolean
  jwtSecret: string
  jwtAlgorithm: string
  multitenantDatabaseUrl?: string
  databaseURL: string
  databaseSSLRootCert?: string
  databasePoolURL?: string
  databaseMaxConnections: number
  databaseFreePoolAfterInactivity: number
  databaseConnectionTimeout: number
  region: string
  requestIdHeader?: string
  serviceKey: string
  storageBackendType: StorageBackendType
  tenantId: string
  urlLengthLimit: number
  xForwardedHostRegExp?: string
  logLevel?: string
  logflareEnabled?: boolean
  logflareApiKey?: string
  logflareSourceToken?: string
  enableQueueEvents: boolean
  pgQueueConnectionURL?: string
  pgQueueDeleteAfterDays?: number
  pgQueueArchiveCompletedAfterSeconds?: number
  pgQueueRetentionDays?: number
  webhookURL?: string
  webhookApiKey?: string
  webhookQueuePullInterval?: number
  webhookQueueTeamSize?: number
  webhookQueueConcurrency?: number
  adminDeleteQueueTeamSize?: number
  adminDeleteConcurrency?: number
  enableImageTransformation: boolean
  imgProxyURL?: string
  imgProxyRequestTimeout: number
  imgProxyHttpMaxSockets: number
  imgProxyHttpKeepAlive: number
  imgLimits: {
    size: {
      min: number
      max: number
    }
  }
  postgrestForwardHeaders?: string
  adminPort: number
  port: number
  host: string
  enableRateLimiter: boolean
  rateLimiterDriver: 'memory' | 'redis' | string
  rateLimiterRedisUrl?: string
  rateLimiterSkipOnError?: boolean
  rateLimiterRenderPathMaxReqSec: number
  rateLimiterRedisConnectTimeout: number
  rateLimiterRedisCommandTimeout: number
  signedUploadUrlExpirationTime: number
  tusUrlExpiryMs: number
  tusPath: string
  tusUseFileVersionSeparator: boolean
  enableDefaultMetrics: boolean
}

function getOptionalConfigFromEnv(key: string): string | undefined {
  return process.env[key]
}

function getConfigFromEnv(key: string): string {
  const value = getOptionalConfigFromEnv(key)
  if (!value) {
    throw new Error(`${key} is undefined`)
  }
  return value
}

function getOptionalIfMultitenantConfigFromEnv(key: string): string | undefined {
  return getOptionalConfigFromEnv('IS_MULTITENANT') === 'true'
    ? getOptionalConfigFromEnv(key)
    : getConfigFromEnv(key)
}

export function getConfig(): StorageConfigType {
  dotenv.config()

  return {
    version: getOptionalConfigFromEnv('VERSION') || '0.0.0',
    keepAliveTimeout: parseInt(getOptionalConfigFromEnv('SERVER_KEEP_ALIVE_TIMEOUT') || '61', 10),
    headersTimeout: parseInt(getOptionalConfigFromEnv('SERVER_HEADERS_TIMEOUT') || '65', 10),
    adminApiKeys: getOptionalConfigFromEnv('ADMIN_API_KEYS') || '',
    adminRequestIdHeader: getOptionalConfigFromEnv('ADMIN_REQUEST_ID_HEADER'),
    anonKey: getOptionalIfMultitenantConfigFromEnv('ANON_KEY') || '',
    encryptionKey: getOptionalConfigFromEnv('ENCRYPTION_KEY') || '',
    fileSizeLimit: Number(getConfigFromEnv('FILE_SIZE_LIMIT')),
    fileStoragePath: getOptionalConfigFromEnv('FILE_STORAGE_BACKEND_PATH'),
    globalS3MaxSockets: parseInt(getOptionalConfigFromEnv('GLOBAL_S3_MAX_SOCKETS') || '200', 10),
    globalS3Protocol: (getOptionalConfigFromEnv('GLOBAL_S3_PROTOCOL') || 'https') as
      | 'http'
      | 'https',
    globalS3Bucket: getConfigFromEnv('GLOBAL_S3_BUCKET'),
    globalS3Endpoint: getOptionalConfigFromEnv('GLOBAL_S3_ENDPOINT'),
    globalS3ForcePathStyle: getOptionalConfigFromEnv('GLOBAL_S3_FORCE_PATH_STYLE') === 'true',
    isMultitenant: getOptionalConfigFromEnv('IS_MULTITENANT') === 'true',
    jwtSecret: getOptionalIfMultitenantConfigFromEnv('PGRST_JWT_SECRET') || '',
    jwtAlgorithm: getOptionalConfigFromEnv('PGRST_JWT_ALGORITHM') || 'HS256',
    multitenantDatabaseUrl: getOptionalConfigFromEnv('MULTITENANT_DATABASE_URL'),
    databaseSSLRootCert: getOptionalConfigFromEnv('DATABASE_SSL_ROOT_CERT'),
    databaseURL: getOptionalIfMultitenantConfigFromEnv('DATABASE_URL') || '',
    databasePoolURL: getOptionalConfigFromEnv('DATABASE_POOL_URL') || '',
    databaseMaxConnections: parseInt(
      getOptionalConfigFromEnv('DATABASE_MAX_CONNECTIONS') || '20',
      10
    ),
    databaseFreePoolAfterInactivity: parseInt(
      getOptionalConfigFromEnv('DATABASE_FREE_POOL_AFTER_INACTIVITY') || (1000 * 60).toString(),
      10
    ),
    databaseConnectionTimeout: parseInt(
      getOptionalConfigFromEnv('DATABASE_CONNECTION_TIMEOUT') || '3000',
      10
    ),
    region: getConfigFromEnv('REGION'),
    requestIdHeader: getOptionalConfigFromEnv('REQUEST_ID_HEADER'),
    serviceKey: getOptionalIfMultitenantConfigFromEnv('SERVICE_KEY') || '',
    storageBackendType: getConfigFromEnv('STORAGE_BACKEND') as StorageBackendType,
    tenantId:
      getOptionalConfigFromEnv('PROJECT_REF') ||
      getOptionalIfMultitenantConfigFromEnv('TENANT_ID') ||
      '',
    urlLengthLimit: Number(getOptionalConfigFromEnv('URL_LENGTH_LIMIT')) || 7_500,
    xForwardedHostRegExp: getOptionalConfigFromEnv('X_FORWARDED_HOST_REGEXP'),
    logLevel: getOptionalConfigFromEnv('LOG_LEVEL') || 'info',
    logflareEnabled: getOptionalConfigFromEnv('LOGFLARE_ENABLED') === 'true',
    logflareApiKey: getOptionalConfigFromEnv('LOGFLARE_API_KEY'),
    logflareSourceToken: getOptionalConfigFromEnv('LOGFLARE_SOURCE_TOKEN'),
    enableQueueEvents: getOptionalConfigFromEnv('ENABLE_QUEUE_EVENTS') === 'true',
    pgQueueConnectionURL: getOptionalConfigFromEnv('PG_QUEUE_CONNECTION_URL'),
    pgQueueDeleteAfterDays: parseInt(
      getOptionalConfigFromEnv('PG_QUEUE_DELETE_AFTER_DAYS') || '2',
      10
    ),
    pgQueueArchiveCompletedAfterSeconds: parseInt(
      getOptionalConfigFromEnv('PG_QUEUE_ARCHIVE_COMPLETED_AFTER_SECONDS') || '7200',
      10
    ),
    pgQueueRetentionDays: parseInt(getOptionalConfigFromEnv('PG_QUEUE_RETENTION_DAYS') || '2', 10),
    webhookURL: getOptionalConfigFromEnv('WEBHOOK_URL'),
    webhookApiKey: getOptionalConfigFromEnv('WEBHOOK_API_KEY'),
    webhookQueuePullInterval: parseInt(
      getOptionalConfigFromEnv('WEBHOOK_QUEUE_PULL_INTERVAL') || '700'
    ),
    webhookQueueTeamSize: parseInt(getOptionalConfigFromEnv('QUEUE_WEBHOOKS_TEAM_SIZE') || '50'),
    webhookQueueConcurrency: parseInt(getOptionalConfigFromEnv('QUEUE_WEBHOOK_CONCURRENCY') || '5'),
    adminDeleteQueueTeamSize: parseInt(
      getOptionalConfigFromEnv('QUEUE_ADMIN_DELETE_TEAM_SIZE') || '50'
    ),
    adminDeleteConcurrency: parseInt(
      getOptionalConfigFromEnv('QUEUE_ADMIN_DELETE_CONCURRENCY') || '5'
    ),
    enableImageTransformation: getOptionalConfigFromEnv('ENABLE_IMAGE_TRANSFORMATION') === 'true',
    imgProxyRequestTimeout: parseInt(
      getOptionalConfigFromEnv('IMGPROXY_REQUEST_TIMEOUT') || '15',
      10
    ),
    imgProxyHttpMaxSockets: parseInt(
      getOptionalConfigFromEnv('IMGPROXY_HTTP_MAX_SOCKETS') || '5000',
      10
    ),
    imgProxyHttpKeepAlive: parseInt(
      getOptionalConfigFromEnv('IMGPROXY_HTTP_KEEP_ALIVE_TIMEOUT') || '61',
      10
    ),
    imgProxyURL: getOptionalConfigFromEnv('IMGPROXY_URL'),
    imgLimits: {
      size: {
        min: parseInt(getOptionalConfigFromEnv('IMG_LIMITS_MIN_SIZE') || '1', 10),
        max: parseInt(getOptionalConfigFromEnv('IMG_LIMITS_MAX_SIZE') || '2000', 10),
      },
    },
    postgrestForwardHeaders: getOptionalConfigFromEnv('POSTGREST_FORWARD_HEADERS'),
    host: getOptionalConfigFromEnv('HOST') || '0.0.0.0',
    port: Number(getOptionalConfigFromEnv('PORT')) || 5000,
    adminPort: Number(getOptionalConfigFromEnv('ADMIN_PORT')) || 5001,
    enableRateLimiter: getOptionalConfigFromEnv('ENABLE_RATE_LIMITER') === 'true',
    rateLimiterSkipOnError: getOptionalConfigFromEnv('RATE_LIMITER_SKIP_ON_ERROR') === 'true',
    rateLimiterDriver: getOptionalConfigFromEnv('RATE_LIMITER_DRIVER') || 'memory',
    rateLimiterRedisUrl: getOptionalConfigFromEnv('RATE_LIMITER_REDIS_URL'),
    rateLimiterRenderPathMaxReqSec: parseInt(
      getOptionalConfigFromEnv('RATE_LIMITER_RENDER_PATH_MAX_REQ_SEC') || '5',
      10
    ),
    rateLimiterRedisConnectTimeout: parseInt(
      getOptionalConfigFromEnv('RATE_LIMITER_REDIS_CONNECT_TIMEOUT') || '2',
      10
    ),
    rateLimiterRedisCommandTimeout: parseInt(
      getOptionalConfigFromEnv('RATE_LIMITER_REDIS_COMMAND_TIMEOUT') || '2',
      10
    ),
    signedUploadUrlExpirationTime: parseInt(
      getOptionalConfigFromEnv('SIGNED_UPLOAD_URL_EXPIRATION_TIME') || '60'
    ),

    tusPath: getOptionalConfigFromEnv('TUS_URL_PATH') || '/upload/resumable',
    tusUrlExpiryMs: parseInt(
      getOptionalConfigFromEnv('TUS_URL_EXPIRY_MS') || (1000 * 60 * 60).toString(),
      10
    ),
    tusUseFileVersionSeparator:
      getOptionalConfigFromEnv('TUS_USE_FILE_VERSION_SEPARATOR') === 'true',
    enableDefaultMetrics: getOptionalConfigFromEnv('ENABLE_DEFAULT_METRICS') === 'true',
  }
}
