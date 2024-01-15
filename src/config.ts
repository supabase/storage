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
  uploadFileSizeLimit: number
  uploadFileSizeLimitStandard?: number
  storageFilePath?: string
  storageS3MaxSockets?: number
  storageS3Bucket: string
  storageS3Endpoint?: string
  storageS3ForcePathStyle?: boolean
  storageS3Region: string
  isMultitenant: boolean
  jwtSecret: string
  jwtAlgorithm: string
  multitenantDatabaseUrl?: string
  dbAnonRole: string
  dbAuthenticatedRole: string
  dbServiceRole: string
  dbInstallRoles: boolean
  dbRefreshMigrationHashesOnMismatch: boolean
  dbSuperUser: string
  dbSearchPath: string
  databaseURL: string
  databaseSSLRootCert?: string
  databasePoolURL?: string
  databaseMaxConnections: number
  databaseFreePoolAfterInactivity: number
  databaseConnectionTimeout: number
  region: string
  requestTraceHeader?: string
  serviceKey: string
  storageBackendType: StorageBackendType
  tenantId: string
  requestUrlLengthLimit: number
  requestXForwardedHostRegExp?: string
  logLevel?: string
  logflareEnabled?: boolean
  logflareApiKey?: string
  logflareSourceToken?: string
  pgQueueEnable: boolean
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
  imageTransformationEnabled: boolean
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
  rateLimiterEnabled: boolean
  rateLimiterDriver: 'memory' | 'redis' | string
  rateLimiterRedisUrl?: string
  rateLimiterSkipOnError?: boolean
  rateLimiterRenderPathMaxReqSec: number
  rateLimiterRedisConnectTimeout: number
  rateLimiterRedisCommandTimeout: number
  uploadSignedUrlExpirationTime: number
  tusUrlExpiryMs: number
  tusPath: string
  tusUseFileVersionSeparator: boolean
  defaultMetricsEnabled: boolean
}

function getOptionalConfigFromEnv(key: string, fallback?: string): string | undefined {
  const envValue = process.env[key]

  if (!envValue && fallback) {
    return getOptionalConfigFromEnv(fallback)
  }

  return envValue
}

function getConfigFromEnv(key: string, fallbackEnv?: string): string {
  const value = getOptionalConfigFromEnv(key)
  if (!value) {
    if (fallbackEnv) {
      return getConfigFromEnv(fallbackEnv)
    }

    throw new Error(`${key} is undefined`)
  }
  return value
}

function getOptionalIfMultitenantConfigFromEnv(key: string, fallback?: string): string | undefined {
  return getOptionalConfigFromEnv('MULTI_TENANT', 'IS_MULTITENANT') === 'true'
    ? getOptionalConfigFromEnv(key, fallback)
    : getConfigFromEnv(key, fallback)
}

let config: StorageConfigType | undefined

export function mergeConfig(newConfig: Partial<StorageConfigType>) {
  config = { ...config, ...(newConfig as Required<StorageConfigType>) }
}

export function getConfig(options?: { reload?: boolean }): StorageConfigType {
  if (config && !options?.reload) {
    return config
  }

  dotenv.config()

  config = {
    // Tenant
    tenantId:
      getOptionalConfigFromEnv('PROJECT_REF') ||
      getOptionalIfMultitenantConfigFromEnv('TENANT_ID') ||
      '',
    isMultitenant: getOptionalConfigFromEnv('MULTI_TENANT', 'IS_MULTITENANT') === 'true',

    // Server
    region: getConfigFromEnv('SERVER_REGION', 'REGION'),
    version: getOptionalConfigFromEnv('VERSION') || '0.0.0',
    keepAliveTimeout: parseInt(getOptionalConfigFromEnv('SERVER_KEEP_ALIVE_TIMEOUT') || '61', 10),
    headersTimeout: parseInt(getOptionalConfigFromEnv('SERVER_HEADERS_TIMEOUT') || '65', 10),
    host: getOptionalConfigFromEnv('SERVER_HOST', 'HOST') || '0.0.0.0',
    port: Number(getOptionalConfigFromEnv('SERVER_PORT', 'PORT')) || 5000,
    adminPort: Number(getOptionalConfigFromEnv('SERVER_ADMIN_PORT', 'ADMIN_PORT')) || 5001,

    // Request
    requestXForwardedHostRegExp: getOptionalConfigFromEnv(
      'REQUEST_X_FORWARDED_HOST_REGEXP',
      'X_FORWARDED_HOST_REGEXP'
    ),
    requestUrlLengthLimit:
      Number(getOptionalConfigFromEnv('REQUEST_URL_LENGTH_LIMIT', 'URL_LENGTH_LIMIT')) || 7_500,
    requestTraceHeader: getOptionalConfigFromEnv('REQUEST_TRACE_HEADER', 'REQUEST_ID_HEADER'),

    // Admin
    adminApiKeys: getOptionalConfigFromEnv('SERVER_ADMIN_API_KEYS', 'ADMIN_API_KEYS') || '',
    adminRequestIdHeader: getOptionalConfigFromEnv(
      'REQUEST_TRACE_HEADER',
      'REQUEST_ADMIN_TRACE_HEADER'
    ),

    // Auth
    anonKey: getOptionalIfMultitenantConfigFromEnv('ANON_KEY') || '',
    serviceKey: getOptionalIfMultitenantConfigFromEnv('SERVICE_KEY') || '',
    encryptionKey: getOptionalConfigFromEnv('AUTH_ENCRYPTION_KEY', 'ENCRYPTION_KEY') || '',
    jwtSecret: getOptionalIfMultitenantConfigFromEnv('AUTH_JWT_SECRET', 'PGRST_JWT_SECRET') || '',
    jwtAlgorithm: getOptionalConfigFromEnv('AUTH_JWT_ALGORITHM', 'PGRST_JWT_ALGORITHM') || 'HS256',

    // Upload
    uploadFileSizeLimit: Number(getConfigFromEnv('UPLOAD_FILE_SIZE_LIMIT', 'FILE_SIZE_LIMIT')),
    uploadFileSizeLimitStandard: parseInt(
      getOptionalConfigFromEnv(
        'UPLOAD_FILE_SIZE_LIMIT_STANDARD',
        'FILE_SIZE_LIMIT_STANDARD_UPLOAD'
      ) || '0'
    ),
    uploadSignedUrlExpirationTime: parseInt(
      getOptionalConfigFromEnv(
        'UPLOAD_SIGNED_URL_EXPIRATION_TIME',
        'SIGNED_UPLOAD_URL_EXPIRATION_TIME'
      ) || '60'
    ),

    // Upload - TUS
    tusPath: getOptionalConfigFromEnv('TUS_URL_PATH') || '/upload/resumable',
    tusUrlExpiryMs: parseInt(
      getOptionalConfigFromEnv('TUS_URL_EXPIRY_MS') || (1000 * 60 * 60).toString(),
      10
    ),
    tusUseFileVersionSeparator:
      getOptionalConfigFromEnv('TUS_USE_FILE_VERSION_SEPARATOR') === 'true',

    // Storage
    storageBackendType: getConfigFromEnv('STORAGE_BACKEND') as StorageBackendType,

    // Storage - File
    storageFilePath: getOptionalConfigFromEnv('STORAGE_FILE_BACKEND_PATH', 'STORAGE_FILE_PATH'),

    // Storage - S3
    storageS3MaxSockets: parseInt(
      getOptionalConfigFromEnv('STORAGE_S3_MAX_SOCKETS', 'GLOBAL_S3_MAX_SOCKETS') || '200',
      10
    ),
    storageS3Bucket: getConfigFromEnv('STORAGE_S3_BUCKET', 'GLOBAL_S3_BUCKET'),
    storageS3Endpoint: getOptionalConfigFromEnv('STORAGE_S3_ENDPOINT', 'GLOBAL_S3_ENDPOINT'),
    storageS3ForcePathStyle:
      getOptionalConfigFromEnv('STORAGE_S3_FORCE_PATH_STYLE', 'GLOBAL_S3_FORCE_PATH_STYLE') ===
      'true',
    storageS3Region: getOptionalConfigFromEnv('STORAGE_S3_REGION', 'REGION') as string,

    // DB - Migrations
    dbAnonRole: getOptionalConfigFromEnv('DB_ANON_ROLE') || 'anon',
    dbServiceRole: getOptionalConfigFromEnv('DB_SERVICE_ROLE') || 'service_role',
    dbAuthenticatedRole: getOptionalConfigFromEnv('DB_AUTHENTICATED_ROLE') || 'authenticated',
    dbInstallRoles: !(getOptionalConfigFromEnv('DB_INSTALL_ROLES') === 'false'),
    dbRefreshMigrationHashesOnMismatch: !(
      getOptionalConfigFromEnv('DB_ALLOW_MIGRATION_REFRESH') === 'false'
    ),
    dbSuperUser: getOptionalConfigFromEnv('DB_SUPER_USER') || 'postgres',

    // Database - Connection
    dbSearchPath: getOptionalConfigFromEnv('DATABASE_SEARCH_PATH', 'DB_SEARCH_PATH') || '',
    multitenantDatabaseUrl: getOptionalConfigFromEnv(
      'DATABASE_MULTITENANT_URL',
      'MULTITENANT_DATABASE_URL'
    ),
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

    // Monitoring
    logLevel: getOptionalConfigFromEnv('LOG_LEVEL') || 'info',
    logflareEnabled: getOptionalConfigFromEnv('LOGFLARE_ENABLED') === 'true',
    logflareApiKey: getOptionalConfigFromEnv('LOGFLARE_API_KEY'),
    logflareSourceToken: getOptionalConfigFromEnv('LOGFLARE_SOURCE_TOKEN'),
    defaultMetricsEnabled:
      getOptionalConfigFromEnv('DEFAULT_METRICS_ENABLED', 'ENABLE_DEFAULT_METRICS') === 'true',

    // Queue
    pgQueueEnable: getOptionalConfigFromEnv('PG_QUEUE_ENABLE', 'ENABLE_QUEUE_EVENTS') === 'true',
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

    // Webhooks
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

    // Image Transformation
    imageTransformationEnabled:
      getOptionalConfigFromEnv('IMAGE_TRANSFORMATION_ENABLED', 'ENABLE_IMAGE_TRANSFORMATION') ===
      'true',
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
        min: parseInt(
          getOptionalConfigFromEnv('IMAGE_TRANSFORMATION_LIMIT_MIN_SIZE', 'IMG_LIMITS_MIN_SIZE') ||
            '1',
          10
        ),
        max: parseInt(
          getOptionalConfigFromEnv('IMAGE_TRANSFORMATION_LIMIT_MAX_SIZE', 'IMG_LIMITS_MAX_SIZE') ||
            '2000',
          10
        ),
      },
    },

    // Rate Limiting
    rateLimiterEnabled:
      getOptionalConfigFromEnv('RATE_LIMITER_ENABLED', 'ENABLE_RATE_LIMITER') === 'true',
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
  }

  return config
}
