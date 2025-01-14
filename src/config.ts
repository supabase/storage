import dotenv from 'dotenv'
import jwt from 'jsonwebtoken'

export type StorageBackendType = 'file' | 's3'
export enum MultitenantMigrationStrategy {
  PROGRESSIVE = 'progressive',
  ON_REQUEST = 'on_request',
  FULL_FLEET = 'full_fleet',
}

type StorageConfigType = {
  isProduction: boolean
  version: string
  exposeDocs: boolean
  keepAliveTimeout: number
  headersTimeout: number
  adminApiKeys: string
  adminRequestIdHeader?: string
  encryptionKey: string
  uploadFileSizeLimit: number
  uploadFileSizeLimitStandard?: number
  storageFilePath?: string
  storageFileEtagAlgorithm: 'mtime' | 'md5'
  storageS3MaxSockets: number
  storageS3Bucket: string
  storageS3Endpoint?: string
  storageS3ForcePathStyle?: boolean
  storageS3Region: string
  storageS3ClientTimeout: number
  isMultitenant: boolean
  jwtSecret: string
  jwtAlgorithm: string
  jwtJWKS?: { keys: { kid?: string; kty: string }[] }
  multitenantDatabaseUrl?: string
  dbAnonRole: string
  dbAuthenticatedRole: string
  dbServiceRole: string
  dbInstallRoles: boolean
  dbRefreshMigrationHashesOnMismatch: boolean
  dbSuperUser: string
  dbSearchPath: string
  dbMigrationStrategy: MultitenantMigrationStrategy
  dbPostgresVersion?: string
  databaseURL: string
  databaseSSLRootCert?: string
  databasePoolURL?: string
  databaseMaxConnections: number
  databaseFreePoolAfterInactivity: number
  databaseConnectionTimeout: number
  region: string
  requestTraceHeader?: string
  requestEtagHeaders: string[]
  responseSMaxAge: number
  anonKey: string
  serviceKey: string
  storageBackendType: StorageBackendType
  tenantId: string
  requestUrlLengthLimit: number
  requestXForwardedHostRegExp?: string
  requestAllowXForwardedPrefix?: boolean
  logLevel?: string
  logflareEnabled?: boolean
  logflareApiKey?: string
  logflareSourceToken?: string
  pgQueueEnable: boolean
  pgQueueEnableWorkers?: boolean
  pgQueueReadWriteTimeout: number
  pgQueueMaxConnections: number
  pgQueueConnectionURL?: string
  pgQueueDeleteAfterHours?: number
  pgQueueDeleteAfterDays?: number
  pgQueueArchiveCompletedAfterSeconds?: number
  pgQueueRetentionDays?: number
  webhookURL?: string
  webhookApiKey?: string
  webhookQueuePullInterval?: number
  webhookQueueTeamSize?: number
  webhookQueueConcurrency?: number
  webhookMaxConnections: number
  webhookQueueMaxFreeSockets: number
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
  tusMaxConcurrentUploads: number
  tusPath: string
  tusPartSize: number
  tusUseFileVersionSeparator: boolean
  tusAllowS3Tags: boolean
  defaultMetricsEnabled: boolean
  s3ProtocolEnabled: boolean
  s3ProtocolPrefix: string
  s3ProtocolAllowForwardedHeader: boolean
  s3ProtocolEnforceRegion: boolean
  s3ProtocolAccessKeyId?: string
  s3ProtocolAccessKeySecret?: string
  s3ProtocolNonCanonicalHostHeader?: string
  tracingEnabled?: boolean
  tracingMode?: string
  tracingTimeMinDuration: number
  tracingReturnServerTimings: boolean
  tracingFeatures?: {
    upload: boolean
  }
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
let envPaths = ['.env']

export function setEnvPaths(paths: string[]) {
  envPaths = paths
}

export function mergeConfig(newConfig: Partial<StorageConfigType>) {
  config = { ...config, ...(newConfig as Required<StorageConfigType>) }
}

export function getConfig(options?: { reload?: boolean }): StorageConfigType {
  if (config && !options?.reload) {
    return config
  }

  envPaths.map((envPath) => dotenv.config({ path: envPath, override: false }))
  const isMultitenant = getOptionalConfigFromEnv('MULTI_TENANT', 'IS_MULTITENANT') === 'true'

  config = {
    isProduction: process.env.NODE_ENV === 'production',
    exposeDocs: getOptionalConfigFromEnv('EXPOSE_DOCS') !== 'false',
    isMultitenant,
    // Tenant
    tenantId: isMultitenant
      ? ''
      : getOptionalConfigFromEnv('PROJECT_REF') ||
        getOptionalConfigFromEnv('TENANT_ID') ||
        'storage-single-tenant',

    // Server
    region: getOptionalConfigFromEnv('SERVER_REGION', 'REGION') || 'not-specified',
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
    requestAllowXForwardedPrefix:
      getOptionalConfigFromEnv('REQUEST_ALLOW_X_FORWARDED_PATH') === 'true',
    requestUrlLengthLimit:
      Number(getOptionalConfigFromEnv('REQUEST_URL_LENGTH_LIMIT', 'URL_LENGTH_LIMIT')) || 7_500,
    requestTraceHeader: getOptionalConfigFromEnv('REQUEST_TRACE_HEADER', 'REQUEST_ID_HEADER'),
    requestEtagHeaders: getOptionalConfigFromEnv('REQUEST_ETAG_HEADERS')?.trim().split(',') || [
      'if-none-match',
    ],
    responseSMaxAge: parseInt(getOptionalConfigFromEnv('RESPONSE_S_MAXAGE') || '0', 10),

    // Admin
    adminApiKeys: getOptionalConfigFromEnv('SERVER_ADMIN_API_KEYS', 'ADMIN_API_KEYS') || '',
    adminRequestIdHeader: getOptionalConfigFromEnv(
      'REQUEST_TRACE_HEADER',
      'REQUEST_ADMIN_TRACE_HEADER'
    ),

    // Auth
    serviceKey: getOptionalConfigFromEnv('SERVICE_KEY') || '',
    anonKey: getOptionalConfigFromEnv('ANON_KEY') || '',

    encryptionKey: getOptionalConfigFromEnv('AUTH_ENCRYPTION_KEY', 'ENCRYPTION_KEY') || '',
    jwtSecret: getOptionalIfMultitenantConfigFromEnv('AUTH_JWT_SECRET', 'PGRST_JWT_SECRET') || '',
    jwtAlgorithm: getOptionalConfigFromEnv('AUTH_JWT_ALGORITHM', 'PGRST_JWT_ALGORITHM') || 'HS256',

    // Upload
    uploadFileSizeLimit: Number(
      getOptionalConfigFromEnv('UPLOAD_FILE_SIZE_LIMIT', 'FILE_SIZE_LIMIT')
    ),
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
    tusPartSize: parseInt(getOptionalConfigFromEnv('TUS_PART_SIZE') || '50', 10),
    tusUrlExpiryMs: parseInt(
      getOptionalConfigFromEnv('TUS_URL_EXPIRY_MS') || (1000 * 60 * 60).toString(),
      10
    ),
    tusMaxConcurrentUploads: parseInt(
      getOptionalConfigFromEnv('TUS_MAX_CONCURRENT_UPLOADS') || '500',
      10
    ),
    tusUseFileVersionSeparator:
      getOptionalConfigFromEnv('TUS_USE_FILE_VERSION_SEPARATOR') === 'true',
    tusAllowS3Tags: getOptionalConfigFromEnv('TUS_ALLOW_S3_TAGS') !== 'false',

    // S3 Protocol
    s3ProtocolEnabled: getOptionalConfigFromEnv('S3_PROTOCOL_ENABLED') !== 'false',
    s3ProtocolPrefix: getOptionalConfigFromEnv('S3_PROTOCOL_PREFIX') || '',
    s3ProtocolAllowForwardedHeader:
      getOptionalConfigFromEnv('S3_ALLOW_FORWARDED_HEADER') === 'true',
    s3ProtocolEnforceRegion: getOptionalConfigFromEnv('S3_PROTOCOL_ENFORCE_REGION') === 'true',
    s3ProtocolAccessKeyId: getOptionalConfigFromEnv('S3_PROTOCOL_ACCESS_KEY_ID'),
    s3ProtocolAccessKeySecret: getOptionalConfigFromEnv('S3_PROTOCOL_ACCESS_KEY_SECRET'),
    s3ProtocolNonCanonicalHostHeader: getOptionalConfigFromEnv(
      'S3_PROTOCOL_NON_CANONICAL_HOST_HEADER'
    ),
    // Storage
    storageBackendType: getOptionalConfigFromEnv('STORAGE_BACKEND') as StorageBackendType,

    // Storage - File
    storageFilePath: getOptionalConfigFromEnv(
      'STORAGE_FILE_BACKEND_PATH',
      'FILE_STORAGE_BACKEND_PATH'
    ),
    storageFileEtagAlgorithm: getOptionalConfigFromEnv('STORAGE_FILE_ETAG_ALGORITHM') || 'md5',

    // Storage - S3
    storageS3MaxSockets: parseInt(
      getOptionalConfigFromEnv('STORAGE_S3_MAX_SOCKETS', 'GLOBAL_S3_MAX_SOCKETS') || '200',
      10
    ),
    storageS3Bucket: getOptionalConfigFromEnv('STORAGE_S3_BUCKET', 'GLOBAL_S3_BUCKET'),
    storageS3Endpoint: getOptionalConfigFromEnv('STORAGE_S3_ENDPOINT', 'GLOBAL_S3_ENDPOINT'),
    storageS3ForcePathStyle:
      getOptionalConfigFromEnv('STORAGE_S3_FORCE_PATH_STYLE', 'GLOBAL_S3_FORCE_PATH_STYLE') ===
      'true',
    storageS3Region: getOptionalConfigFromEnv('STORAGE_S3_REGION', 'REGION') as string,
    storageS3ClientTimeout: Number(getOptionalConfigFromEnv('STORAGE_S3_CLIENT_TIMEOUT') || `0`),

    // DB - Migrations
    dbAnonRole: getOptionalConfigFromEnv('DB_ANON_ROLE') || 'anon',
    dbServiceRole: getOptionalConfigFromEnv('DB_SERVICE_ROLE') || 'service_role',
    dbAuthenticatedRole: getOptionalConfigFromEnv('DB_AUTHENTICATED_ROLE') || 'authenticated',
    dbInstallRoles: getOptionalConfigFromEnv('DB_INSTALL_ROLES') === 'true',
    dbRefreshMigrationHashesOnMismatch: !(
      getOptionalConfigFromEnv('DB_ALLOW_MIGRATION_REFRESH') === 'false'
    ),
    dbSuperUser: getOptionalConfigFromEnv('DB_SUPER_USER') || 'postgres',
    dbMigrationStrategy: getOptionalConfigFromEnv('DB_MIGRATIONS_STRATEGY') || 'on_request',

    // Database - Connection
    dbSearchPath: getOptionalConfigFromEnv('DATABASE_SEARCH_PATH', 'DB_SEARCH_PATH') || '',
    dbPostgresVersion: getOptionalConfigFromEnv('DATABASE_POSTGRES_VERSION'),
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
    defaultMetricsEnabled: !(
      getOptionalConfigFromEnv('DEFAULT_METRICS_ENABLED', 'ENABLE_DEFAULT_METRICS') === 'false'
    ),
    tracingEnabled: getOptionalConfigFromEnv('TRACING_ENABLED') === 'true',
    tracingMode: getOptionalConfigFromEnv('TRACING_MODE') ?? 'basic',
    tracingTimeMinDuration: parseFloat(
      getOptionalConfigFromEnv('TRACING_SERVER_TIME_MIN_DURATION') ?? '100.0'
    ),
    tracingReturnServerTimings:
      getOptionalConfigFromEnv('TRACING_RETURN_SERVER_TIMINGS') === 'true',
    tracingFeatures: {
      upload: getOptionalConfigFromEnv('TRACING_FEATURE_UPLOAD') === 'true',
    },

    // Queue
    pgQueueEnable: getOptionalConfigFromEnv('PG_QUEUE_ENABLE', 'ENABLE_QUEUE_EVENTS') === 'true',
    pgQueueEnableWorkers: getOptionalConfigFromEnv('PG_QUEUE_WORKERS_ENABLE') !== 'false',
    pgQueueReadWriteTimeout: Number(getOptionalConfigFromEnv('PG_QUEUE_READ_WRITE_TIMEOUT')) || 0,
    pgQueueMaxConnections: Number(getOptionalConfigFromEnv('PG_QUEUE_MAX_CONNECTIONS')) || 4,
    pgQueueConnectionURL: getOptionalConfigFromEnv('PG_QUEUE_CONNECTION_URL'),
    pgQueueDeleteAfterDays: parseInt(
      getOptionalConfigFromEnv('PG_QUEUE_DELETE_AFTER_DAYS') || '2',
      10
    ),
    pgQueueDeleteAfterHours:
      Number(getOptionalConfigFromEnv('PG_QUEUE_DELETE_AFTER_HOURS')) || undefined,
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
    webhookMaxConnections: parseInt(
      getOptionalConfigFromEnv('QUEUE_WEBHOOK_MAX_CONNECTIONS') || '500'
    ),
    webhookQueueMaxFreeSockets: parseInt(
      getOptionalConfigFromEnv('QUEUE_WEBHOOK_MAX_FREE_SOCKETS') || '20'
    ),
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
  } as StorageConfigType

  if (!config.isMultitenant && !config.serviceKey) {
    config.serviceKey = jwt.sign({ role: config.dbServiceRole }, config.jwtSecret, {
      expiresIn: '10y',
      algorithm: config.jwtAlgorithm as jwt.Algorithm,
    })
  }

  if (!config.isMultitenant && !config.anonKey) {
    config.anonKey = jwt.sign({ role: config.dbAnonRole }, config.jwtSecret, {
      expiresIn: '10y',
      algorithm: config.jwtAlgorithm as jwt.Algorithm,
    })
  }

  const jwtJWKS = getOptionalConfigFromEnv('JWT_JWKS') || null

  if (jwtJWKS) {
    try {
      const parsed = JSON.parse(jwtJWKS)
      config.jwtJWKS = parsed
    } catch (e: any) {
      throw new Error('Unable to parse JWT_JWKS value to JSON')
    }
  }

  return config
}
