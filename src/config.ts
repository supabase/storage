import dotenv from 'dotenv'

type StorageBackendType = 'file' | 's3'
type StorageConfigType = {
  adminApiKeys: string
  adminRequestIdHeader?: string
  anonKey: string
  encryptionKey: string
  fileSizeLimit: number
  fileStoragePath?: string
  globalS3Bucket: string
  globalS3Endpoint?: string
  globalS3ForcePathStyle?: boolean
  isMultitenant: boolean
  jwtSecret: string
  jwtAlgorithm: string
  multitenantDatabaseUrl?: string
  postgrestURL: string
  postgrestURLSuffix?: string
  postgrestURLScheme?: string
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
  webhookURL?: string
  webhookApiKey?: string
  webhookQueuePullInterval?: number
  enableImageTransformation: boolean
  imgProxyURL?: string
  imgProxyRequestTimeout: number
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
  rateLimiterRenderPathMaxReqSec: number
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
    adminApiKeys: getOptionalConfigFromEnv('ADMIN_API_KEYS') || '',
    adminRequestIdHeader: getOptionalConfigFromEnv('ADMIN_REQUEST_ID_HEADER'),
    anonKey: getOptionalIfMultitenantConfigFromEnv('ANON_KEY') || '',
    encryptionKey: getOptionalConfigFromEnv('ENCRYPTION_KEY') || '',
    fileSizeLimit: Number(getConfigFromEnv('FILE_SIZE_LIMIT')),
    fileStoragePath: getOptionalConfigFromEnv('FILE_STORAGE_BACKEND_PATH'),
    globalS3Bucket: getConfigFromEnv('GLOBAL_S3_BUCKET'),
    globalS3Endpoint: getOptionalConfigFromEnv('GLOBAL_S3_ENDPOINT'),
    globalS3ForcePathStyle: getOptionalConfigFromEnv('GLOBAL_S3_FORCE_PATH_STYLE') === 'true',
    isMultitenant: getOptionalConfigFromEnv('IS_MULTITENANT') === 'true',
    jwtSecret: getOptionalIfMultitenantConfigFromEnv('PGRST_JWT_SECRET') || '',
    jwtAlgorithm: getOptionalConfigFromEnv('PGRST_JWT_ALGORITHM') || 'HS256',
    multitenantDatabaseUrl: getOptionalConfigFromEnv('MULTITENANT_DATABASE_URL'),
    postgrestURL: getOptionalIfMultitenantConfigFromEnv('POSTGREST_URL') || '',
    postgrestURLSuffix: getOptionalConfigFromEnv('POSTGREST_URL_SUFFIX'),
    postgrestURLScheme: getOptionalConfigFromEnv('POSTGREST_URL_SCHEME') || 'http',
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
    webhookURL: getOptionalConfigFromEnv('WEBHOOK_URL'),
    webhookApiKey: getOptionalConfigFromEnv('WEBHOOK_API_KEY'),
    webhookQueuePullInterval: parseInt(
      getOptionalConfigFromEnv('WEBHOOK_QUEUE_PULL_INTERVAL') || '700'
    ),
    enableImageTransformation: getOptionalConfigFromEnv('ENABLE_IMAGE_TRANSFORMATION') === 'true',
    imgProxyRequestTimeout: parseInt(
      getOptionalConfigFromEnv('IMGPROXY_REQUEST_TIMEOUT') || '15',
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
    rateLimiterDriver: getOptionalConfigFromEnv('RATE_LIMITER_DRIVER') || 'memory',
    rateLimiterRedisUrl: getOptionalConfigFromEnv('RATE_LIMITER_REDIS_URL'),
    rateLimiterRenderPathMaxReqSec: parseInt(
      getOptionalConfigFromEnv('RATE_LIMITER_RENDER_PATH_MAX_REQ_SEC') || '5',
      10
    ),
  }
}
