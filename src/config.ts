import dotenv from 'dotenv'

type StorageBackendType = 'file' | 's3'

export interface StorageProviderConfig {
  name: string
  endpoint?: string
  region?: string
  forcePathStyle?: boolean
  accessKey?: string
  secretKey?: string
  isDefault: boolean
}

export interface StorageProviders {
  default: StorageProviderConfig
  [key: string]: StorageProviderConfig
}

type StorageConfigType = {
  keepAliveTimeout: number
  headersTimeout: number
  adminApiKeys: string
  adminRequestIdHeader?: string
  anonKey: string
  encryptionKey: string
  fileSizeLimit: number
  fileStoragePath?: string
  storageS3MaxSockets?: number
  storageS3Bucket: string
  storageS3Protocol?: 'http' | 'https' | string
  storageProviders: StorageProviders
  isMultitenant: boolean
  jwtSecret: string
  jwtAlgorithm: string
  multitenantDatabaseUrl?: string
  databaseURL: string
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
  webhookURL?: string
  webhookApiKey?: string
  webhookQueuePullInterval?: number
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
  sMaxAge: string
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
  return getOptionalConfigFromEnv('IS_MULTITENANT') === 'true'
    ? getOptionalConfigFromEnv(key, fallback)
    : getConfigFromEnv(key, fallback)
}

let config: StorageConfigType | undefined

export function setConfig(newConfig: StorageConfigType) {
  config = newConfig
}

export function getConfig(options?: { reload?: boolean }): StorageConfigType {
  if (config && !options?.reload) {
    return config
  }

  dotenv.config()

  config = {
    keepAliveTimeout: parseInt(getOptionalConfigFromEnv('SERVER_KEEP_ALIVE_TIMEOUT') || '61', 10),
    headersTimeout: parseInt(getOptionalConfigFromEnv('SERVER_HEADERS_TIMEOUT') || '65', 10),
    adminApiKeys: getOptionalConfigFromEnv('ADMIN_API_KEYS') || '',
    adminRequestIdHeader: getOptionalConfigFromEnv('ADMIN_REQUEST_ID_HEADER'),
    anonKey: getOptionalIfMultitenantConfigFromEnv('ANON_KEY') || '',
    encryptionKey: getOptionalConfigFromEnv('ENCRYPTION_KEY') || '',
    fileSizeLimit: Number(getConfigFromEnv('FILE_SIZE_LIMIT')),
    fileStoragePath: getOptionalConfigFromEnv('FILE_STORAGE_BACKEND_PATH'),
    storageProviders: loadStorageS3ProviderFromEnv(),
    storageS3MaxSockets: parseInt(
      getOptionalConfigFromEnv('STORAGE_S3_MAX_SOCKETS', 'GLOBAL_S3_MAX_SOCKETS') || '200',
      10
    ),
    storageS3Protocol: getOptionalConfigFromEnv('GLOBAL_S3_PROTOCOL') || 'https',
    storageS3Bucket: getConfigFromEnv('STORAGE_S3_BUCKET', 'GLOBAL_S3_BUCKET'),
    isMultitenant: getOptionalConfigFromEnv('IS_MULTITENANT') === 'true',
    jwtSecret: getOptionalIfMultitenantConfigFromEnv('AUTH_JWT_SECRET', 'PGRST_JWT_SECRET') || '',
    jwtAlgorithm: getOptionalConfigFromEnv('AUTH_JWT_ALGORITHM', 'PGRST_JWT_ALGORITHM') || 'HS256',
    multitenantDatabaseUrl: getOptionalConfigFromEnv('MULTITENANT_DATABASE_URL'),
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
        min: parseInt(getOptionalConfigFromEnv('IMG_LIMITS_MIN_SIZE') || '0', 10),
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
    sMaxAge: getOptionalConfigFromEnv('S_MAXAGE') || '31536000',
  }

  return config
}

/**
 * Load S3 storage providers from env variables
 * The convention is STORAGE_S3_PROVIDER_{PROVIDER_NAME}_{CONFIGURATION}
 * When specifying more than one provider you must also specify the default provider using STORAGE_S3_PROVIDER_{PROVIDER_NAME}_DEFAULT=true
 *
 * Example Minio provider:
 *
 * STORAGE_S3_PROVIDER_MINIO_DEFAULT=true
 * STORAGE_S3_PROVIDER_MINIO_ENDPOINT=http://127.0.0.1:9000
 * STORAGE_S3_PROVIDER_MINIO_FORCE_PATH_STYLE=true
 * STORAGE_S3_PROVIDER_MINIO_ACCESS_KEY_ID=supa-storage
 * STORAGE_S3_PROVIDER_MINIO_SECRET_ACCESS_KEY=secret1234
 * STORAGE_S3_PROVIDER_MINIO_REGION=us-east-1
 */
function loadStorageS3ProviderFromEnv() {
  const providersENV = Object.keys(process.env).filter((key) =>
    key.startsWith('STORAGE_S3_PROVIDER_')
  )

  const providers = providersENV.reduce((all, providerEnv) => {
    const providerRegex = new RegExp('(STORAGE_S3_PROVIDER)_([A-Z0-9]+)_(.*)', 'gi')
    const matches = providerRegex.exec(providerEnv)

    if (matches?.length !== 4) {
      throw new Error(
        `Invalid storage provider env variable: ${providerEnv} format is STORAGE_PROVIDER_<provider name>_<config>`
      )
    }

    const providerName = matches[2].toLowerCase()
    const configName = matches[3].toLowerCase()

    if (!all[providerName]) {
      all[providerName] = {
        name: providerName,
        isDefault: false,
      }
    }

    switch (configName) {
      case 'region':
        all[providerName].region = process.env[providerEnv] || ''
        break
      case 'endpoint':
        all[providerName].endpoint = process.env[providerEnv] || ''
        break
      case 'access_key_id':
        all[providerName].accessKey = process.env[providerEnv] || ''
        break
      case 'secret_access_key':
        all[providerName].secretKey = process.env[providerEnv] || ''
        break
      case 'force_path_style':
        all[providerName].forcePathStyle = process.env[providerEnv] === 'true'
        break
      case 'default':
        all[providerName].isDefault = process.env[providerEnv] === 'true'
        break
      default:
        throw new Error(`Invalid storage provider config: ${configName}`)
    }

    return all
  }, {} as Record<string, StorageProviderConfig>)

  const providersNumber = Object.keys(providers).length

  // If multiple providers are configured we check if one is default
  if (providersNumber > 1) {
    const defaultProviderName = Object.keys(providers).find((providerName) => {
      return providers[providerName].isDefault
    })

    if (!defaultProviderName) {
      throw new Error(
        `Missing default storage provider config please provide STORAGE_PROVIDER_<name>_DEFAULT=true`
      )
    }

    providers.default = providers[defaultProviderName]
  }

  // Only 1 provider specified, we set it as default
  if (providersNumber === 1) {
    providers.default = Object.values(providers)[0]
  }

  if (providersNumber === 0) {
    // Backwards compatibility with old env variables
    const endpoint = getOptionalConfigFromEnv('GLOBAL_S3_ENDPOINT')
    const pathStyle = getOptionalConfigFromEnv('GLOBAL_S3_FORCE_PATH_STYLE')

    if (endpoint || pathStyle) {
      providers.default = {
        isDefault: true,
        name: 'default',
        endpoint,
        forcePathStyle: pathStyle === 'true',
        region: getOptionalConfigFromEnv('AWS_DEFAULT_REGION', 'REGION'),
        accessKey: getOptionalConfigFromEnv('AWS_ACCESS_KEY_ID'),
        secretKey: getOptionalConfigFromEnv('AWS_SECRET_ACCESS_KEY'),
      }
    } else if (getConfigFromEnv('STORAGE_BACKEND') === 's3') {
      throw new Error('Missing storage provider config please provide STORAGE_PROVIDER_*')
    }
  }

  return providers as StorageProviders
}
