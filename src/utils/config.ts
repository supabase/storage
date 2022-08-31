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
  isMultitenant: boolean
  jwtSecret: string
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
    isMultitenant: getOptionalConfigFromEnv('IS_MULTITENANT') === 'true',
    jwtSecret: getOptionalIfMultitenantConfigFromEnv('PGRST_JWT_SECRET') || '',
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
    logLevel: getOptionalConfigFromEnv('LOG_LEVEL') || 'trace',
    logflareEnabled: getOptionalConfigFromEnv('LOGFLARE_ENABLED') === 'true',
    logflareApiKey: getOptionalConfigFromEnv('LOGFLARE_API_KEY'),
    logflareSourceToken: getOptionalConfigFromEnv('LOGFLARE_SOURCE_TOKEN'),
  }
}
