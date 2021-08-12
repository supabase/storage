import dotenv from 'dotenv'

type StorageBackendType = 'file' | 's3'
type StorageConfigType = {
  anonKey: string
  serviceKey: string
  projectRef: string
  region: string
  postgrestURL: string
  globalS3Bucket: string
  globalS3Endpoint?: string
  jwtSecret: string
  fileSizeLimit: number
  storageBackendType: StorageBackendType
  fileStoragePath?: string
  xForwardedHostRegExp?: string
  supabaseReadOnlyUrl?: string
  supabaseApiKey?: string
  supabaseEncryptionKey?: string
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

export function getConfig(): StorageConfigType {
  dotenv.config()

  return {
    anonKey: getConfigFromEnv('ANON_KEY'),
    serviceKey: getConfigFromEnv('SERVICE_KEY'),
    projectRef: getConfigFromEnv('PROJECT_REF'),
    region: getConfigFromEnv('REGION'),
    postgrestURL: getConfigFromEnv('POSTGREST_URL'),
    globalS3Bucket: getConfigFromEnv('GLOBAL_S3_BUCKET'),
    globalS3Endpoint: getOptionalConfigFromEnv('GLOBAL_S3_ENDPOINT'),
    jwtSecret: getConfigFromEnv('PGRST_JWT_SECRET'),
    fileSizeLimit: Number(getConfigFromEnv('FILE_SIZE_LIMIT')),
    storageBackendType: getConfigFromEnv('STORAGE_BACKEND') as StorageBackendType,
    fileStoragePath: getOptionalConfigFromEnv('FILE_STORAGE_BACKEND_PATH'),
    xForwardedHostRegExp: getOptionalConfigFromEnv('X_FORWARDED_HOST_REGEXP'),
    supabaseReadOnlyUrl: getOptionalConfigFromEnv('SUPABASE_READ_ONLY_URL'),
    supabaseApiKey: getOptionalConfigFromEnv('SUPABASE_API_KEY'),
    supabaseEncryptionKey: getOptionalConfigFromEnv('SUPABASE_ENCRYPTION_KEY'),
  }
}
