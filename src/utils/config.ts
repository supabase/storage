import dotenv from 'dotenv'

type StorageBackendType = 'file' | 's3' | 'oss'
type StorageConfigType = {
  anonKey: string
  serviceKey: string
  projectRef: string
  region: string
  postgrestURL: string
  globalBucket: string
  globalEndpoint: string
  jwtSecret: string
  fileSizeLimit: number
  storageBackendType: StorageBackendType
  fileStoragePath?: string
  ossAccessKey: string
  ossAccessSecret: string
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
    globalBucket: getConfigFromEnv('GLOBAL_BUCKET'),
    globalEndpoint: getOptionalConfigFromEnv('GLOBAL_ENDPOINT') || '',
    jwtSecret: getConfigFromEnv('PGRST_JWT_SECRET'),
    fileSizeLimit: Number(getConfigFromEnv('FILE_SIZE_LIMIT')),
    storageBackendType: getConfigFromEnv('STORAGE_BACKEND') as StorageBackendType,
    fileStoragePath: getOptionalConfigFromEnv('FILE_STORAGE_BACKEND_PATH'),
    ossAccessKey: getConfigFromEnv('OSS_ACCESS_KEY'),
    ossAccessSecret: getConfigFromEnv('OSS_ACCESS_SECRET'),
  }
}
