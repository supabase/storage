import dotenv from 'dotenv'

type storageConfigType = {
  anonKey: string
  serviceKey: string
  projectRef: string
  region: string
  postgrestURL: string
  globalS3Bucket: string
  globalS3Endpoint?: string
  jwtSecret: string
}

function getConfigFromEnv(key: string): string {
  const value = process.env[key]
  if (!value) {
    throw new Error(`${key} is undefined`)
  }
  return value
}

function getOptionalConfigFromEnv(key: string): string | undefined {
  return process.env[key]
}

export function getConfig(): storageConfigType {
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
  }
}
