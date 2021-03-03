import dotenv from 'dotenv'

type storageConfigType = {
  anonKey: string
  serviceKey: string
  projectRef: string
  region: string
  supabaseDomain: string
  globalS3Bucket: string
  jwtSecret: string
}

function getConfigFromEnv(key: string): string {
  const value = process.env[key]
  if (!value) {
    throw new Error(`${key} is undefined`)
  }
  return value
}

export function getConfig(): storageConfigType {
  dotenv.config()

  return {
    anonKey: getConfigFromEnv('ANON_KEY'),
    serviceKey: getConfigFromEnv('SERVICE_KEY'),
    projectRef: getConfigFromEnv('PROJECT_REF'),
    region: getConfigFromEnv('REGION'),
    supabaseDomain: getConfigFromEnv('SUPABASE_DOMAIN'),
    globalS3Bucket: getConfigFromEnv('GLOBAL_S3_BUCKET'),
    jwtSecret: getConfigFromEnv('JWT_SECRET'),
  }
}
