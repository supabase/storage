export const JWT_CACHE_NAME = 'jwt' as const
export const TENANT_CONFIG_CACHE_NAME = 'tenant_config' as const
export const TENANT_JWKS_CACHE_NAME = 'tenant_jwks' as const
export const TENANT_S3_CREDENTIALS_CACHE_NAME = 'tenant_s3_credentials' as const

export type CacheName =
  | typeof JWT_CACHE_NAME
  | typeof TENANT_CONFIG_CACHE_NAME
  | typeof TENANT_JWKS_CACHE_NAME
  | typeof TENANT_S3_CREDENTIALS_CACHE_NAME
