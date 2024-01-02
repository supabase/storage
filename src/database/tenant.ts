import { getConfig } from '../config'
import { decrypt, verifyJWT } from '../auth'
import { runMigrationsOnTenant } from './migrate'
import { knex } from './multitenant-db'
import { StorageBackendError } from '../storage'
import { JwtPayload } from 'jsonwebtoken'
import { PubSubAdapter } from '../pubsub'

interface TenantConfig {
  anonKey?: string
  databaseUrl: string
  databasePoolUrl?: string
  maxConnections?: number
  fileSizeLimit: number
  features: Features
  jwtSecret: string
  jwks?: {
    keys: {
      kid?: string
      kty: string
      // other fields are present too but are dependent on kid, alg and other fields, cast to unknown to access those
    }[]
  } | null
  serviceKey: string
  serviceKeyPayload: {
    role: string
  }
}

export interface Features {
  imageTransformation: {
    enabled: boolean
  }
}

const { isMultitenant, dbServiceRole, serviceKey, jwtSecret, jwtJWKS } = getConfig()

const tenantConfigCache = new Map<string, TenantConfig>()

const singleTenantServiceKey:
  | {
      jwt: string
      payload: { role: string } & JwtPayload
    }
  | undefined = !isMultitenant
  ? {
      jwt: serviceKey,
      payload: {
        role: dbServiceRole,
      },
    }
  : undefined

/**
 * Runs migrations in a specific tenant
 * @param tenantId
 * @param databaseUrl
 * @param logOnError
 */
export async function runMigrations(
  tenantId: string,
  databaseUrl: string,
  logOnError = false
): Promise<void> {
  try {
    await runMigrationsOnTenant(databaseUrl, tenantId)
    console.log(`${tenantId} migrations ran successfully`)
  } catch (error: any) {
    if (logOnError) {
      console.error(`${tenantId} migration error:`, error.message)
      return
    } else {
      throw error
    }
  }
}

/**
 * Deletes tenants config from the in-memory cache
 * @param tenantId
 */
export function deleteTenantConfig(tenantId: string): void {
  tenantConfigCache.delete(tenantId)
}

/**
 * Queries the tenant config from the multi-tenant database and stores them in a local cache
 * for quick subsequent access
 * @param tenantId
 */
export async function getTenantConfig(tenantId: string): Promise<TenantConfig> {
  if (tenantConfigCache.has(tenantId)) {
    return tenantConfigCache.get(tenantId) as TenantConfig
  }
  const tenant = await knex('tenants').first().where('id', tenantId)
  if (!tenant) {
    throw new StorageBackendError(
      'Missing Tenant config',
      400,
      `Tenant config for ${tenantId} not found`
    )
  }
  const {
    anon_key,
    database_url,
    file_size_limit,
    jwt_secret,
    jwks,
    service_key,
    feature_image_transformation,
    database_pool_url,
    max_connections,
  } = tenant

  const serviceKey = decrypt(service_key)
  const jwtSecret = decrypt(jwt_secret)

  const serviceKeyPayload = await verifyJWT<{ role: string }>(serviceKey, jwtSecret)

  const config: TenantConfig = {
    anonKey: decrypt(anon_key),
    databaseUrl: decrypt(database_url),
    databasePoolUrl: database_pool_url ? decrypt(database_pool_url) : undefined,
    fileSizeLimit: Number(file_size_limit),
    jwtSecret: jwtSecret,
    jwks,
    serviceKey: serviceKey,
    serviceKeyPayload,
    maxConnections: max_connections ? Number(max_connections) : undefined,
    features: {
      imageTransformation: {
        enabled: feature_image_transformation,
      },
    },
  }

  await cacheTenantConfigAndRunMigrations(tenantId, config)
  return config
}

export async function getServiceKeyUser(tenantId: string) {
  if (isMultitenant) {
    const tenant = await getTenantConfig(tenantId)

    return {
      jwt: tenant.serviceKey,
      payload: tenant.serviceKeyPayload,
      jwtSecret: tenant.jwtSecret,
    }
  }

  return {
    jwt: singleTenantServiceKey!.jwt,
    payload: singleTenantServiceKey!.payload,
    jwtSecret: jwtSecret,
  }
}

/**
 * Get the service key from the tenant config
 * @param tenantId
 */
export async function getServiceKey(tenantId: string): Promise<string> {
  const { serviceKey } = await getTenantConfig(tenantId)
  return serviceKey
}

/**
 * Get the jwt key from the tenant config
 * @param tenantId
 */
export async function getJwtSecret(
  tenantId: string
): Promise<{ secret: string; jwks: TenantConfig['jwks'] | null }> {
  if (isMultitenant) {
    const { jwtSecret, jwks } = await getTenantConfig(tenantId)
    return { secret: jwtSecret, jwks: jwks || null }
  }

  return { secret: jwtSecret, jwks: jwtJWKS || null }
}

/**
 * Get the file size limit from the tenant config
 * @param tenantId
 */
export async function getFileSizeLimit(tenantId: string): Promise<number> {
  const { fileSizeLimit } = await getTenantConfig(tenantId)
  return fileSizeLimit
}

/**
 * Get features flags config for a specific tenant
 * @param tenantId
 */
export async function getFeatures(tenantId: string): Promise<Features> {
  const { features } = await getTenantConfig(tenantId)
  return features
}

const TENANTS_UPDATE_CHANNEL = 'tenants_update'

/**
 * Keeps the in memory config cache up to date
 */
export async function listenForTenantUpdate(pubSub: PubSubAdapter): Promise<void> {
  await pubSub.subscribe(TENANTS_UPDATE_CHANNEL, (tenantId) => {
    tenantConfigCache.delete(tenantId)
  })
}

async function cacheTenantConfigAndRunMigrations(
  tenantId: string,
  config: TenantConfig,
  logOnError = false
): Promise<void> {
  await runMigrations(tenantId, config.databaseUrl, logOnError)
  tenantConfigCache.set(tenantId, config)
}
