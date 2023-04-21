import createSubscriber from 'pg-listen'
import { getConfig } from '../config'
import { decrypt, verifyJWT } from '../auth'
import { runMigrationsOnTenant } from './migrate'
import { knex } from './multitenant-db'
import { StorageBackendError } from '../storage'
import { JwtPayload } from 'jsonwebtoken'

interface TenantConfig {
  anonKey: string
  databaseUrl: string
  databasePoolUrl?: string
  maxConnections?: number
  fileSizeLimit: number
  features: Features
  jwtSecret: string
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

const { multitenantDatabaseUrl, isMultitenant, serviceKey, jwtSecret } = getConfig()

const tenantConfigCache = new Map<string, TenantConfig>()

let singleTenantServiceKeyPayload: ({ role: string } & JwtPayload) | undefined = undefined

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
    await runMigrationsOnTenant(databaseUrl)
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
    service_key,
    feature_image_transformation,
    database_pool_url,
    max_connections,
  } = tenant

  const serviceKey = decrypt(service_key)
  const jwtSecret = decrypt(jwt_secret)

  const serviceKeyPayload = await verifyJWT<{ role: string }>(serviceKey, jwtSecret)

  const config = {
    anonKey: decrypt(anon_key),
    databaseUrl: decrypt(database_url),
    databasePoolUrl: database_pool_url ? decrypt(database_pool_url) : undefined,
    fileSizeLimit: Number(file_size_limit),
    jwtSecret: jwtSecret,
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

/**
 * Get the anon key from the tenant config
 * @param tenantId
 */
export async function getAnonKey(tenantId: string): Promise<string> {
  const { anonKey } = await getTenantConfig(tenantId)
  return anonKey
}

export async function getServiceKeyUser(tenantId: string) {
  let serviceKeyPayload: { role?: string } | undefined
  let tenantJwtSecret = jwtSecret
  let tenantServiceKey = serviceKey

  if (isMultitenant) {
    const tenant = await getTenantConfig(tenantId)
    serviceKeyPayload = tenant.serviceKeyPayload
    tenantJwtSecret = tenant.jwtSecret
    tenantServiceKey = tenant.serviceKey
  } else {
    serviceKeyPayload = await getSingleTenantServiceKeyPayload()
  }

  return {
    jwt: tenantServiceKey,
    payload: serviceKeyPayload,
    jwtSecret: tenantJwtSecret,
  }
}

export async function getSingleTenantServiceKeyPayload() {
  if (singleTenantServiceKeyPayload) {
    return singleTenantServiceKeyPayload
  }

  singleTenantServiceKeyPayload = await verifyJWT(serviceKey, jwtSecret)

  return singleTenantServiceKeyPayload
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
export async function getJwtSecret(tenantId: string): Promise<string> {
  const { jwtSecret } = await getTenantConfig(tenantId)
  return jwtSecret
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
export async function listenForTenantUpdate(): Promise<void> {
  const subscriber = createSubscriber({ connectionString: multitenantDatabaseUrl })

  subscriber.notifications.on(TENANTS_UPDATE_CHANNEL, (tenantId) => {
    tenantConfigCache.delete(tenantId)
  })

  subscriber.events.on('error', (error) => {
    console.error('Postgres notification subscription error:', error)
  })

  await subscriber.connect()
  await subscriber.listenTo(TENANTS_UPDATE_CHANNEL)
}

async function cacheTenantConfigAndRunMigrations(
  tenantId: string,
  config: TenantConfig,
  logOnError = false
): Promise<void> {
  await runMigrations(tenantId, config.databaseUrl, logOnError)
  tenantConfigCache.set(tenantId, config)
}
