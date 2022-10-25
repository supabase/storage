import createSubscriber from 'pg-listen'
import { getConfig } from '../config'
import { decrypt } from '../auth'
import { runMigrationsOnTenant } from './migrate'
import { knex } from './multitenant-db'
import { StorageBackendError } from '../storage'

interface TenantConfig {
  anonKey: string
  databaseUrl: string
  fileSizeLimit: number
  jwtSecret: string
  serviceKey: string
}

const { multitenantDatabaseUrl } = getConfig()

const tenantConfigCache = new Map<string, TenantConfig>()

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
  const { anon_key, database_url, file_size_limit, jwt_secret, service_key } = tenant
  const config = {
    anonKey: decrypt(anon_key),
    databaseUrl: decrypt(database_url),
    fileSizeLimit: Number(file_size_limit),
    jwtSecret: decrypt(jwt_secret),
    serviceKey: decrypt(service_key),
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
