import createSubscriber from 'pg-listen'
import { getConfig } from './config'
import { decrypt } from './crypto'
import { runMigrationsOnTenant } from './migrate'
import { knex } from './multitenant-db'

interface TenantConfig {
  anonKey: string
  databaseUrl: string
  fileSizeLimit: number
  jwtSecret: string
  serviceKey: string
}

const { multitenantDatabaseUrl } = getConfig()

const tenantConfigCache = new Map<string, TenantConfig>()

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

export async function cacheTenantConfigAndRunMigrations(
  tenantId: string,
  config: TenantConfig,
  logOnError = false
): Promise<void> {
  await runMigrations(tenantId, config.databaseUrl, logOnError)
  tenantConfigCache.set(tenantId, config)
}

export function deleteTenantConfig(tenantId: string): void {
  tenantConfigCache.delete(tenantId)
}

async function getTenantConfig(tenantId: string): Promise<TenantConfig> {
  if (tenantConfigCache.has(tenantId)) {
    return tenantConfigCache.get(tenantId) as TenantConfig
  }
  const tenant = await knex('tenants').first().where('id', tenantId)
  if (!tenant) {
    throw new Error(`Tenant config for ${tenantId} not found`)
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

export async function getAnonKey(tenantId: string): Promise<string> {
  const { anonKey } = await getTenantConfig(tenantId)
  return anonKey
}

export async function getServiceKey(tenantId: string): Promise<string> {
  const { serviceKey } = await getTenantConfig(tenantId)
  return serviceKey
}

export async function getJwtSecret(tenantId: string): Promise<string> {
  const { jwtSecret } = await getTenantConfig(tenantId)
  return jwtSecret
}

export async function getFileSizeLimit(tenantId: string): Promise<number> {
  const { fileSizeLimit } = await getTenantConfig(tenantId)
  return fileSizeLimit
}

const TENANTS_UPDATE_CHANNEL = 'tenants_update'

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
