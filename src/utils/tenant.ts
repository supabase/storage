import pLimit from 'p-limit'
import { decrypt } from './crypto'
import { runMigrationsOnTenant } from './migrate'
import { knex } from './multitenant-db'

interface TenantConfig {
  anonKey: string
  databaseUrl: string
  jwtSecret: string
  serviceKey: string
}

const tenantConfigCache = new Map<string, TenantConfig>()

export async function cacheTenantConfigAndRunMigrations(
  tenantId: string,
  config: TenantConfig,
  logOnError = false
): Promise<void> {
  try {
    await runMigrationsOnTenant(config.databaseUrl)
  } catch (error) {
    if (logOnError) {
      console.error('Migration error:', error.message)
      return
    } else {
      throw error
    }
  }
  tenantConfigCache.set(tenantId, config)
}

export function deleteTenantConfig(tenantId: string): void {
  tenantConfigCache.delete(tenantId)
}

export async function cacheTenantConfigsFromDbAndRunMigrations(): Promise<void> {
  const tenants = await knex('tenants').select()
  const limit = pLimit(100)
  await Promise.all(
    tenants.map(({ id, anon_key, database_url, jwt_secret, service_key }) =>
      limit(() =>
        cacheTenantConfigAndRunMigrations(
          id,
          {
            anonKey: decrypt(anon_key),
            databaseUrl: decrypt(database_url),
            jwtSecret: decrypt(jwt_secret),
            serviceKey: decrypt(service_key),
          },
          true
        )
      )
    )
  )
}

async function getTenantConfig(tenantId: string): Promise<TenantConfig> {
  if (tenantConfigCache.has(tenantId)) {
    return tenantConfigCache.get(tenantId) as TenantConfig
  }
  const tenant = await knex('tenants').first().where('id', tenantId)
  if (!tenant) {
    throw new Error(`Tenant config for ${tenantId} not found`)
  }
  const { anon_key, database_url, jwt_secret, service_key } = tenant
  const config = {
    anonKey: decrypt(anon_key),
    databaseUrl: decrypt(database_url),
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
  const tenant = await knex('tenants').first('file_size_limit').where('id', tenantId)
  if (!tenant) {
    throw new Error(`Tenant config for ${tenantId} not found`)
  }
  return Number(tenant.file_size_limit)
}
