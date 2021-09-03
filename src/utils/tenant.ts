import pLimit from 'p-limit'
import { decrypt } from './crypto'
import { runMigrationsOnTenant } from './migrate'
import { pool } from './multitenant-db'

interface TenantConfig {
  anonKey: string
  databaseUrl: string
  jwtSecret: string
  serviceKey: string
}

const tenantConfigCache = new Map<string, TenantConfig>()

export async function cacheTenantConfigAndRunMigrations(
  tenantId: string,
  config: TenantConfig
): Promise<void> {
  await runMigrationsOnTenant(config.databaseUrl)
  tenantConfigCache.set(tenantId, config)
}

export function deleteTenantConfig(tenantId: string): void {
  tenantConfigCache.delete(tenantId)
}

export async function cacheTenantConfigsFromDbAndRunMigrations(): Promise<void> {
  const result = await pool.query(
    `
    SELECT
      id,
      anon_key,
      database_url,
      jwt_secret,
      service_key
    FROM
      tenants
    `
  )
  const limit = pLimit(100)
  await Promise.all(
    result.rows.map(({ id, anon_key, database_url, jwt_secret, service_key }) =>
      limit(() =>
        cacheTenantConfigAndRunMigrations(id, {
          anonKey: anon_key,
          databaseUrl: decrypt(database_url),
          jwtSecret: jwt_secret,
          serviceKey: service_key,
        })
      )
    )
  )
}

async function getTenantConfig(tenantId: string): Promise<TenantConfig> {
  if (tenantConfigCache.has(tenantId)) {
    return tenantConfigCache.get(tenantId) as TenantConfig
  }
  const result = await pool.query(
    `
    SELECT
      anon_key,
      database_url,
      jwt_secret,
      service_key
    FROM
      tenants
    WHERE
      id = $1
    `,
    [tenantId]
  )
  if (result.rows.length === 0) {
    throw new Error(`Tenant config for ${tenantId} not found`)
  }
  const { anon_key, database_url, jwt_secret, service_key } = result.rows[0]
  const config = {
    anonKey: anon_key,
    databaseUrl: decrypt(database_url),
    jwtSecret: jwt_secret,
    serviceKey: service_key,
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
