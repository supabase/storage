import { runMigrationsOnTenant } from './migrate'
import { pool } from './multitenant-db'

interface TenantConfig {
  anonKey: string
  databaseUrl: string
  jwtSecret: string
  serviceKey: string
}

const tenantConfigCache: {
  [tenantId: string]: TenantConfig
} = {}

export async function cacheTenantConfigAndRunMigrations(
  tenantId: string,
  config: TenantConfig
): Promise<void> {
  await runMigrationsOnTenant(config.databaseUrl)
  tenantConfigCache[tenantId] = config
}

export function deleteTenantConfig(tenantId: string): void {
  delete tenantConfigCache[tenantId]
}

export async function cacheTenantConfigsFromDbAndRunMigrations(): Promise<void> {
  const result = await pool.query(
    `
    SELECT
      id,
      config
    FROM
      storage.tenants
    `
  )
  for (const tenant of result.rows) {
    const { id, config } = tenant
    await cacheTenantConfigAndRunMigrations(id, config)
  }
}

async function getTenantConfig(tenantId: string): Promise<TenantConfig> {
  if (tenantConfigCache[tenantId]) {
    return tenantConfigCache[tenantId]
  }
  const result = await pool.query(
    `
    SELECT
      config
    FROM
      storage.tenants
    WHERE
      id = $1
    `,
    [tenantId]
  )
  if (result.rows.length === 0) {
    throw new Error(`Tenant config for ${tenantId} not found`)
  }
  const { config } = result.rows[0]
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
