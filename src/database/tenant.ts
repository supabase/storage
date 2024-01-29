import { getConfig } from '../config'
import { decrypt, verifyJWT } from '../auth'
import { knex } from './multitenant-db'
import { StorageBackendError } from '../storage'
import { JwtPayload } from 'jsonwebtoken'
import { PubSubAdapter } from '../pubsub'
import { RunMigrationsEvent } from '../queue/events/run-migrations'

interface TenantConfig {
  anonKey?: string
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

const {
  isMultitenant,
  dbServiceRole,
  serviceKey,
  jwtSecret,
  dbMigrationHash,
  dbDisableTenantMigrations,
} = getConfig()

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
 * List all tenants that have not run migrations yet
 */
export async function* listTenantsToMigrate() {
  let lastCursor = 0

  while (true) {
    const data = await knex
      .table('tenants')
      .select('id', 'cursor_id')
      .where('cursor_id', '>', lastCursor)
      .where((builder) => {
        builder
          .where('migrations_version', '=', dbMigrationHash || '')
          .orWhere('migrations_version', null)
      })
      .orderBy('cursor_id', 'desc')
      .limit(100)

    yield data.map((tenant) => tenant.id)

    if (data.length === 0) {
      break
    }

    lastCursor = data[data.length - 1].cursor_id
  }
}

/**
 * Runs migrations for all tenants
 */
export async function runMigrations() {
  if (dbDisableTenantMigrations) {
    return
  }
  const result = await knex.raw(`SELECT pg_try_advisory_lock(?);`, ['-8575985245963000605'])
  const lockAcquired = result.rows.shift()?.pg_try_advisory_lock || false

  if (!lockAcquired) {
    return
  }

  try {
    const tenants = listTenantsToMigrate()
    for await (const tenantBatch of tenants) {
      await Promise.allSettled(
        tenantBatch.map((tenant) => {
          return RunMigrationsEvent.send({
            tenantId: tenant,
            singletonKey: tenant,
            tenant: {
              ref: tenant,
            },
          })
        })
      )
    }
  } finally {
    try {
      await knex.raw(`SELECT pg_advisory_unlock(?);`, ['-8575985245963000605'])
    } catch (e) {}
  }
}

export function updateTenantMigrationVersion(tenantIds: string[]) {
  return knex
    .table('tenants')
    .whereIn('id', tenantIds)
    .update({ migrations_version: dbMigrationHash })
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
  tenantConfigCache.set(tenantId, config)
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
export async function getJwtSecret(tenantId: string): Promise<string> {
  if (isMultitenant) {
    const { jwtSecret } = await getTenantConfig(tenantId)
    return jwtSecret
  }
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
export async function listenForTenantUpdate(pubSub: PubSubAdapter): Promise<void> {
  await pubSub.subscribe(TENANTS_UPDATE_CHANNEL, (tenantId) => {
    tenantConfigCache.delete(tenantId)
  })
}
