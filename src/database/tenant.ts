import { getConfig } from '../config'
import { decrypt, verifyJWT } from '../auth'
import { multitenantKnex } from './multitenant-db'
import { StorageBackendError } from '../storage'
import { JwtPayload } from 'jsonwebtoken'
import { PubSubAdapter } from '../pubsub'
import { lastMigrationName } from './migrations'
import { createMutexByKey } from '../concurrency'

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
  migrationVersion?: string
  migrationStatus?: TenantMigrationStatus
  syncMigrationsDone?: boolean
}

export interface Features {
  imageTransformation: {
    enabled: boolean
  }
}

export enum TenantMigrationStatus {
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
  FAILED_STALE = 'FAILED_STALE',
}

const { isMultitenant, dbServiceRole, serviceKey, jwtSecret, jwtJWKS } = getConfig()

const tenantConfigCache = new Map<string, TenantConfig>()
const tenantMutex = createMutexByKey()

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
 * List all tenants that needs to have the migrations run
 */
export async function* listTenantsToMigrate(signal: AbortSignal) {
  let lastCursor = 0

  while (true) {
    if (signal.aborted) {
      break
    }

    const migrationVersion = await lastMigrationName()

    const data = await multitenantKnex
      .table<{ id: string; cursor_id: number }>('tenants')
      .select('id', 'cursor_id')
      .where('cursor_id', '>', lastCursor)
      .where((builder) => {
        builder
          .where((whereBuilder) => {
            whereBuilder
              .where('migrations_version', '!=', migrationVersion)
              .whereNotIn('migrations_status', [
                TenantMigrationStatus.FAILED,
                TenantMigrationStatus.FAILED_STALE,
              ])
          })
          .orWhere('migrations_status', null)
      })
      .orderBy('cursor_id', 'asc')
      .limit(200)

    if (data.length === 0) {
      break
    }

    lastCursor = data[data.length - 1].cursor_id
    yield data.map((tenant) => tenant.id)
  }
}

/**
 * Update tenant migration version and status
 * @param tenantId
 * @param options
 */
export async function updateTenantMigrationsState(
  tenantId: string,
  options?: { state: TenantMigrationStatus }
) {
  const migrationVersion = await lastMigrationName()
  const state = options?.state || TenantMigrationStatus.COMPLETED
  return multitenantKnex
    .table('tenants')
    .where('id', tenantId)
    .update({
      migrations_version: [
        TenantMigrationStatus.FAILED,
        TenantMigrationStatus.FAILED_STALE,
      ].includes(state)
        ? undefined
        : migrationVersion,
      migrations_status: state,
    })
}

/**
 * Determine if a tenant has the migrations up to date
 * @param tenantId
 */
export async function areMigrationsUpToDate(tenantId: string) {
  const latestMigrationVersion = await lastMigrationName()
  const tenant = await getTenantConfig(tenantId)

  return (
    latestMigrationVersion === tenant.migrationVersion &&
    tenant.migrationStatus === TenantMigrationStatus.COMPLETED
  )
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

  return tenantMutex(tenantId, async () => {
    if (tenantConfigCache.has(tenantId)) {
      return tenantConfigCache.get(tenantId) as TenantConfig
    }

    const tenant = await multitenantKnex('tenants').first().where('id', tenantId)
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
      migrations_version,
      migrations_status,
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
      jwks,
      serviceKey: serviceKey,
      serviceKeyPayload,
      maxConnections: max_connections ? Number(max_connections) : undefined,
      features: {
        imageTransformation: {
          enabled: feature_image_transformation,
        },
      },
      migrationVersion: migrations_version,
      migrationStatus: migrations_status,
      migrationsRun: false,
    }
    tenantConfigCache.set(tenantId, config)

    return tenantConfigCache.get(tenantId)
  })
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
