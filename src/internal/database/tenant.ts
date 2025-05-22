import { getConfig, JwksConfig, JwksConfigKey, JwksConfigKeyOCT } from '../../config'
import { decrypt, verifyJWT } from '../auth'
import { multitenantKnex } from './multitenant-db'
import { JWTPayload } from 'jose'
import { PubSubAdapter } from '../pubsub'
import { createMutexByKey } from '../concurrency'
import { ERRORS } from '@internal/errors'
import { DBMigration, lastLocalMigrationName } from '@internal/database/migrations'
import { JWKSManager } from './jwks-manager'
import { JWKSManagerStoreKnex } from './jwks-manager/store-knex'
import {
  S3CredentialsManagerStoreKnex,
  S3CredentialsManager,
} from '@storage/protocols/s3/credentials-manager'
import { TenantConnection } from '@internal/database/connection'
import { logger, logSchema } from '@internal/monitoring'

type DBPoolMode = 'single_use' | 'recycled'

interface TenantConfig {
  anonKey?: string
  databaseUrl: string
  databasePoolUrl?: string
  databasePoolMode?: DBPoolMode
  maxConnections?: number
  fileSizeLimit: number
  features: Features
  jwtSecret: string
  jwks?: { keys: JwksConfigKey[] } | null
  serviceKey: string
  serviceKeyPayload: {
    role: string
  }
  migrationVersion?: keyof typeof DBMigration
  migrationStatus?: TenantMigrationStatus
  syncMigrationsDone?: boolean
  tracingMode?: string
  disableEvents?: string[]
}

export interface Features {
  imageTransformation: {
    enabled: boolean
    maxResolution?: number
  }
  s3Protocol: {
    enabled: boolean
  }
  purgeCache: {
    enabled: boolean
  }
}

export enum TenantMigrationStatus {
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
  FAILED_STALE = 'FAILED_STALE',
}

const { isMultitenant, dbServiceRole, serviceKeyAsync, jwtSecret, dbMigrationFreezeAt } =
  getConfig()

const tenantConfigCache = new Map<string, TenantConfig>()

const tenantMutex = createMutexByKey<TenantConfig>()

export const jwksManager = new JWKSManager(new JWKSManagerStoreKnex(multitenantKnex))

export const s3CredentialsManager = new S3CredentialsManager(
  new S3CredentialsManagerStoreKnex(multitenantKnex)
)

const singleTenantServiceKey:
  | {
      jwt: Promise<string>
      payload: { role: string } & JWTPayload
    }
  | undefined = !isMultitenant
  ? {
      jwt: serviceKeyAsync,
      payload: {
        role: dbServiceRole,
      },
    }
  : undefined

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
  if (!tenantId) {
    throw ERRORS.InvalidTenantId()
  }

  if (tenantConfigCache.has(tenantId)) {
    return tenantConfigCache.get(tenantId)!
  }

  return tenantMutex(tenantId, async () => {
    if (tenantConfigCache.has(tenantId)) {
      return tenantConfigCache.get(tenantId)!
    }

    const tenant = await multitenantKnex.table('tenants').first().where('id', tenantId)
    if (!tenant) {
      throw ERRORS.MissingTenantConfig(tenantId)
    }
    const {
      anon_key,
      database_url,
      database_pool_mode,
      file_size_limit,
      jwt_secret,
      jwks,
      service_key,
      feature_purge_cache,
      feature_image_transformation,
      feature_s3_protocol,
      image_transformation_max_resolution,
      database_pool_url,
      max_connections,
      migrations_version,
      migrations_status,
      tracing_mode,
      disable_events,
    } = tenant

    const serviceKey = decrypt(service_key)
    const jwtSecret = decrypt(jwt_secret)

    const serviceKeyPayload = await verifyJWT<{ role: string }>(serviceKey, jwtSecret)

    const config = {
      anonKey: decrypt(anon_key),
      databaseUrl: decrypt(database_url),
      databasePoolUrl: database_pool_url ? decrypt(database_pool_url) : undefined,
      databasePoolMode: database_pool_mode,
      fileSizeLimit: Number(file_size_limit),
      jwtSecret: jwtSecret,
      jwks,
      serviceKey: serviceKey,
      serviceKeyPayload,
      maxConnections: max_connections ? Number(max_connections) : undefined,
      features: {
        imageTransformation: {
          enabled: feature_image_transformation,
          maxResolution: image_transformation_max_resolution,
        },
        s3Protocol: {
          enabled: feature_s3_protocol,
        },
        purgeCache: {
          enabled: feature_purge_cache,
        },
      },
      migrationVersion: migrations_version,
      migrationStatus: migrations_status,
      migrationsRun: false,
      tracingMode: tracing_mode,
      disableEvents: disable_events,
    }
    tenantConfigCache.set(tenantId, config)

    return tenantConfigCache.get(tenantId)!
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
    jwt: await singleTenantServiceKey!.jwt,
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

enum Capability {
  LIST_V2 = 'list_V2',
}

/**
 * Get the capabilities for a specific tenant
 * @param tenantId
 */
export async function getTenantCapabilities(tenantId: string) {
  const capabilities: Record<Capability, boolean> = {
    [Capability.LIST_V2]: false,
  }

  let latestMigrationName = dbMigrationFreezeAt || (await lastLocalMigrationName())

  if (isMultitenant) {
    const { migrationVersion } = await getTenantConfig(tenantId)
    latestMigrationName = migrationVersion || 'initialmigration'
  }

  if (DBMigration[latestMigrationName] >= DBMigration['optimise-existing-functions']) {
    capabilities[Capability.LIST_V2] = true
  }

  return capabilities
}

/**
 * Get the jwt key from the tenant config
 * @param tenantId
 */
export async function getJwtSecret(
  tenantId: string
): Promise<{ secret: string; urlSigningKey: string | JwksConfigKeyOCT; jwks: JwksConfig }> {
  const { jwtJWKS } = getConfig()
  let secret = jwtSecret
  let jwks = jwtJWKS || { keys: [] }

  if (isMultitenant) {
    const config = await getTenantConfig(tenantId)
    const tenantJwks = await jwksManager.getJwksTenantConfig(tenantId)
    if (config.jwks?.keys) {
      // merge jwks from legacy jwks column if they exist
      tenantJwks.keys = [...tenantJwks.keys, ...config.jwks.keys]
    }
    secret = config.jwtSecret
    jwks = tenantJwks
  }

  const urlSigningKey = jwks.urlSigningKey || secret
  return { secret, urlSigningKey, jwks }
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
  await pubSub.subscribe(TENANTS_UPDATE_CHANNEL, onTenantConfigChange)
  await s3CredentialsManager.listenForTenantUpdate(pubSub)
  await jwksManager.listenForTenantUpdate(pubSub)
}

/**
 * Handles the tenant config change event
 * @param cacheKey
 */
async function onTenantConfigChange(cacheKey: string) {
  const oldConfig = tenantConfigCache.get(cacheKey)
  tenantConfigCache.delete(cacheKey)

  if (!oldConfig) {
    return
  }

  try {
    const newConfig = await getTenantConfig(cacheKey)

    if (newConfig.databasePoolMode === 'single_use' && oldConfig.databasePoolMode === 'recycled') {
      // if the pool mode changed to single use, we need destroy the current pool
      return TenantConnection.poolManager.destroy(cacheKey).catch((e) => {
        logSchema.error(logger, 'Error destroying the pool', {
          type: 'pool',
          error: e as Error,
          project: cacheKey,
        })
      })
    }

    // Rebalance the pool if the max connections changed
    if (newConfig.maxConnections && newConfig.maxConnections !== oldConfig.maxConnections) {
      TenantConnection.poolManager.rebalance(cacheKey, {
        clusterSize: newConfig.maxConnections,
      })
    }
  } catch {
    // if the tenant config is not found, we can ignore it
    // this can happen if the tenant was deleted
    // or if the tenant was updated and the cache was invalidated
    // before we could get the new config
  }
}
