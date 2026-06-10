import {
  createLruCache,
  DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS,
  TENANT_CONFIG_CACHE_NAME,
} from '@internal/cache'
import { lastLocalMigrationName } from '@internal/database/migrations/files'
import { ERRORS } from '@internal/errors'
import { logger, logSchema } from '@internal/monitoring'
import {
  S3CredentialsManager,
  S3CredentialsManagerStorePg,
} from '@storage/protocols/s3/credentials'
import { JWTPayload } from 'jose'
import objectSizeOf from 'object-sizeof'
import {
  type DatabasePoolMode,
  getConfig,
  JwksConfig,
  JwksConfigKey,
  JwksConfigKeyOCT,
  normalizeDatabasePoolModeForRead,
} from '../../config'
import { decrypt } from '../auth'
import { JWKSManager, JWKSManagerStorePg } from '../auth/jwks'
import { createMutexByKey } from '../concurrency'
import { isStringMessage, PubSubAdapter } from '../pubsub'
import { DBMigration } from './migrations/types'
import { multitenantPgExecutor } from './multitenant-pg'
import { PgTenantConnection } from './pg-connection'
import { TenantConfigStorePg } from './tenant-store-pg'

interface TenantConfig {
  anonKey?: string
  databaseUrl: string
  databasePoolUrl?: string
  databasePoolMode?: DatabasePoolMode | null
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

type GetTenantConfigOptions = {
  recordMetrics?: boolean
}

type LegacyJwksConfig = NonNullable<TenantConfig['jwks']>

export interface Features {
  vectorBuckets: {
    enabled: boolean
    maxBuckets: number
    maxIndexes: number
  }
  imageTransformation: {
    enabled: boolean
    maxResolution?: number | null
  }
  s3Protocol: {
    enabled: boolean
  }
  purgeCache: {
    enabled: boolean
  }
  icebergCatalog: {
    enabled: boolean
    maxNamespaces: number
    maxTables: number
    maxCatalogs: number
  }
}

export enum TenantMigrationStatus {
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
  FAILED_STALE = 'FAILED_STALE',
}

const {
  isMultitenant,
  dbServiceRole,
  dbMigrationFreezeAt,
  icebergEnabled,
  vectorEnabled,
  databaseMaxConnections,
  databasePoolMode,
} = getConfig()

export const TENANT_CONFIG_CACHE_MAX_ITEMS = 16384
export const TENANT_CONFIG_CACHE_MAX_SIZE_BYTES = 1024 * 1024 * 50 // 50 MiB
export const TENANT_CONFIG_CACHE_TTL_MS = 1000 * 60 * 60 // 1h

const tenantConfigCache = createLruCache<string, TenantConfig>(TENANT_CONFIG_CACHE_NAME, {
  max: TENANT_CONFIG_CACHE_MAX_ITEMS,
  maxSize: TENANT_CONFIG_CACHE_MAX_SIZE_BYTES,
  ttl: TENANT_CONFIG_CACHE_TTL_MS,
  sizeCalculation: (value) => objectSizeOf(value),
  updateAgeOnGet: true,
  allowStale: false,
  purgeStaleIntervalMs: DEFAULT_CACHE_PURGE_STALE_INTERVAL_MS,
})

const tenantMutex = createMutexByKey<TenantConfig>()

export const jwksManager = new JWKSManager(new JWKSManagerStorePg(multitenantPgExecutor))

export const s3CredentialsManager = new S3CredentialsManager(
  new S3CredentialsManagerStorePg(multitenantPgExecutor)
)

const tenantConfigStorePg = new TenantConfigStorePg(multitenantPgExecutor)

// Cache merged legacy JWKS objects by the active + legacy config object identities
// so repeated reads reuse a stable merged object without mutating either input.
const mergedTenantJwksCache = new WeakMap<JwksConfig, WeakMap<LegacyJwksConfig, JwksConfig>>()

function getSingleTenantJwtConfig(): {
  secret: string
  jwks: JwksConfig
} {
  const { jwtSecret, jwtJWKS } = getConfig()
  const jwks = (jwtJWKS || { keys: [] }) as JwksConfig

  return {
    secret: jwtSecret,
    jwks,
  }
}

async function getSingleTenantServiceKeyUser(): Promise<{
  jwt: string
  payload: { role: string } & JWTPayload
}> {
  const { serviceKeyAsync, dbServiceRole } = getConfig()

  return {
    jwt: await serviceKeyAsync,
    payload: {
      role: dbServiceRole,
    },
  }
}

function mergeTenantJwksWithLegacyKeys(
  tenantJwks: JwksConfig,
  legacyJwks: LegacyJwksConfig
): JwksConfig {
  let mergedByLegacyJwks = mergedTenantJwksCache.get(tenantJwks)

  if (!mergedByLegacyJwks) {
    mergedByLegacyJwks = new WeakMap<LegacyJwksConfig, JwksConfig>()
    mergedTenantJwksCache.set(tenantJwks, mergedByLegacyJwks)
  }

  const cachedMergedJwks = mergedByLegacyJwks.get(legacyJwks)
  if (cachedMergedJwks) {
    return cachedMergedJwks
  }

  const mergedJwks: JwksConfig = {
    ...tenantJwks,
    keys: [...tenantJwks.keys, ...legacyJwks.keys],
  }

  mergedByLegacyJwks.set(legacyJwks, mergedJwks)

  return mergedJwks
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
export async function getTenantConfig(
  tenantId: string,
  options?: GetTenantConfigOptions
): Promise<TenantConfig> {
  if (!tenantId) {
    throw ERRORS.InvalidTenantId()
  }

  const cachedConfig = tenantConfigCache.get(tenantId, {
    recordMetrics: options?.recordMetrics,
  })
  if (cachedConfig !== undefined) {
    return cachedConfig
  }

  return tenantMutex(tenantId, async () => {
    const cachedConfig = tenantConfigCache.get(tenantId, { recordMetrics: false })
    if (cachedConfig !== undefined) {
      return cachedConfig
    }

    const tenant = await getTenantConfigRow(tenantId)

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
      feature_iceberg_catalog,
      feature_iceberg_catalog_max_catalogs,
      feature_iceberg_catalog_max_namespaces,
      feature_iceberg_catalog_max_tables,
      feature_vector_buckets,
      feature_vector_buckets_max_buckets,
      feature_vector_buckets_max_indexes,
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

    const config = {
      anonKey: decrypt(anon_key),
      databaseUrl: decrypt(database_url),
      databasePoolUrl: database_pool_url ? decrypt(database_pool_url) : undefined,
      databasePoolMode: normalizeDatabasePoolModeForRead(database_pool_mode, databasePoolMode),
      fileSizeLimit: Number(file_size_limit),
      jwtSecret,
      jwks: jwks ? { keys: jwks.keys ?? [] } : jwks,
      serviceKey,
      serviceKeyPayload: { role: dbServiceRole },
      maxConnections: max_connections ? Number(max_connections) : undefined,
      features: {
        imageTransformation: {
          enabled: Boolean(feature_image_transformation),
          maxResolution: image_transformation_max_resolution,
        },
        s3Protocol: {
          enabled: Boolean(feature_s3_protocol),
        },
        purgeCache: {
          enabled: Boolean(feature_purge_cache),
        },
        icebergCatalog: {
          enabled: Boolean(icebergEnabled || feature_iceberg_catalog),
          maxNamespaces: feature_iceberg_catalog_max_namespaces ?? 0,
          maxTables: feature_iceberg_catalog_max_tables ?? 0,
          maxCatalogs: feature_iceberg_catalog_max_catalogs ?? 0,
        },
        vectorBuckets: {
          enabled: Boolean(vectorEnabled || feature_vector_buckets),
          maxBuckets: feature_vector_buckets_max_buckets ?? 0,
          maxIndexes: feature_vector_buckets_max_indexes ?? 0,
        },
      },
      migrationVersion: migrations_version as keyof typeof DBMigration | undefined,
      migrationStatus: migrations_status as TenantMigrationStatus | undefined,
      migrationsRun: false,
      tracingMode: tracing_mode ?? undefined,
      disableEvents: disable_events ?? undefined,
    }
    tenantConfigCache.set(tenantId, config)

    return config
  })
}

function getTenantConfigRow(tenantId: string) {
  return tenantConfigStorePg.findById(tenantId)
}

export async function getServiceKeyUser(tenantId: string) {
  if (isMultitenant) {
    const tenant = await getTenantConfig(tenantId)

    return {
      jwt: tenant.serviceKey,
      payload: tenant.serviceKeyPayload,
    }
  }

  return getSingleTenantServiceKeyUser()
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
  ICEBERG_CATALOG = 'iceberg_catalog',
}

/**
 * Get the capabilities for a specific tenant
 * @param tenantId
 */
export async function getTenantCapabilities(tenantId: string) {
  const capabilities: Record<Capability, boolean> = {
    [Capability.LIST_V2]: false,
    [Capability.ICEBERG_CATALOG]: false,
  }

  let latestMigrationName = dbMigrationFreezeAt || (await lastLocalMigrationName())

  if (isMultitenant) {
    const { migrationVersion } = await getTenantConfig(tenantId)
    latestMigrationName = migrationVersion || 'initialmigration'
  }

  if (DBMigration[latestMigrationName] >= DBMigration['optimise-existing-functions']) {
    capabilities[Capability.LIST_V2] = true
  }

  if (DBMigration[latestMigrationName] >= DBMigration['iceberg-catalog-flag-on-buckets']) {
    capabilities[Capability.ICEBERG_CATALOG] = true
  }

  return capabilities
}

/**
 * Check if a tenant has a specific feature enabled
 *
 * @param tenantId
 * @param feature
 */
export async function tenantHasFeature(
  tenantId: string,
  feature: keyof Features
): Promise<boolean> {
  if (!isMultitenant) {
    return true // single tenant always has all features
  }

  const { features } = await getTenantConfig(tenantId)
  return features ? features[feature].enabled : false
}

/**
 * Get the jwt key from the tenant config
 * @param tenantId
 */
export async function getJwtSecret(tenantId: string): Promise<{
  secret: string
  urlSigningKey: string | JwksConfigKeyOCT
  jwks: JwksConfig
}> {
  let { secret, jwks } = getSingleTenantJwtConfig()

  if (isMultitenant) {
    const config = await getTenantConfig(tenantId)
    const tenantJwks = await jwksManager.getJwksTenantConfig(tenantId)
    secret = config.jwtSecret
    jwks = config.jwks?.keys ? mergeTenantJwksWithLegacyKeys(tenantJwks, config.jwks) : tenantJwks
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
  await pubSub.subscribe(TENANTS_UPDATE_CHANNEL, (cacheKey) => {
    if (!isStringMessage(cacheKey)) {
      return
    }

    void onTenantConfigChange(cacheKey)
  })
  await s3CredentialsManager.listenForTenantUpdate(pubSub)
  await jwksManager.listenForTenantUpdate(pubSub)
}

/**
 * Handles the tenant config change event
 * @param cacheKey
 */
export async function onTenantConfigChange(cacheKey: string) {
  const oldConfig = tenantConfigCache.get(cacheKey, { recordMetrics: false })
  tenantConfigCache.delete(cacheKey)

  if (!oldConfig) {
    return
  }

  try {
    const newConfig = await getTenantConfig(cacheKey, { recordMetrics: false })

    // 1. Pool mode flipped recycled → single_use: tear down the long-lived pool.
    if (newConfig.databasePoolMode === 'single_use' && oldConfig.databasePoolMode === 'recycled') {
      return destroyTenantPool(cacheKey)
    }

    // 2. DB endpoint changed: the cached pool's connection string is stale, and
    //    so are its open sockets. Hard destroy so the next request rebuilds
    //    against the new endpoint.
    if (
      newConfig.databaseUrl !== oldConfig.databaseUrl ||
      newConfig.databasePoolUrl !== oldConfig.databasePoolUrl
    ) {
      return destroyTenantPool(cacheKey)
    }

    // 3. Max connections changed: endpoint is fine, only the budget moved.
    //    Rebalance the cached pg pool in place so new acquire attempts observe
    //    the new budget while in-flight queries keep their checked-out clients.
    if (
      normalizeMaxConnections(newConfig.maxConnections) !==
      normalizeMaxConnections(oldConfig.maxConnections)
    ) {
      PgTenantConnection.poolManager.rebalance(cacheKey, {
        maxConnections: resolveMaxConnections(newConfig.maxConnections),
      })
    }
  } catch {
    // if the tenant config is not found, we can ignore it
    // this can happen if the tenant was deleted
    // or if the tenant was updated and the cache was invalidated
    // before we could get the new config
  }
}

function normalizeMaxConnections(maxConnections: number | null | undefined): number | null {
  return maxConnections ?? null
}

function resolveMaxConnections(maxConnections: number | null | undefined): number {
  return maxConnections ?? databaseMaxConnections
}

async function destroyTenantPool(cacheKey: string): Promise<void> {
  await Promise.allSettled([PgTenantConnection.poolManager.destroy(cacheKey)]).then((results) => {
    for (const result of results) {
      if (result.status === 'rejected') {
        logSchema.error(logger, 'Error destroying the pool', {
          type: 'pool',
          error: result.reason as Error,
          project: cacheKey,
        })
      }
    }
  })
}
