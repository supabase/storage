import { getConfig, JwksConfig, JwksConfigKey, JwksConfigKeyOCT } from '../../config'
import { decrypt, verifyJWT } from '../auth'
import { multitenantKnex } from './multitenant-db'
import { JwtPayload } from 'jsonwebtoken'
import { PubSubAdapter } from '../pubsub'
import { createMutexByKey } from '../concurrency'
import { ERRORS } from '@internal/errors'
import { DBMigration } from '@internal/database/migrations'
import { JWKSManager } from './jwks-manager'
import { JWKSManagerStoreKnex } from './jwks-manager/store-knex'
import { S3CredentialsManagerStoreKnex } from '../../storage/protocols/s3/credentials-manager/store-knex'
import { S3CredentialsManager } from '../../storage/protocols/s3/credentials-manager'

interface TenantConfig {
  anonKey?: string
  databaseUrl: string
  databasePoolUrl?: string
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

const { isMultitenant, dbServiceRole, serviceKey, jwtSecret } = getConfig()

const tenantConfigCache = new Map<string, TenantConfig>()

const tenantMutex = createMutexByKey<TenantConfig>()

export const jwksManager = new JWKSManager(new JWKSManagerStoreKnex(multitenantKnex))
export const s3CredentialsManager = new S3CredentialsManager(
  new S3CredentialsManagerStoreKnex(multitenantKnex)
)

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
  await pubSub.subscribe(TENANTS_UPDATE_CHANNEL, (cacheKey) => {
    tenantConfigCache.delete(cacheKey)
  })
  await s3CredentialsManager.listenForTenantUpdate(pubSub)
  await jwksManager.listenForTenantUpdate(pubSub)
}
