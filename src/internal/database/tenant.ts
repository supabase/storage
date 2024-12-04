import crypto from 'node:crypto'
import { getConfig } from '../../config'
import { decrypt, encrypt, verifyJWT } from '../auth'
import { multitenantKnex } from './multitenant-db'
import { JwtPayload } from 'jsonwebtoken'
import { PubSubAdapter } from '../pubsub'
import { createMutexByKey } from '../concurrency'
import { LRUCache } from 'lru-cache'
import objectSizeOf from 'object-sizeof'
import { lastMigrationName } from './migrations/migrate'
import { ERRORS } from '@internal/errors'
import { DBMigration } from '@internal/database/migrations'

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
  migrationVersion?: keyof typeof DBMigration
  migrationStatus?: TenantMigrationStatus
  syncMigrationsDone?: boolean
  tracingMode?: string
}

export interface Features {
  imageTransformation: {
    enabled: boolean
    maxResolution?: number
  }
  s3Protocol: {
    enabled: boolean
  }
}

export enum TenantMigrationStatus {
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
  FAILED_STALE = 'FAILED_STALE',
}

interface S3Credentials {
  accessKey: string
  secretKey: string
  claims: { role: string; sub?: string; [key: string]: any }
}

const { isMultitenant, dbServiceRole, serviceKey, jwtSecret, jwtJWKS } = getConfig()

const tenantConfigCache = new Map<string, TenantConfig>()

const tenantS3CredentialsCache = new LRUCache<string, S3Credentials>({
  maxSize: 1024 * 1024 * 50, // 50MB
  ttl: 1000 * 60 * 60, // 1 hour
  sizeCalculation: (value) => objectSizeOf(value),
  updateAgeOnGet: true,
  allowStale: false,
})

const tenantMutex = createMutexByKey()
const s3CredentialsMutex = createMutexByKey()

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
  if (!tenantId) {
    throw ERRORS.InvalidTenantId()
  }

  if (tenantConfigCache.has(tenantId)) {
    return tenantConfigCache.get(tenantId) as TenantConfig
  }

  return tenantMutex(tenantId, async () => {
    if (tenantConfigCache.has(tenantId)) {
      return tenantConfigCache.get(tenantId) as TenantConfig
    }

    const tenant = await multitenantKnex('tenants').first().where('id', tenantId)
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
      feature_image_transformation,
      feature_s3_protocol,
      image_transformation_max_resolution,
      database_pool_url,
      max_connections,
      migrations_version,
      migrations_status,
      tracing_mode,
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
      },
      migrationVersion: migrations_version,
      migrationStatus: migrations_status,
      migrationsRun: false,
      tracingMode: tracing_mode,
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
const TENANTS_S3_CREDENTIALS_UPDATE_CHANNEL = 'tenants_s3_credentials_update'

/**
 * Keeps the in memory config cache up to date
 */
export async function listenForTenantUpdate(pubSub: PubSubAdapter): Promise<void> {
  await pubSub.subscribe(TENANTS_UPDATE_CHANNEL, (cacheKey) => {
    tenantConfigCache.delete(cacheKey)
  })

  await pubSub.subscribe(TENANTS_S3_CREDENTIALS_UPDATE_CHANNEL, (cacheKey) => {
    tenantS3CredentialsCache.delete(cacheKey)
  })
}

/**
 * Create S3 Credential for a tenant
 * @param tenantId
 * @param data
 */
export async function createS3Credentials(
  tenantId: string,
  data: { description: string; claims?: S3Credentials['claims'] }
) {
  const existingCount = await countS3Credentials(tenantId)

  if (existingCount >= 50) {
    throw ERRORS.MaximumCredentialsLimit()
  }

  const secretAccessKeyId = crypto.randomBytes(32).toString('hex').slice(0, 32)
  const secretAccessKey = crypto.randomBytes(64).toString('hex').slice(0, 64)

  if (data.claims) {
    delete data.claims.iss
    delete data.claims.issuer
    delete data.claims.exp
    delete data.claims.iat
  }

  data.claims = {
    ...(data.claims || {}),
    role: data.claims?.role ?? dbServiceRole,
    issuer: `supabase.storage.${tenantId}`,
    sub: data.claims?.sub,
  }

  const credentials = await multitenantKnex
    .table('tenants_s3_credentials')
    .insert({
      tenant_id: tenantId,
      description: data.description,
      access_key: secretAccessKeyId,
      secret_key: encrypt(secretAccessKey),
      claims: JSON.stringify(data.claims),
    })
    .returning('id')

  return {
    id: credentials[0].id,
    access_key: secretAccessKeyId,
    secret_key: secretAccessKey,
  }
}

export async function getS3CredentialsByAccessKey(
  tenantId: string,
  accessKey: string
): Promise<S3Credentials> {
  const cacheKey = `${tenantId}:${accessKey}`
  const cachedCredentials = tenantS3CredentialsCache.get(cacheKey)

  if (cachedCredentials) {
    return cachedCredentials
  }

  return s3CredentialsMutex(cacheKey, async () => {
    const cachedCredentials = tenantS3CredentialsCache.get(cacheKey)

    if (cachedCredentials) {
      return cachedCredentials
    }

    const data = await multitenantKnex
      .table('tenants_s3_credentials')
      .select('access_key', 'secret_key', 'claims')
      .where('tenant_id', tenantId)
      .where('access_key', accessKey)
      .first()

    if (!data) {
      throw ERRORS.MissingS3Credentials()
    }

    const secretKey = decrypt(data.secret_key)

    tenantS3CredentialsCache.set(cacheKey, {
      accessKey: data.access_key,
      secretKey: secretKey,
      claims: data.claims,
    })

    return {
      accessKey: data.access_key,
      secretKey: secretKey,
      claims: data.claims,
    }
  })
}

export function deleteS3Credential(tenantId: string, credentialId: string) {
  return multitenantKnex
    .table('tenants_s3_credentials')
    .where('tenant_id', tenantId)
    .where('id', credentialId)
    .delete()
    .returning('id')
}

export function listS3Credentials(tenantId: string) {
  return multitenantKnex
    .table('tenants_s3_credentials')
    .select('id', 'description', 'access_key', 'created_at')
    .where('tenant_id', tenantId)
    .orderBy('created_at', 'asc')
}

export async function countS3Credentials(tenantId: string) {
  const data = await multitenantKnex
    .table('tenants_s3_credentials')
    .count('id')
    .where('tenant_id', tenantId)

  return Number((data as any)?.count || 0)
}
