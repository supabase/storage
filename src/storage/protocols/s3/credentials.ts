import crypto from 'crypto'
import { LRUCache } from 'lru-cache'
import objectSizeOf from 'object-sizeof'

import { ERRORS } from '@internal/errors'
import { decrypt, encrypt } from '@internal/auth'
import { multitenantKnex } from '@internal/database'
import { createMutexByKey } from '@internal/concurrency'
import { PubSubAdapter } from '@internal/pubsub'

import { getConfig } from '../../../config'

const { dbServiceRole } = getConfig()

interface S3Credentials {
  accessKey: string
  secretKey: string
  claims: { role: string; sub?: string; [key: string]: any }
}

const TENANTS_S3_CREDENTIALS_UPDATE_CHANNEL = 'tenants_s3_credentials_update'

const _cache = new LRUCache<string, S3Credentials>({
  maxSize: 1024 * 1024 * 50, // 50MB
  ttl: 1000 * 60 * 60, // 1 hour
  sizeCalculation: (value) => objectSizeOf(value),
  updateAgeOnGet: true,
  allowStale: false,
})

const _mutex = createMutexByKey()

export function listenForS3CredentialsUpdate(pubSub: PubSubAdapter) {
  return pubSub.subscribe(TENANTS_S3_CREDENTIALS_UPDATE_CHANNEL, (cacheKey) => {
    _cache.delete(cacheKey)
  })
}

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

export async function countS3Credentials(tenantId: string) {
  const data = await multitenantKnex
    .table('tenants_s3_credentials')
    .count('id')
    .where('tenant_id', tenantId)

  return Number((data as any)?.count || 0)
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

export async function getS3CredentialsByAccessKey(
  tenantId: string,
  accessKey: string
): Promise<S3Credentials> {
  const cacheKey = `${tenantId}:${accessKey}`
  const cachedCredentials = _cache.get(cacheKey)

  if (cachedCredentials) {
    return cachedCredentials
  }

  return _mutex(cacheKey, async () => {
    const cachedCredentials = _cache.get(cacheKey)

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

    _cache.set(cacheKey, {
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
