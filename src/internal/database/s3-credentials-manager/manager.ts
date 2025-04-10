import crypto from 'node:crypto'
import { LRUCache } from 'lru-cache'
import objectSizeOf from 'object-sizeof'
import { S3Credentials, S3CredentialsManagerStore, S3CredentialsRaw } from './store'
import { createMutexByKey } from '@internal/concurrency'
import { ERRORS } from '@internal/errors'
import { getConfig } from '../../../config'
import { decrypt, encrypt } from '@internal/auth'
import { PubSubAdapter } from '@internal/pubsub'

const TENANTS_S3_CREDENTIALS_UPDATE_CHANNEL = 'tenants_s3_credentials_update'

const tenantS3CredentialsCache = new LRUCache<string, S3Credentials>({
  maxSize: 1024 * 1024 * 50, // 50MB
  ttl: 1000 * 60 * 60, // 1 hour
  sizeCalculation: (value) => objectSizeOf(value),
  updateAgeOnGet: true,
  allowStale: false,
})

const s3CredentialsMutex = createMutexByKey<S3Credentials>()

export class S3CredentialsManager {
  private dbServiceRole: string

  constructor(private storage: S3CredentialsManagerStore) {
    const { dbServiceRole } = getConfig()
    this.dbServiceRole = dbServiceRole
  }

  /**
   * Keeps the in memory config cache up to date
   */
  async listenForTenantUpdate(pubSub: PubSubAdapter): Promise<void> {
    await pubSub.subscribe(TENANTS_S3_CREDENTIALS_UPDATE_CHANNEL, (cacheKey) => {
      tenantS3CredentialsCache.delete(cacheKey)
    })
  }

  /**
   * Create S3 Credential for a tenant
   * @param tenantId
   * @param data
   */
  async createS3Credentials(
    tenantId: string,
    data: { description: string; claims?: S3Credentials['claims'] }
  ) {
    const existingCount = await this.countS3Credentials(tenantId)

    if (existingCount >= 50) {
      throw ERRORS.MaximumCredentialsLimit()
    }

    const accessKey = crypto.randomBytes(32).toString('hex').slice(0, 32)
    const secretKey = crypto.randomBytes(64).toString('hex').slice(0, 64)

    if (data.claims) {
      delete data.claims.iss
      delete data.claims.issuer
      delete data.claims.exp
      delete data.claims.iat
    }

    const claims = {
      ...(data.claims || {}),
      role: data.claims?.role ?? this.dbServiceRole,
      issuer: `supabase.storage.${tenantId}`,
      sub: data.claims?.sub,
    }

    const id = await this.storage.insert(tenantId, {
      description: data.description,
      claims,
      accessKey,
      secretKey: encrypt(secretKey),
    })

    return {
      id,
      access_key: accessKey,
      secret_key: secretKey,
    }
  }

  async getS3CredentialsByAccessKey(tenantId: string, accessKey: string): Promise<S3Credentials> {
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

      const data = await this.storage.getOneByAccessKey(tenantId, accessKey)

      if (!data) {
        throw ERRORS.MissingS3Credentials()
      }

      data.secretKey = decrypt(data.secretKey)

      tenantS3CredentialsCache.set(cacheKey, data)

      return data
    })
  }

  deleteS3Credential(tenantId: string, credentialId: string): Promise<number> {
    return this.storage.delete(tenantId, credentialId)
  }

  listS3Credentials(tenantId: string): Promise<S3CredentialsRaw[]> {
    return this.storage.list(tenantId)
  }

  async countS3Credentials(tenantId: string) {
    return this.storage.count(tenantId)
  }
}
