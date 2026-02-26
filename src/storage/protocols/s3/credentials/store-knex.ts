import { Knex } from 'knex'
import { getConfig } from '../../../../config'
import {
  S3Credentials,
  S3CredentialsManagerStore,
  S3CredentialsRaw,
  S3CredentialWithDescription,
} from './store'

const { multitenantDatabaseQueryTimeout } = getConfig()

export class S3CredentialsManagerStoreKnex implements S3CredentialsManagerStore {
  constructor(private knex: Knex) {}

  async insert(tenantId: string, credential: S3CredentialWithDescription): Promise<string> {
    const credentials = await this.knex
      .table('tenants_s3_credentials')
      .insert({
        tenant_id: tenantId,
        description: credential.description,
        access_key: credential.accessKey,
        secret_key: credential.secretKey,
        claims: JSON.stringify(credential.claims),
      })
      .returning('id')
    return credentials[0].id
  }

  list(tenantId: string): Promise<S3CredentialsRaw[]> {
    return this.knex
      .table('tenants_s3_credentials')
      .select<S3CredentialsRaw[]>('id', 'description', 'access_key', 'created_at')
      .where('tenant_id', tenantId)
      .orderBy('created_at', 'asc')
  }

  getOneByAccessKey(tenantId: string, accessKey: string): Promise<S3Credentials> {
    return this.knex
      .table('tenants_s3_credentials')
      .select({ accessKey: 'access_key', secretKey: 'secret_key', claims: 'claims' })
      .where('tenant_id', tenantId)
      .where('access_key', accessKey)
      .first()
      .abortOnSignal(AbortSignal.timeout(multitenantDatabaseQueryTimeout))
  }

  async count(tenantId: string): Promise<number> {
    const data = await this.knex
      .table('tenants_s3_credentials')
      .count<{ count: number }>('id')
      .where('tenant_id', tenantId)
      .first()
    return Number(data?.count || 0)
  }

  delete(tenantId: string, credentialId: string): Promise<number> {
    return this.knex
      .table('tenants_s3_credentials')
      .where('tenant_id', tenantId)
      .where('id', credentialId)
      .delete()
  }
}
