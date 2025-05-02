import { Knex } from 'knex'
import { S3CredentialsManagerStoreKnex } from './store-knex'
import { S3CredentialsManager } from './manager'

let credentialManager: S3CredentialsManager | undefined = undefined

export function createDefaultS3CredentialsManager(knex: Knex) {
  if (credentialManager) {
    throw new Error('S3CredentialsManager already initialized')
  }
  const store = new S3CredentialsManagerStoreKnex(knex)
  const manager = new S3CredentialsManager(store)

  credentialManager = manager
  return manager
}

export function getDefaultS3CredentialsManager() {
  if (!credentialManager) {
    throw new Error('S3CredentialsManager not initialized')
  }
  return credentialManager
}
