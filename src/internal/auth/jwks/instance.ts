import { Knex } from 'knex'
import { JWKSManagerStoreKnex } from './store-knex'
import { JWKSManager } from './manager'

let defaultJWKSManager: JWKSManager | undefined

export function createDefaultJWKSManager(db: Knex): JWKSManager {
  if (defaultJWKSManager) {
    throw new Error('JWKSManager already initialized')
  }

  const store = new JWKSManagerStoreKnex(db)
  const jwksManager = new JWKSManager(store)

  defaultJWKSManager = jwksManager

  return jwksManager
}

export function getDefaultJWKSManager(): JWKSManager {
  if (!defaultJWKSManager) {
    throw new Error('JWKSManager not initialized')
  }
  return defaultJWKSManager
}
