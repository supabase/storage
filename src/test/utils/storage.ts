import { getPostgresConnection, getServiceKeyUser, TenantConnection } from '@internal/database'
import { Storage } from '@storage/storage'
import { createStorageBackend, StorageBackendAdapter } from '@storage/backend'
import { Database, StorageKnexDB } from '@storage/database'
import { ObjectScanner } from '@storage/scanner/scanner'
import { getConfig } from '../../config'
import { Uploader } from '@storage/uploader'

const { tenantId, storageBackendType } = getConfig()

export function useStorage() {
  let connection: TenantConnection
  let storage: Storage
  let adapter: StorageBackendAdapter
  let database: Database
  let scanner: ObjectScanner
  let uploader: Uploader

  beforeAll(async () => {
    const adminUser = await getServiceKeyUser(tenantId)
    connection = await getPostgresConnection({
      tenantId,
      user: adminUser,
      superUser: adminUser,
      host: 'localhost',
      disableHostCheck: true,
    })
    database = new StorageKnexDB(connection, {
      tenantId,
      host: 'localhost',
    })
    adapter = createStorageBackend(storageBackendType)
    storage = new Storage(adapter, database)
    scanner = new ObjectScanner(storage)
    uploader = new Uploader(adapter, database)
  })

  afterAll(async () => {
    await connection.dispose()
  })

  return {
    get connection() {
      return connection
    },
    get storage() {
      return storage
    },
    get adapter() {
      return adapter
    },
    get database() {
      return database
    },
    get scanner() {
      return scanner
    },
    get uploader() {
      return uploader
    },
  }
}
