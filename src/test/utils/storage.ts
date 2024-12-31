import { getPostgresConnection, getServiceKeyUser, TenantConnection } from '@internal/database'
import { Storage } from '@storage/storage'
import { createStorageBackend, StorageBackendAdapter } from '@storage/backend'
import { Database, StorageKnexDB } from '@storage/database'
import { ObjectScanner } from '@storage/scanner/scanner'
import { getConfig } from '../../config'
import { Uploader } from '@storage/uploader'
import { CreateBucketCommand, HeadBucketCommand, S3Client } from '@aws-sdk/client-s3'
import { isS3Error } from '@internal/errors'

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

export function createBucketIfNotExists(bucket: string, client: S3Client) {
  return checkBucketExists(client, bucket).then((exists) => {
    if (!exists) {
      return createS3Bucket(bucket, client)
    }
  })
}

export function createS3Bucket(bucketName: string, client: S3Client) {
  const createBucketCommand = new CreateBucketCommand({
    Bucket: bucketName,
  })

  return client.send(createBucketCommand)
}

export const checkBucketExists = async (client: S3Client, bucket: string) => {
  const options = {
    Bucket: bucket,
  }

  try {
    await client.send(new HeadBucketCommand(options))
    return true
  } catch (error) {
    const err = error as Error

    if (err && isS3Error(err) && err.$metadata.httpStatusCode === 404) {
      return false
    }
    throw error
  }
}
