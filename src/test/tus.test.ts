import dotenv from 'dotenv'
import path from 'path'
dotenv.config({ path: path.resolve(__dirname, '..', '..', '.env.test') })

import { getPostgresConnection } from '../database'
import { getConfig } from '../config'
import { StorageKnexDB } from '../storage/database'
import { randomUUID } from 'crypto'
import * as tus from 'tus-js-client'
import fs from 'fs'
import app from '../app'
import { FastifyInstance } from 'fastify'
import { isS3Error, Storage } from '../storage'
import { createStorageBackend } from '../storage/backend'
import { CreateBucketCommand, S3Client } from '@aws-sdk/client-s3'
import { logger } from '../monitoring'
import { DetailedError } from 'tus-js-client'
import { getServiceKeyUser } from '../database/tenant'
import { checkBucketExists } from './common'
import { PostgresLocker } from '../http/routes/tus/postgres-locker'

const { serviceKey, tenantId, globalS3Bucket, storageBackendType } = getConfig()
const oneChunkFile = fs.createReadStream(path.resolve(__dirname, 'assets', 'sadcat.jpg'))
const localServerAddress = 'http://127.0.0.1:8999'

const backend = createStorageBackend(storageBackendType)
const client = backend.client

async function createDB() {
  const superUser = await getServiceKeyUser(tenantId)
  const pg = await getPostgresConnection({
    superUser,
    user: superUser,
    tenantId: tenantId,
    host: 'localhost',
  })

  return new StorageKnexDB(pg, {
    host: 'localhost',
    tenantId,
  })
}

describe('Tus multipart', () => {
  let db: StorageKnexDB
  let storage: Storage
  let server: FastifyInstance
  let bucketName: string

  beforeAll(async () => {
    server = await app({
      logger: logger,
    })

    await server.listen({
      port: 8999,
    })

    if (client instanceof S3Client) {
      const bucketExists = await checkBucketExists(client, globalS3Bucket)

      if (!bucketExists) {
        const createBucketCommand = new CreateBucketCommand({
          Bucket: globalS3Bucket,
        })
        await client.send(createBucketCommand)
      }
    }
  })

  afterAll(async () => {
    await server.close()
  })

  beforeEach(async () => {
    db = await createDB()

    bucketName = randomUUID()
    storage = new Storage(backend, db)
  })

  it('Can upload an asset with the TUS protocol', async () => {
    const objectName = randomUUID() + '-cat.jpeg'

    const bucket = await storage.createBucket({
      id: bucketName,
      name: bucketName,
      public: true,
    })

    const result = await new Promise((resolve, reject) => {
      const upload = new tus.Upload(oneChunkFile, {
        endpoint: `${localServerAddress}/upload/resumable`,
        onShouldRetry: () => false,
        uploadDataDuringCreation: false,
        headers: {
          authorization: `Bearer ${serviceKey}`,
          'x-upsert': 'true',
        },
        metadata: {
          bucketName: bucketName,
          objectName: objectName,
          contentType: 'image/jpeg',
          cacheControl: '3600',
        },
        onError: function (error) {
          console.log('Failed because: ' + error)
          reject(error)
        },
        onSuccess: () => {
          resolve(true)
        },
      })

      upload.start()
    })

    expect(result).toEqual(true)

    const dbAsset = await storage.from(bucket.id).findObject(objectName, '*')
    expect(dbAsset).toEqual({
      bucket_id: bucket.id,
      created_at: expect.any(Date),
      id: expect.any(String),
      last_accessed_at: expect.any(Date),
      metadata: {
        cacheControl: 'max-age=3600',
        contentLength: 29526,
        eTag: '"53e1323c929d57b09b95fbe6d531865c-1"',
        httpStatusCode: 200,
        lastModified: expect.any(String),
        mimetype: 'image/jpeg',
        size: 29526,
      },
      name: objectName,
      owner: null,
      owner_id: null,
      path_tokens: [objectName],
      updated_at: expect.any(Date),
      version: expect.any(String),
    })
  })

  describe('TUS Validation', () => {
    it('Cannot upload to a non-existing bucket', async () => {
      const objectName = randomUUID() + '-cat.jpeg'

      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
        fileSizeLimit: '10kb',
      })

      try {
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(oneChunkFile, {
            endpoint: `${localServerAddress}/upload/resumable`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
            headers: {
              authorization: `Bearer ${serviceKey}`,
              'x-upsert': 'true',
            },
            metadata: {
              bucketName: 'doesn-exist',
              objectName: objectName,
              contentType: 'image/jpeg',
              cacheControl: '3600',
            },
            onError: function (error) {
              console.log('Failed because: ' + error)
              reject(error)
            },
            onSuccess: () => {
              resolve(true)
            },
          })

          upload.start()
        })

        throw Error('it should error with max-size exceeded')
      } catch (e: any) {
        expect(e).toBeInstanceOf(DetailedError)

        const err = e as DetailedError
        expect(err.originalResponse.getBody()).toEqual('Bucket not found')
        expect(err.originalResponse.getStatus()).toEqual(404)
      }
    })

    it('Cannot upload an asset that exceed the maximum bucket size', async () => {
      const objectName = randomUUID() + '-cat.jpeg'

      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
        fileSizeLimit: '10kb',
      })

      try {
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(oneChunkFile, {
            endpoint: `${localServerAddress}/upload/resumable`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
            headers: {
              authorization: `Bearer ${serviceKey}`,
              'x-upsert': 'true',
            },
            metadata: {
              bucketName: bucketName,
              objectName: objectName,
              contentType: 'image/jpeg',
              cacheControl: '3600',
            },
            onError: function (error) {
              console.log('Failed because: ' + error)
              reject(error)
            },
            onSuccess: () => {
              resolve(true)
            },
          })

          upload.start()
        })

        throw Error('it should error with max-size exceeded')
      } catch (e: any) {
        expect(e).toBeInstanceOf(DetailedError)

        const err = e as DetailedError
        expect(err.originalResponse.getBody()).toEqual('Request Entity Too Large\n')
        expect(err.originalResponse.getStatus()).toEqual(413)
      }
    })
  })

  describe('Postgres locker', () => {
    it('will hold the lock for subsequent calls until released', async () => {
      const locker = new PostgresLocker(db)
      const lockId = 'tenant/bucket/file/version'

      const date = new Date()
      await locker.lock(lockId)
      setTimeout(() => {
        locker.unlock(lockId)
      }, 300)
      await locker.lock(lockId) // will wait until the other lock is released
      await locker.unlock(lockId)
      const endDate = new Date().valueOf() - date.valueOf()
      expect(endDate >= 300).toEqual(true)
    })

    it('locking a locked lock should not resolve', async () => {
      const locker = new PostgresLocker(db)
      const lockId = 'tenant/bucket/file/version'

      await locker.lock(lockId)

      const p1 = locker.lock(lockId)
      const p2 = new Promise((resolve) => setTimeout(resolve, 500, 'timeout'))

      expect(await Promise.race([p1, p2])).toEqual('timeout')

      // clean up
      await locker.unlock(lockId)
      await p1
      await locker.unlock(lockId)
    })

    it('unlocking a lock should resolve only one pending lock', async () => {
      const locker = new PostgresLocker(db)
      const lockId = 'tenant/bucket/file/version'

      const spy = jest.fn()
      const locks: Promise<void>[] = []

      await locker.lock(lockId)

      locks.push(
        locker.lock(lockId).then(() => {
          spy('2')
        })
      )

      locks.push(
        locker.lock(lockId).then(() => {
          spy('3')
          return locker.unlock(lockId)
        })
      )

      locks.push(
        locker.lock(lockId).then(() => {
          spy('4')
          return locker.unlock(lockId)
        })
      )

      locks.push(
        locker.lock(lockId).then(() => {
          spy('5')
          return locker.unlock(lockId)
        })
      )

      await locker.unlock(lockId)
      await new Promise((resolve) => setTimeout(resolve, 100))
      expect(spy).toBeCalledTimes(1)

      await locker.unlock(lockId)

      // cleanup
      for (const lock of locks) {
        await lock
      }
    })

    it('unlocking a lock should first resolve unlock promise and then pending lock promise', async () => {
      const locker = new PostgresLocker(db)
      const lockId = 'tenant/bucket/file/version'

      await locker.lock(lockId)

      const resolveOrder = new Array<number>()
      const p1 = locker.lock(lockId).then(() => {
        resolveOrder.push(2)
      })
      const p2 = locker.unlock(lockId).then(() => {
        resolveOrder.push(1)
      })

      await Promise.all([p1, p2])
      expect(resolveOrder).toEqual([1, 2])

      // cleanup
      await locker.unlock(lockId)
    })
  })
})
