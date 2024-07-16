import dotenv from 'dotenv'
import path from 'path'
dotenv.config({ path: path.resolve(__dirname, '..', '..', '.env.test') })

import fs from 'fs'
import { FastifyInstance } from 'fastify'
import { randomUUID } from 'crypto'
import * as tus from 'tus-js-client'
import { DetailedError } from 'tus-js-client'
import { CreateBucketCommand, S3Client } from '@aws-sdk/client-s3'

import { logger } from '@internal/monitoring'
import { getServiceKeyUser, getPostgresConnection } from '@internal/database'
import { getConfig } from '../config'
import app from '../app'
import { checkBucketExists } from './common'
import { Storage, backends, StorageKnexDB } from '../storage'

const { serviceKey, tenantId, storageS3Bucket, storageBackendType } = getConfig()
const oneChunkFile = fs.createReadStream(path.resolve(__dirname, 'assets', 'sadcat.jpg'))
const localServerAddress = 'http://127.0.0.1:8999'

const backend = backends.createStorageBackend(storageBackendType)
const client = backend.client

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
      const bucketExists = await checkBucketExists(client, storageS3Bucket)

      if (!bucketExists) {
        const createBucketCommand = new CreateBucketCommand({
          Bucket: storageS3Bucket,
        })
        await client.send(createBucketCommand)
      }
    }
  })

  afterAll(async () => {
    await server.close()
  })

  beforeEach(async () => {
    const superUser = await getServiceKeyUser(tenantId)
    const pg = await getPostgresConnection({
      superUser,
      user: superUser,
      tenantId: tenantId,
      host: 'localhost',
    })

    db = new StorageKnexDB(pg, {
      host: 'localhost',
      tenantId,
    })

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
          metadata: JSON.stringify({
            test1: 'test1',
            test2: 'test2',
          }),
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
      user_metadata: {
        test1: 'test1',
        test2: 'test2',
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
        expect(err.originalResponse.getBody()).toEqual('Maximum size exceeded\n')
        expect(err.originalResponse.getStatus()).toEqual(413)
      }
    })
  })

  describe('Signed Upload URL', () => {
    it('will allow uploading using signed upload url without authorization token', async () => {
      const bucket = await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const objectName = randomUUID() + '-cat.jpeg'

      const signedUpload = await storage
        .from(bucketName)
        .signUploadObjectUrl(objectName, `${bucketName}/${objectName}`, 3600)

      const result = await new Promise((resolve, reject) => {
        const upload = new tus.Upload(oneChunkFile, {
          endpoint: `${localServerAddress}/upload/resumable/sign`,
          onShouldRetry: () => false,
          uploadDataDuringCreation: false,
          headers: {
            'x-signature': signedUpload.token,
          },
          metadata: {
            bucketName: bucketName,
            objectName: objectName,
            contentType: 'image/jpeg',
            cacheControl: '3600',
            metadata: JSON.stringify({
              test1: 'test1',
              test3: 'test3',
            }),
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
        user_metadata: {
          test1: 'test1',
          test3: 'test3',
        },
        name: objectName,
        owner: null,
        owner_id: null,
        path_tokens: [objectName],
        updated_at: expect.any(Date),
        version: expect.any(String),
      })
    })

    it('will allow uploading using signed upload url without authorization token, honouring the owner id', async () => {
      const bucket = await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const objectName = randomUUID() + '-cat.jpeg'

      const signedUpload = await storage
        .from(bucketName)
        .signUploadObjectUrl(objectName, `${bucketName}/${objectName}`, 3600, 'some-owner-id')

      const result = await new Promise((resolve, reject) => {
        const upload = new tus.Upload(oneChunkFile, {
          endpoint: `${localServerAddress}/upload/resumable/sign`,
          onShouldRetry: () => false,
          uploadDataDuringCreation: false,
          headers: {
            'x-signature': signedUpload.token,
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
        user_metadata: null,
        name: objectName,
        owner: null,
        owner_id: 'some-owner-id',
        path_tokens: [objectName],
        updated_at: expect.any(Date),
        version: expect.any(String),
      })
    })

    it('will not allow uploading using signed upload url with an expired token', async () => {
      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const objectName = randomUUID() + '-cat.jpeg'

      const signedUpload = await storage
        .from(bucketName)
        .signUploadObjectUrl(objectName, `${bucketName}/${objectName}`, 1)

      await new Promise((resolve) => setTimeout(resolve, 2000))

      try {
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(oneChunkFile, {
            endpoint: `${localServerAddress}/upload/resumable/sign`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
            headers: {
              'x-signature': signedUpload.token,
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

        throw new Error('it should error with expired token')
      } catch (e) {
        expect((e as Error).message).not.toEqual('it should error with expired token')

        const err = e as DetailedError
        expect(err.originalResponse.getBody()).toEqual('jwt expired')
        expect(err.originalResponse.getStatus()).toEqual(400)
      }
    })

    it('will not allow uploading using signed upload url with an invalid token', async () => {
      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const objectName = randomUUID() + '-cat.jpeg'

      await new Promise((resolve) => setTimeout(resolve, 2000))

      try {
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(oneChunkFile, {
            endpoint: `${localServerAddress}/upload/resumable/sign`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
            headers: {
              'x-signature': 'invalid-token',
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

        throw new Error('it should error with expired token')
      } catch (e) {
        expect((e as Error).message).not.toEqual('it should error with expired token')

        const err = e as DetailedError
        expect(err.originalResponse.getBody()).toEqual('jwt malformed')
        expect(err.originalResponse.getStatus()).toEqual(400)
      }
    })

    it('will not allow uploading using signed upload url without a token', async () => {
      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const objectName = randomUUID() + '-cat.jpeg'

      await new Promise((resolve) => setTimeout(resolve, 2000))

      try {
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(oneChunkFile, {
            endpoint: `${localServerAddress}/upload/resumable/sign`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
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

        throw new Error('it should error with expired token')
      } catch (e) {
        expect((e as Error).message).not.toEqual('it should error with expired token')

        const err = e as DetailedError
        expect(err.originalResponse.getBody()).toEqual('Missing x-signature header')
        expect(err.originalResponse.getStatus()).toEqual(400)
      }
    })
  })
})
