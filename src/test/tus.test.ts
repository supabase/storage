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

const { serviceKey, tenantId, globalS3Bucket, storageBackendType } = getConfig()
const oneChunkFile = fs.createReadStream(path.resolve(__dirname, 'assets', 'sadcat.jpg'))
const localServerAddress = 'http://127.0.0.1:8999'

const backend = createStorageBackend(storageBackendType)
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
})
