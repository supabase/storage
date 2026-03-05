import dotenv from 'dotenv'
import path from 'path'

dotenv.config({ path: path.resolve(__dirname, '..', '..', '.env.test') })

import { CreateBucketCommand, S3Client } from '@aws-sdk/client-s3'
import { wait } from '@internal/concurrency'
import { getPostgresConnection, getServiceKeyUser } from '@internal/database'
import { logger } from '@internal/monitoring'
import { TenantLocation } from '@storage/locator'
import { randomUUID } from 'crypto'
import { FastifyInstance } from 'fastify'
import fs from 'fs'
import * as tus from 'tus-js-client'
import { DetailedError } from 'tus-js-client'
import app from '../app'
import { getConfig } from '../config'
import { backends, Storage, StorageKnexDB } from '../storage'
import { checkBucketExists, getInvalidObjectName, getUnicodeObjectName } from './common'

const { serviceKeyAsync, tenantId, storageS3Bucket, storageBackendType } = getConfig()
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
      loggerInstance: logger,
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
      tenantId,
      host: 'localhost',
    })

    db = new StorageKnexDB(pg, {
      host: 'localhost',
      tenantId,
    })

    bucketName = randomUUID()
    storage = new Storage(backend, db, new TenantLocation(storageS3Bucket))
  })

  it('Can upload an asset with the TUS protocol', async () => {
    const objectName = randomUUID() + '-cat.jpeg'

    const bucket = await storage.createBucket({
      id: bucketName,
      name: bucketName,
      public: true,
    })

    const authorization = `Bearer ${await serviceKeyAsync}`

    const result = await new Promise((resolve, reject) => {
      const upload = new tus.Upload(oneChunkFile, {
        endpoint: `${localServerAddress}/upload/resumable`,
        onShouldRetry: () => false,
        uploadDataDuringCreation: false,
        headers: {
          authorization,
          'x-upsert': 'true',
        },
        metadata: {
          bucketName,
          objectName,
          contentType: 'image/jpeg',
          cacheControl: '3600',
          metadata: JSON.stringify({
            test1: 'test1',
            test2: 'test2',
          }),
        },
        onError(error) {
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
        const authorization = `Bearer ${await serviceKeyAsync}`
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(oneChunkFile, {
            endpoint: `${localServerAddress}/upload/resumable`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
            headers: {
              authorization,
              'x-upsert': 'true',
            },
            metadata: {
              bucketName: 'doesn-exist',
              objectName,
              contentType: 'image/jpeg',
              cacheControl: '3600',
            },
            onError(error) {
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
      } catch (e) {
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
        const authorization = `Bearer ${await serviceKeyAsync}`
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(oneChunkFile, {
            endpoint: `${localServerAddress}/upload/resumable`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
            headers: {
              authorization,
              'x-upsert': 'true',
            },
            metadata: {
              bucketName,
              objectName,
              contentType: 'image/jpeg',
              cacheControl: '3600',
            },
            onError(error) {
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
      } catch (e) {
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
            bucketName,
            objectName,
            contentType: 'image/jpeg',
            cacheControl: '3600',
            metadata: JSON.stringify({
              test1: 'test1',
              test3: 'test3',
            }),
          },
          onError(error) {
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

    it('will allow uploading using signed upload url with a Unicode object key', async () => {
      const bucket = await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const objectName = `${randomUUID()}-${getUnicodeObjectName()}`
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
            bucketName,
            objectName,
            contentType: 'image/jpeg',
            cacheControl: '3600',
          },
          onError(error) {
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
      expect(dbAsset?.name).toBe(objectName)
      expect(dbAsset?.bucket_id).toBe(bucket.id)
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
            bucketName,
            objectName,
            contentType: 'image/jpeg',
            cacheControl: '3600',
          },
          onError(error) {
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

      await wait(2000)

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
              bucketName,
              objectName,
              contentType: 'image/jpeg',
              cacheControl: '3600',
            },
            onError(error) {
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
        expect(err.originalResponse.getBody()).toEqual('"exp" claim timestamp check failed')
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

      await wait(2000)

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
              bucketName,
              objectName,
              contentType: 'image/jpeg',
              cacheControl: '3600',
            },
            onError(error) {
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
        expect(err.originalResponse.getBody()).toEqual('Invalid Compact JWS')
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

      await wait(2000)

      try {
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(oneChunkFile, {
            endpoint: `${localServerAddress}/upload/resumable/sign`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
            metadata: {
              bucketName,
              objectName,
              contentType: 'image/jpeg',
              cacheControl: '3600',
            },
            onError(error) {
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

  describe('TUS control endpoints', () => {
    function encodeTusMetadataValue(value: string) {
      return Buffer.from(value, 'utf8').toString('base64')
    }

    function buildTusMetadata(objectName: string) {
      return [
        `bucketName ${encodeTusMetadataValue(bucketName)}`,
        `objectName ${encodeTusMetadataValue(objectName)}`,
        `contentType ${encodeTusMetadataValue('image/jpeg')}`,
        `cacheControl ${encodeTusMetadataValue('3600')}`,
      ].join(',')
    }

    async function createTusUploadSession(objectName: string, authorization: string) {
      const createResponse = await fetch(`${localServerAddress}/upload/resumable`, {
        method: 'POST',
        headers: {
          authorization,
          'x-upsert': 'true',
          'tus-resumable': '1.0.0',
          'upload-length': '32',
          'upload-metadata': buildTusMetadata(objectName),
        },
      })

      expect(createResponse.status).toBe(201)
      const location = createResponse.headers.get('location')
      expect(location).toBeTruthy()

      return new URL(location || '', localServerAddress).toString()
    }

    async function patchTusUploadSession(uploadUrl: string, authorization: string, body: Buffer) {
      const patchResponse = await fetch(uploadUrl, {
        method: 'PATCH',
        headers: {
          authorization,
          'tus-resumable': '1.0.0',
          'upload-offset': '0',
          'content-type': 'application/offset+octet-stream',
          'content-length': String(body.length),
        },
        body,
      })

      expect(patchResponse.status).toBe(204)
      expect(patchResponse.headers.get('upload-offset')).toBe(String(body.length))
    }

    test('supports HEAD and DELETE flow for ASCII object keys', async () => {
      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const authorization = `Bearer ${await serviceKeyAsync}`
      const objectName = `${randomUUID()}-ascii-control-q?foo=1&bar=%25+plus;semi:colon,.jpg`
      const uploadUrl = await createTusUploadSession(objectName, authorization)

      const headBeforeDelete = await fetch(uploadUrl, {
        method: 'HEAD',
        headers: {
          authorization,
          'tus-resumable': '1.0.0',
        },
      })
      expect(headBeforeDelete.status).toBe(200)
      expect(headBeforeDelete.headers.get('upload-offset')).toBe('0')
      expect(headBeforeDelete.headers.get('upload-length')).toBe('32')

      const deleteResponse = await fetch(uploadUrl, {
        method: 'DELETE',
        headers: {
          authorization,
          'tus-resumable': '1.0.0',
        },
      })
      expect([200, 204]).toContain(deleteResponse.status)

      const headAfterDelete = await fetch(uploadUrl, {
        method: 'HEAD',
        headers: {
          authorization,
          'tus-resumable': '1.0.0',
        },
      })
      expect([404, 410]).toContain(headAfterDelete.status)
    })

    test('supports HEAD and DELETE flow for Unicode object keys', async () => {
      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const authorization = `Bearer ${await serviceKeyAsync}`
      const objectName = `${randomUUID()}-${getUnicodeObjectName()}`
      const uploadUrl = await createTusUploadSession(objectName, authorization)

      const headBeforeDelete = await fetch(uploadUrl, {
        method: 'HEAD',
        headers: {
          authorization,
          'tus-resumable': '1.0.0',
        },
      })
      expect(headBeforeDelete.status).toBe(200)
      expect(headBeforeDelete.headers.get('upload-offset')).toBe('0')
      expect(headBeforeDelete.headers.get('upload-length')).toBe('32')

      const deleteResponse = await fetch(uploadUrl, {
        method: 'DELETE',
        headers: {
          authorization,
          'tus-resumable': '1.0.0',
        },
      })
      expect([200, 204]).toContain(deleteResponse.status)

      const headAfterDelete = await fetch(uploadUrl, {
        method: 'HEAD',
        headers: {
          authorization,
          'tus-resumable': '1.0.0',
        },
      })
      expect([404, 410]).toContain(headAfterDelete.status)
    })

    test('supports upload completion and object GET for ASCII URL-reserved keys', async () => {
      const bucket = await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const authorization = `Bearer ${await serviceKeyAsync}`
      const objectName = `${randomUUID()}-ascii-get-q?foo=1&bar=%25+plus;semi:colon,#frag.jpg`
      const uploadUrl = await createTusUploadSession(objectName, authorization)
      const payload = Buffer.from('abcdefghijklmnopqrstuvwxyz012345')

      await patchTusUploadSession(uploadUrl, authorization, payload)

      const appInstance = app()
      try {
        const getResponse = await appInstance.inject({
          method: 'GET',
          url: `/object/${bucket.id}/${encodeURIComponent(objectName)}`,
          headers: {
            authorization,
          },
        })
        expect(getResponse.statusCode).toBe(200)
        expect(getResponse.headers['content-length']).toBe(String(payload.length))
      } finally {
        await appInstance.close()
      }
    })

    test('supports upload completion and object GET for Unicode URL-reserved keys', async () => {
      const bucket = await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const authorization = `Bearer ${await serviceKeyAsync}`
      const objectName = `${randomUUID()}-${getUnicodeObjectName()}-q?foo=1&bar=%25+plus;semi:colon,#frag.jpg`
      const uploadUrl = await createTusUploadSession(objectName, authorization)
      const payload = Buffer.from('abcdefghijklmnopqrstuvwxyz012345')

      await patchTusUploadSession(uploadUrl, authorization, payload)

      const appInstance = app()
      try {
        const getResponse = await appInstance.inject({
          method: 'GET',
          url: `/object/${bucket.id}/${encodeURIComponent(objectName)}`,
          headers: {
            authorization,
          },
        })
        expect(getResponse.statusCode).toBe(200)
        expect(getResponse.headers['content-length']).toBe(String(payload.length))
      } finally {
        await appInstance.close()
      }
    })
  })

  describe('Object key names with Unicode characters', () => {
    it('can be uploaded with the TUS protocol', async () => {
      const objectName = randomUUID() + '-' + getUnicodeObjectName()
      const authorization = `Bearer ${await serviceKeyAsync}`

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
            authorization,
            'x-upsert': 'true',
          },
          metadata: {
            bucketName,
            objectName,
            contentType: 'image/jpeg',
            cacheControl: '3600',
            metadata: JSON.stringify({
              test1: 'test1',
              test2: 'test2',
            }),
          },
          onError(error) {
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
        path_tokens: objectName.split('/'),
        updated_at: expect.any(Date),
        version: expect.any(String),
      })

      const appInstance = app()
      try {
        const getResponse = await appInstance.inject({
          method: 'GET',
          url: `/object/${bucketName}/${encodeURIComponent(objectName)}`,
          headers: {
            authorization,
          },
        })
        expect(getResponse.statusCode).toBe(200)
        expect(getResponse.headers['etag']).toBe('"53e1323c929d57b09b95fbe6d531865c-1"')
        expect(getResponse.headers['cache-control']).toBe('max-age=3600')
        expect(getResponse.headers['content-length']).toBe('29526')
        expect(getResponse.headers['content-type']).toBe('image/jpeg')
      } finally {
        await appInstance.close()
      }
    })

    it('should not upload if the name contains invalid characters', async () => {
      await storage.createBucket({
        id: bucketName,
        name: bucketName,
        public: true,
      })

      const invalidObjectName = randomUUID() + '-' + getInvalidObjectName()
      const authorization = `Bearer ${await serviceKeyAsync}`
      try {
        await new Promise((resolve, reject) => {
          const upload = new tus.Upload(oneChunkFile, {
            endpoint: `${localServerAddress}/upload/resumable`,
            onShouldRetry: () => false,
            uploadDataDuringCreation: false,
            headers: {
              authorization,
              'x-upsert': 'true',
            },
            metadata: {
              bucketName,
              objectName: invalidObjectName,
              contentType: 'image/jpeg',
              cacheControl: '3600',
            },
            onError(error) {
              reject(error)
            },
            onSuccess: () => {
              resolve(true)
            },
          })

          upload.start()
        })

        throw new Error('it should error with invalid key')
      } catch (e) {
        expect((e as Error).message).not.toEqual('it should error with invalid key')
        const err = e as DetailedError
        expect(err.originalResponse.getStatus()).toEqual(400)
        expect(err.originalResponse.getBody()).toEqual(
          `Invalid key: ${encodeURIComponent(invalidObjectName)}`
        )
      }
    })
  })
})
