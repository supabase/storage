import {
  CreateBucketCommand,
  CreateBucketRequest,
  CreateMultipartUploadCommand,
  ListBucketsCommand,
  ListObjectsV2Command,
  S3Client,
} from '@aws-sdk/client-s3'
import { getConfig } from '../config'
import app from '../app'
import { FastifyInstance } from 'fastify'
import { Upload } from '@aws-sdk/lib-storage'

const { tenantId, serviceKey } = getConfig()

describe('S3 Protocol', () => {
  describe('Bucket', () => {
    let testApp: FastifyInstance
    let client: S3Client

    beforeAll(async () => {
      testApp = app({
        ignoreTrailingSlash: true,
      })
      const listener = await testApp.listen()
      console.log('listen', `${listener.replace('[::1]', 'localhost')}/s3`)
      client = new S3Client({
        endpoint: `${listener.replace('[::1]', 'localhost')}/s3`,
        forcePathStyle: true,
        region: 'us-east-1',
        credentials: {
          accessKeyId: tenantId,
          secretAccessKey: serviceKey,
        },
      })
    })

    afterAll(async () => {
      await testApp.close()
    })

    it('creates a bucket', async () => {
      const createBucketRequest = new CreateBucketCommand({
        Bucket: `SomeBucket-${Date.now()}`,
        ACL: 'public-read',
      })

      const { Location, $metadata, ...rest } = await client.send(createBucketRequest)
      console.log(Location)

      expect(Location).toBeTruthy()
    })

    it('can list buckets', async () => {
      const listBuckets = new ListBucketsCommand({
        Bucket: `SomeBucket-${Date.now()}`,
      })

      const resp = await client.send(listBuckets)
      console.log(resp)
    })

    it('can list content', async () => {
      const listBuckets = new ListObjectsV2Command({
        Bucket: `super`,
      })

      const resp = await client.send(listBuckets)
      console.log(resp)
    })

    it('creates a multi part upload', async () => {
      const bucketName = 'SomeBucket-1708340404949'
      const createMultiPartUpload = new CreateMultipartUploadCommand({
        Bucket: bucketName,
        Key: 'test-1.jpg',
        ContentType: 'image/jpg',
        CacheControl: 'max-age=2000',
      })
      const resp = await client.send(createMultiPartUpload)
      expect(resp.UploadId).toBeTruthy()
    })

    it('upload a part', async () => {
      const bucketName = 'SomeBucket-1708340404949'
      const createMultiPartUpload = new CreateMultipartUploadCommand({
        Bucket: bucketName,
        Key: 'test-1.jpg',
        ContentType: 'image/jpg',
        CacheControl: 'max-age=2000',
      })
      const resp = await client.send(createMultiPartUpload)
      expect(resp.UploadId).toBeTruthy()
    })

    it('upload a file', async () => {
      const bucketName = 'SomeBucket-1708340404949'

      const uploader = new Upload({
        client: client,
        params: {
          Bucket: bucketName,
          Key: 'test-1.jpg',
          ContentType: 'image/jpg',
          Body: Buffer.alloc(1024 * 1024 * 12),
        },
      })

      const resp = await uploader.done()

      expect(resp.$metadata).toBeTruthy()
    })
  })
})
