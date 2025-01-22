import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CopyObjectCommand,
  CreateBucketCommand,
  CreateMultipartUploadCommand,
  DeleteBucketCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  GetBucketLocationCommand,
  GetBucketVersioningCommand,
  GetObjectCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  ListBucketsCommand,
  ListMultipartUploadsCommand,
  ListObjectsCommand,
  ListObjectsV2Command,
  ListPartsCommand,
  PutObjectCommand,
  S3Client,
  S3ServiceException,
  UploadPartCommand,
  UploadPartCopyCommand,
} from '@aws-sdk/client-s3'
import { getConfig, mergeConfig } from '../config'
import app from '../app'
import { FastifyInstance } from 'fastify'
import { Upload } from '@aws-sdk/lib-storage'
import { ReadableStreamBuffer } from 'stream-buffers'
import { randomUUID } from 'crypto'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import axios from 'axios'
import { createPresignedPost } from '@aws-sdk/s3-presigned-post'

const { s3ProtocolAccessKeySecret, s3ProtocolAccessKeyId, storageS3Region } = getConfig()

async function createBucket(client: S3Client, name?: string, publicRead = true) {
  let bucketName: string
  if (!name) {
    bucketName = `TestBucket-${randomUUID()}`
  } else {
    bucketName = `${name}-${randomUUID()}`
  }

  const createBucketRequest = new CreateBucketCommand({
    Bucket: bucketName,
    ACL: publicRead ? 'public-read' : undefined,
  })

  await client.send(createBucketRequest)

  return bucketName
}

async function uploadFile(client: S3Client, bucketName: string, key: string, mb: number) {
  const uploader = new Upload({
    client: client,
    params: {
      Bucket: bucketName,
      Key: key,
      ContentType: 'image/jpg',
      Body: Buffer.alloc(1024 * 1024 * mb),
    },
  })

  return await uploader.done()
}

jest.setTimeout(10 * 1000)

describe('S3 Protocol', () => {
  describe('Bucket', () => {
    let testApp: FastifyInstance
    let client: S3Client

    beforeAll(async () => {
      testApp = app()
      const listener = await testApp.listen()
      client = new S3Client({
        endpoint: `${listener.replace('[::1]', 'localhost')}/s3`,
        forcePathStyle: true,
        region: storageS3Region,
        credentials: {
          accessKeyId: s3ProtocolAccessKeyId!,
          secretAccessKey: s3ProtocolAccessKeySecret!,
        },
      })

      // clientMinio = new S3Client({
      //   forcePathStyle: true,
      //   region: storageS3Region,
      //   logger: console,
      //   endpoint: 'http://localhost:9000',
      //   credentials: {
      //     accessKeyId: 'supa-storage',
      //     secretAccessKey: 'secret1234',
      //   },
      // })
    })

    afterEach(() => {
      getConfig({ reload: true })
    })

    afterAll(async () => {
      await Promise.race([testApp.close(), new Promise((resolve) => setTimeout(resolve, 1000))])
    })

    describe('CreateBucketCommand', () => {
      it('creates a bucket', async () => {
        const createBucketRequest = new CreateBucketCommand({
          Bucket: `SomeBucket-${randomUUID()}`,
          ACL: 'public-read',
        })

        const { Location } = await client.send(createBucketRequest)

        expect(Location).toBeTruthy()
      })

      it('can get bucket versioning', async () => {
        const bucket = await createBucket(client)
        const bucketVersioningCommand = new GetBucketVersioningCommand({
          Bucket: bucket,
        })

        const resp = await client.send(bucketVersioningCommand)
        expect(resp.Status).toEqual('Suspended')
        expect(resp.MFADelete).toEqual('Disabled')
      })

      it('can get bucket location', async () => {
        const bucket = await createBucket(client)
        const bucketVersioningCommand = new GetBucketLocationCommand({
          Bucket: bucket,
        })

        const resp = await client.send(bucketVersioningCommand)
        expect(resp.LocationConstraint).toEqual(storageS3Region)
      })
    })

    describe('DeleteBucketCommand', () => {
      it('can delete an empty bucket', async () => {
        const bucketName = await createBucket(client)
        const deleteBucketRequest = new DeleteBucketCommand({
          Bucket: bucketName,
        })

        const resp = await client.send(deleteBucketRequest)
        expect(resp.$metadata.httpStatusCode).toBe(204)
      })

      it('cannot delete a non empty bucket', async () => {
        const bucketName = await createBucket(client)
        await uploadFile(client, bucketName, 'test-1.jpg', 1)
        const deleteBucketRequest = new DeleteBucketCommand({
          Bucket: bucketName,
        })

        try {
          await client.send(deleteBucketRequest)
          throw new Error('Should not reach here')
        } catch (e) {
          expect((e as Error).message).not.toBe('Should not reach here')
          expect((e as S3ServiceException).$metadata.httpStatusCode).toBe(409)
          expect((e as S3ServiceException).message).toBe(
            'The bucket you tried to delete is not empty'
          )
        }
      })
    })

    describe('HeadBucketCommand', () => {
      it('return bucket information when exists', async () => {
        const bucketName = await createBucket(client)
        const headBucketRequest = new HeadBucketCommand({
          Bucket: bucketName,
        })

        const resp = await client.send(headBucketRequest)
        expect(resp.$metadata.httpStatusCode).toBe(200)
        expect(resp.BucketRegion).toBe(storageS3Region)
      })
      it('will return bucket not found error', async () => {
        const headBucketRequest = new HeadBucketCommand({
          Bucket: 'dont-exist-bucket',
        })

        try {
          await client.send(headBucketRequest)
          throw new Error('Should not reach here')
        } catch (e) {
          expect((e as S3ServiceException).$metadata.httpStatusCode).toBe(404)
        }
      })
    })

    describe('ListBucketsCommand', () => {
      it('can list buckets', async () => {
        await createBucket(client)
        const listBuckets = new ListBucketsCommand({})

        const resp = await client.send(listBuckets)
        expect(resp.Buckets?.length || 0).toBeGreaterThan(0)
      })
    })

    describe('ListObjectCommand', () => {
      it('list empty bucket', async () => {
        const bucket = await createBucket(client)
        const listBuckets = new ListObjectsCommand({
          Bucket: bucket,
        })

        const resp = await client.send(listBuckets)
        expect(resp.Contents?.length).toBe(undefined)
      })

      it('list all keys', async () => {
        const bucket = await createBucket(client)
        const listBuckets = new ListObjectsCommand({
          Bucket: bucket,
        })

        await Promise.all([
          uploadFile(client, bucket, 'test-1.jpg', 1),
          uploadFile(client, bucket, 'prefix-1/test-1.jpg', 1),
          uploadFile(client, bucket, 'prefix-3/test-1.jpg', 1),
        ])

        const resp = await client.send(listBuckets)
        expect(resp.Contents?.length).toBe(3)
      })
    })

    describe('ListObjectsV2Command', () => {
      it('list empty bucket', async () => {
        const bucket = await createBucket(client)
        const listBuckets = new ListObjectsV2Command({
          Bucket: bucket,
        })

        const resp = await client.send(listBuckets)
        expect(resp.Contents?.length).toBe(undefined)
      })

      it('list all keys', async () => {
        const bucket = await createBucket(client)
        const listBuckets = new ListObjectsV2Command({
          Bucket: bucket,
        })

        await Promise.all([
          uploadFile(client, bucket, 'test-1.jpg', 1),
          uploadFile(client, bucket, 'prefix-1/test-1.jpg', 1),
          uploadFile(client, bucket, 'prefix-3/test-1.jpg', 1),
        ])

        const resp = await client.send(listBuckets)
        expect(resp.Contents?.length).toBe(3)
      })

      it('list keys and common prefixes', async () => {
        const bucket = await createBucket(client)
        const listBuckets = new ListObjectsV2Command({
          Bucket: bucket,
          Delimiter: '/',
        })

        await Promise.all([
          uploadFile(client, bucket, 'test-1.jpg', 1),
          uploadFile(client, bucket, 'prefix-1/test-1.jpg', 1),
          uploadFile(client, bucket, 'prefix-3/test-1.jpg', 1),
        ])

        const resp = await client.send(listBuckets)
        expect(resp.Contents?.length).toBe(1)
        expect(resp.CommonPrefixes?.length).toBe(2)
      })

      it('paginate keys and common prefixes', async () => {
        const bucket = await createBucket(client)
        const listBucketsPage1 = new ListObjectsV2Command({
          Bucket: bucket,
          Delimiter: '/',
          MaxKeys: 1,
        })

        await Promise.all([
          uploadFile(client, bucket, 'test-1.jpg', 1),
          uploadFile(client, bucket, 'prefix-1/test-1.jpg', 1),
          uploadFile(client, bucket, 'prefix-3/test-1.jpg', 1),
        ])

        const objectsPage1 = await client.send(listBucketsPage1)
        expect(objectsPage1.Contents?.length).toBe(undefined)
        expect(objectsPage1.CommonPrefixes?.length).toBe(1)
        expect(objectsPage1.CommonPrefixes?.[0].Prefix).toBe('prefix-1/')

        const listBucketsPage2 = new ListObjectsV2Command({
          Bucket: bucket,
          Delimiter: '/',
          MaxKeys: 1,
          ContinuationToken: objectsPage1.NextContinuationToken,
        })

        const objectsPage2 = await client.send(listBucketsPage2)

        expect(objectsPage2.Contents?.length).toBe(undefined)
        expect(objectsPage2.CommonPrefixes?.length).toBe(1)
        expect(objectsPage2.CommonPrefixes?.[0].Prefix).toBe('prefix-3/')

        const listBucketsPage3 = new ListObjectsV2Command({
          Bucket: bucket,
          Delimiter: '/',
          MaxKeys: 1,
          ContinuationToken: objectsPage2.NextContinuationToken,
        })

        const objectsPage3 = await client.send(listBucketsPage3)

        expect(objectsPage3.Contents?.length).toBe(1)
        expect(objectsPage3.CommonPrefixes?.length).toBe(undefined)
        expect(objectsPage3.Contents?.[0].Key).toBe('test-1.jpg')
      })

      it('paginate keys and common prefixes using StartAfter', async () => {
        const bucket = await createBucket(client)
        const listBucketsPage1 = new ListObjectsV2Command({
          Bucket: bucket,
          Delimiter: '/',
          MaxKeys: 1,
          StartAfter: 'prefix-1/test-1.jpg',
        })

        await Promise.all([
          uploadFile(client, bucket, 'test-1.jpg', 1),
          uploadFile(client, bucket, 'prefix-1/test-1.jpg', 1),
          uploadFile(client, bucket, 'prefix-3/test-1.jpg', 1),
        ])

        const objectsPage1 = await client.send(listBucketsPage1)
        expect(objectsPage1.Contents?.length).toBe(undefined)
        expect(objectsPage1.CommonPrefixes?.length).toBe(1)
        expect(objectsPage1.CommonPrefixes?.[0].Prefix).toBe('prefix-3/')
        expect(objectsPage1.IsTruncated).toBe(true)

        const listBucketsPage2 = new ListObjectsV2Command({
          Bucket: bucket,
          Delimiter: '/',
          MaxKeys: 1,
          ContinuationToken: objectsPage1.NextContinuationToken,
        })

        const objectsPage2 = await client.send(listBucketsPage2)

        expect(objectsPage2.Contents?.length).toBe(1)
        expect(objectsPage2.CommonPrefixes?.length).toBe(undefined)

        const listBucketsPage3 = new ListObjectsV2Command({
          Bucket: bucket,
          Delimiter: '/',
          MaxKeys: 1,
          ContinuationToken: objectsPage2.NextContinuationToken,
          StartAfter: 'prefix-3/test-1.jpg',
        })

        const objectsPage3 = await client.send(listBucketsPage3)

        expect(objectsPage3.Contents?.length).toBe(1)
        expect(objectsPage3.CommonPrefixes?.length).toBe(undefined)
        expect(objectsPage3.Contents?.[0].Key).toBe('test-1.jpg')
        expect(objectsPage3.IsTruncated).toBe(false)
      })
    })

    describe('MultiPart Form Data Upload', () => {
      it('can upload using multipart/form-data', async () => {
        const bucketName = await createBucket(client)
        const signedURL = await createPresignedPost(client, {
          Bucket: bucketName,
          Key: 'test.jpg',
          Expires: 5000,
          Fields: {
            'Content-Type': 'image/jpg',
            'X-Amz-Meta-Custom': 'meta-field',
          },
        })

        const formData = new FormData()
        Object.keys(signedURL.fields).forEach((key) => {
          formData.set(key, signedURL.fields[key])
        })

        const data = Buffer.alloc(1024 * 1024)
        formData.set('file', new Blob([data]), 'test.jpg')

        const resp = await axios.post(signedURL.url, formData, {
          validateStatus: () => true,
        })

        expect(resp.status).toBe(200)
      })

      it('prevent uploading files larger than the maxFileSize limit', async () => {
        mergeConfig({
          uploadFileSizeLimit: 1024 * 1024,
        })
        const bucketName = await createBucket(client)
        const signedURL = await createPresignedPost(client, {
          Bucket: bucketName,
          Key: 'test.jpg',
          Expires: 5000,
          Fields: {
            'Content-Type': 'image/jpg',
          },
        })

        const formData = new FormData()
        Object.keys(signedURL.fields).forEach((key) => {
          formData.set(key, signedURL.fields[key])
        })

        const data = Buffer.alloc(1024 * 1024 * 2)
        formData.set('file', new Blob([data]), 'test.jpg')

        const resp = await axios.post(signedURL.url, formData, {
          validateStatus: () => true,
        })

        expect(resp.status).toBe(413)
        expect(resp.statusText).toBe('Payload Too Large')
      })
    })

    describe('MultiPartUpload', () => {
      it('creates a multi part upload', async () => {
        const bucketName = await createBucket(client)
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
        const bucketName = await createBucket(client)
        const createMultiPartUpload = new CreateMultipartUploadCommand({
          Bucket: bucketName,
          Key: 'test-1.jpg',
          ContentType: 'image/jpg',
          CacheControl: 'max-age=2000',
        })
        const resp = await client.send(createMultiPartUpload)
        expect(resp.UploadId).toBeTruthy()

        const data = Buffer.alloc(1024 * 1024 * 5)

        const uploadPart = new UploadPartCommand({
          Bucket: bucketName,
          Key: 'test-1.jpg',
          ContentLength: data.length,
          UploadId: resp.UploadId,
          Body: data,
          PartNumber: 1,
        })

        const partResp = await client.send(uploadPart)
        expect(partResp.ETag).toBeTruthy()
      })

      it('completes a multipart upload', async () => {
        const bucketName = await createBucket(client)
        const createMultiPartUpload = new CreateMultipartUploadCommand({
          Bucket: bucketName,
          Key: 'test-1.jpg',
          ContentType: 'image/jpg',
          CacheControl: 'max-age=2000',
        })
        const resp = await client.send(createMultiPartUpload)
        expect(resp.UploadId).toBeTruthy()

        const data = Buffer.alloc(1024 * 1024 * 5)
        const uploadPart = new UploadPartCommand({
          Bucket: bucketName,
          Key: 'test-1.jpg',
          ContentLength: data.length,
          UploadId: resp.UploadId,
          Body: data,
          PartNumber: 1,
        })

        const part1 = await client.send(uploadPart)

        const completeMultiPartUpload = new CompleteMultipartUploadCommand({
          Bucket: bucketName,
          Key: 'test-1.jpg',
          UploadId: resp.UploadId,
          MultipartUpload: {
            Parts: [
              {
                PartNumber: 1,
                ETag: part1.ETag,
              },
            ],
          },
        })

        const completeResp = await client.send(completeMultiPartUpload)
        expect(completeResp.$metadata.httpStatusCode).toBe(200)
        expect(completeResp.Key).toEqual('test-1.jpg')
      })

      it('aborts a multipart upload', async () => {
        const bucketName = await createBucket(client)
        const createMultiPartUpload = new CreateMultipartUploadCommand({
          Bucket: bucketName,
          Key: 'test-1.jpg',
          ContentType: 'image/jpg',
          CacheControl: 'max-age=2000',
        })
        const resp = await client.send(createMultiPartUpload)
        expect(resp.UploadId).toBeTruthy()

        const data = Buffer.alloc(1024 * 1024 * 5)
        const uploadPart = new UploadPartCommand({
          Bucket: bucketName,
          Key: 'test-1.jpg',
          ContentLength: data.length,
          UploadId: resp.UploadId,
          Body: data,
          PartNumber: 1,
        })

        await client.send(uploadPart)

        const completeMultiPartUpload = new AbortMultipartUploadCommand({
          Bucket: bucketName,
          Key: 'test-1.jpg',
          UploadId: resp.UploadId,
        })

        const completeResp = await client.send(completeMultiPartUpload)
        expect(completeResp.$metadata.httpStatusCode).toBe(200)
      })

      it('upload a file using putObject', async () => {
        const bucketName = await createBucket(client)

        const putObject = new PutObjectCommand({
          Bucket: bucketName,
          Key: 'test-1-put-object.jpg',
          Body: Buffer.alloc(1024 * 1024 * 12),
        })

        const resp = await client.send(putObject)
        expect(resp.$metadata.httpStatusCode).toEqual(200)
      })

      it('upload a broken JSON body using putObject ', async () => {
        const bucketName = await createBucket(client)

        const putObject = new PutObjectCommand({
          Bucket: bucketName,
          Key: 'test-1-put-object.jpg',
          ContentType: 'application/json',
          Body: '{"hello": "world"', // (no-closing tag)
        })

        const resp = await client.send(putObject)
        expect(resp.$metadata.httpStatusCode).toEqual(200)
      })

      it('upload a file using putObject with custom metadata', async () => {
        const bucketName = await createBucket(client)

        const putObject = new PutObjectCommand({
          Bucket: bucketName,
          Key: 'test-1-put-object.jpg',
          Body: Buffer.alloc(1024 * 1024 * 12),
          Metadata: {
            nice: '1111',
            test2: 'test3',
          },
        })

        const resp = await client.send(putObject)
        expect(resp.$metadata.httpStatusCode).toEqual(200)

        const getObject = new HeadObjectCommand({
          Bucket: bucketName,
          Key: 'test-1-put-object.jpg',
        })

        const headResp = await client.send(getObject)
        expect(headResp.Metadata?.nice).toEqual('1111')
        expect(headResp.Metadata?.test2).toEqual('test3')
      })

      it('it will not allow to upload a file using putObject when exceeding maxFileSize', async () => {
        const bucketName = await createBucket(client)

        mergeConfig({
          uploadFileSizeLimit: 1024 * 1024 * 10,
        })

        const putObject = new PutObjectCommand({
          Bucket: bucketName,
          Key: 'test-1-put-object.jpg',
          Body: Buffer.alloc(1024 * 1024 * 12),
        })

        try {
          await client.send(putObject)
          throw new Error('Should not reach here')
        } catch (e) {
          expect((e as Error).message).not.toEqual('Should not reach here')
          expect((e as S3ServiceException).$metadata.httpStatusCode).toEqual(413)
          expect((e as S3ServiceException).message).toEqual(
            'The object exceeded the maximum allowed size'
          )
          expect((e as S3ServiceException).name).toEqual('EntityTooLarge')
        }
      })

      it('will not allow uploading a file that exceeded the maxFileSize', async () => {
        const bucketName = await createBucket(client)

        mergeConfig({
          uploadFileSizeLimit: 1024 * 1024 * 10,
        })

        const uploader = new Upload({
          client: client,
          leavePartsOnError: true,

          params: {
            Bucket: bucketName,
            Key: 'test-1.jpg',
            ContentType: 'image/jpg',
            Body: Buffer.alloc(1024 * 1024 * 12),
          },
        })

        try {
          await uploader.done()
          throw new Error('Should not reach here')
        } catch (e) {
          expect((e as Error).message).not.toEqual('Should not reach here')
          expect((e as S3ServiceException).$metadata.httpStatusCode).toEqual(413)
          expect((e as S3ServiceException).message).toEqual(
            'The object exceeded the maximum allowed size'
          )
          expect((e as S3ServiceException).name).toEqual('EntityTooLarge')
        }
      })

      it('will not allow uploading a part that exceeded the maxFileSize', async () => {
        const bucketName = await createBucket(client, 'try-test-1')

        mergeConfig({
          uploadFileSizeLimit: 1024 * 1024 * 10,
        })

        const createMultiPartUpload = new CreateMultipartUploadCommand({
          Bucket: bucketName,
          Key: 'test-1.jpg',
          ContentType: 'image/jpg',
          CacheControl: 'max-age=2000',
        })
        const resp = await client.send(createMultiPartUpload)
        expect(resp.UploadId).toBeTruthy()

        const readable = new ReadableStreamBuffer({
          frequency: 500,
          chunkSize: 1024 * 1024 * 3,
        })

        readable.put(Buffer.alloc(1024 * 1024 * 12))
        readable.stop()

        const uploadPart = new UploadPartCommand({
          Bucket: bucketName,
          Key: 'test-1.jpg',
          UploadId: resp.UploadId,
          Body: readable,
          PartNumber: 1,
          ContentLength: 1024 * 1024 * 12,
        })

        try {
          await client.send(uploadPart)
          throw new Error('Should not reach here')
        } catch (e) {
          expect((e as Error).message).not.toEqual('Should not reach here')
          expect((e as S3ServiceException).$metadata.httpStatusCode).toEqual(413)
          expect((e as S3ServiceException).message).toEqual(
            'The object exceeded the maximum allowed size'
          )
          expect((e as S3ServiceException).name).toEqual('EntityTooLarge')
        }
      })

      it('upload a file using multipart upload', async () => {
        const bucketName = await createBucket(client)

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

    describe('GetObject', () => {
      it('can get an existing object', async () => {
        const bucketName = await createBucket(client)
        const key = 'test-1.jpg'
        await uploadFile(client, bucketName, key, 1)

        const getObject = new GetObjectCommand({
          Bucket: bucketName,
          Key: key,
        })

        const resp = await client.send(getObject)
        const data = await resp.Body?.transformToByteArray()
        expect(data).toBeTruthy()
        expect(resp.ETag).toBeTruthy()
      })

      it('will return an error when object does not exist', async () => {
        const bucketName = await createBucket(client)
        const key = 'test-1.jpg'

        const getObject = new GetObjectCommand({
          Bucket: bucketName,
          Key: key,
        })

        try {
          await client.send(getObject)
        } catch (e) {
          expect((e as S3ServiceException).$metadata.httpStatusCode).toEqual(404)
          expect((e as S3ServiceException).message).toEqual('Object not found')
          expect((e as S3ServiceException).name).toEqual('NoSuchKey')
        }
      })

      it('can get an object using range requests', async () => {
        const bucketName = await createBucket(client)
        const key = 'test-1.jpg'
        await uploadFile(client, bucketName, key, 1)

        const getObject = new GetObjectCommand({
          Bucket: bucketName,
          Key: key,
          Range: 'bytes=0-100',
        })

        const resp = await client.send(getObject)
        const data = await resp.Body?.transformToByteArray()
        expect(resp.$metadata.httpStatusCode).toEqual(206)
        expect(data).toBeTruthy()
        expect(resp.ETag).toBeTruthy()
      })
    })

    describe('DeleteObjectCommand', () => {
      it('can delete an existing object', async () => {
        const bucketName = await createBucket(client)
        const key = 'test-1.jpg'
        await uploadFile(client, bucketName, key, 1)

        const deleteObject = new DeleteObjectCommand({
          Bucket: bucketName,
          Key: key,
        })

        await client.send(deleteObject)

        const getObject = new GetObjectCommand({
          Bucket: bucketName,
          Key: key,
        })

        try {
          await client.send(getObject)
        } catch (e) {
          expect((e as S3ServiceException).$metadata.httpStatusCode).toEqual(404)
        }
      })
    })

    describe('DeleteObjectsCommand', () => {
      it('can delete a single object', async () => {
        const bucketName = await createBucket(client)
        await Promise.all([uploadFile(client, bucketName, 'test-1.jpg', 1)])

        const deleteObjectsCommand = new DeleteObjectsCommand({
          Bucket: bucketName,
          Delete: {
            Objects: [
              {
                Key: 'test-1.jpg',
              },
            ],
          },
        })

        const deleteResp = await client.send(deleteObjectsCommand)

        expect(deleteResp.Deleted).toEqual([
          {
            Key: 'test-1.jpg',
          },
        ])

        const listObjectsCommand = new ListObjectsV2Command({
          Bucket: bucketName,
        })

        const resp = await client.send(listObjectsCommand)
        expect(resp.Contents).toBe(undefined)
      })

      it('can delete multiple objects', async () => {
        const bucketName = await createBucket(client)
        await Promise.all([
          uploadFile(client, bucketName, 'test-1.jpg', 1),
          uploadFile(client, bucketName, 'test-2.jpg', 1),
          uploadFile(client, bucketName, 'test-3.jpg', 1),
        ])

        const deleteObjectsCommand = new DeleteObjectsCommand({
          Bucket: bucketName,
          Delete: {
            Objects: [
              {
                Key: 'test-1.jpg',
              },
              {
                Key: 'test-2.jpg',
              },
              {
                Key: 'test-3.jpg',
              },
            ],
          },
        })

        const deleteResp = await client.send(deleteObjectsCommand)

        expect(deleteResp.Deleted).toEqual([
          {
            Key: 'test-1.jpg',
          },
          {
            Key: 'test-2.jpg',
          },
          {
            Key: 'test-3.jpg',
          },
        ])

        const listObjectsCommand = new ListObjectsV2Command({
          Bucket: bucketName,
        })

        const resp = await client.send(listObjectsCommand)
        expect(resp.Contents).toBe(undefined)
      })

      it('try to delete multiple objects that dont exists', async () => {
        const bucketName = await createBucket(client)

        await uploadFile(client, bucketName, 'test-1.jpg', 1)

        const deleteObjectsCommand = new DeleteObjectsCommand({
          Bucket: bucketName,
          Delete: {
            Objects: [
              {
                Key: 'test-1.jpg',
              },
              {
                Key: 'test-2.jpg',
              },
              {
                Key: 'test-3.jpg',
              },
            ],
          },
        })

        const deleteResp = await client.send(deleteObjectsCommand)
        expect(deleteResp.Deleted).toEqual([
          {
            Key: 'test-1.jpg',
          },
        ])
        expect(deleteResp.Errors).toEqual([
          {
            Key: 'test-2.jpg',
            Code: 'AccessDenied',
            Message:
              "You do not have permission to delete this object or the object doesn't exists",
          },
          {
            Key: 'test-3.jpg',
            Code: 'AccessDenied',
            Message:
              "You do not have permission to delete this object or the object doesn't exists",
          },
        ])

        const listObjectsCommand = new ListObjectsV2Command({
          Bucket: bucketName,
        })

        const resp = await client.send(listObjectsCommand)
        expect(resp.Contents).toBe(undefined)
      })
    })

    describe('CopyObjectCommand', () => {
      it('will copy an object in the same bucket', async () => {
        const bucketName = await createBucket(client)
        await uploadFile(client, bucketName, 'test-copy-1.jpg', 1)

        const copyObjectCommand = new CopyObjectCommand({
          Bucket: bucketName,
          Key: 'test-copied-2.jpg',
          CopySource: `${bucketName}/test-copy-1.jpg`,
        })

        const resp = await client.send(copyObjectCommand)
        expect(resp.CopyObjectResult?.ETag).toBeTruthy()
      })

      it('will copy an object in a different bucket', async () => {
        const bucketName1 = await createBucket(client)
        const bucketName2 = await createBucket(client)
        await uploadFile(client, bucketName1, 'test-copy-1.jpg', 1)

        const copyObjectCommand = new CopyObjectCommand({
          Bucket: bucketName2,
          Key: 'test-copied-2.jpg',
          CopySource: `${bucketName1}/test-copy-1.jpg`,
        })

        const resp = await client.send(copyObjectCommand)
        expect(resp.CopyObjectResult?.ETag).toBeTruthy()
      })

      it('will copy an object overwriting the metadata', async () => {
        const bucketName = await createBucket(client)
        await uploadFile(client, bucketName, 'test-copy-1.jpg', 1)

        const copyObjectCommand = new CopyObjectCommand({
          Bucket: bucketName,
          Key: 'test-copied-2.png',
          CopySource: `${bucketName}/test-copy-1.jpg`,
          ContentType: 'image/png',
          CacheControl: 'max-age=2009',
          MetadataDirective: 'REPLACE',
        })

        const resp = await client.send(copyObjectCommand)
        expect(resp.CopyObjectResult?.ETag).toBeTruthy()

        const headObjectCommand = new HeadObjectCommand({
          Bucket: bucketName,
          Key: 'test-copied-2.png',
        })

        const headObj = await client.send(headObjectCommand)
        expect(headObj.ContentType).toBe('image/png')
        expect(headObj.CacheControl).toBe('max-age=2009')
      })

      it('will allow copying an object in the same path, just altering its metadata', async () => {
        const bucketName = await createBucket(client)
        const fileName = 'test-copy-1.jpg'

        await uploadFile(client, bucketName, fileName, 1)

        const copyObjectCommand = new CopyObjectCommand({
          Bucket: bucketName,
          Key: fileName,
          CopySource: `${bucketName}/${fileName}`,
          ContentType: 'image/png',
          CacheControl: 'max-age=2009',
          MetadataDirective: 'REPLACE',
        })

        const resp = await client.send(copyObjectCommand)
        expect(resp.CopyObjectResult?.ETag).toBeTruthy()

        const headObjectCommand = new HeadObjectCommand({
          Bucket: bucketName,
          Key: fileName,
        })

        const headObj = await client.send(headObjectCommand)
        expect(headObj.ContentType).toBe('image/png')
        expect(headObj.CacheControl).toBe('max-age=2009')
      })

      it('will not be able to copy an object that doesnt exists', async () => {
        const bucketName1 = await createBucket(client)
        await uploadFile(client, bucketName1, 'test-copy-1.jpg', 1)

        const copyObjectCommand = new CopyObjectCommand({
          Bucket: bucketName1,
          Key: 'test-copied-2.jpg',
          CopySource: `${bucketName1}/test-dont-exists.jpg`,
        })

        try {
          await client.send(copyObjectCommand)
          throw new Error('Should not reach here')
        } catch (e) {
          expect((e as Error).message).not.toEqual('Should not reach here')
          expect((e as S3ServiceException).$metadata.httpStatusCode).toEqual(404)
          expect((e as S3ServiceException).message).toEqual('Object not found')
        }
      })
    })

    describe('ListMultipartUploads', () => {
      it('will list multipart uploads', async () => {
        const bucketName = await createBucket(client)
        const createMultiPartUpload = (key: string) =>
          new CreateMultipartUploadCommand({
            Bucket: bucketName,
            Key: key,
            ContentType: 'image/jpg',
            CacheControl: 'max-age=2000',
          })

        await Promise.all([
          client.send(createMultiPartUpload('test-1.jpg')),
          client.send(createMultiPartUpload('test-2.jpg')),
          client.send(createMultiPartUpload('test-3.jpg')),
          client.send(createMultiPartUpload('nested/test-4.jpg')),
        ])

        const listMultipartUploads = new ListMultipartUploadsCommand({
          Bucket: bucketName,
        })

        const resp = await client.send(listMultipartUploads)
        expect(resp.Uploads?.length).toBe(4)
        expect(resp.Uploads?.[0].Key).toBe('nested/test-4.jpg')
        expect(resp.Uploads?.[1].Key).toBe('test-1.jpg')
        expect(resp.Uploads?.[2].Key).toBe('test-2.jpg')
        expect(resp.Uploads?.[3].Key).toBe('test-3.jpg')
      })

      it('will list multipart uploads with delimiter', async () => {
        const bucketName = await createBucket(client)
        const createMultiPartUpload = (key: string) =>
          new CreateMultipartUploadCommand({
            Bucket: bucketName,
            Key: key,
            ContentType: 'image/jpg',
            CacheControl: 'max-age=2000',
          })

        await Promise.all([
          client.send(createMultiPartUpload('test-1.jpg')),
          client.send(createMultiPartUpload('test-2.jpg')),
          client.send(createMultiPartUpload('test-3.jpg')),
          client.send(createMultiPartUpload('nested/test-4.jpg')),
        ])

        const listMultipartUploads = new ListMultipartUploadsCommand({
          Bucket: bucketName,
          Delimiter: '/',
        })

        const resp = await client.send(listMultipartUploads)
        expect(resp.Uploads?.length).toBe(3)
        expect(resp.CommonPrefixes?.length).toBe(1)
        expect(resp.Uploads?.[0].Key).toBe('test-1.jpg')
        expect(resp.Uploads?.[1].Key).toBe('test-2.jpg')
        expect(resp.Uploads?.[2].Key).toBe('test-3.jpg')
        expect(resp.CommonPrefixes?.[0].Prefix).toBe('nested/')
      })
    })

    it('will list multipart uploads with delimiter and pagination', async () => {
      const bucketName = await createBucket(client)
      const createMultiPartUpload = (key: string) =>
        new CreateMultipartUploadCommand({
          Bucket: bucketName,
          Key: key,
          ContentType: 'image/jpg',
          CacheControl: 'max-age=2000',
        })

      await Promise.all([
        client.send(createMultiPartUpload('test-1.jpg')),
        client.send(createMultiPartUpload('test-2.jpg')),
        client.send(createMultiPartUpload('test-3.jpg')),
        client.send(createMultiPartUpload('nested/test-4.jpg')),
      ])

      const listMultipartUploads1 = new ListMultipartUploadsCommand({
        Bucket: bucketName,
        Delimiter: '/',
        MaxUploads: 1,
      })

      const page1 = await client.send(listMultipartUploads1)
      expect(page1.Uploads?.length).toBe(undefined)
      expect(page1.CommonPrefixes?.length).toBe(1)
      expect(page1.CommonPrefixes?.[0].Prefix).toBe('nested/')

      const listMultipartUploads2 = new ListMultipartUploadsCommand({
        Bucket: bucketName,
        Delimiter: '/',
        MaxUploads: 1,
        KeyMarker: page1.NextKeyMarker,
      })

      const page2 = await client.send(listMultipartUploads2)
      expect(page2.CommonPrefixes?.length).toBe(undefined)
      expect(page2.Uploads?.length).toBe(1)
      expect(page2.Uploads?.[0].Key).toBe('test-1.jpg')

      const listMultipartUploads3 = new ListMultipartUploadsCommand({
        Bucket: bucketName,
        Delimiter: '/',
        MaxUploads: 1,
        KeyMarker: page2.NextKeyMarker,
      })

      const page3 = await client.send(listMultipartUploads3)
      expect(page3.CommonPrefixes?.length).toBe(undefined)
      expect(page3.Uploads?.length).toBe(1)
      expect(page3.Uploads?.[0].Key).toBe('test-2.jpg')

      const listMultipartUploads4 = new ListMultipartUploadsCommand({
        Bucket: bucketName,
        Delimiter: '/',
        MaxUploads: 1,
        KeyMarker: page3.NextKeyMarker,
      })

      const page4 = await client.send(listMultipartUploads4)
      expect(page4.CommonPrefixes?.length).toBe(undefined)
      expect(page4.Uploads?.length).toBe(1)
      expect(page4.Uploads?.[0].Key).toBe('test-3.jpg')
    })

    describe('ListParts', () => {
      it('cannot list parts for an upload that doesnt exists', async () => {
        const listParts = new ListPartsCommand({
          Bucket: 'no-bucket',
          Key: 'test-1.jpg',
          UploadId: 'test-upload-id',
        })

        try {
          await client.send(listParts)
          throw new Error('Should not reach here')
        } catch (e) {
          expect((e as Error).message).not.toBe('Should not reach here')
          expect((e as S3ServiceException).$metadata.httpStatusCode).toBe(404)
          expect((e as S3ServiceException).message).toBe('Upload not found')
        }
      })

      it('will list parts of a multipart upload', async () => {
        const bucket = await createBucket(client)
        const createMultiPartUpload = new CreateMultipartUploadCommand({
          Bucket: bucket,
          Key: 'test-1.jpg',
          ContentType: 'image/jpg',
          CacheControl: 'max-age=2000',
        })
        const resp = await client.send(createMultiPartUpload)
        expect(resp.UploadId).toBeTruthy()

        const data = Buffer.alloc(1024 * 1024 * 5)
        const uploadPart = (partNumber: number) =>
          new UploadPartCommand({
            Bucket: bucket,
            Key: 'test-1.jpg',
            ContentLength: data.length,
            UploadId: resp.UploadId,
            Body: data,
            PartNumber: partNumber,
          })

        await Promise.all([
          client.send(uploadPart(1)),
          client.send(uploadPart(2)),
          client.send(uploadPart(3)),
        ])

        const listParts = new ListPartsCommand({
          Bucket: bucket,
          Key: 'test-1.jpg',
          UploadId: resp.UploadId,
        })

        const parts = await client.send(listParts)
        expect(parts.Parts?.length).toBe(3)
      })

      it('will list parts of a multipart upload with pagination', async () => {
        const bucket = await createBucket(client)
        const createMultiPartUpload = new CreateMultipartUploadCommand({
          Bucket: bucket,
          Key: 'test-1.jpg',
          ContentType: 'image/jpg',
          CacheControl: 'max-age=2000',
        })
        const resp = await client.send(createMultiPartUpload)
        expect(resp.UploadId).toBeTruthy()

        const data = Buffer.alloc(1024 * 1024 * 5)
        const uploadPart = (partNumber: number) =>
          new UploadPartCommand({
            Bucket: bucket,
            Key: 'test-1.jpg',
            ContentLength: data.length,
            UploadId: resp.UploadId,
            Body: data,
            PartNumber: partNumber,
          })

        await Promise.all([
          client.send(uploadPart(1)),
          client.send(uploadPart(2)),
          client.send(uploadPart(3)),
        ])

        const listParts1 = new ListPartsCommand({
          Bucket: bucket,
          Key: 'test-1.jpg',
          UploadId: resp.UploadId,
          MaxParts: 1,
        })

        const parts1 = await client.send(listParts1)
        expect(parts1.Parts?.length).toBe(1)
        expect(parts1.Parts?.[0].PartNumber).toBe(1)

        const listParts2 = new ListPartsCommand({
          Bucket: bucket,
          Key: 'test-1.jpg',
          UploadId: resp.UploadId,
          MaxParts: 1,
          PartNumberMarker: parts1.NextPartNumberMarker,
        })

        const parts2 = await client.send(listParts2)
        expect(parts2.Parts?.length).toBe(1)
        expect(parts2.Parts?.[0].PartNumber).toBe(2)

        const listParts3 = new ListPartsCommand({
          Bucket: bucket,
          Key: 'test-1.jpg',
          UploadId: resp.UploadId,
          MaxParts: 1,
          PartNumberMarker: parts2.NextPartNumberMarker,
        })

        const parts3 = await client.send(listParts3)
        expect(parts3.Parts?.length).toBe(1)
        expect(parts3.Parts?.[0].PartNumber).toBe(3)
      })
    })

    describe('UploadPartCopyCommand', () => {
      it('will copy a part from an existing object and upload it as a part', async () => {
        const bucket = await createBucket(client)

        const sourceKey = `${randomUUID()}.jpg`
        const newKey = `new-${randomUUID()}.jpg`

        await uploadFile(client, bucket, sourceKey, 12)

        const createMultiPartUpload = new CreateMultipartUploadCommand({
          Bucket: bucket,
          Key: newKey,
          ContentType: 'image/jpg',
          CacheControl: 'max-age=2000',
        })
        const resp = await client.send(createMultiPartUpload)
        expect(resp.UploadId).toBeTruthy()

        const copyPart = new UploadPartCopyCommand({
          Bucket: bucket,
          Key: newKey,
          UploadId: resp.UploadId,
          PartNumber: 1,
          CopySource: `${bucket}/${sourceKey}`,
          CopySourceRange: `bytes=0-${1024 * 1024 * 4}`,
        })

        const copyResp = await client.send(copyPart)
        expect(copyResp.CopyPartResult?.ETag).toBeTruthy()
        expect(copyResp.CopyPartResult?.LastModified).toBeTruthy()

        const listPartsCmd = new ListPartsCommand({
          Bucket: bucket,
          Key: newKey,
          UploadId: resp.UploadId,
        })

        const parts = await client.send(listPartsCmd)
        expect(parts.Parts?.length).toBe(1)
      })
    })

    describe('S3 Presigned URL', () => {
      it('can call a simple method with presigned url', async () => {
        const bucket = await createBucket(client)
        const bucketVersioningCommand = new GetBucketVersioningCommand({
          Bucket: bucket,
        })
        const signedUrl = await getSignedUrl(client, bucketVersioningCommand, { expiresIn: 100 })
        const resp = await fetch(signedUrl)

        expect(resp.ok).toBeTruthy()
      })

      it('cannot request a presigned url if expired', async () => {
        const bucket = await createBucket(client)
        const bucketVersioningCommand = new GetBucketVersioningCommand({
          Bucket: bucket,
        })
        const signedUrl = await getSignedUrl(client, bucketVersioningCommand, { expiresIn: 1 })
        await new Promise((resolve) => setTimeout(resolve, 1500))
        const resp = await fetch(signedUrl)

        expect(resp.ok).toBeFalsy()
        expect(resp.status).toBe(400)
      })

      it('can upload with presigned URL', async () => {
        const bucket = await createBucket(client)
        const key = 'test-1.jpg'
        const body = Buffer.alloc(1024 * 1024 * 2)

        const uploadUrl = await getSignedUrl(
          client,
          new PutObjectCommand({
            Bucket: bucket,
            Key: key,
            Body: body,
          }),
          { expiresIn: 100 }
        )

        const resp = await fetch(uploadUrl, {
          method: 'PUT',
          body: body,
          headers: {
            'Content-Length': body.length.toString(),
          },
        })

        expect(resp.ok).toBeTruthy()
      })

      it('can fetch an asset via presigned URL', async () => {
        const bucket = await createBucket(client)
        const key = 'test-1.jpg'

        await uploadFile(client, bucket, key, 2)

        const getUrl = await getSignedUrl(
          client,
          new GetObjectCommand({
            Bucket: bucket,
            Key: key,
          }),
          { expiresIn: 100 }
        )

        const resp = await fetch(getUrl)

        expect(resp.ok).toBeTruthy()
      })
    })
  })
})
