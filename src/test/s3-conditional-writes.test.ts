import {
  CompleteMultipartUploadCommand,
  CreateBucketCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
  S3ServiceException,
  UploadPartCommand,
} from '@aws-sdk/client-s3'
import { randomUUID } from 'crypto'
import { FastifyInstance } from 'fastify'
import app from '../app'
import { getConfig } from '../config'

const { s3ProtocolAccessKeyId, s3ProtocolAccessKeySecret, storageS3Region } = getConfig()

type Conditions = { IfMatch?: string; IfNoneMatch?: string }

// Expected behaviour as documented by AWS:
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html
describe('S3 conditional writes', () => {
  let testApp: FastifyInstance
  let client: S3Client
  let bucketName: string

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
    bucketName = `conditional-writes-${randomUUID()}`
    await client.send(new CreateBucketCommand({ Bucket: bucketName }))
  })

  afterAll(async () => {
    client.destroy()
    await testApp.close()
  })

  const newKey = () => `conditional-${randomUUID()}.txt`

  const put = (Key: string, Body: string, conditions: Conditions = {}) =>
    client.send(new PutObjectCommand({ Bucket: bucketName, Key, Body, ...conditions }))

  const read = async (Key: string) => {
    const object = await client.send(new GetObjectCommand({ Bucket: bucketName, Key }))
    return object.Body?.transformToString()
  }

  const statusOf = (request: Promise<{ $metadata: { httpStatusCode?: number } }>) =>
    request.then(
      (result) => result.$metadata.httpStatusCode,
      (e: S3ServiceException) => e.$metadata.httpStatusCode
    )

  // Uploads one part, then returns a function that completes the upload with the given conditions
  async function startMultipartUpload(Key: string, Body: string) {
    const { UploadId } = await client.send(
      new CreateMultipartUploadCommand({ Bucket: bucketName, Key })
    )
    const { ETag } = await client.send(
      new UploadPartCommand({ Bucket: bucketName, Key, UploadId, PartNumber: 1, Body })
    )
    return (conditions: Conditions) =>
      client.send(
        new CompleteMultipartUploadCommand({
          Bucket: bucketName,
          Key,
          UploadId,
          MultipartUpload: { Parts: [{ ETag, PartNumber: 1 }] },
          ...conditions,
        })
      )
  }

  describe('If-None-Match: * (create only)', () => {
    it('PutObject does not replace an existing object', async () => {
      const key = newKey()
      await put(key, 'first')

      expect(await statusOf(put(key, 'second', { IfNoneMatch: '*' }))).toBe(412)
      expect(await read(key)).toBe('first')
    })

    it('exactly one of 8 concurrent PutObject calls creates the object', async () => {
      const key = newKey()
      const bodies = Array.from({ length: 8 }, (_, i) => `writer-${i}`)

      const statuses = await Promise.all(
        bodies.map((body) => statusOf(put(key, body, { IfNoneMatch: '*' })))
      )

      expect(statuses.filter((status) => status === 200)).toHaveLength(1)
      expect(statuses.filter((status) => status !== 200 && status !== 412)).toEqual([])
      expect(await read(key)).toBe(bodies[statuses.indexOf(200)])
    })

    it('CompleteMultipartUpload does not replace an existing object', async () => {
      const key = newKey()
      await put(key, 'first')
      const complete = await startMultipartUpload(key, 'second')

      expect(await statusOf(complete({ IfNoneMatch: '*' }))).toBe(412)
      expect(await read(key)).toBe('first')
    })
  })

  describe('If-Match: <etag> (compare and swap)', () => {
    it('PutObject with a stale ETag is rejected', async () => {
      const key = newKey()
      const v1 = await put(key, 'v1')
      await put(key, 'v2')

      expect(await statusOf(put(key, 'v3', { IfMatch: v1.ETag }))).toBe(412)
      expect(await read(key)).toBe('v2')
    })

    it('PutObject on a missing key returns 404 and creates nothing', async () => {
      const key = newKey()

      expect(
        await statusOf(put(key, 'v1', { IfMatch: '"0123456789abcdef0123456789abcdef"' }))
      ).toBe(404)
      expect(
        await statusOf(client.send(new GetObjectCommand({ Bucket: bucketName, Key: key })))
      ).toBe(404)
    })

    it('exactly one of 8 concurrent PutObject calls with the same ETag wins', async () => {
      const key = newKey()
      const { ETag } = await put(key, 'base')
      const bodies = Array.from({ length: 8 }, (_, i) => `writer-${i}`)

      const statuses = await Promise.all(
        bodies.map((body) => statusOf(put(key, body, { IfMatch: ETag })))
      )

      expect(statuses.filter((status) => status === 200)).toHaveLength(1)
      expect(await read(key)).toBe(bodies[statuses.indexOf(200)])
    })

    it('CompleteMultipartUpload with a stale ETag is rejected', async () => {
      const key = newKey()
      const v1 = await put(key, 'v1')
      await put(key, 'v2')
      const complete = await startMultipartUpload(key, 'v3')

      expect(await statusOf(complete({ IfMatch: v1.ETag }))).toBe(412)
      expect(await read(key)).toBe('v2')
    })

    it('DeleteObject with a stale ETag is rejected and keeps the object', async () => {
      const key = newKey()
      const v1 = await put(key, 'v1')
      await put(key, 'v2')

      const deleteStale = client.send(
        new DeleteObjectCommand({ Bucket: bucketName, Key: key, IfMatch: v1.ETag })
      )

      expect(await statusOf(deleteStale)).toBe(412)
      expect(await read(key)).toBe('v2')
    })
  })
})
