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
  UploadPartCommand,
  UploadPartCopyCommand,
} from '@aws-sdk/client-s3'
import { describeAcceptance } from '../support/config'
import { uniqueBucketName, uniqueObjectKey } from '../support/resources'
import { cleanupS3Bucket, createAcceptanceS3Client } from '../support/s3'

describeAcceptance(
  'S3 protocol contract',
  {
    destructive: true,
    profiles: ['smoke', 'core'],
  },
  () => {
    it('creates a bucket, writes, reads, lists, copies, and deletes an object', async () => {
      const client = createAcceptanceS3Client()
      const bucketName = uniqueBucketName('s3')
      const key = uniqueObjectKey('s3')
      const copyKey = uniqueObjectKey('s3-copy')
      const payload = Buffer.from('acceptance-s3-object')

      try {
        const created = await client.send(
          new CreateBucketCommand({
            ACL: 'public-read',
            Bucket: bucketName,
          })
        )
        expect(created.Location).toBeTruthy()

        await client.send(
          new PutObjectCommand({
            Body: payload,
            Bucket: bucketName,
            ContentType: 'text/plain',
            Key: key,
          })
        )

        const head = await client.send(
          new HeadObjectCommand({
            Bucket: bucketName,
            Key: key,
          })
        )
        expect(head.ContentLength).toBe(payload.length)
        expect(head.ContentType).toBe('text/plain')

        const downloaded = await client.send(
          new GetObjectCommand({
            Bucket: bucketName,
            Key: key,
          })
        )
        expect(await downloaded.Body?.transformToString()).toBe(payload.toString())

        const listed = await client.send(
          new ListObjectsV2Command({
            Bucket: bucketName,
            Prefix: key.split('/')[0],
          })
        )
        expect(listed.Contents?.map((object) => object.Key)).toContain(key)

        await client.send(
          new CopyObjectCommand({
            Bucket: bucketName,
            CopySource: `${bucketName}/${key}`,
            Key: copyKey,
          })
        )
        const copied = await client.send(
          new HeadObjectCommand({
            Bucket: bucketName,
            Key: copyKey,
          })
        )
        expect(copied.ContentLength).toBe(payload.length)

        await client.send(new DeleteObjectCommand({ Bucket: bucketName, Key: key }))
        await client.send(new DeleteObjectCommand({ Bucket: bucketName, Key: copyKey }))
      } finally {
        await cleanupS3Bucket(client, bucketName)
        client.destroy()
      }
    })

    it('completes a multipart upload and exposes uploaded parts', async () => {
      const client = createAcceptanceS3Client()
      const bucketName = uniqueBucketName('s3mp')
      const key = uniqueObjectKey('multipart', 'bin')
      const payload = Buffer.alloc(5 * 1024 * 1024, 7)

      try {
        await client.send(new CreateBucketCommand({ Bucket: bucketName }))
        const multipart = await client.send(
          new CreateMultipartUploadCommand({
            Bucket: bucketName,
            ContentType: 'application/octet-stream',
            Key: key,
          })
        )
        expect(multipart.UploadId).toBeTruthy()

        const part = await client.send(
          new UploadPartCommand({
            Body: payload,
            Bucket: bucketName,
            ContentLength: payload.length,
            Key: key,
            PartNumber: 1,
            UploadId: multipart.UploadId,
          })
        )
        expect(part.ETag).toBeTruthy()

        const parts = await client.send(
          new ListPartsCommand({
            Bucket: bucketName,
            Key: key,
            UploadId: multipart.UploadId,
          })
        )
        expect(parts.Parts?.map((listedPart) => listedPart.PartNumber)).toEqual([1])

        await client.send(
          new CompleteMultipartUploadCommand({
            Bucket: bucketName,
            Key: key,
            MultipartUpload: {
              Parts: [{ ETag: part.ETag, PartNumber: 1 }],
            },
            UploadId: multipart.UploadId,
          })
        )

        const head = await client.send(new HeadObjectCommand({ Bucket: bucketName, Key: key }))
        expect(head.ContentLength).toBe(payload.length)
      } finally {
        await cleanupS3Bucket(client, bucketName)
        client.destroy()
      }
    })
  }
)

describeAcceptance(
  'extended S3 protocol contract',
  {
    destructive: true,
    profiles: ['core'],
  },
  () => {
    it('covers bucket metadata, list-v1, range reads, bulk delete, empty-bucket deletion, upload-part-copy, and abort', async () => {
      const client = createAcceptanceS3Client()
      const bucketName = uniqueBucketName('s3x')
      const prefix = uniqueObjectKey('s3-prefix').replace(/\.txt$/, '')
      const keyA = `${prefix}/a.txt`
      const keyB = `${prefix}/b.txt`
      const keyC = `${prefix}/nested/c.txt`
      const copiedPartKey = `${prefix}/copied-part.txt`
      const abortedKey = `${prefix}/aborted.txt`
      const payload = Buffer.from('acceptance-s3-extended-object')

      try {
        await client.send(new CreateBucketCommand({ Bucket: bucketName }))

        await client.send(new HeadBucketCommand({ Bucket: bucketName }))
        const buckets = await client.send(new ListBucketsCommand({}))
        expect(buckets.Buckets?.map((bucket) => bucket.Name)).toContain(bucketName)

        const location = await client.send(new GetBucketLocationCommand({ Bucket: bucketName }))
        expect(location.LocationConstraint ?? '').toBeTypeOf('string')

        const versioning = await client.send(new GetBucketVersioningCommand({ Bucket: bucketName }))
        expect([undefined, 'Suspended']).toContain(versioning.Status)

        await client.send(
          new PutObjectCommand({
            Body: payload,
            Bucket: bucketName,
            ContentType: 'text/plain',
            Key: keyA,
          })
        )
        await client.send(new PutObjectCommand({ Body: 'b', Bucket: bucketName, Key: keyB }))
        await client.send(new PutObjectCommand({ Body: 'c', Bucket: bucketName, Key: keyC }))

        const ranged = await client.send(
          new GetObjectCommand({
            Bucket: bucketName,
            Key: keyA,
            Range: 'bytes=0-9',
          })
        )
        expect(await ranged.Body?.transformToString()).toBe(payload.subarray(0, 10).toString())
        expect(ranged.ContentRange).toBe(`bytes 0-9/${payload.length}`)

        const listedV1 = await client.send(
          new ListObjectsCommand({
            Bucket: bucketName,
            Delimiter: '/',
            Prefix: `${prefix}/`,
          })
        )
        expect(listedV1.Contents?.map((object) => object.Key)).toEqual(
          expect.arrayContaining([keyA, keyB])
        )
        expect(listedV1.CommonPrefixes?.map((object) => object.Prefix)).toContain(
          `${prefix}/nested/`
        )

        const multipartCopy = await client.send(
          new CreateMultipartUploadCommand({
            Bucket: bucketName,
            Key: copiedPartKey,
          })
        )
        const copiedPart = await client.send(
          new UploadPartCopyCommand({
            Bucket: bucketName,
            CopySource: `${bucketName}/${keyA}`,
            CopySourceRange: `bytes=0-${payload.length - 1}`,
            Key: copiedPartKey,
            PartNumber: 1,
            UploadId: multipartCopy.UploadId,
          })
        )
        expect(copiedPart.CopyPartResult?.ETag).toBeTruthy()
        await client.send(
          new CompleteMultipartUploadCommand({
            Bucket: bucketName,
            Key: copiedPartKey,
            MultipartUpload: {
              Parts: [{ ETag: copiedPart.CopyPartResult?.ETag, PartNumber: 1 }],
            },
            UploadId: multipartCopy.UploadId,
          })
        )
        const copiedPartHead = await client.send(
          new HeadObjectCommand({ Bucket: bucketName, Key: copiedPartKey })
        )
        expect(copiedPartHead.ContentLength).toBe(payload.length)

        const aborted = await client.send(
          new CreateMultipartUploadCommand({
            Bucket: bucketName,
            Key: abortedKey,
          })
        )
        await client.send(
          new AbortMultipartUploadCommand({
            Bucket: bucketName,
            Key: abortedKey,
            UploadId: aborted.UploadId,
          })
        )
        const activeUploads = await client.send(
          new ListMultipartUploadsCommand({ Bucket: bucketName, Prefix: abortedKey })
        )
        expect(activeUploads.Uploads ?? []).toHaveLength(0)

        await client.send(
          new DeleteObjectsCommand({
            Bucket: bucketName,
            Delete: {
              Objects: [keyA, keyB, keyC, copiedPartKey].map((Key) => ({ Key })),
            },
          })
        )

        await client.send(new DeleteBucketCommand({ Bucket: bucketName }))
      } finally {
        await cleanupS3Bucket(client, bucketName)
        client.destroy()
      }
    })
  }
)
