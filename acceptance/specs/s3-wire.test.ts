import {
  CreateBucketCommand,
  CreateMultipartUploadCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListPartsCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { describeAcceptance, getAcceptanceConfig } from '../support/config'
import { withAcceptanceHeaders } from '../support/headers'
import { uniqueBucketName, uniqueObjectKey } from '../support/resources'
import { cleanupS3Bucket, createAcceptanceS3Client } from '../support/s3'
import {
  buildS3RequestUrl,
  EMPTY_SHA256_HASH,
  sendAwsChunkedPutObject,
  sendAwsChunkedTrailerModeWithoutTrailer,
  sendAwsChunkedUploadPart,
  signS3Request,
} from '../support/sigv4'

describeAcceptance(
  'S3 SigV4 wire contract',
  {
    destructive: true,
    profiles: ['wire'],
    requires: ['wire'],
  },
  () => {
    it('accepts aws-chunked PutObject and persists decoded content length', async () => {
      const client = createAcceptanceS3Client()
      const bucketName = uniqueBucketName('wire')
      const key = uniqueObjectKey('chunked', 'bin')
      const payload = Buffer.alloc(123, 1)

      try {
        await client.send(new CreateBucketCommand({ Bucket: bucketName }))
        const response = await sendAwsChunkedPutObject({ bucketName, key, payload })

        expect(response.status).toBe(200)

        const head = await client.send(new HeadObjectCommand({ Bucket: bucketName, Key: key }))
        expect(head.ContentLength).toBe(payload.length)
      } finally {
        await cleanupS3Bucket(client, bucketName)
        client.destroy()
      }
    })

    it('accepts aws-chunked UploadPart and lists the uploaded part', async () => {
      const client = createAcceptanceS3Client()
      const bucketName = uniqueBucketName('wiremp')
      const key = uniqueObjectKey('chunked-part', 'bin')
      const payload = Buffer.alloc(123, 2)

      try {
        await client.send(new CreateBucketCommand({ Bucket: bucketName }))
        const multipart = await client.send(
          new CreateMultipartUploadCommand({
            Bucket: bucketName,
            Key: key,
          })
        )
        const uploadId = expectUploadId(multipart.UploadId)

        const response = await sendAwsChunkedUploadPart({
          bucketName,
          key,
          partNumber: 1,
          payload,
          uploadId,
        })

        expect(response.status).toBe(200)

        const parts = await client.send(
          new ListPartsCommand({
            Bucket: bucketName,
            Key: key,
            UploadId: uploadId,
          })
        )
        expect(parts.Parts?.map((part) => part.PartNumber)).toEqual([1])
      } finally {
        await cleanupS3Bucket(client, bucketName)
        client.destroy()
      }
    })

    it('rejects trailer-mode aws-chunked PutObject when the trailer block is missing', async () => {
      const client = createAcceptanceS3Client()
      const bucketName = uniqueBucketName('wiretr')
      const key = uniqueObjectKey('trailer', 'bin')
      const payload = Buffer.alloc(123, 3)

      try {
        await client.send(new CreateBucketCommand({ Bucket: bucketName }))
        const response = await sendAwsChunkedTrailerModeWithoutTrailer({
          bucketName,
          key,
          payload,
        })

        expect(response.status).toBeGreaterThanOrEqual(400)
        await expect(
          client.send(new HeadObjectCommand({ Bucket: bucketName, Key: key }))
        ).rejects.toMatchObject({
          $metadata: {
            httpStatusCode: 404,
          },
        })
      } finally {
        await cleanupS3Bucket(client, bucketName)
        client.destroy()
      }
    })

    it('rejects a header-signed PUT carrying an unsigned x-amz-copy-source header', async () => {
      const client = createAcceptanceS3Client()
      const srcBucket = uniqueBucketName('wirecpsrc')
      const dstBucket = uniqueBucketName('wirecpdst')
      const srcKey = uniqueObjectKey('source', 'txt')
      const dstKey = uniqueObjectKey('target', 'txt')
      const config = getAcceptanceConfig()

      try {
        await client.send(new CreateBucketCommand({ Bucket: srcBucket }))
        await client.send(new CreateBucketCommand({ Bucket: dstBucket }))
        await client.send(
          new PutObjectCommand({ Bucket: srcBucket, Key: srcKey, Body: 'source-object-body' })
        )
        await client.send(
          new PutObjectCommand({ Bucket: dstBucket, Key: dstKey, Body: 'original-content' })
        )

        const requestUrl = buildS3RequestUrl(config, `/${dstBucket}/${dstKey}`)
        const signedRequest = signS3Request({
          config,
          contentSha: EMPTY_SHA256_HASH,
          method: 'PUT',
          url: requestUrl,
        })

        const response = await fetch(requestUrl, {
          method: 'PUT',
          headers: withAcceptanceHeaders({
            ...signedRequest.headers,
            'x-amz-copy-source': `${srcBucket}/${srcKey}`,
          }),
        })

        expect(response.status).toBe(403)
        expect(await response.text()).toContain('AccessDenied')

        const unchanged = await client.send(
          new GetObjectCommand({ Bucket: dstBucket, Key: dstKey })
        )
        expect(await unchanged.Body?.transformToString()).toBe('original-content')
      } finally {
        await cleanupS3Bucket(client, srcBucket)
        await cleanupS3Bucket(client, dstBucket)
        client.destroy()
      }
    })

    it('rejects a presigned-URL PUT carrying an unsigned x-amz-copy-source header', async () => {
      const client = createAcceptanceS3Client()
      const srcBucket = uniqueBucketName('wirecpsrc')
      const dstBucket = uniqueBucketName('wirecpdst')
      const srcKey = uniqueObjectKey('source', 'txt')
      const dstKey = uniqueObjectKey('target', 'txt')

      try {
        await client.send(new CreateBucketCommand({ Bucket: srcBucket }))
        await client.send(new CreateBucketCommand({ Bucket: dstBucket }))
        await client.send(
          new PutObjectCommand({ Bucket: srcBucket, Key: srcKey, Body: 'source-object-body' })
        )
        await client.send(
          new PutObjectCommand({ Bucket: dstBucket, Key: dstKey, Body: 'original-content' })
        )

        const putUrl = await getSignedUrl(
          client,
          new PutObjectCommand({ Bucket: dstBucket, Key: dstKey }),
          { expiresIn: 100 }
        )

        const response = await fetch(putUrl, {
          method: 'PUT',
          headers: withAcceptanceHeaders({
            'x-amz-copy-source': `${srcBucket}/${srcKey}`,
          }),
        })

        expect(response.status).toBe(403)
        expect(await response.text()).toContain('AccessDenied')

        const unchanged = await client.send(
          new GetObjectCommand({ Bucket: dstBucket, Key: dstKey })
        )
        expect(await unchanged.Body?.transformToString()).toBe('original-content')
      } finally {
        await cleanupS3Bucket(client, srcBucket)
        await cleanupS3Bucket(client, dstBucket)
        client.destroy()
      }
    })
  }
)

function expectUploadId(uploadId: string | undefined): string {
  if (!uploadId) {
    throw new Error('CreateMultipartUpload did not return UploadId')
  }

  return uploadId
}
