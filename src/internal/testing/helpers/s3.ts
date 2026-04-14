import {
  CreateBucketCommand,
  DeleteObjectsCommand,
  HeadBucketCommand,
  ListObjectsV2Command,
  S3Client,
} from '@aws-sdk/client-s3'
import { isS3Error } from '@internal/errors'
import { getConfig } from '../../../config'

let client: S3Client | undefined

/**
 * Lazily-built S3 client pointing at the storage backend the storage-api
 * itself uses (MinIO in local infra). Tests get the same client by calling
 * `getTestS3Client()` so all uploads/cleanups go through the same connection.
 */
export function getTestS3Client(): S3Client {
  if (client) return client
  const { storageS3Endpoint, storageS3Region, storageS3ForcePathStyle } = getConfig()
  client = new S3Client({
    endpoint: storageS3Endpoint,
    region: storageS3Region,
    forcePathStyle: storageS3ForcePathStyle,
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
    },
  })
  return client
}

/**
 * Make sure the configured root storage S3 bucket exists in the backend. Tests
 * that hit the upload pipeline call this once in their before-all so MinIO has
 * the target bucket ready. Idempotent.
 */
export async function ensureRootBucket(): Promise<void> {
  const { storageS3Bucket } = getConfig()
  if (!storageS3Bucket) {
    throw new Error('STORAGE_S3_BUCKET / GLOBAL_S3_BUCKET must be set for test_v2')
  }
  const s3 = getTestS3Client()
  try {
    await s3.send(new HeadBucketCommand({ Bucket: storageS3Bucket }))
  } catch (err) {
    if (err && isS3Error(err) && err.$metadata?.httpStatusCode === 404) {
      await s3.send(new CreateBucketCommand({ Bucket: storageS3Bucket }))
      return
    }
    throw err
  }
}

/**
 * Delete every S3 key under `<tenantId>/<bucketId>/` from the root storage
 * bucket. Used by per-test-file teardown.
 */
export async function deleteS3PrefixesForBuckets(bucketIds: string[]): Promise<void> {
  if (bucketIds.length === 0) return
  const { storageS3Bucket, tenantId } = getConfig()
  const s3 = getTestS3Client()

  for (const bucketId of bucketIds) {
    const prefix = `${tenantId}/${bucketId}/`
    let continuationToken: string | undefined
    do {
      const listed = await s3.send(
        new ListObjectsV2Command({
          Bucket: storageS3Bucket,
          Prefix: prefix,
          ContinuationToken: continuationToken,
        })
      )
      const keys = listed.Contents?.map((c) => ({ Key: c.Key! })) ?? []
      if (keys.length > 0) {
        await s3.send(
          new DeleteObjectsCommand({
            Bucket: storageS3Bucket,
            Delete: { Objects: keys, Quiet: true },
          })
        )
      }
      continuationToken = listed.IsTruncated ? listed.NextContinuationToken : undefined
    } while (continuationToken)
  }
}
