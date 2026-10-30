import {
  DeleteBucketLifecycleCommand,
  GetBucketLifecycleConfigurationCommand,
  PutBucketLifecycleConfigurationCommand,
} from '@aws-sdk/client-s3'
import { describeAcceptance } from '../support/config'
import { createRestClient } from '../support/http'
import {
  cleanupRestResources,
  createRestBucket,
  requireServiceKey,
  uniqueBucketName,
} from '../support/resources'
import { createAcceptanceS3Client } from '../support/s3'

const restConfiguration = {
  rules: [
    {
      id: 'expire-history',
      status: 'Enabled',
      filter: {},
      noncurrentVersionExpiration: { noncurrentDays: 30, newerNoncurrentVersions: 2 },
    },
  ],
}

const s3Rules = [
  {
    ID: 'expire-history',
    Status: 'Enabled' as const,
    Filter: {},
    NoncurrentVersionExpiration: { NoncurrentDays: 30, NewerNoncurrentVersions: 2 },
  },
]

const replacementRules = [
  {
    ID: 'replacement',
    Status: 'Disabled' as const,
    Filter: {},
    NoncurrentVersionExpiration: { NoncurrentDays: 7 },
  },
]

const replacementConfiguration = {
  rules: [
    {
      id: 'replacement',
      status: 'Disabled',
      filter: {},
      noncurrentVersionExpiration: { noncurrentDays: 7 },
    },
  ],
}

describeAcceptance(
  'Bucket lifecycle configuration',
  { destructive: true, profiles: ['full'], requires: ['versioning'] },
  () => {
    it('shares REST and S3 policy writes, reads, replacements and deletes', async () => {
      const rest = createRestClient()
      const s3 = createAcceptanceS3Client()
      const token = requireServiceKey()
      const bucket = uniqueBucketName('lifecycle')
      const route = `/bucket/${bucket}/lifecycle`

      try {
        await createRestBucket(bucket)

        const missing = await rest.request<{ code: string }>('GET', route, {
          expectedStatus: 400,
          token,
        })
        expect(missing.json).toMatchObject({ code: 'NoSuchLifecycleConfiguration' })
        await expect(
          s3.send(new GetBucketLifecycleConfigurationCommand({ Bucket: bucket }))
        ).rejects.toMatchObject({
          name: 'NoSuchLifecycleConfiguration',
          $metadata: { httpStatusCode: 404 },
        })

        const put = await rest.request('PUT', route, {
          body: restConfiguration,
          expectedStatus: 200,
          token,
        })
        expect(put.json).toEqual(restConfiguration)
        expect((await rest.request('GET', route, { expectedStatus: 200, token })).json).toEqual(
          restConfiguration
        )
        const s3Read = await s3.send(new GetBucketLifecycleConfigurationCommand({ Bucket: bucket }))
        expect(s3Read.Rules).toEqual(s3Rules)

        await s3.send(
          new PutBucketLifecycleConfigurationCommand({
            Bucket: bucket,
            LifecycleConfiguration: { Rules: replacementRules },
          })
        )
        expect((await rest.request('GET', route, { expectedStatus: 200, token })).json).toEqual(
          replacementConfiguration
        )
        expect(
          (await s3.send(new GetBucketLifecycleConfigurationCommand({ Bucket: bucket }))).Rules
        ).toEqual(replacementRules)

        const deleted = await rest.request('DELETE', route, { expectedStatus: 200, token })
        expect(deleted.json).toEqual({ message: 'Successfully deleted' })
        await expect(
          s3.send(new GetBucketLifecycleConfigurationCommand({ Bucket: bucket }))
        ).rejects.toMatchObject({
          name: 'NoSuchLifecycleConfiguration',
          $metadata: { httpStatusCode: 404 },
        })
        const repeatDelete = await rest.request('DELETE', route, { expectedStatus: 200, token })
        expect(repeatDelete.json).toEqual({ message: 'Successfully deleted' })

        await rest.request('PUT', route, { body: restConfiguration, expectedStatus: 200, token })
        await expect(
          s3.send(new DeleteBucketLifecycleCommand({ Bucket: bucket }))
        ).resolves.toMatchObject({
          $metadata: { httpStatusCode: 204 },
        })
        await expect(
          s3.send(new DeleteBucketLifecycleCommand({ Bucket: bucket }))
        ).resolves.toMatchObject({
          $metadata: { httpStatusCode: 204 },
        })
        const afterS3Delete = await rest.request<{ code: string }>('GET', route, {
          expectedStatus: 400,
          token,
        })
        expect(afterS3Delete.json).toMatchObject({ code: 'NoSuchLifecycleConfiguration' })
      } finally {
        await cleanupRestResources(bucket, [], rest)
        s3.destroy()
      }
    })
  }
)
