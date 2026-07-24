import { describeAcceptance, encodePathSegments, getAcceptanceConfig } from '../support/config'
import { createRestClient } from '../support/http'
import {
  cleanupRestResources,
  createRestBucket,
  requireServiceKey,
  uniqueBucketName,
  uniqueObjectKey,
  uploadRestObject,
} from '../support/resources'

interface ListObjectsV2Response {
  objects: Array<{ name: string }>
}

const describeDatabaseWattAcceptance =
  process.env.ACCEPTANCE_DATABASE_WATT === 'true' ? describeAcceptance : describe.skip

describeDatabaseWattAcceptance(
  'Storage through the Database Watt runtime',
  {
    destructive: true,
    profiles: ['core'],
  },
  () => {
    it('persists tenant metadata through the external Storage API', async () => {
      const config = getAcceptanceConfig()
      const client = createRestClient()
      const bucketName = uniqueBucketName('dbwatt')
      const objectKey = uniqueObjectKey('dbwatt')
      const payload = `database-watt-acceptance-${config.runId}`

      try {
        await createRestBucket(bucketName)
        await uploadRestObject(bucketName, objectKey, payload)

        const listed = await client.request<ListObjectsV2Response>(
          'POST',
          `/object/list-v2/${bucketName}`,
          {
            body: {
              limit: 100,
              prefix: `${config.resourcePrefix}/`,
              with_delimiter: false,
            },
            expectedStatus: 200,
            token: requireServiceKey(config),
          }
        )

        expect(listed.json?.objects.map((object) => object.name)).toContain(objectKey)

        const downloaded = await client.request(
          'GET',
          `/object/authenticated/${bucketName}/${encodePathSegments(objectKey)}`,
          {
            expectedStatus: 200,
            token: requireServiceKey(config),
          }
        )

        expect(downloaded.body).toBe(payload)
      } finally {
        await cleanupRestResources(bucketName, [objectKey], client)
      }
    })
  }
)
