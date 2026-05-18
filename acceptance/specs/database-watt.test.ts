import { describeAcceptance, encodePathSegments, getAcceptanceConfig } from '../support/config'
import { AcceptanceHttpClient, createRestClient } from '../support/http'
import {
  cleanupRestResources,
  createRestBucket,
  requireServiceKey,
  uniqueBucketName,
  uniqueObjectKey,
  uploadRestObject,
} from '../support/resources'

type DatabaseWattStats = {
  acquire: number
  beginTransaction: number
  cancel: number
  commitTransaction: number
  lockedQuery: number
  query: number
  release: number
  rollbackTransaction: number
}

type BucketResponse = {
  id: string
  name: string
  public: boolean
}

type RollbackResponse = {
  bucketName: string
}

type SavepointResponse = {
  innerBucket: string
  outerBucket: string
}

describeAcceptance(
  'Database Watt E2E',
  {
    destructive: true,
    profiles: ['core'],
  },
  () => {
    it('executes stateless queries in the Database Watt worker', async () => {
      const databaseClient = createDatabaseWattClient()
      await resetDatabaseWattStats(databaseClient)

      const response = await databaseClient.request<{ rows: Array<{ value: number }> }>('POST', '/query', {
        expectedStatus: 200,
      })
      const stats = await getDatabaseWattStats(databaseClient)

      expect(response.json?.rows[0]).toEqual({ value: 1 })
      expect(stats.query).toBeGreaterThanOrEqual(1)
    })

    it('commits REST object changes through Database Watt transactions', async () => {
      const client = createRestClient()
      const databaseClient = createDatabaseWattClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('dbwatt-commit')
      const objectKey = uniqueObjectKey('dbwatt-commit')
      await resetDatabaseWattStats(databaseClient)

      try {
        await createRestBucket(bucketName, { isPublic: false })
        await uploadRestObject(bucketName, objectKey, 'database-watt-commit')

        const read = await client.request('GET', `/object/${bucketName}/${encodePathSegments(objectKey)}`, {
          expectedStatus: 200,
          token,
        })
        const stats = await getDatabaseWattStats(databaseClient)

        expect(read.body).toBe('database-watt-commit')
        expect(stats.beginTransaction).toBeGreaterThanOrEqual(1)
        expect(stats.lockedQuery).toBeGreaterThanOrEqual(1)
        expect(stats.commitTransaction).toBeGreaterThanOrEqual(1)
      } finally {
        await cleanupRestResources(bucketName, [objectKey], client)
      }
    })

    it('rolls back failed transaction work and leaves no partial bucket state', async () => {
      const client = createRestClient()
      const databaseClient = createDatabaseWattClient()
      const token = requireServiceKey()
      await resetDatabaseWattStats(databaseClient)

      const rollback = await databaseClient.request<RollbackResponse>('POST', '/rollback', {
        expectedStatus: 200,
      })
      const bucketName = rollback.json?.bucketName
      expect(bucketName).toBeTruthy()

      const lookup = await client.request('GET', `/bucket/${bucketName}`, {
        expectedStatus: 400,
        token,
      })
      const stats = await getDatabaseWattStats(databaseClient)

      expect(lookup.status).toBe(400)
      expect(stats.rollbackTransaction).toBeGreaterThanOrEqual(1)
    })

    it('preserves nested savepoint semantics in Database Watt transactions', async () => {
      const client = createRestClient()
      const databaseClient = createDatabaseWattClient()
      const token = requireServiceKey()
      await resetDatabaseWattStats(databaseClient)

      const savepoint = await databaseClient.request<SavepointResponse>('POST', '/savepoint', {
        expectedStatus: 200,
      })
      const outerBucket = savepoint.json?.outerBucket
      const innerBucket = savepoint.json?.innerBucket

      try {
        expect(outerBucket).toBeTruthy()
        expect(innerBucket).toBeTruthy()

        const outer = await client.request<BucketResponse>('GET', `/bucket/${outerBucket}`, {
          expectedStatus: 200,
          token,
        })
        const inner = await client.request('GET', `/bucket/${innerBucket}`, {
          expectedStatus: 400,
          token,
        })
        const stats = await getDatabaseWattStats(databaseClient)

        expect(outer.json?.id).toBe(outerBucket)
        expect(inner.status).toBe(400)
        expect(stats.lockedQuery).toBeGreaterThanOrEqual(3)
        expect(stats.commitTransaction).toBeGreaterThanOrEqual(1)
      } finally {
        if (outerBucket) {
          await cleanupRestResources(outerBucket, [], client)
        }
      }
    })

    it('preserves storage error mapping when Database Watt returns PostgreSQL errors', async () => {
      const client = createRestClient()
      const databaseClient = createDatabaseWattClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('dbwatt-error')
      await resetDatabaseWattStats(databaseClient)

      try {
        await createRestBucket(bucketName, { isPublic: false })
        const duplicate = await client.request('POST', '/bucket', {
          body: {
            id: bucketName,
            name: bucketName,
            public: false,
          },
          expectedStatus: 400,
          token,
        })
        const stats = await getDatabaseWattStats(databaseClient)

        expect(duplicate.status).toBe(400)
        expect(stats.lockedQuery).toBeGreaterThanOrEqual(1)
      } finally {
        await cleanupRestResources(bucketName, [], client)
      }
    })

    it('translates request aborts into Database Watt cancellation', async () => {
      const databaseClient = createDatabaseWattClient()
      await resetDatabaseWattStats(databaseClient)

      const response = await databaseClient.request('POST', '/sleep', {
        expectedStatus: 500,
      })
      const stats = await getDatabaseWattStats(databaseClient)

      expect(response.status).toBe(500)
      expect(stats.cancel).toBeGreaterThanOrEqual(1)
    })

    it('exercises multitenant destination resolution when the target is multitenant', async () => {
      const config = getAcceptanceConfig()
      const client = createRestClient()
      const databaseClient = createDatabaseWattClient()
      const token = requireServiceKey(config)
      const bucketName = uniqueBucketName('dbwatt-tenant')

      if (!config.tenantId) {
        expect(config.tenantId).toBeUndefined()
        return
      }

      await resetDatabaseWattStats(databaseClient)
      try {
        await createRestBucket(bucketName, { isPublic: false })
        const bucket = await client.request<BucketResponse>('GET', `/bucket/${bucketName}`, {
          expectedStatus: 200,
          token,
        })
        const stats = await getDatabaseWattStats(databaseClient)

        expect(bucket.json?.id).toBe(bucketName)
        expect(stats.beginTransaction).toBeGreaterThanOrEqual(1)
      } finally {
        await cleanupRestResources(bucketName, [], client)
      }
    })
  }
)

function createDatabaseWattClient() {
  const baseUrl = process.env.ACCEPTANCE_DATABASE_WATT_BASE_URL
  if (!baseUrl) {
    throw new Error('ACCEPTANCE_DATABASE_WATT_BASE_URL is required')
  }

  return new AcceptanceHttpClient(baseUrl)
}

async function getDatabaseWattStats(client = createDatabaseWattClient()): Promise<DatabaseWattStats> {
  const response = await client.request<DatabaseWattStats>('GET', '/stats', {
    expectedStatus: 200,
  })

  if (!response.json) {
    throw new Error('Database Watt stats response was empty')
  }

  return response.json
}

async function resetDatabaseWattStats(client = createDatabaseWattClient()): Promise<void> {
  await client.request('POST', '/reset', { expectedStatus: 200 })
}
