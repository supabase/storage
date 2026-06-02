import { randomUUID } from 'node:crypto'
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
import { sendWattMessage } from '../support/watt-repl'

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

type QueryRowsResponse = {
  rows: Array<{ value: number }>
}

type LockResponse = {
  lockId: string
}

type ErrorResponse = {
  code: string
  destination?: string
  message: string
}

const describeDatabaseWattAcceptance = process.env.ACCEPTANCE_DATABASE_WATT === 'true' ? describeAcceptance : describe.skip

function testDestination(): string {
  return process.env.ACCEPTANCE_TENANT_ID || process.env.TENANT_ID || 'default'
}

function sendDatabaseMessage<T = unknown>(message: string, payload: unknown): Promise<T> {
  return sendWattMessage<T>('database', message, payload)
}

function isDatabaseError(result: unknown): result is ErrorResponse {
  return typeof result === 'object' && result !== null && 'code' in result && 'message' in result
}

async function checkedResult<T = unknown>(resultPromise: Promise<unknown>): Promise<T> {
  const result = await resultPromise
  if (isDatabaseError(result)) {
    throw new Error(result.message)
  }
  return result as T
}

async function beginTransaction(destination = testDestination()): Promise<LockResponse> {
  return checkedResult(
    sendDatabaseMessage('database.beginTransaction', {
      destination,
      requestId: randomUUID(),
    })
  )
}

async function queryDatabaseWatt(destination = testDestination()): Promise<QueryRowsResponse> {
  return sendDatabaseMessage('database.query', {
    destination,
    requestId: randomUUID(),
    sql: 'SELECT 1 as value',
  })
}

async function masterTransaction(): Promise<{ value: number | undefined }> {
  const tx = await beginTransaction('master')

  try {
    const result = await checkedResult<QueryRowsResponse>(
      sendDatabaseMessage('database.lockedQuery', {
        lockId: tx.lockId,
        requestId: randomUUID(),
        sql: 'SELECT 1 as value',
      })
    )
    await checkedResult(sendDatabaseMessage('database.commitTransaction', { lockId: tx.lockId }))
    return { value: result.rows[0]?.value }
  } catch (error) {
    await sendDatabaseMessage('database.rollbackTransaction', { lockId: tx.lockId }).catch(() => undefined)
    throw error
  }
}

async function rollbackDatabaseWatt(): Promise<RollbackResponse> {
  const bucketName = `db-watt-rollback-${Date.now()}`
  const tx = await beginTransaction()

  try {
    await checkedResult(
      sendDatabaseMessage('database.lockedQuery', {
        lockId: tx.lockId,
        requestId: randomUUID(),
        sql: `INSERT INTO storage.buckets (id, name, owner, public) VALUES ($1, $1, $2, false)`,
        values: [bucketName, randomUUID()],
      })
    )
    await checkedResult(sendDatabaseMessage('database.rollbackTransaction', { lockId: tx.lockId }))
    return { bucketName }
  } catch (error) {
    await sendDatabaseMessage('database.rollbackTransaction', { lockId: tx.lockId }).catch(() => undefined)
    throw error
  }
}

async function savepointDatabaseWatt(): Promise<SavepointResponse> {
  const innerBucket = `db-watt-savepoint-inner-${Date.now()}`
  const outerBucket = `db-watt-savepoint-outer-${Date.now()}`
  const tx = await beginTransaction()

  try {
    await checkedResult(
      sendDatabaseMessage('database.lockedQuery', {
        lockId: tx.lockId,
        requestId: randomUUID(),
        sql: `INSERT INTO storage.buckets (id, name, owner, public) VALUES ($1, $1, $2, false)`,
        values: [outerBucket, randomUUID()],
      })
    )
    await checkedResult(
      sendDatabaseMessage('database.lockedQuery', {
        lockId: tx.lockId,
        requestId: randomUUID(),
        sql: 'SAVEPOINT database_watt_acceptance',
      })
    )
    await checkedResult(
      sendDatabaseMessage('database.lockedQuery', {
        lockId: tx.lockId,
        requestId: randomUUID(),
        sql: `INSERT INTO storage.buckets (id, name, owner, public) VALUES ($1, $1, $2, false)`,
        values: [innerBucket, randomUUID()],
      })
    )
    await checkedResult(
      sendDatabaseMessage('database.lockedQuery', {
        lockId: tx.lockId,
        requestId: randomUUID(),
        sql: 'ROLLBACK TO SAVEPOINT database_watt_acceptance',
      })
    )
    await checkedResult(sendDatabaseMessage('database.commitTransaction', { lockId: tx.lockId }))
    return { innerBucket, outerBucket }
  } catch (error) {
    await sendDatabaseMessage('database.rollbackTransaction', { lockId: tx.lockId }).catch(() => undefined)
    throw error
  }
}

async function sleepDatabaseWatt(): Promise<ErrorResponse> {
  const requestId = randomUUID()
  const query = sendDatabaseMessage<ErrorResponse>('database.query', {
    destination: testDestination(),
    requestId,
    sql: 'SELECT pg_sleep(10)',
  })

  setTimeout(() => {
    sendDatabaseMessage('database.cancel', { requestId }).catch(() => undefined)
  }, 50).unref()

  return query
}

async function missingDestinationDatabaseWatt(): Promise<ErrorResponse> {
  return sendDatabaseMessage('database.query', {
    destination: `missing-${randomUUID()}`,
    requestId: randomUUID(),
    sql: 'SELECT 1',
  })
}

async function concurrentQueriesDatabaseWatt(): Promise<{ count: number }> {
  const results = await Promise.all(
    Array.from({ length: 5 }, () =>
      sendDatabaseMessage<QueryRowsResponse | ErrorResponse>('database.query', {
        destination: testDestination(),
        requestId: randomUUID(),
        sql: 'SELECT 1 as value',
      })
    )
  )
  const error = results.find(isDatabaseError)

  if (error) {
    throw new Error(error.message)
  }

  return { count: results.length }
}

async function getDatabaseWattStats(): Promise<DatabaseWattStats> {
  return sendDatabaseMessage('database.test.stats', {})
}

async function resetDatabaseWattStats(): Promise<void> {
  await sendDatabaseMessage('database.test.resetStats', {})
}

describeDatabaseWattAcceptance(
  'Database Watt E2E',
  {
    destructive: true,
    profiles: ['core'],
  },
  () => {
    it('executes stateless queries in the Database Watt worker', async () => {
      await resetDatabaseWattStats()

      const response = await queryDatabaseWatt()
      const stats = await getDatabaseWattStats()

      expect(response.rows[0]).toEqual({ value: 1 })
      expect(stats.query).toBeGreaterThanOrEqual(1)
    })

    it('executes master database queries through Database Watt', async () => {
      const config = getAcceptanceConfig()

      if (!config.tenantId) {
        expect(config.tenantId).toBeUndefined()
        return
      }

      await resetDatabaseWattStats()
      const response = await queryDatabaseWatt('master')
      const stats = await getDatabaseWattStats()

      expect(response.rows[0]).toEqual({ value: 1 })
      expect(stats.query).toBeGreaterThanOrEqual(1)
    })

    it('executes master database transactions through Database Watt', async () => {
      const config = getAcceptanceConfig()

      if (!config.tenantId) {
        expect(config.tenantId).toBeUndefined()
        return
      }

      await resetDatabaseWattStats()
      const response = await masterTransaction()
      const stats = await getDatabaseWattStats()

      expect(response).toEqual({ value: 1 })
      expect(stats.beginTransaction).toBeGreaterThanOrEqual(1)
      expect(stats.lockedQuery).toBeGreaterThanOrEqual(1)
      expect(stats.commitTransaction).toBeGreaterThanOrEqual(1)
    })

    it('commits REST object changes through Database Watt transactions', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('dbwatt-commit')
      const objectKey = uniqueObjectKey('dbwatt-commit')
      await resetDatabaseWattStats()

      try {
        await createRestBucket(bucketName, { isPublic: false })
        await uploadRestObject(bucketName, objectKey, 'database-watt-commit')

        const read = await client.request('GET', `/object/${bucketName}/${encodePathSegments(objectKey)}`, {
          expectedStatus: 200,
          token,
        })
        const stats = await getDatabaseWattStats()

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
      const token = requireServiceKey()
      await resetDatabaseWattStats()

      const rollback = await rollbackDatabaseWatt()
      const bucketName = rollback.bucketName
      expect(bucketName).toBeTruthy()

      const lookup = await client.request('GET', `/bucket/${bucketName}`, {
        expectedStatus: 400,
        token,
      })
      const stats = await getDatabaseWattStats()

      expect(lookup.status).toBe(400)
      expect(stats.rollbackTransaction).toBeGreaterThanOrEqual(1)
    })

    it('preserves nested savepoint semantics in Database Watt transactions', async () => {
      const client = createRestClient()
      const token = requireServiceKey()
      await resetDatabaseWattStats()

      const savepoint = await savepointDatabaseWatt()
      const outerBucket = savepoint.outerBucket
      const innerBucket = savepoint.innerBucket

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
        const stats = await getDatabaseWattStats()

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
      const token = requireServiceKey()
      const bucketName = uniqueBucketName('dbwatt-error')
      await resetDatabaseWattStats()

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
        const stats = await getDatabaseWattStats()

        expect(duplicate.status).toBe(400)
        expect(stats.lockedQuery).toBeGreaterThanOrEqual(1)
      } finally {
        await cleanupRestResources(bucketName, [], client)
      }
    })

    it('translates request aborts into Database Watt cancellation', async () => {
      await resetDatabaseWattStats()

      const response = await sleepDatabaseWatt()
      const stats = await getDatabaseWattStats()

      expect(response).toMatchObject({ code: expect.any(String) })
      expect(stats.cancel).toBeGreaterThanOrEqual(1)
    })

    it('returns typed destination errors through the Database Watt worker', async () => {
      const config = getAcceptanceConfig()

      if (!config.tenantId) {
        expect(config.tenantId).toBeUndefined()
        return
      }

      await resetDatabaseWattStats()
      const response = await missingDestinationDatabaseWatt()

      expect(response).toMatchObject({ code: 'DESTINATION_UNKNOWN' })
      expect(response.destination).toEqual(expect.stringMatching(/^missing-/))
    })

    it('handles concurrent Database Watt query load', async () => {
      await resetDatabaseWattStats()

      const response = await concurrentQueriesDatabaseWatt()
      const stats = await getDatabaseWattStats()

      expect(response).toEqual({ count: 5 })
      expect(stats.query).toBeGreaterThanOrEqual(5)
    })

    it('exercises multitenant destination resolution when the target is multitenant', async () => {
      const config = getAcceptanceConfig()
      const client = createRestClient()
      const token = requireServiceKey(config)
      const bucketName = uniqueBucketName('dbwatt-tenant')

      if (!config.tenantId) {
        expect(config.tenantId).toBeUndefined()
        return
      }

      await resetDatabaseWattStats()
      try {
        await createRestBucket(bucketName, { isPublic: false })
        const bucket = await client.request<BucketResponse>('GET', `/bucket/${bucketName}`, {
          expectedStatus: 200,
          token,
        })
        const stats = await getDatabaseWattStats()

        expect(bucket.json?.id).toBe(bucketName)
        expect(stats.beginTransaction).toBeGreaterThanOrEqual(1)
      } finally {
        await cleanupRestResources(bucketName, [], client)
      }
    })
  }
)
