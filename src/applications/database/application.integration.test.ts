import { randomUUID } from 'node:crypto'
import { removeGlobals } from '@platformatic/globals'
import { setupLoopbackMessaging } from '@platformatic/runtime'
import dotenv from 'dotenv'
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { getConfig } from '../../config.js'
import type { DatabasePoolTarget } from './protocol.js'

dotenv.config({ path: '.env.test', override: false })
dotenv.config({ path: '.env', override: false })

type ApplicationContext = {
  close: () => Promise<void>
  isBackgroundApplication: boolean
}

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

type BucketRowsResponse = {
  rows: BucketResponse[]
}

type LockResponse = {
  lockId: string
}

type ErrorResponse = {
  code: string
  destination?: string
  message: string
  sqlState?: string
}

let app: ApplicationContext | undefined
let messaging: ReturnType<typeof setupLoopbackMessaging> | undefined

async function createDatabaseWattApp(): Promise<ApplicationContext> {
  const databaseApp = await import('./index.js')
  return databaseApp.create()
}

function testDestination(): DatabasePoolTarget {
  const { databaseMaxConnections, databasePoolURL, databaseURL } = getConfig()
  const connectionString = databasePoolURL || databaseURL

  if (!connectionString) {
    throw new Error('Database Watt integration requires DATABASE_URL or DATABASE_POOL_URL')
  }

  return {
    connectionString,
    id: process.env.ACCEPTANCE_TENANT_ID || process.env.TENANT_ID || 'default',
    isExternalPool: Boolean(databasePoolURL),
    maxConnections: databaseMaxConnections,
  }
}

function uniqueBucketName(kind: string): string {
  return `dbwatt-${kind}-${randomUUID().replaceAll('-', '').slice(0, 16)}`.slice(0, 63)
}

function sendDatabaseMessage<T = unknown>(message: string, payload: unknown): Promise<T> {
  if (!messaging) {
    throw new Error('Database Watt loopback messaging is not initialized')
  }

  return messaging.send('database', message, payload) as Promise<T>
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

async function cleanupBucket(bucketName: string, destination = testDestination()): Promise<void> {
  const tx = await beginTransaction(destination)

  try {
    await checkedResult(
      sendDatabaseMessage('database.lockedQuery', {
        lockId: tx.lockId,
        requestId: randomUUID(),
        sql: `SELECT set_config('storage.allow_delete_query', 'true', true)`,
      })
    )
    await checkedResult(
      sendDatabaseMessage('database.lockedQuery', {
        lockId: tx.lockId,
        requestId: randomUUID(),
        sql: 'DELETE FROM storage.buckets WHERE id = $1',
        values: [bucketName],
      })
    )
    await checkedResult(sendDatabaseMessage('database.commitTransaction', { lockId: tx.lockId }))
  } catch (error) {
    await sendDatabaseMessage('database.rollbackTransaction', { lockId: tx.lockId }).catch(
      () => undefined
    )
    throw error
  }
}

async function getBucket(
  bucketName: string,
  destination = testDestination()
): Promise<BucketResponse | undefined> {
  const result = await checkedResult<BucketRowsResponse>(
    sendDatabaseMessage('database.query', {
      destination,
      requestId: randomUUID(),
      sql: 'SELECT id, name, public FROM storage.buckets WHERE id = $1',
      values: [bucketName],
    })
  )

  return result.rows[0]
}

async function insertBucket(bucketName: string, destination = testDestination()): Promise<unknown> {
  return sendDatabaseMessage('database.query', {
    destination,
    requestId: randomUUID(),
    sql: `INSERT INTO storage.buckets (id, name, owner, public) VALUES ($1, $1, $2, false)`,
    values: [bucketName, randomUUID()],
  })
}

async function commitBucketDatabaseWatt(): Promise<{ bucketName: string }> {
  const bucketName = uniqueBucketName('dbwatt-commit')
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
    await checkedResult(sendDatabaseMessage('database.commitTransaction', { lockId: tx.lockId }))
    return { bucketName }
  } catch (error) {
    await sendDatabaseMessage('database.rollbackTransaction', { lockId: tx.lockId }).catch(
      () => undefined
    )
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
    await sendDatabaseMessage('database.rollbackTransaction', { lockId: tx.lockId }).catch(
      () => undefined
    )
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
    await sendDatabaseMessage('database.rollbackTransaction', { lockId: tx.lockId }).catch(
      () => undefined
    )
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

describe('Database Watt application loopback integration', () => {
  beforeAll(async () => {
    messaging = setupLoopbackMessaging('database-watt-acceptance')
    app = await createDatabaseWattApp()
  })

  beforeEach(async () => {
    await resetDatabaseWattStats()
  })

  afterAll(async () => {
    await app?.close()
    app = undefined
    messaging = undefined
    removeGlobals(['messaging'])
  })

  it('executes stateless queries in the Database Watt worker', async () => {
    const response = await queryDatabaseWatt()
    const stats = await getDatabaseWattStats()

    expect(response.rows[0]).toEqual({ value: 1 })
    expect(stats.query).toBeGreaterThanOrEqual(1)
  })

  it('commits bucket changes through Database Watt transactions', async () => {
    let bucketName: string | undefined
    try {
      const committed = await commitBucketDatabaseWatt()
      bucketName = committed.bucketName
      const bucket = await getBucket(bucketName)
      const stats = await getDatabaseWattStats()

      expect(bucket).toMatchObject({ id: bucketName, name: bucketName, public: false })
      expect(stats.beginTransaction).toBeGreaterThanOrEqual(1)
      expect(stats.lockedQuery).toBeGreaterThanOrEqual(1)
      expect(stats.commitTransaction).toBeGreaterThanOrEqual(1)
    } finally {
      if (bucketName) {
        await cleanupBucket(bucketName)
      }
    }
  })

  it('rolls back failed transaction work and leaves no partial bucket state', async () => {
    const rollback = await rollbackDatabaseWatt()
    const bucketName = rollback.bucketName
    expect(bucketName).toBeTruthy()

    const bucket = await getBucket(bucketName)
    const stats = await getDatabaseWattStats()

    expect(bucket).toBeUndefined()
    expect(stats.rollbackTransaction).toBeGreaterThanOrEqual(1)
  })

  it('preserves nested savepoint semantics in Database Watt transactions', async () => {
    const savepoint = await savepointDatabaseWatt()
    const outerBucket = savepoint.outerBucket
    const innerBucket = savepoint.innerBucket

    try {
      expect(outerBucket).toBeTruthy()
      expect(innerBucket).toBeTruthy()

      const outer = await getBucket(outerBucket)
      const inner = await getBucket(innerBucket)
      const stats = await getDatabaseWattStats()

      expect(outer?.id).toBe(outerBucket)
      expect(inner).toBeUndefined()
      expect(stats.lockedQuery).toBeGreaterThanOrEqual(3)
      expect(stats.commitTransaction).toBeGreaterThanOrEqual(1)
    } finally {
      if (outerBucket) {
        await cleanupBucket(outerBucket)
      }
    }
  })

  it('preserves PostgreSQL error mapping when Database Watt returns PostgreSQL errors', async () => {
    const bucketName = uniqueBucketName('dbwatt-error')

    try {
      await checkedResult(insertBucket(bucketName))
      const duplicate = (await insertBucket(bucketName)) as ErrorResponse
      const stats = await getDatabaseWattStats()

      expect(duplicate).toMatchObject({ code: 'POSTGRES_ERROR', sqlState: '23505' })
      expect(stats.query).toBeGreaterThanOrEqual(2)
    } finally {
      await cleanupBucket(bucketName)
    }
  })

  it('translates request aborts into Database Watt cancellation', async () => {
    const response = await sleepDatabaseWatt()
    const stats = await getDatabaseWattStats()

    expect(response).toMatchObject({ code: expect.any(String) })
    expect(stats.cancel).toBeGreaterThanOrEqual(1)
  })

  it('handles concurrent Database Watt query load', async () => {
    const response = await concurrentQueriesDatabaseWatt()
    const stats = await getDatabaseWattStats()

    expect(response).toEqual({ count: 5 })
    expect(stats.query).toBeGreaterThanOrEqual(5)
  })
})
