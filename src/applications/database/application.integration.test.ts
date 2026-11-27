import { randomUUID } from 'node:crypto'
import { removeGlobals } from '@platformatic/globals'
import { setupLoopbackMessaging } from '@platformatic/runtime'
import { types } from 'pg'
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest'
import { getConfig } from '../../config.js'
import { DBMigration } from '../../internal/database/migrations/types.js'
import {
  DatabaseWattResponseError,
  type DatabaseWattTransport,
} from '../../internal/database/watt/client.js'
import { getWattPostgresConnection } from '../../internal/database/watt/connection.js'
import { StoragePgDB } from '../../storage/database/pg.js'
import type { DatabaseErrorResponse, DatabasePoolTarget } from './protocol.js'

type ApplicationContext = {
  close: () => Promise<void>
  isBackgroundApplication: boolean
}

type DatabaseWattStats = {
  acquire: number
  batch: number
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

// Rows come back as raw text values; the storage worker parses them with the pg type parsers.
type RawRowsResponse = {
  fields: Array<{ name: string; dataTypeID: number }>
  rows: Array<Array<string | null>>
  rowCount: number
}

type QueryRowsResponse = RawRowsResponse

type BatchResponse = RawRowsResponse & { lockId?: string; results?: RawRowsResponse[] }

function decodeRows<T>(response: RawRowsResponse): T[] {
  return response.rows.map(
    (raw) =>
      Object.fromEntries(
        response.fields.map((field, i) => {
          const value = raw[i]
          return [
            field.name,
            value === null ? null : types.getTypeParser(field.dataTypeID, 'text')(value),
          ]
        })
      ) as T
  )
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

// The global messaging is the Database application's side of the loopback, so a storage
// worker connection sends through the test's side instead.
function loopbackTransport(): DatabaseWattTransport {
  const sender =
    (message: string) =>
    async (request: object): Promise<never> => {
      const response = await sendDatabaseMessage(message, { requestId: randomUUID(), ...request })
      if (isDatabaseError(response)) {
        throw new DatabaseWattResponseError(response as DatabaseErrorResponse)
      }
      return response as never
    }

  return {
    query: sender('database.query'),
    acquire: sender('database.acquire'),
    batch: sender('database.batch'),
    lockedQuery: sender('database.lockedQuery'),
    release: sender('database.release'),
    beginTransaction: sender('database.beginTransaction'),
    commitTransaction: sender('database.commitTransaction'),
    rollbackTransaction: sender('database.rollbackTransaction'),
  }
}

function latestDBMigration(): keyof typeof DBMigration {
  const names = Object.keys(DBMigration) as Array<keyof typeof DBMigration>
  return names.reduce((latest, name) => (DBMigration[name] > DBMigration[latest] ? name : latest))
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
  const result = await checkedResult<RawRowsResponse>(
    sendDatabaseMessage('database.query', {
      destination,
      requestId: randomUUID(),
      sql: 'SELECT id, name, public FROM storage.buckets WHERE id = $1',
      values: [bucketName],
    })
  )

  return decodeRows<BucketResponse>(result)[0]
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

    expect(decodeRows(response)[0]).toEqual({ value: 1 })
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

  it('runs a whole scoped transaction from one batch message', async () => {
    const bucketName = uniqueBucketName('dbwatt-batch')

    try {
      const response = await checkedResult<BatchResponse>(
        sendDatabaseMessage('database.batch', {
          destination: testDestination(),
          requestId: randomUUID(),
          statements: [
            { sql: `SELECT set_config('storage.operation', $1, true)`, values: ['batch-test'] },
            {
              sql: `INSERT INTO storage.buckets (id, name, owner, public) VALUES ($1, $1, $2, false)
                    RETURNING id, public, created_at, current_setting('storage.operation') AS operation`,
              values: [bucketName, randomUUID()],
            },
          ],
          commit: true,
        })
      )
      const stats = await getDatabaseWattStats()
      const [row] = decodeRows<{
        id: string
        public: boolean
        created_at: Date
        operation: string
      }>(response)

      expect(response.lockId).toBeUndefined()
      expect(row).toMatchObject({ id: bucketName, public: false, operation: 'batch-test' })
      expect(row.created_at).toBeInstanceOf(Date)
      expect(await getBucket(bucketName)).toMatchObject({ id: bucketName })
      expect(stats.batch).toBe(1)
      expect(stats.beginTransaction).toBe(0)
      expect(stats.commitTransaction).toBe(0)
    } finally {
      await cleanupBucket(bucketName)
    }
  })

  it('returns each flagged batch statement result, read under the scope before it', async () => {
    const response = await checkedResult<BatchResponse>(
      sendDatabaseMessage('database.batch', {
        destination: testDestination(),
        requestId: randomUUID(),
        statements: [
          { sql: `SELECT set_config('storage.operation', $1, true)`, values: ['first'] },
          { sql: `SELECT current_setting('storage.operation') AS operation`, result: true },
          { sql: `SELECT set_config('storage.operation', $1, true)`, values: ['second'] },
          { sql: `SELECT current_setting('storage.operation') AS operation` },
        ],
        commit: true,
      })
    )

    expect(response.lockId).toBeUndefined()
    expect(response.results).toHaveLength(1)
    expect(decodeRows<{ operation: string }>(response.results?.[0] as BatchResponse)).toEqual([
      { operation: 'first' },
    ])
    expect(decodeRows<{ operation: string }>(response)).toEqual([{ operation: 'second' }])
  })

  it('reads a bucket and an object in one batch with the results of the regular reads', async () => {
    const bucketName = uniqueBucketName('dbwatt-pair')
    const destination = testDestination()
    const connection = await getWattPostgresConnection(
      {
        dbUrl: destination.connectionString,
        isExternalPool: destination.isExternalPool,
        maxConnections: destination.maxConnections,
        tenantId: destination.id,
        operation: () => 'storage.object.get',
        superUser: { jwt: '', payload: { role: 'service_role' } },
        user: { jwt: '', payload: { role: 'authenticated', sub: randomUUID() } },
      },
      loopbackTransport()
    )
    const db = new StoragePgDB(connection, {
      tenantId: destination.id,
      host: 'localhost',
      latestMigration: latestDBMigration(),
    })
    const query = {
      bucketId: bucketName,
      bucketColumns: 'id,public',
      objectName: 'folder/a.txt',
      objectColumns: 'id,name,version',
      objectFilters: { excludeDeleteMarkers: true },
    }

    try {
      await checkedResult(
        sendDatabaseMessage('database.batch', {
          destination,
          statements: [
            {
              sql: `INSERT INTO storage.buckets (id, name, public) VALUES ($1, $1, false)`,
              values: [bucketName],
            },
            {
              sql: `INSERT INTO storage.objects (bucket_id, name, version) VALUES ($1, $2, $3)`,
              values: [bucketName, query.objectName, randomUUID()],
            },
          ],
          commit: true,
        })
      )
      await resetDatabaseWattStats()

      const withRls = await db.findBucketAndObject({ ...query, objectAsSuperUser: false })
      const asSuperUser = await db.findBucketAndObject({ ...query, objectAsSuperUser: true })
      const stats = await getDatabaseWattStats()

      expect(stats.batch).toBe(2)
      expect(stats.query + stats.beginTransaction + stats.lockedQuery).toBe(0)
      expect(withRls?.bucket).toEqual({ id: bucketName, public: false })
      expect(asSuperUser?.bucket).toEqual({ id: bucketName, public: false })
      const regularRead = { excludeDeleteMarkers: true, dontErrorOnEmpty: true }
      expect(withRls?.object).toEqual(
        await db.findObject(bucketName, query.objectName, query.objectColumns, regularRead)
      )
      expect(asSuperUser?.object).toEqual(
        await db
          .asSuperUser()
          .findObject(bucketName, query.objectName, query.objectColumns, regularRead)
      )
      expect(asSuperUser?.object).toMatchObject({ name: query.objectName })
    } finally {
      await checkedResult(
        sendDatabaseMessage('database.batch', {
          destination,
          statements: [
            { sql: `SELECT set_config('storage.allow_delete_query', 'true', true)` },
            { sql: 'DELETE FROM storage.objects WHERE bucket_id = $1', values: [bucketName] },
            { sql: 'DELETE FROM storage.buckets WHERE id = $1', values: [bucketName] },
          ],
          commit: true,
        })
      )
    }
  })

  it('opens a transaction from a batch message and continues it by lock', async () => {
    const bucketName = uniqueBucketName('dbwatt-batch-open')

    try {
      const opened = await checkedResult<BatchResponse>(
        sendDatabaseMessage('database.batch', {
          destination: testDestination(),
          requestId: randomUUID(),
          isolationLevel: 'repeatable read',
          statements: [
            {
              sql: `INSERT INTO storage.buckets (id, name, owner, public) VALUES ($1, $1, $2, false)`,
              values: [bucketName, randomUUID()],
            },
          ],
          commit: false,
        })
      )

      expect(opened.lockId).toEqual(expect.any(String))
      // Not visible outside the transaction until it commits.
      expect(await getBucket(bucketName)).toBeUndefined()

      const isolation = await checkedResult<RawRowsResponse>(
        sendDatabaseMessage('database.lockedQuery', {
          lockId: opened.lockId,
          requestId: randomUUID(),
          sql: 'SHOW transaction_isolation',
        })
      )
      expect(decodeRows(isolation)[0]).toEqual({ transaction_isolation: 'repeatable read' })

      await checkedResult(
        sendDatabaseMessage('database.commitTransaction', { lockId: opened.lockId })
      )
      expect(await getBucket(bucketName)).toMatchObject({ id: bucketName })
    } finally {
      await cleanupBucket(bucketName)
    }
  })

  it('rolls back a failed batch and leaves no partial state', async () => {
    const bucketName = uniqueBucketName('dbwatt-batch-fail')

    const response = (await sendDatabaseMessage('database.batch', {
      destination: testDestination(),
      requestId: randomUUID(),
      statements: [
        {
          sql: `INSERT INTO storage.buckets (id, name, owner, public) VALUES ($1, $1, $2, false)`,
          values: [bucketName, randomUUID()],
        },
        { sql: 'SELECT * FROM storage.does_not_exist' },
      ],
      commit: true,
    })) as ErrorResponse & { connectionDiscarded?: boolean }

    expect(response).toMatchObject({
      code: 'POSTGRES_ERROR',
      sqlState: '42P01',
      connectionDiscarded: true,
    })
    expect(await getBucket(bucketName)).toBeUndefined()
    // The pooled connection is clean again, not stuck in an aborted transaction.
    expect(decodeRows(await queryDatabaseWatt())[0]).toEqual({ value: 1 })
  })

  it('handles concurrent Database Watt query load', async () => {
    const response = await concurrentQueriesDatabaseWatt()
    const stats = await getDatabaseWattStats()

    expect(response).toEqual({ count: 5 })
    expect(stats.query).toBeGreaterThanOrEqual(5)
  })
})
