import { type PgExecutor, PgPoolExecutor, PgTenantConnection } from '@internal/database'
import { DBMigration } from '@internal/database/migrations'
import { normalizeRawError } from '@internal/errors'
import { dbQueryPerformance } from '@internal/monitoring/metrics'
import { EventEmitter } from 'events'
import { DatabaseError, type Pool, type PoolClient } from 'pg'
import { vi } from 'vitest'
import { escapeLike, StoragePgDB } from './pg'

class TestStoragePgDB extends StoragePgDB {
  runMetricProbe(): Promise<string> {
    return this.runUnscopedQuery('MetricWithoutTenantAttribute', async () => 'ok')
  }

  runScopedMetricProbe(): Promise<string> {
    return this.runQuery('ScopedMetricDuration', async () => 'ok')
  }

  runErrorMappingProbe(): Promise<unknown> {
    return this.runQuery('FindBucketById', (db) => {
      return this.query(db, 'SELECT * FROM storage.buckets WHERE id = $1')
    })
  }

  runUnscopedErrorMappingProbe(): Promise<string> {
    return this.runUnscopedQuery('CreateS3KeysTempTable', async () => 'ok')
  }
}

function createQueryCaptureStorage(latestMigration?: keyof typeof DBMigration) {
  const transaction = {
    commit: vi.fn(),
    rollback: vi.fn(),
    isCompleted: vi.fn().mockReturnValue(false),
    query: vi.fn().mockResolvedValue({ rows: [{ id: 'row' }], rowCount: 1 }),
  }
  const connection = {
    getAbortSignal: vi.fn().mockReturnValue(undefined),
    transaction: vi.fn().mockResolvedValue(transaction),
    setScope: vi.fn(),
  } as unknown as PgTenantConnection
  const storage = new StoragePgDB(connection, {
    tenantId: 'column-selection-tenant',
    host: 'localhost',
    latestMigration,
  })

  return { storage, transaction }
}

describe('escapeLike', () => {
  test('escapes SQL wildcard characters', () => {
    expect(escapeLike('%_abc')).toBe('\\%\\_abc')
    expect(escapeLike('a%b_c')).toBe('a\\%b\\_c')
    expect(escapeLike('plain-text')).toBe('plain-text')
  })

  test('escapes backslashes before SQL wildcard characters', () => {
    expect(escapeLike('path\\name')).toBe(String.raw`path\\name`)
    expect(escapeLike(String.raw`a\%b_c`)).toBe(String.raw`a\\\%b\_c`)
  })
})

describe('StoragePgDB column selection', () => {
  test('keeps all requested object columns when their migrations are available', async () => {
    const { storage, transaction } = createQueryCaptureStorage()

    await storage.findObject('bucket', 'object', 'id,user_metadata,metadata')

    expect(transaction.query.mock.calls[0]?.[0]).toMatchObject({
      text: expect.stringContaining('SELECT "id", "user_metadata", "metadata"'),
    })
  })

  test('strips unavailable object columns directly while compiling the SELECT list', async () => {
    const { storage, transaction } = createQueryCaptureStorage('initialmigration')

    await storage.findObject('bucket', 'object', 'id,user_metadata,metadata')

    expect(transaction.query.mock.calls[0]?.[0]).toMatchObject({
      text: expect.stringContaining('SELECT "id", "metadata"'),
    })
    expect((transaction.query.mock.calls[0]?.[0] as { text: string }).text).not.toContain(
      '"user_metadata"'
    )
  })

  test('strips only multipart metadata after custom metadata is available', async () => {
    const { storage, transaction } = createQueryCaptureStorage('custom-metadata')

    await storage.findMultipartUpload('upload', 'id,user_metadata,metadata')

    expect(transaction.query.mock.calls[0]?.[0]).toMatchObject({
      text: expect.stringContaining('SELECT "id", "user_metadata"'),
    })
    expect((transaction.query.mock.calls[0]?.[0] as { text: string }).text).not.toContain(
      '"metadata"'
    )
  })

  test('preserves listBuckets synthetic type placement', async () => {
    const { storage, transaction } = createQueryCaptureStorage()

    await storage.listBuckets('type,id,name')

    expect(transaction.query.mock.calls[0]?.[0]).toMatchObject({
      text: expect.stringContaining('SELECT "id", "name", \'STANDARD\' AS "type"'),
    })
  })
})

describe('StoragePgDB metrics', () => {
  test('records DB query duration without tenantId attribute', async () => {
    const connection = {
      getAbortSignal: vi.fn().mockReturnValue(undefined),
      pool: {
        acquire: vi.fn(),
      },
    } as unknown as PgTenantConnection
    const storage = new TestStoragePgDB(connection, {
      tenantId: 'metric-cardinality-tenant',
      host: 'localhost',
    })
    const recordSpy = vi.spyOn(dbQueryPerformance, 'record')
    const performanceNowSpy = vi
      .spyOn(performance, 'now')
      .mockReturnValueOnce(10)
      .mockReturnValueOnce(15)

    try {
      await expect(storage.runMetricProbe()).resolves.toBe('ok')

      expect(recordSpy).toHaveBeenCalledWith(0.005, {
        name: 'MetricWithoutTenantAttribute',
        requestAborted: false,
        requestAbortedBeforeStart: false,
        requestAbortedAfterStart: false,
      })
      expect(recordSpy.mock.calls[0]?.[1]).not.toHaveProperty('tenantId')
    } finally {
      performanceNowSpy.mockRestore()
      recordSpy.mockRestore()
    }
  })

  test('records scoped DB query duration from numeric monotonic timestamps', async () => {
    const transaction = {
      commit: vi.fn(),
      rollback: vi.fn(),
      isCompleted: vi.fn().mockReturnValue(false),
    }
    const connection = {
      getAbortSignal: vi.fn().mockReturnValue(undefined),
      transaction: vi.fn().mockResolvedValue(transaction),
      setScope: vi.fn(),
    } as unknown as PgTenantConnection
    const storage = new TestStoragePgDB(connection, {
      tenantId: 'metric-cardinality-tenant',
      host: 'localhost',
    })
    const recordSpy = vi.spyOn(dbQueryPerformance, 'record')
    const performanceNowSpy = vi
      .spyOn(performance, 'now')
      .mockReturnValueOnce(2)
      .mockReturnValueOnce(9)

    try {
      await expect(storage.runScopedMetricProbe()).resolves.toBe('ok')

      expect(recordSpy).toHaveBeenCalledWith(0.007, {
        name: 'ScopedMetricDuration',
        requestAborted: false,
        requestAbortedBeforeStart: false,
        requestAbortedAfterStart: false,
      })
    } finally {
      performanceNowSpy.mockRestore()
      recordSpy.mockRestore()
    }
  })
})

describe('StoragePgDB healthcheck', () => {
  const probeSql = 'SELECT id from storage.buckets limit 1'
  const expectedAbortError = {
    name: 'AbortError',
    code: 'ABORT_ERR',
    message: 'Query was aborted',
  }
  let UnscopedStoragePgDB: typeof StoragePgDB

  beforeAll(async () => {
    vi.resetModules()
    const configModule = await import('../../config')
    const { databaseHealthcheckUnscoped } = configModule.getConfig()
    configModule.mergeConfig({ databaseHealthcheckUnscoped: true })

    try {
      const pgModule = await import('./pg')
      UnscopedStoragePgDB = pgModule.StoragePgDB
    } finally {
      configModule.mergeConfig({ databaseHealthcheckUnscoped })
    }
  })

  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.clearAllTimers()
    vi.useRealTimers()
  })

  function createHealthcheckFixture(
    executor: PgExecutor,
    options: {
      requestSignal?: AbortSignal
      StorageClass?: typeof StoragePgDB
    } = {}
  ) {
    const { requestSignal, StorageClass = TestStoragePgDB } = options
    const transaction = {
      commit: vi.fn(),
      rollback: vi.fn(),
      isCompleted: vi.fn().mockReturnValue(false),
      query: vi.fn().mockResolvedValue({ rows: [] }),
    }
    const connection = {
      getAbortSignal: vi.fn().mockReturnValue(requestSignal),
      pool: {
        acquire: vi.fn().mockReturnValue(executor),
      },
      transaction: vi.fn().mockResolvedValue(transaction),
      setScope: vi.fn(),
    } as unknown as PgTenantConnection
    const storage = new StorageClass(connection, {
      tenantId: 'healthcheck-tenant',
      host: 'localhost',
    })

    return { connection, storage, transaction }
  }

  function createProbe(requestSignal?: AbortSignal) {
    let finishQuery: (() => void) | undefined
    const executor = {
      query: vi.fn().mockImplementation(
        () =>
          new Promise((resolve) => {
            finishQuery = () => resolve({ rows: [] })
          })
      ),
    }
    const fixture = createHealthcheckFixture(executor, {
      requestSignal,
      StorageClass: UnscopedStoragePgDB,
    })

    return {
      ...fixture,
      executor,
      probeSignal: () => executor.query.mock.calls[0]?.[1]?.signal as AbortSignal | undefined,
      finishQuery: () => {
        if (!finishQuery) {
          throw new Error('Probe query has not started')
        }
        finishQuery()
      },
    }
  }

  function createPendingPoolExecutor() {
    const release = vi.fn()
    const client = Object.assign(new EventEmitter(), {
      query: vi.fn(() => new Promise(() => undefined)),
      release,
    }) as unknown as PoolClient & EventEmitter
    const connect = vi.fn().mockResolvedValue(client)
    const pool = { connect } as unknown as Pool

    return { client, connect, executor: new PgPoolExecutor(pool), release }
  }

  test('rejects and disposes the client when the healthcheck timeout elapses', async () => {
    const { client, executor, release } = createPendingPoolExecutor()
    const fixture = createHealthcheckFixture(executor, {
      StorageClass: UnscopedStoragePgDB,
    })
    const probe = fixture.storage.healthcheck()
    const rejection = expect(probe).rejects.toMatchObject(expectedAbortError)

    await vi.advanceTimersByTimeAsync(0)
    expect(client.query).toHaveBeenCalledWith(probeSql, undefined)

    await vi.advanceTimersToNextTimerAsync()

    await rejection
    expect(release).toHaveBeenCalledWith(expect.objectContaining(expectedAbortError))

    expect(fixture.connection.pool.acquire).toHaveBeenCalledTimes(1)
    expect(fixture.connection.transaction).not.toHaveBeenCalled()
    expect(fixture.connection.setScope).not.toHaveBeenCalled()
  })

  test('rejects and disposes the client when the request is canceled in flight', async () => {
    const requestController = new AbortController()
    const { client, executor, release } = createPendingPoolExecutor()
    const fixture = createHealthcheckFixture(executor, {
      requestSignal: requestController.signal,
      StorageClass: UnscopedStoragePgDB,
    })
    const probe = fixture.storage.healthcheck()
    const rejection = expect(probe).rejects.toMatchObject(expectedAbortError)

    await vi.advanceTimersByTimeAsync(0)
    expect(client.query).toHaveBeenCalledWith(probeSql, undefined)

    requestController.abort()

    await rejection
    expect(release).toHaveBeenCalledWith(expect.objectContaining(expectedAbortError))
    expect(vi.getTimerCount()).toBe(0)
  })

  test('rejects before checkout when the request is already canceled', async () => {
    const requestController = new AbortController()
    requestController.abort()
    const { connect, executor } = createPendingPoolExecutor()
    const fixture = createHealthcheckFixture(executor, {
      requestSignal: requestController.signal,
      StorageClass: UnscopedStoragePgDB,
    })

    await expect(fixture.storage.healthcheck()).rejects.toMatchObject(expectedAbortError)

    expect(connect).not.toHaveBeenCalled()
    expect(vi.getTimerCount()).toBe(0)
  })

  test('uses the scoped readiness probe by default', async () => {
    const executor = {
      query: vi.fn().mockResolvedValue({ rows: [] }),
    } as unknown as PgExecutor
    const fixture = createHealthcheckFixture(executor)

    await expect(fixture.storage.healthcheck()).resolves.toBeUndefined()

    expect(fixture.connection.pool.acquire).not.toHaveBeenCalled()
    expect(fixture.connection.transaction).toHaveBeenCalledTimes(1)
    expect(fixture.connection.setScope).toHaveBeenCalledWith(fixture.transaction)
    expect(fixture.transaction.query).toHaveBeenCalledWith(probeSql, { signal: undefined })
    expect(fixture.transaction.commit).toHaveBeenCalledTimes(1)
  })

  test('clears the timeout and stops observing the request signal once the probe settles', async () => {
    const requestController = new AbortController()
    const probeFixture = createProbe(requestController.signal)
    const probe = probeFixture.storage.healthcheck()
    probeFixture.finishQuery()
    await expect(probe).resolves.toBeUndefined()

    expect(vi.getTimerCount()).toBe(0)

    requestController.abort()
    expect(probeFixture.probeSignal()?.aborted).toBe(false)
  })
})

describe('StoragePgDB error mapping', () => {
  test('preserves query name for pg errors thrown by inner SQL statements', async () => {
    const error = createPgError('08P01', 'no more connections allowed (max_client_conn)')
    error.severity = 'FATAL'
    error.routine = 'pooler_error'
    const transaction = {
      commit: vi.fn(),
      rollback: vi.fn(),
      isCompleted: vi.fn().mockReturnValue(false),
      query: vi.fn().mockRejectedValue(error),
    }
    const connection = {
      getAbortSignal: vi.fn().mockReturnValue(undefined),
      transaction: vi.fn().mockResolvedValue(transaction),
      setScope: vi.fn(),
    } as unknown as PgTenantConnection
    const storage = new TestStoragePgDB(connection, {
      tenantId: 'tenant-with-protocol-error',
      host: 'localhost',
    })

    const mappedError = await storage.runErrorMappingProbe().catch((error) => error)

    expect(mappedError).toMatchObject({
      code: 'DatabaseError',
      message: 'database error, code: 08P01',
      originalError: error,
      metadata: {
        code: '08P01',
        pgMessage: 'no more connections allowed (max_client_conn)',
        query: 'SELECT * FROM storage.buckets WHERE id = $1',
        queryName: 'FindBucketById',
      },
    })
    // severity/routine are set on the pg error but must not be duplicated into metadata.
    expect(JSON.parse(normalizeRawError(mappedError, 'info').raw).metadata).toEqual({
      code: '08P01',
      pgMessage: 'no more connections allowed (max_client_conn)',
      query: 'SELECT * FROM storage.buckets WHERE id = $1',
      queryName: 'FindBucketById',
    })
  })

  test('preserves query name for pg errors thrown while starting the transaction', async () => {
    const error = createPgError('08P01', 'no more connections allowed (max_client_conn)')
    error.severity = 'FATAL'
    error.routine = 'pooler_error'
    const connection = {
      getAbortSignal: vi.fn().mockReturnValue(undefined),
      transaction: vi.fn().mockRejectedValue(error),
      setScope: vi.fn(),
    } as unknown as PgTenantConnection
    const storage = new TestStoragePgDB(connection, {
      tenantId: 'tenant-with-transaction-setup-error',
      host: 'localhost',
    })

    const mappedError = await storage.runErrorMappingProbe().catch((error) => error)

    expect(mappedError).toMatchObject({
      code: 'DatabaseError',
      message: 'database error, code: 08P01',
      originalError: error,
      metadata: {
        code: '08P01',
        pgMessage: 'no more connections allowed (max_client_conn)',
        queryName: 'FindBucketById',
      },
    })
    expect(JSON.parse(normalizeRawError(mappedError, 'info').raw).metadata).toEqual({
      code: '08P01',
      pgMessage: 'no more connections allowed (max_client_conn)',
      queryName: 'FindBucketById',
    })
  })

  test('preserves query name for pg errors thrown while acquiring an unscoped executor', async () => {
    const error = createPgError('08006', 'connection failure')
    error.severity = 'FATAL'
    const connection = {
      getAbortSignal: vi.fn().mockReturnValue(undefined),
      pool: {
        acquire: vi.fn(() => {
          throw error
        }),
      },
    } as unknown as PgTenantConnection
    const storage = new TestStoragePgDB(connection, {
      tenantId: 'tenant-with-unscoped-acquire-error',
      host: 'localhost',
    })

    const mappedError = await storage.runUnscopedErrorMappingProbe().catch((error) => error)

    expect(mappedError).toMatchObject({
      code: 'DatabaseError',
      originalError: error,
      metadata: {
        code: '08006',
        pgMessage: 'connection failure',
        queryName: 'CreateS3KeysTempTable',
      },
    })
    expect(JSON.parse(normalizeRawError(mappedError, 'info').raw).metadata).toEqual({
      code: '08006',
      pgMessage: 'connection failure',
      queryName: 'CreateS3KeysTempTable',
    })
  })

  test('passes through non-pg errors from transaction setup', async () => {
    const error = new Error('connection setup failed before pg error mapping')
    const connection = {
      getAbortSignal: vi.fn().mockReturnValue(undefined),
      transaction: vi.fn().mockRejectedValue(error),
      setScope: vi.fn(),
    } as unknown as PgTenantConnection
    const storage = new TestStoragePgDB(connection, {
      tenantId: 'tenant-with-non-pg-error',
      host: 'localhost',
    })

    await expect(storage.runErrorMappingProbe()).rejects.toBe(error)
  })

  test('does not attach query name to non-pg storage errors from transaction setup', async () => {
    // transaction() resolving without a handle makes runQuery throw
    // ERRORS.InternalError('Could not create transaction') — a StorageBackendError
    // whose originalError is not a pg error, so the gate must leave it untouched.
    const connection = {
      getAbortSignal: vi.fn().mockReturnValue(undefined),
      transaction: vi.fn().mockResolvedValue(undefined),
      setScope: vi.fn(),
    } as unknown as PgTenantConnection
    const storage = new TestStoragePgDB(connection, {
      tenantId: 'tenant-with-missing-transaction-handle',
      host: 'localhost',
    })

    const mappedError = await storage.runErrorMappingProbe().catch((error) => error)

    expect(mappedError).toMatchObject({
      code: 'InternalError',
      metadata: expect.not.objectContaining({ queryName: expect.anything() }),
    })
  })
})

function createPgError(code: string, message: string): DatabaseError {
  const error = new DatabaseError(message, message.length, 'error')
  error.code = code
  return error
}
