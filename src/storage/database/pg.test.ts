import { PgTenantConnection } from '@internal/database'
import { normalizeRawError } from '@internal/errors'
import { dbQueryPerformance } from '@internal/monitoring/metrics'
import { DatabaseError } from 'pg'
import { vi } from 'vitest'
import { escapeLike, StoragePgDB } from './pg'

class TestStoragePgDB extends StoragePgDB {
  runMetricProbe(): Promise<string> {
    return this.runUnscopedQuery('MetricWithoutTenantAttribute', async () => 'ok')
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

    try {
      await expect(storage.runMetricProbe()).resolves.toBe('ok')

      expect(recordSpy).toHaveBeenCalledWith(expect.any(Number), {
        name: 'MetricWithoutTenantAttribute',
        requestAborted: false,
        requestAbortedBeforeStart: false,
        requestAbortedAfterStart: false,
      })
      expect(recordSpy.mock.calls[0]?.[1]).not.toHaveProperty('tenantId')
    } finally {
      recordSpy.mockRestore()
    }
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
