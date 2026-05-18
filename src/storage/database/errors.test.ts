import { DatabaseError } from 'pg'
import { DBError, escapeLike, mapPgTransactionAbortedError } from './errors'

describe('escapeLike', () => {
  test('escapes SQL wildcard characters', () => {
    expect(escapeLike('%_abc')).toBe('\\%\\_abc')
    expect(escapeLike('a%b_c')).toBe('a\\%b\\_c')
    expect(escapeLike('plain-text')).toBe('plain-text')
  })
})

describe('DBError', () => {
  test('preserves non-RLS permission failure messages', () => {
    const error = createPgError('42501', 'permission denied for table objects')

    expect(DBError.fromDBError(error, 'SELECT * FROM storage.objects')).toMatchObject({
      code: 'AccessDenied',
      message: 'permission denied for table objects',
      originalError: error,
      metadata: {
        query: 'SELECT * FROM storage.objects',
        code: '42501',
      },
    })
  })

  test('normalizes RLS permission failures', () => {
    const error = createPgError('42501', 'new row violates row-level security policy for table')

    expect(DBError.fromDBError(error, 'INSERT INTO storage.objects')).toMatchObject({
      code: 'AccessDenied',
      message: 'new row violates row-level security policy',
      originalError: error,
      metadata: {
        query: 'INSERT INTO storage.objects',
        code: '42501',
      },
    })
  })

  test('maps aborted transactions to a typed database error', () => {
    const error = createPgError('25P02', 'current transaction is aborted')

    expect(DBError.fromDBError(error, 'SAVEPOINT "storage_pg_query_1"')).toMatchObject({
      code: 'DatabaseTransactionAborted',
      message:
        'The database transaction has been aborted. Roll back the transaction before retrying.',
      originalError: error,
      metadata: {
        query: 'SAVEPOINT "storage_pg_query_1"',
        code: '25P02',
      },
    })
  })

  test('maps aborted transaction errors at savepoint acquisition boundaries', () => {
    const error = createPgError('25P02', 'current transaction is aborted')

    expect(
      mapPgTransactionAbortedError(error, 'SAVEPOINT "iceberg_pg_transaction_1"')
    ).toMatchObject({
      code: 'DatabaseTransactionAborted',
      originalError: error,
      metadata: {
        query: 'SAVEPOINT "iceberg_pg_transaction_1"',
        code: '25P02',
      },
    })
  })
})

function createPgError(code: string, message: string): DatabaseError {
  const error = new DatabaseError(message, message.length, 'error')
  error.code = code
  return error
}
