import { ErrorCode } from '@internal/errors'
import { DatabaseError } from 'pg'
import { DBError } from '../storage/database/knex'

function createPgError(message: string, overrides?: Partial<DatabaseError>): DatabaseError {
  return Object.assign(new Error(message), overrides) as DatabaseError
}

describe('DBError.fromDBError', () => {
  test.each([
    ['objects_name_check', 'insert into storage.objects ...'],
    ['s3_multipart_uploads_key_check', 'insert into storage.s3_multipart_uploads ...'],
    ['s3_multipart_uploads_parts_key_check', 'insert into storage.s3_multipart_uploads_parts ...'],
  ])('maps %s violations to a stable Invalid object name message', (constraint, query) => {
    const pgError = createPgError(`violates check constraint "${constraint}"`, {
      code: '23514',
      constraint,
    })

    const err = DBError.fromDBError(pgError, query)

    expect(err.code).toBe(ErrorCode.InvalidKey)
    expect(err.message).toBe('Invalid object name')
    expect(err.metadata).toMatchObject({
      code: '23514',
      query,
    })
  })

  it('preserves the original database message for other check constraints', () => {
    const pgError = createPgError('violates check constraint "something_else"', {
      code: '23514',
      constraint: 'something_else',
    })

    const err = DBError.fromDBError(pgError)

    expect(err.code).toBe(ErrorCode.DatabaseError)
    expect(err.message).toBe('violates check constraint "something_else"')
  })
})
