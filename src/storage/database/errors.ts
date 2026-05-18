import { ERRORS, RenderableError, StorageBackendError, StorageErrorOptions } from '@internal/errors'
import { DatabaseError } from 'pg'

export class DBError extends StorageBackendError implements RenderableError {
  constructor(options: StorageErrorOptions) {
    super(options)
    Object.setPrototypeOf(this, DBError.prototype)
  }

  static fromDBError(pgError: DatabaseError, query?: string) {
    switch (pgError.code) {
      case '42501':
        return ERRORS.AccessDenied(
          pgError.message.includes('row-level security')
            ? 'new row violates row-level security policy'
            : pgError.message,
          pgError
        ).withMetadata({
          query,
          code: pgError.code,
        })
      case '23505':
        return ERRORS.ResourceAlreadyExists(pgError).withMetadata({
          query,
          code: pgError.code,
        })
      case '23503':
        return ERRORS.RelatedResourceNotFound(pgError).withMetadata({
          query,
          code: pgError.code,
        })
      case '55P03':
      case 'resource_locked':
        return ERRORS.ResourceLocked(pgError).withMetadata({
          query,
          code: pgError.code,
        })
      case '57014':
        return ERRORS.DatabaseTimeout(pgError).withMetadata({
          query,
          code: pgError.code,
        })
      case '25006':
        return ERRORS.DatabaseReadOnly(pgError).withMetadata({
          query,
          code: pgError.code,
        })
      case '25P02':
        return ERRORS.DatabaseTransactionAborted(pgError).withMetadata({
          query,
          code: pgError.code,
        })
      case '42P17':
        return ERRORS.InvalidObjectDefinition(pgError).withMetadata({
          query,
          code: pgError.code,
        })
      case '22P02':
        return ERRORS.InvalidParameter('value', {
          error: pgError,
          message: pgError.message || 'Invalid value format or type conversion failed',
        }).withMetadata({
          query,
          code: pgError.code,
        })
      case '42703':
      case '42P01':
        return ERRORS.DatabaseSchemaMismatch(pgError).withMetadata({
          query,
          code: pgError.code,
        })
      default:
        return ERRORS.DatabaseError(`database error, code: ${pgError.code}`, pgError).withMetadata({
          query,
          code: pgError.code,
        })
    }
  }
}

export function mapPgTransactionAbortedError(error: unknown, query?: string): unknown {
  if (isPgTransactionAbortedError(error)) {
    return ERRORS.DatabaseTransactionAborted(error).withMetadata({
      query,
      code: error.code,
    })
  }

  return error
}

function isPgTransactionAbortedError(error: unknown): error is DatabaseError {
  return error instanceof DatabaseError && error.code === '25P02'
}
