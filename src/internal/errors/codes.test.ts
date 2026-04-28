import { IcebergError, IcebergErrorType } from '@storage/protocols/iceberg/catalog/errors'
import { ErrorCode, normalizeRawError } from './codes'
import { StorageBackendError } from './storage-error'

describe('normalizeRawError', () => {
  it('includes stack for 5xx errors', () => {
    const error = new StorageBackendError({
      code: ErrorCode.InternalError,
      httpStatusCode: 500,
      message: 'Internal server error',
    })

    const result = normalizeRawError(error, 'info')

    expect(result.statusCode).toBe(500)
    expect(result.errorCode).toBe(ErrorCode.InternalError)
    expect(result.stack).toBeTruthy()
  })

  it('excludes stack for 4xx errors', () => {
    const error = new StorageBackendError({
      code: ErrorCode.InvalidRequest,
      httpStatusCode: 400,
      message: 'Bad request',
    })

    const result = normalizeRawError(error, 'info')

    expect(result.statusCode).toBe(400)
    expect(result.errorCode).toBe(ErrorCode.InvalidRequest)
    expect(result.stack).toBe('')
  })

  it('includes stack for UnknownError regardless of status code', () => {
    const error = new Error('Something unexpected')

    const result = normalizeRawError(error, 'info')

    expect(result.errorCode).toBe(ErrorCode.UnknownError)
    expect(result.stack).toBeTruthy()
  })

  it('includes stack when log level is debug', () => {
    const error = new StorageBackendError({
      code: ErrorCode.InvalidRequest,
      httpStatusCode: 400,
      message: 'Bad request',
    })

    const result = normalizeRawError(error, 'debug')

    expect(result.stack).toBeTruthy()
  })

  it('recognizes error codes by value in KNOWN_ERROR_CODES', () => {
    const error = new Error('test')
    Object.assign(error, { code: ErrorCode.S3InvalidAccessKeyId })

    const result = normalizeRawError(error, 'info')

    expect(result.errorCode).toBe(ErrorCode.S3InvalidAccessKeyId)
  })

  it('falls back to UnknownError when code is not in KNOWN_ERROR_CODES', () => {
    const error = new Error('test')
    Object.assign(error, { code: 'UNKNOWN_CODE' })

    const result = normalizeRawError(error, 'info')

    expect(result.errorCode).toBe(ErrorCode.UnknownError)
  })

  it('maps Fastify error codes to ErrorCode', () => {
    const error = new Error('Validation failed')
    Object.assign(error, { code: 'FST_ERR_VALIDATION', statusCode: 400 })

    const result = normalizeRawError(error, 'info')

    expect(result.errorCode).toBe(ErrorCode.InvalidRequest)
    expect(result.statusCode).toBe(400)
  })

  it('handles IcebergError with error property', () => {
    const error = new IcebergError(
      'Namespace not found',
      IcebergErrorType.NoSuchNamespaceException,
      400
    )

    const result = normalizeRawError(error, 'info')

    expect(result.errorCode).toBe(IcebergErrorType.NoSuchNamespaceException)
    expect(result.statusCode).toBe(400)
  })

  it('handles non-Error objects', () => {
    const result = normalizeRawError({ some: 'object' }, 'info')

    expect(result).toHaveProperty('raw')
    expect(result).not.toHaveProperty('errorCode')
  })

  it('handles unstringifiable errors', () => {
    const circular: any = {}
    circular.self = circular

    const result = normalizeRawError(circular, 'info')

    expect(result.raw).toBe('Failed to stringify error')
  })
})
