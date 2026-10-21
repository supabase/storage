import { Writable } from 'node:stream'
import { IcebergError, IcebergErrorType } from '@storage/protocols/iceberg/catalog/errors'
import { errors as joseErrors } from 'jose'
import pino from 'pino'
import { fetch } from 'undici'
import { ERRORS, ErrorCode, getErrorCode, normalizeRawError } from './codes'
import { StorageBackendError } from './storage-error'

describe('normalizeRawError', () => {
  it.each([
    ['function', () => undefined],
    ['symbol', Symbol('ignored')],
    ['object whose toJSON returns undefined', { toJSON: () => undefined }],
  ] as const)('omits raw when %s produces no JSON', (_name, error) => {
    const lines: string[] = []
    const testLogger = pino(
      { serializers: { error: (value) => normalizeRawError(value, 'info') } },
      new Writable({
        write(chunk, _encoding, callback) {
          lines.push(chunk.toString())
          callback()
        },
      })
    )

    testLogger.error({ error }, 'serialization probe')

    expect(JSON.parse(lines[0]).error).toEqual({})
    expect(normalizeRawError(error, 'info').raw).toBeUndefined()
  })

  it.each([
    ['name', undefined],
    ['name', null],
    ['message', undefined],
    ['message', null],
  ] as const)('keeps a non-string %s: %s', (field, value) => {
    const error = Object.assign(new Error('failed'), { [field]: value })

    expect(normalizeRawError(error, 'info')[field]).toBe(value)
  })

  it('retains a root error cause', () => {
    expect(normalizeRawError(new Error('x', { cause: 'boom' }), 'info').raw).toBe(
      '{"cause":"boom"}'
    )
  })

  it('retains the individual errors in a root AggregateError', () => {
    const error = new AggregateError([new Error('first'), new Error('second')], 'failed', {
      cause: 'root cause',
    })
    const raw = JSON.parse(normalizeRawError(error, 'info').raw ?? '')

    expect(raw.cause).toBe('root cause')
    expect(raw.errors).toMatchObject([
      { name: 'Error', message: 'first' },
      { name: 'Error', message: 'second' },
    ])
  })

  it.each(['root', 'nested'])('retains an enumerable errors property on a %s error', (location) => {
    const error = Object.assign(new Error('validation failed'), {
      errors: [{ message: 'name is required' }],
    })
    const raw = JSON.parse(
      normalizeRawError(location === 'root' ? error : new Error('failed', { cause: error }), 'info')
        .raw ?? ''
    )

    expect((location === 'root' ? raw : raw.cause).errors).toEqual(error.errors)
  })

  it.each([
    ['JWTExpired', joseErrors.JWTExpired],
    ['JWTClaimValidationFailed', joseErrors.JWTClaimValidationFailed],
  ] as const)('omits token claims from a wrapped %s error and its cause', (_name, JwtError) => {
    const error = new JwtError(
      'JWT claim validation failed',
      { sub: 'private-user-id', email: 'private@example.invalid', exp: 1 },
      'exp',
      'check_failed'
    )
    const result = normalizeRawError(ERRORS.AccessDenied(error.message, error), 'info')
    const raw = JSON.parse(result.raw ?? '')

    expect(result.raw).not.toContain('private-user-id')
    expect(result.raw).not.toContain('private@example.invalid')
    expect(raw.originalError.payload).toBeUndefined()
    expect(raw.originalError.cause).toEqual({ claim: 'exp', reason: 'check_failed' })
    expect(raw.originalError.code).toBe(error.code)
    expect(error.payload.email).toBe('private@example.invalid')
  })

  it('omits Redis command arguments while retaining the command name', () => {
    const error = Object.assign(new Error('WRONGPASS invalid username-password pair'), {
      command: { name: 'auth', args: ['default', 'private-redis-password'] },
    })
    const result = normalizeRawError(new Error('rate limit failed', { cause: error }), 'info')

    expect(result.raw).not.toContain('private-redis-password')
    expect(JSON.parse(result.raw ?? '').cause.command).toEqual({ name: 'auth' })
    expect(error.command.args).toEqual(['default', 'private-redis-password'])
  })

  it('omits command arguments even when the command has no name', () => {
    const result = normalizeRawError({ command: { args: ['private-redis-password'] } }, 'info')

    expect(JSON.parse(result.raw ?? '')).toEqual({ command: {} })
  })

  it.each([
    'https://user:private-password@example.invalid/path?token=private-token',
    'https://user:private-password@[invalid/path?token=private-token',
    'invalid private-password input\nprivate-token',
  ])('redacts fetch URL errors for %s', async (input) => {
    const error = await fetch(input).catch((error: unknown) => error)
    expect(error).toBeInstanceOf(Error)
    const originalMessage = (error as Error).message

    for (const value of [
      error,
      new Error('fetch failed', { cause: error }),
      StorageBackendError.fromError(error),
      new AggregateError([error], 'fetch failed'),
    ]) {
      const result = normalizeRawError(value, 'info')
      const serialized = JSON.stringify(result)
      expect(serialized).not.toContain('private-password')
      expect(serialized).not.toContain('private-token')
      expect(serialized).toContain('[Redacted URL]')
      expect(serialized).toContain(' at ')
      expect(result.raw).not.toContain('"input"')
    }
    expect((error as Error).message).toBe(originalMessage)
    expect(originalMessage).toContain('private-password')
  })

  it('omits an invalid URL input from root and nested errors', () => {
    let error: unknown
    try {
      new URL('postgres://user:private-password@[invalid/db?token=private-token')
    } catch (caught) {
      error = caught
    }
    expect(error).toBeInstanceOf(TypeError)

    for (const value of [error, new Error('startup failed', { cause: error })]) {
      const result = normalizeRawError(value, 'info')
      expect(JSON.stringify(result)).not.toContain('private-password')
      expect(result.raw).not.toContain('"input"')
      expect(result.raw).toContain('ERR_INVALID_URL')
    }
  })

  it('omits signing data copied from an unmodelled S3 XML error', () => {
    const error = Object.assign(new Error('signature failed'), {
      name: 'SignatureDoesNotMatch',
      RequestId: 'test-id',
      CanonicalRequest: 'private-canonical',
      CanonicalRequestBytes: 'private-canonical-bytes',
      StringToSign: 'private-signing',
      StringToSignBytes: 'private-signing-bytes',
      AWSAccessKeyId: 'private-access-key-id',
      SignatureProvided: 'private-signature',
      'Token-0': 'private-session-token',
    })

    for (const value of [error, StorageBackendError.fromError(error)]) {
      const result = normalizeRawError(value, 'info')
      expect(result.raw).not.toContain('private-')
      expect(result.raw).toContain('test-id')
    }
  })

  it('filters and expands errors inside a non-Error cleanup array', () => {
    const error = Object.assign(new Error('cleanup failed'), {
      client: { password: 'private-database-password' },
    })
    const result = normalizeRawError([error], 'info')

    expect(result.raw).not.toContain('private-database-password')
    expect(JSON.parse(result.raw ?? '')).toEqual([{ name: 'Error', message: 'cleanup failed' }])
  })

  it('filters non-Error objects and limits their depth and breadth', () => {
    const result = normalizeRawError(
      {
        client: { password: 'private-database-password' },
        payload: { sub: 'private-user-id' },
        values: Array.from({ length: 100 }, (_, i) => i),
        nested: { a: { b: { c: { d: { e: { f: { g: { secret: 'too deep' } } } } } } } },
      },
      'info'
    )

    expect(result.raw).not.toContain('private-database-password')
    expect(result.raw).not.toContain('private-user-id')
    expect(result.raw).not.toContain('too deep')
    expect(JSON.parse(result.raw ?? '').values.length).toBeLessThan(100)
  })

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

  it('handles circular non-Error objects', () => {
    const circular: { self?: unknown } = {}
    circular.self = circular

    const result = normalizeRawError(circular, 'info')

    expect(JSON.parse(result.raw ?? '')).toEqual({ self: '[Circular]' })
  })

  it('handles unstringifiable errors', () => {
    const error = {
      get message() {
        throw new Error('getter failed')
      },
    }

    expect(normalizeRawError(error, 'info').raw).toBe('Failed to stringify error')
  })

  it('surfaces a nested originalError/cause chain instead of collapsing it to {}', () => {
    const cause = Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:443'), {
      code: 'ECONNREFUSED',
    })
    const networkError = new TypeError('fetch failed', { cause })

    const error = new StorageBackendError({
      code: ErrorCode.InternalError,
      httpStatusCode: 500,
      message: 'Error purging cache',
      originalError: networkError,
    })

    const result = normalizeRawError(error, 'info')
    const raw = JSON.parse(result.raw ?? '')

    expect(raw.originalError.name).toBe('TypeError')
    expect(raw.originalError.message).toBe('fetch failed')
    expect(raw.originalError.cause.code).toBe('ECONNREFUSED')
    expect(raw.originalError.cause.message).toBe('connect ECONNREFUSED 127.0.0.1:443')
  })

  it('surfaces the individual errors wrapped by a nested AggregateError', () => {
    const aggregate = new AggregateError(
      [new Error('connect ECONNREFUSED 127.0.0.1:443'), new Error('connect ECONNREFUSED ::1:443')],
      'All promises were rejected'
    )

    const error = new StorageBackendError({
      code: ErrorCode.InternalError,
      httpStatusCode: 500,
      message: 'Error purging cache',
      originalError: aggregate,
    })

    const result = normalizeRawError(error, 'info')
    const raw = JSON.parse(result.raw ?? '')

    expect(raw.originalError.name).toBe('AggregateError')
    expect(raw.originalError.message).toBe('All promises were rejected')
    expect(raw.originalError.errors).toHaveLength(2)
    expect(raw.originalError.errors[0].message).toBe('connect ECONNREFUSED 127.0.0.1:443')
    expect(raw.originalError.errors[1].message).toBe('connect ECONNREFUSED ::1:443')
  })

  it('includes a nested cause stack for 5xx errors', () => {
    const originalError = new Error('boom')

    const error = new StorageBackendError({
      code: ErrorCode.InternalError,
      httpStatusCode: 500,
      message: 'Error purging cache',
      originalError,
    })

    const result = normalizeRawError(error, 'info')
    const raw = JSON.parse(result.raw ?? '')

    expect(raw.originalError.stack).toBeTruthy()
  })

  it('omits a nested cause stack for 4xx errors', () => {
    const originalError = new Error('boom')

    const error = new StorageBackendError({
      code: ErrorCode.InvalidRequest,
      httpStatusCode: 400,
      message: 'Bad request',
      originalError,
    })

    const result = normalizeRawError(error, 'info')
    const raw = JSON.parse(result.raw ?? '')

    expect(raw.originalError.stack).toBeUndefined()
  })

  it('omits pg client internals attached to Error objects', () => {
    const error = new Error('Connection terminated unexpectedly') as Error & {
      client?: unknown
      code?: string
    }
    const client: { ssl: { ca: string }; self?: unknown } = {
      ssl: { ca: 'secret root cert' },
    }
    client.self = client
    error.client = client
    error.code = '08006'

    const result = normalizeRawError(error, 'info')

    expect(result.raw).not.toContain('client')
    expect(result.raw).not.toContain('secret root cert')
    expect(JSON.parse(result.raw ?? '')).toEqual({ code: '08006' })
  })

  it('handles S3 errors with correct errorCode and statusCode', () => {
    const s3Error = new Error('The specified upload does not exist.')
    s3Error.name = 'NoSuchUpload'
    Object.assign(s3Error, {
      $metadata: {
        httpStatusCode: 404,
      },
    })

    const result = normalizeRawError(s3Error, 'info')

    expect(result.errorCode).toBe(ErrorCode.S3Error)
    expect(result.statusCode).toBe(404)
    expect(result.name).toBe('NoSuchUpload')
    expect(result.message).toBe('The specified upload does not exist.')
  })
})

describe('getErrorCode', () => {
  it.each([
    ['FST_ERR_VALIDATION', ErrorCode.InvalidRequest],
    ['FST_ERR_CTP_EMPTY_JSON_BODY', ErrorCode.InvalidRequest],
    ['FST_ERR_CTP_INVALID_JSON_BODY', ErrorCode.InvalidRequest],
    ['FST_ERR_CTP_INVALID_CONTENT_LENGTH', ErrorCode.InvalidRequest],
    ['FST_ERR_CTP_INVALID_MEDIA_TYPE', ErrorCode.InvalidMimeType],
    ['FST_ERR_CTP_BODY_TOO_LARGE', ErrorCode.EntityTooLarge],
  ])('maps Fastify error code %s to %s', (fastifyCode, expectedErrorCode) => {
    const error = new Error('Fastify error')
    Object.assign(error, { code: fastifyCode })

    expect(getErrorCode(error)).toBe(expectedErrorCode)
  })
})
