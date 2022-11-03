import { S3ServiceException } from '@aws-sdk/client-s3'
import { PostgrestError } from '@supabase/postgrest-js'

export type StorageError = {
  statusCode: string
  error: string
  message: string
}

/**
 * A renderable error is a handled error
 *  that we want to display to our users
 */
export interface RenderableError {
  render(): StorageError
  getOriginalError(): unknown
}

/**
 * A specific database error
 */
export class DatabaseError extends Error implements RenderableError {
  constructor(
    message: string,
    public readonly status: number,
    public readonly postgresError: PostgrestError,
    public readonly metadata?: Record<string, any>
  ) {
    super(message)
    Object.setPrototypeOf(this, DatabaseError.prototype)
  }

  render(): StorageError {
    let { message, details: type, code } = this.postgresError
    const responseStatus = this.status

    if (responseStatus === 406) {
      code = '404'
      message = 'The resource was not found'
      type = 'Not found'
    } else if (responseStatus === 401) {
      code = '401'
      type = 'Invalid JWT'
    } else if (responseStatus === 409) {
      const relationNotPresent = type?.includes('not present')

      code = relationNotPresent ? '404' : '409'
      type = relationNotPresent ? 'Not Found' : 'Duplicate'
      message = relationNotPresent
        ? 'The parent resource is not found'
        : 'The resource already exists'
    }

    return {
      statusCode: code,
      error: type,
      message,
    }
  }

  getOriginalError() {
    return this.postgresError
  }
}

/**
 * Determines if an error is a renderable error
 * @param error
 */
export function isRenderableError(error: unknown): error is RenderableError {
  return !!error && typeof error === 'object' && 'render' in error
}

/**
 * Determines if an error is an S3 error
 * @param error
 */
export function isS3Error(error: unknown): error is S3ServiceException {
  return !!error && typeof error === 'object' && '$metadata' in error
}

/**
 * A generic error that should be always thrown for generic exceptions
 */
export class StorageBackendError extends Error implements RenderableError {
  httpStatusCode: number
  originalError: unknown

  constructor(name: string, httpStatusCode: number, message: string, originalError?: unknown) {
    super(message)
    this.name = name
    this.httpStatusCode = httpStatusCode
    this.message = message
    this.originalError = originalError
    Object.setPrototypeOf(this, StorageBackendError.prototype)
  }

  static fromError(error?: unknown) {
    let name: string
    let httpStatusCode: number
    let message: string

    if (isS3Error(error)) {
      name = error.message
      httpStatusCode = error.$metadata.httpStatusCode ?? 500
      message = error.name
    } else if (error instanceof Error) {
      name = error.name
      httpStatusCode = 500
      message = error.message
    } else {
      name = 'Internal server error'
      httpStatusCode = 500
      message = 'Internal server error'
    }

    return new StorageBackendError(name, httpStatusCode, message, error)
  }

  render() {
    return {
      statusCode: this.httpStatusCode.toString(),
      error: this.name,
      message: this.message,
    }
  }

  getOriginalError() {
    return this.originalError
  }
}
