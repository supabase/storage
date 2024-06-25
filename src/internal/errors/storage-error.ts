import { ErrorCode } from './codes'
import { RenderableError, StorageErrorOptions } from './renderable'
import { S3ServiceException } from '@aws-sdk/client-s3'

/**
 * A generic error that should be always thrown for generic exceptions
 */
export class StorageBackendError extends Error implements RenderableError {
  httpStatusCode: number
  originalError: unknown
  userStatusCode: number
  resource?: string
  code: ErrorCode
  metadata?: Record<string, any> = {}
  error?: string // backwards compatible error

  constructor(options: StorageErrorOptions) {
    super(options.message)
    this.code = options.code
    this.httpStatusCode = options.httpStatusCode
    this.userStatusCode = options.httpStatusCode === 500 ? 500 : 400
    this.message = options.message
    this.originalError = options.originalError
    this.resource = options.resource
    this.error = options.error
    Object.setPrototypeOf(this, StorageBackendError.prototype)
  }

  static withStatusCode(statusCode: number, options: StorageErrorOptions) {
    const error = new StorageBackendError(options)
    error.userStatusCode = statusCode
    return error
  }

  static fromError(error?: unknown) {
    let oldErrorMessage: string
    let httpStatusCode: number
    let message: string
    let code: ErrorCode

    if (isS3Error(error)) {
      code = ErrorCode.S3Error
      oldErrorMessage = error.message
      httpStatusCode = error.$metadata.httpStatusCode ?? 500
      message = error.name
    } else if (error instanceof Error) {
      code = ErrorCode.InternalError
      oldErrorMessage = error.name
      httpStatusCode = 500
      message = error.message
    } else {
      code = ErrorCode.InternalError
      oldErrorMessage = 'Internal server error'
      httpStatusCode = 500
      message = 'Internal server error'
    }

    return new StorageBackendError({
      error: oldErrorMessage,
      code: code,
      httpStatusCode,
      message,
      originalError: error,
    })
  }

  withMetadata(metadata: Record<string, any>) {
    this.metadata = metadata
    return this
  }

  render() {
    return {
      statusCode: this.httpStatusCode.toString(),
      code: this.code,
      error: this.code,
      message: this.message,
    }
  }

  getOriginalError() {
    return this.originalError
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
