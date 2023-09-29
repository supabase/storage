import { S3ServiceException } from '@aws-sdk/client-s3'

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
  userStatusCode?: number
  render(): StorageError
  getOriginalError(): unknown
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
  userStatusCode: number

  constructor(name: string, httpStatusCode: number, message: string, originalError?: unknown) {
    super(message)
    this.name = name
    this.httpStatusCode = httpStatusCode
    this.userStatusCode = httpStatusCode === 500 ? 500 : 400
    this.message = message
    this.originalError = originalError
    Object.setPrototypeOf(this, StorageBackendError.prototype)
  }

  static withStatusCode(
    name: string,
    statusCode: number,
    message: string,
    originalError?: unknown
  ) {
    const error = new StorageBackendError(name, statusCode, message, originalError)
    error.userStatusCode = statusCode
    return error
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

export function normalizeRawError(error: any) {
  if (error instanceof Error) {
    return {
      raw: JSON.stringify(error),
      name: error.name,
      message: error.message,
      stack: error.stack,
    }
  }

  try {
    return {
      raw: JSON.stringify(error),
    }
  } catch (e) {
    return {
      raw: 'Failed to stringify error',
    }
  }
}
