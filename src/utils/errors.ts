import { S3ServiceException } from '@aws-sdk/client-s3'

export function isS3Error(error: unknown): error is S3ServiceException {
  return !!error && typeof error === 'object' && '$metadata' in error
}

/**
 * Converts any errors thrown to a consistent `StorageBackendError` type.
 */
export function convertErrorToStorageBackendError(error: unknown) {
  let name: string
  let httpStatusCode: number
  let message: string

  if (isS3Error(error)) {
    name = error.name
    httpStatusCode = error.$metadata.httpStatusCode ?? 500
    message = error.message
  } else if (error instanceof Error) {
    name = error.name
    httpStatusCode = 500
    message = error.message
  } else {
    name = 'Internal service error'
    httpStatusCode = 500
    message = 'Internal service error'
  }

  return new StorageBackendError(name, httpStatusCode, message, error)
}

export class StorageBackendError extends Error {
  httpStatusCode: number
  originalError: unknown

  constructor(name: string, httpStatusCode: number, message: string, originalError?: unknown) {
    super(message)
    this.name = name
    this.httpStatusCode = httpStatusCode
    this.message = message
    this.originalError = originalError
  }
}
