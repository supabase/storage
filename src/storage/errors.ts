import { S3ServiceException } from '@aws-sdk/client-s3'

export type StorageError = {
  statusCode: string
  code: ErrorCode
  error: string
  message: string
  query?: string
}

export enum ErrorCode {
  NoSuchBucket = 'NoSuchBucket',
  NoSuchKey = 'NoSuchKey',
  NoSuchUpload = 'NoSuchUpload',
  InvalidJWT = 'InvalidJWT',
  InvalidRequest = 'InvalidRequest',
  TenantNotFound = 'TenantNotFound',
  EntityTooLarge = 'EntityTooLarge',
  InternalError = 'InternalError',
  ResourceAlreadyExists = 'ResourceAlreadyExists',
  InvalidBucketName = 'InvalidBucketName',
  InvalidKey = 'InvalidKey',
  KeyAlreadyExists = 'KeyAlreadyExists',
  BucketAlreadyExists = 'BucketAlreadyExists',
  DatabaseTimeout = 'DatabaseTimeout',
  InvalidSignature = 'InvalidSignature',
  AccessDenied = 'AccessDenied',
  ResourceLocked = 'ResourceLocked',
  DatabaseError = 'DatabaseError',
  MissingContentLength = 'MissingContentLength',
  MissingParameter = 'MissingParameter',
  InvalidUploadSignature = 'InvalidUploadSignature',
  LockTimeout = 'LockTimeout',
  S3Error = 'S3Error',
  SlowDown = 'SlowDown',
}

export const ERRORS = {
  BucketNotEmpty: (bucket: string, e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.InvalidRequest,
      resource: bucket,
      httpStatusCode: 409,
      message: `The bucket you tried to delete is not empty`,
      originalError: e,
    }),
  NoSuchBucket: (bucket: string, e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.NoSuchBucket,
      resource: bucket,
      httpStatusCode: 404,
      message: `Bucket not found`,
      originalError: e,
    }),
  NoSuchUpload: (uploadId: string, e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.NoSuchUpload,
      resource: uploadId,
      httpStatusCode: 404,
      message: `Upload not found`,
      originalError: e,
    }),
  NoSuchKey: (resource: string, e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.NoSuchKey,
      resource,
      httpStatusCode: 404,
      message: `Object not found`,
      originalError: e,
    }),

  MissingParameter: (parameter: string, e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.MissingParameter,
      httpStatusCode: 404,
      message: `Missing Required Parameter ${parameter}`,
      originalError: e,
    }),

  InvalidJWT: (e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.InvalidJWT,
      httpStatusCode: 400,
      message: e?.message || 'Invalid JWT',
    }),

  MissingContentLength: (e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.MissingContentLength,
      httpStatusCode: 400,
      message: e?.message || 'You must provide the Content-Length HTTP header.',
    }),

  AccessDenied: (action: string, e?: Error) =>
    new StorageBackendError({
      error: 'Unauthorized',
      code: ErrorCode.AccessDenied,
      httpStatusCode: 403,
      message: action || 'Access denied',
      originalError: e,
    }),

  ResourceAlreadyExists: (e?: Error) =>
    new StorageBackendError({
      error: 'Duplicate',
      code: ErrorCode.ResourceAlreadyExists,
      httpStatusCode: 409,
      message: 'The resource already exists',
      originalError: e,
    }),

  MetadataRequired: (e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.InvalidRequest,
      httpStatusCode: 400,
      message: 'Metadata header is required',
      originalError: e,
    }),

  InvalidSignature: (e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.InvalidSignature,
      httpStatusCode: 400,
      message: 'Invalid signature',
      originalError: e,
    }),

  ExpiredSignature: (e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.InvalidSignature,
      httpStatusCode: 400,
      message: 'Expired signature',
      originalError: e,
    }),

  InvalidXForwardedHeader: (message?: string, e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.InvalidRequest,
      httpStatusCode: 400,
      message: message || 'Invalid X-Forwarded-Host header',
      originalError: e,
    }),

  InvalidTenantId: (e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.TenantNotFound,
      httpStatusCode: 400,
      message: e?.message || 'Invalid tenant id',
      originalError: e,
    }),

  InvalidUploadId: (message?: string, e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.InvalidRequest,
      httpStatusCode: 400,
      message: message || 'Invalid upload id',
      originalError: e,
    }),

  MissingTenantConfig: (tenantId: string) =>
    new StorageBackendError({
      code: ErrorCode.TenantNotFound,
      httpStatusCode: 400,
      message: `Missing tenant config for tenant ${tenantId}`,
    }),

  InvalidMimeType: (mimeType: string) =>
    new StorageBackendError({
      error: 'invalid_mime_type',
      code: ErrorCode.InvalidRequest,
      httpStatusCode: 415,
      message: `mime type ${mimeType} is not supported`,
    }),

  EntityTooLarge: (e?: Error) =>
    new StorageBackendError({
      error: 'Payload too large',
      code: ErrorCode.EntityTooLarge,
      httpStatusCode: 413,
      message: 'The object exceeded the maximum allowed size',
      originalError: e,
    }),

  InternalError: (e?: Error, message?: string) =>
    new StorageBackendError({
      code: ErrorCode.InternalError,
      httpStatusCode: 500,
      message: message || 'Internal server error',
      originalError: e,
    }),

  ImageProcessingError: (statusCode: number, message: string, e?: Error) =>
    new StorageBackendError({
      code: statusCode > 499 ? ErrorCode.InternalError : ErrorCode.InvalidRequest,
      httpStatusCode: statusCode,
      message: message,
      originalError: e,
    }),

  InvalidBucketName: (bucket: string, e?: Error) =>
    new StorageBackendError({
      error: 'Invalid Input',
      code: ErrorCode.InvalidBucketName,
      resource: bucket,
      httpStatusCode: 400,
      message: `Bucket name invalid`,
      originalError: e,
    }),

  InvalidFileSizeLimit: (e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.InvalidRequest,
      httpStatusCode: 400,
      message: e?.message || 'Invalid file size format, hint: use 20GB / 20MB / 30KB / 3B',
      originalError: e,
    }),

  InvalidUploadSignature: (e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.InvalidUploadSignature,
      httpStatusCode: 400,
      message: e?.message || 'Invalid upload Signature',
      originalError: e,
    }),

  InvalidKey: (key: string, e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.InvalidKey,
      resource: key,
      httpStatusCode: 400,
      message: `Invalid key: ${key}`,
      originalError: e,
    }),

  KeyAlreadyExists: (key: string, e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.KeyAlreadyExists,
      resource: key,
      httpStatusCode: 409,
      message: `Key already exists: ${key}`,
      originalError: e,
    }),

  BucketAlreadyExists: (bucket: string, e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.BucketAlreadyExists,
      resource: bucket,
      httpStatusCode: 409,
      message: `Bucket already exists: ${bucket}`,
      originalError: e,
    }),

  NoContentProvided: (e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.InvalidRequest,
      httpStatusCode: 400,
      message: e?.message || 'No content provided',
      originalError: e,
    }),

  DatabaseTimeout: (e?: Error) =>
    StorageBackendError.withStatusCode(544, {
      code: ErrorCode.DatabaseTimeout,
      httpStatusCode: 544,
      message: 'The connection to the database timed out',
      originalError: e,
    }),

  ResourceLocked: (e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.ResourceLocked,
      httpStatusCode: 423,
      message: `The resource is locked`,
      originalError: e,
    }),

  RelatedResourceNotFound: (e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.InvalidRequest,
      httpStatusCode: 404,
      message: `The related resource does not exist`,
      originalError: e,
    }),

  DatabaseError: (message: string, err?: Error) =>
    new StorageBackendError({
      code: ErrorCode.DatabaseError,
      httpStatusCode: 500,
      message: message,
      originalError: err,
    }),

  LockTimeout: (err?: Error) =>
    new StorageBackendError({
      error: 'acquiring_lock_timeout',
      code: ErrorCode.LockTimeout,
      httpStatusCode: 503,
      message: 'acquiring lock timeout',
      originalError: err,
    }),
}

export function isStorageError(errorType: ErrorCode, error: any): error is StorageBackendError {
  return error instanceof StorageBackendError && error.code === errorType
}

/**
 * A renderable error is a handled error
 *  that we want to display to our users
 */
export interface RenderableError {
  error?: string
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

export interface StorageErrorOptions {
  code: ErrorCode
  httpStatusCode: number
  message: string
  resource?: string
  originalError?: unknown
  error?: string
}

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
