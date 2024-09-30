import { StorageBackendError } from './storage-error'

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
  InvalidRange = 'InvalidRange',
  InvalidMimeType = 'InvalidMimeType',
  InvalidUploadId = 'InvalidUploadId',
  KeyAlreadyExists = 'KeyAlreadyExists',
  BucketAlreadyExists = 'BucketAlreadyExists',
  DatabaseTimeout = 'DatabaseTimeout',
  InvalidSignature = 'InvalidSignature',
  ExpiredToken = 'ExpiredToken',
  SignatureDoesNotMatch = 'SignatureDoesNotMatch',
  AccessDenied = 'AccessDenied',
  ResourceLocked = 'ResourceLocked',
  DatabaseError = 'DatabaseError',
  MissingContentLength = 'MissingContentLength',
  MissingParameter = 'MissingParameter',
  InvalidParameter = 'InvalidParameter',
  InvalidUploadSignature = 'InvalidUploadSignature',
  LockTimeout = 'LockTimeout',
  S3Error = 'S3Error',
  S3InvalidAccessKeyId = 'InvalidAccessKeyId',
  S3MaximumCredentialsLimit = 'MaximumCredentialsLimit',
  InvalidChecksum = 'InvalidChecksum',
  MissingPart = 'MissingPart',
  SlowDown = 'SlowDown',
  TusError = 'TusError',
  Aborted = 'Aborted',
  AbortedTerminate = 'AbortedTerminate',
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
      error: 'Bucket not found',
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
      error: 'not_found',
      httpStatusCode: 404,
      message: `Object not found`,
      originalError: e,
    }),

  MissingParameter: (parameter: string, e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.MissingParameter,
      httpStatusCode: 400,
      message: `Missing Required Parameter ${parameter}`,
      originalError: e,
    }),

  InvalidParameter: (parameter: string, e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.MissingParameter,
      httpStatusCode: 400,
      message: `Invalid Parameter ${parameter}`,
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

  SignatureDoesNotMatch: (message?: string) =>
    new StorageBackendError({
      code: ErrorCode.SignatureDoesNotMatch,
      httpStatusCode: 403,
      message: message || 'Signature does not match',
    }),

  InvalidSignature: (message?: string, e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.InvalidSignature,
      httpStatusCode: 400,
      message: message || 'Invalid signature',
      originalError: e,
    }),

  ExpiredSignature: (e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.ExpiredToken,
      httpStatusCode: 400,
      message: 'The provided token has expired.',
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
      code: ErrorCode.InvalidUploadId,
      httpStatusCode: 400,
      message: message || 'Invalid upload id',
      originalError: e,
    }),

  TusError: (message: string, statusCode: number) =>
    new StorageBackendError({
      code: ErrorCode.TusError,
      httpStatusCode: statusCode,
      message: message,
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
      code: ErrorCode.InvalidMimeType,
      httpStatusCode: 415,
      message: `mime type ${mimeType} is not supported`,
    }),

  InvalidRange: () =>
    new StorageBackendError({
      error: 'invalid_range',
      code: ErrorCode.InvalidRange,
      httpStatusCode: 400,
      message: `invalid range provided`,
    }),

  EntityTooLarge: (e?: Error, entity = 'object') =>
    new StorageBackendError({
      error: 'Payload too large',
      code: ErrorCode.EntityTooLarge,
      httpStatusCode: 413,
      message: `The ${entity} exceeded the maximum allowed size`,
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
      error: 'Duplicate',
      httpStatusCode: 409,
      message: `The resource already exists`,
      originalError: e,
    }),

  BucketAlreadyExists: (bucket: string, e?: Error) =>
    new StorageBackendError({
      code: ErrorCode.BucketAlreadyExists,
      resource: bucket,
      error: 'Duplicate',
      httpStatusCode: 409,
      message: `The resource already exists`,
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

  MissingS3Credentials: () =>
    new StorageBackendError({
      code: ErrorCode.S3InvalidAccessKeyId,
      httpStatusCode: 403,
      message: 'The Access Key Id you provided does not exist in our records.',
    }),

  MaximumCredentialsLimit: () =>
    new StorageBackendError({
      code: ErrorCode.S3MaximumCredentialsLimit,
      httpStatusCode: 400,
      message: 'You have reached the maximum number of credentials allowed',
    }),

  InvalidChecksum: (message: string) =>
    new StorageBackendError({
      code: ErrorCode.InvalidChecksum,
      httpStatusCode: 400,
      message: message,
    }),

  MissingPart: (partNumber: number, uploadId: string) =>
    new StorageBackendError({
      code: ErrorCode.MissingPart,
      httpStatusCode: 400,
      message: `Part ${partNumber} is missing for upload id ${uploadId}`,
    }),

  Aborted: (message: string, originalError?: unknown) =>
    new StorageBackendError({
      code: ErrorCode.Aborted,
      httpStatusCode: 500,
      message: message,
      originalError,
    }),
  AbortedTerminate: (message: string, originalError?: unknown) =>
    new StorageBackendError({
      code: ErrorCode.AbortedTerminate,
      httpStatusCode: 500,
      message: message,
      originalError,
    }),
}

export function isStorageError(errorType: ErrorCode, error: any): error is StorageBackendError {
  return error instanceof StorageBackendError && error.code === errorType
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
