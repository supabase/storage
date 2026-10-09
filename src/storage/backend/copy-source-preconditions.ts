import { ErrorCode, StorageBackendError } from '@internal/errors'

/**
 * Preconditions from x-amz-copy-source-if-match, if-none-match,
 * if-modified-since, and if-unmodified-since.
 */
export interface CopySourcePreconditions {
  ifMatch?: string
  ifNoneMatch?: string
  ifModifiedSince?: Date
  ifUnmodifiedSince?: Date
}

export function hasCopySourcePreconditions(
  conditions: CopySourcePreconditions | undefined
): conditions is CopySourcePreconditions {
  return (
    conditions !== undefined &&
    (conditions.ifMatch !== undefined ||
      conditions.ifNoneMatch !== undefined ||
      conditions.ifModifiedSince !== undefined ||
      conditions.ifUnmodifiedSince !== undefined)
  )
}

/**
 * Evaluates copy-source preconditions the way S3 CopyObject and UploadPartCopy do.
 * if-match takes precedence over if-unmodified-since, and if-none-match takes
 * precedence over if-modified-since. Invalid dates are ignored (RFC 9110 13.1).
 */
export function assertCopySourcePreconditions(
  conditions: CopySourcePreconditions,
  eTag: string,
  lastModified: Date
) {
  let failed = false

  if (conditions.ifMatch !== undefined) {
    failed = !matchesETag(conditions.ifMatch, eTag)
  } else if (conditions.ifUnmodifiedSince) {
    failed = toSeconds(lastModified) > toSeconds(conditions.ifUnmodifiedSince)
  }

  if (!failed && conditions.ifNoneMatch !== undefined) {
    failed = matchesETag(conditions.ifNoneMatch, eTag)
  } else if (!failed && conditions.ifModifiedSince) {
    failed = toSeconds(lastModified) <= toSeconds(conditions.ifModifiedSince)
  }

  if (failed) {
    throw StorageBackendError.withStatusCode(412, {
      error: 'PreconditionFailed',
      code: ErrorCode.PreconditionFailed,
      httpStatusCode: 412,
      message: 'PreconditionFailed',
    })
  }
}

// HTTP dates have one-second precision, invalid is false.
export function toSeconds(date: Date) {
  return Math.floor(date.getTime() / 1000)
}

function unquoteETag(value: string) {
  return value
    .trim()
    .replace(/^W\//, '')
    .replace(/^"(.*)"$/, '$1')
}

export function matchesETag(condition: string, eTag: string) {
  const target = unquoteETag(eTag)

  // RFC 9110 8.8.3: commas are valid inside a quoted entity-tag
  // so split on commas outside quotes only.
  return (condition.match(/(?:"[^"]*"|[^,])+/g) ?? []).some((candidate) => {
    const value = candidate.trim()
    return value === '*' || unquoteETag(value) === target
  })
}
