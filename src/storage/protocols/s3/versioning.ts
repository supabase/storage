import { ERRORS } from '@internal/errors'
import { isUuid } from '@storage/limits'
import type { FindObjectFilters } from '../../database'
import type { BucketVersioningStatus } from '../../schemas'

/** The version columns of an object row or listing entry. */
interface VersionColumns {
  version?: string | null
  is_versioned?: boolean | null
}

interface TimestampColumns {
  archived_at?: string | null
  updated_at?: string | null
  created_at?: string | null
  metadata?: Record<string, unknown> | null
}

export const NULL_VERSION_ID = 'null'

export type S3VersionRef = { kind: 'null' } | { kind: 'id'; version: string }

export function parseVersionId(versionId: string | undefined): S3VersionRef | undefined {
  if (versionId === undefined) {
    return undefined
  }
  if (versionId === '') {
    throw ERRORS.InvalidArgument('Version id cannot be the empty string')
  }
  if (versionId === NULL_VERSION_ID) {
    return { kind: 'null' }
  }
  if (!isUuid(versionId)) {
    throw ERRORS.InvalidArgument('Invalid version id specified')
  }
  return { kind: 'id', version: versionId }
}

export function toS3VersionId(row: VersionColumns): string {
  return row.is_versioned && row.version ? row.version : NULL_VERSION_ID
}

/**
 * The findObject version argument and filters that select the row a version
 * ref addresses, current or not. A version id only matches is_versioned rows,
 * so the null version's internal version can't be used to address it.
 */
export function versionLookup(ref: S3VersionRef | undefined): {
  version?: string
  filters: FindObjectFilters
} {
  if (!ref) {
    return { filters: {} }
  }
  if (ref.kind === 'null') {
    return { filters: { includeNoncurrent: true, isVersioned: false } }
  }
  return { version: ref.version, filters: { isVersioned: true } }
}

/**
 * The x-amz-version-id response header for a row that a request read or
 * wrote. A versioned row sends its version id. A null-version row sends
 * "null" when the bucket's versioning has ever been configured (ENABLED or
 * SUSPENDED; it can't return to DISABLED) and nothing when it never was.
 * Writes pass no status, since S3 sends no header for a write that lands on
 * the null version.
 */
export function versionIdHeader(
  row: VersionColumns | undefined,
  versioningStatus?: BucketVersioningStatus
): Record<string, string> {
  if (row?.is_versioned && row.version) {
    return { 'x-amz-version-id': row.version }
  }
  if (row && (versioningStatus === 'ENABLED' || versioningStatus === 'SUSPENDED')) {
    return { 'x-amz-version-id': NULL_VERSION_ID }
  }
  return {}
}

/**
 * When a version was written, which S3 reports as its LastModified.
 *
 * metadata.lastModified is when the version's bytes were uploaded. It is set
 * on every write and never changes afterwards, so it is used whenever present.
 *
 * Rows without it (delete markers, which have no bytes, and rows written
 * before it was recorded) fall back to the row's timestamps, neither of which
 * is reliable on its own:
 * - updated_at is set to now() by every UPDATE (trigger from migration 0011),
 *   including the one that archives a row, so a noncurrent row would report
 *   when it was archived rather than when it was written.
 * - created_at does not change when a null version is overwritten in place
 *   (versioning DISABLED or SUSPENDED), so a current row would report its
 *   first write rather than its last.
 * Hence updated_at for a current row and created_at for a noncurrent one.
 */
export function versionLastModified(row: TimestampColumns): Date | undefined {
  const uploadedAt = row.metadata?.lastModified
  if (typeof uploadedAt === 'string') {
    return new Date(uploadedAt)
  }

  const fallback = row.archived_at ? row.created_at : row.updated_at
  return fallback ? new Date(fallback) : undefined
}

/** Headers S3 sends when a request lands on a delete marker. */
export function deleteMarkerHeaders(row: VersionColumns) {
  return {
    'x-amz-delete-marker': 'true',
    'x-amz-version-id': toS3VersionId(row),
  }
}
